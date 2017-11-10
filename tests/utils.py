"sqlbag functions required for testing"
from __future__ import absolute_import, division, print_function, \
    unicode_literals

import os
import copy
import getpass
import random
import string

import sqlalchemy.exc

from contextlib import contextmanager

from sqlalchemy.sql import text
from sqlalchemy import create_engine
from sqlalchemy.pool import NullPool
from sqlalchemy.engine.url import make_url
from sqlalchemy.orm import scoped_session, sessionmaker
from sqlalchemy.exc import \
    ProgrammingError, \
    OperationalError, \
    InternalError


SCOPED_SESSION_MAKERS = {}

MYSQL_KILL = """
    select
        *,
        ID as process_id,
        connection_id() as cid
    from
        information_schema.processlist
    where
        ID != connection_id()
"""
PG_KILL = """
    with psa as (
        SELECT
            *,
            (select datname from pg_database d where d.oid = s.datid)
                as datname
        FROM pg_stat_get_activity(NULL::integer) s
    )
    select
        pg_terminate_backend(psa.pid)
    from
        psa
    where psa.pid != pg_backend_pid()
"""


def quoted_identifier(identifier):
    """One-liner to add double-quote marks around an SQL identifier
    (table name, view name, etc), and to escape double-quote marks.
    Args:
        identifier(str): the unquoted identifier
    """

    return '"{0}"'.format(identifier.replace('"', '""'))


def copy_url(db_url):
    """
    Args:
        db_url: Already existing SQLAlchemy :class:`URL`, or URL string.
    Returns:
        A brand new SQLAlchemy :class:`URL`.
    Make a copy of a SQLAlchemy :class:`URL`.
    """
    return copy.copy(make_url(db_url))


def connection_from_s_or_c(s_or_c):
    """Args:
        s_or_c (str): Either an SQLAlchemy ORM :class:`Session`, or a core
            :class:`Connection`.
    Returns:
        Connection: An SQLAlchemy Core connection. If you passed in a
            :class:`Session`, it's the Connection associated with that session.
    Get you a method that can do both. This is handy for writing methods
    that can accept both :class:`Session`s and core :class:`Connection`s.
    """
    try:
        s_or_c.engine
        return s_or_c
    except AttributeError:
        return s_or_c.connection()


def database_exists(db_url, test_can_select=False):
    url = copy_url(db_url)
    name = url.database
    db_type = url.get_dialect().name

    if not test_can_select:
        if db_type == 'sqlite':
            return name is None or name == ':memory:' \
                or os.path.exists(name)
        elif db_type in ['postgresql', 'mysql']:
            with admin_db_connection(url) as _sn:
                return _database_exists(_sn, name)
    return can_select(url)


def can_select(url):
    sql = 'select 1'

    _en = create_engine(url)

    try:
        _en.execute(sql)
        return True
    except (ProgrammingError, OperationalError, InternalError):
        return False


@contextmanager
def C(*args, **kwargs):
    """
    Hello it's me.
    """
    _en = create_engine(*args, **kwargs)
    _cn = _en.connect()
    trans = _cn.begin()

    try:
        yield _cn
        trans.commit()
    except BaseException:
        trans.rollback()
        raise
    finally:
        _cn.close()


@contextmanager
def admin_db_connection(db_url):
    "db admin conn"
    url = copy_url(db_url)
    dbtype = url.get_dialect().name

    if dbtype == 'postgresql':
        url.database = ''

        if not url.username:
            url.username = getpass.getuser()

    elif not dbtype == 'sqlite':
        url.database = None

    if dbtype == 'postgresql':
        with C(url, poolclass=NullPool, isolation_level='AUTOCOMMIT') as _cn:
            yield _cn

    elif dbtype == 'mysql':
        with C(url, poolclass=NullPool) as _cn:
            _cn.execute("""
                SET sql_mode = 'ANSI';
            """)
            yield _cn

    elif dbtype == 'sqlite':
        with C(url, poolclass=NullPool) as _cn:
            yield _cn


def get_scoped_session_maker(*args, **kwargs):
    """
    Creates a scoped session maker, and saves it for reuse next time.
    """
    tup = (args, frozenset(kwargs.items()))
    if tup not in SCOPED_SESSION_MAKERS:
        SCOPED_SESSION_MAKERS[tup] = scoped_session(sessionmaker(
            bind=create_engine(*args, **kwargs)))
    return SCOPED_SESSION_MAKERS[tup]


def session(*args, **kwargs):
    """
    Returns:
        Session: A new SQLAlchemy :class:`Session`.
    Boilerplate method to create a database session.
    Pass in the same parameters as you'd pass to create_engine. Internally,
    this uses SQLAlchemy's `scoped_session` session constructors, which means
    that calling it again with the same parameters will reuse the
    `scoped_session`.
    :class:`S <S>` creates a session in the same way but in the form of a
    context manager.
    """
    Session = get_scoped_session_maker(*args, **kwargs)
    return Session()


@contextmanager
def S(*args, **kwargs):
    """Boilerplate context manager for creating and using sessions.
    This makes using a database session as simple as:
    .. code-block:: python
        with S('postgresql:///databasename') as s:
            s.execute('select 1;')
    Does `commit()` on close, `rollback()` on exception.
    Also uses `scoped_session` under the hood.
    """
    Session = get_scoped_session_maker(*args, **kwargs)

    try:
        Sess = Session()
        yield Sess
        Sess.commit()
    except BaseException:
        Sess.rollback()
        raise
    finally:
        Sess.close()


def _database_exists(session_or_connection, name):
    "check db existance"
    _cn = connection_from_s_or_c(session_or_connection)
    _en = copy.copy(_cn.engine)
    url = copy_url(_en.url)
    dbtype = url.get_dialect().name

    if dbtype == 'postgresql':
        sql = """
            SELECT 1
            FROM pg_catalog.pg_database
            WHERE datname = %s
        """

        result = _cn.execute(sql, (name, )).scalar()

        return bool(result)
    elif dbtype == 'mysql':
        sql = """
            SELECT SCHEMA_NAME
            FROM INFORMATION_SCHEMA.SCHEMATA
            WHERE SCHEMA_NAME = %s
        """

        result = _cn.execute(sql, (name, )).scalar()

        return bool(result)


def create_database(db_url, template=None, wipe_if_existing=False):
    "create database"
    target_url = copy_url(db_url)
    dbtype = target_url.get_dialect().name

    if wipe_if_existing:
        drop_database(db_url)

    if database_exists(target_url):
        return False
    else:

        if dbtype == 'sqlite':
            can_select(target_url)
            return True

        with admin_db_connection(target_url) as _cn:
            if template:
                _tp = 'template {0}'.format(quoted_identifier(template))
            else:
                _tp = ''

            _cn.execute("""
                create database {0} {1};
            """.format(
                quoted_identifier(target_url.database), _tp))
        return True


def drop_database(db_url):
    "drop database"
    url = copy_url(db_url)

    dbtype = url.get_dialect().name
    name = url.database

    if database_exists(url):
        if dbtype == 'sqlite':
            if name and name != ':memory:':
                os.remove(name)
                return True
            else:
                return False
        else:
            with admin_db_connection(url) as _cn:
                if dbtype == 'postgresql':

                    sql = 'revoke connect on database {0} from public'
                    revoke = sql.format(quoted_identifier(name))
                    _cn.execute(revoke)

                kill_other_connections(_cn, name, hardkill=True)

                _cn.execute("""
                    drop database if exists {0};
                """.format(quoted_identifier(name)))
            return True
    else:
        return False


@contextmanager
def temporary_database(dialect='postgresql', do_not_delete=False):
    "create temp db"
    rnd = ''.join([random.choice(string.ascii_lowercase)
                   for _ in range(10)])
    tempname = 'sqlbag_tmp_' + rnd

    current_username = _current_username()

    url = '{}://{}@/{}'.format(dialect, current_username, tempname)

    if url.startswith('mysql:'):
        url = url.replace('mysql:', 'mysql+pymysql:', 1)

    try:
        create_database(url)
        yield url
    finally:
        if not do_not_delete:
            drop_database(url)


def _killquery(dbtype, dbname, hardkill):
    where = []

    if dbtype == 'postgresql':
        sql = PG_KILL

        if not hardkill:
            where.append("psa.state = 'idle'")
        if dbname:
            where.append('datname = :databasename')
    elif dbtype == 'mysql':
        sql = MYSQL_KILL

        if not hardkill:
            where.append("COMMAND = 'Sleep'")
        if dbname:
            where.append('DB = :databasename')
    else:
        raise NotImplementedError

    where = ' and '.join(where)

    if where:
        sql += ' and {}'.format(where)
    return sql


def kill_other_connections(s_or_c, dbname=None, hardkill=False):
    """
    Kill other connections to this database (or entire database server).
    """
    _cn = connection_from_s_or_c(s_or_c)

    dbtype = _cn.engine.dialect.name

    killquery = _killquery(dbtype, dbname=dbname, hardkill=hardkill)

    if dbname:
        results = _cn.execute(text(killquery), databasename=dbname)
    else:  # pragma: no cover
        results = _cn.execute(text(killquery))

    if dbtype == 'mysql':
        for _xr in results:
            kill = text('kill connection :pid')

            try:
                _cn.execute(kill, pid=_xr.process_id)
            except sqlalchemy.exc.InternalError as _ex:  # pragma: no cover
                code, message = _ex.orig.args
                if 'Unknown thread id' in message:
                    pass
                else:
                    raise


def _current_username():
    return getpass.getuser()
