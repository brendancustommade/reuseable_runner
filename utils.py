import os

from django.core.management.commands.loaddata import Command
from django.db import connections, DEFAULT_DB_ALIAS


# ReuseDBTestRunner helpers, stolen/borrowed from django_nose

def _reusing_db():
    """Return whether the ``REUSE_DB`` flag was passed"""
    return os.getenv('REUSE_DB', 'false').lower() in ('true', '1', '')


def _can_support_reuse_db(connection):
    """Return whether it makes any sense to
    use REUSE_DB with the backend of a connection."""
    # Perhaps this is a SQLite in-memory DB. Those are created implicitly when
    # you try to connect to them, so our usual test doesn't work.
    return not connection.creation._get_test_db_name() == ':memory:'


def _should_create_database(connection):
    """Return whether we should recreate the given DB.

    This is true if the DB doesn't exist or the REUSE_DB env var isn't truthy.

    """
    # TODO: Notice when the Model classes change and return True. Worst case,
    # we can generate sqlall and hash it, though it's a bit slow (2 secs) and
    # hits the DB for no good reason. Until we find a faster way, I'm inclined
    # to keep making people explicitly saying REUSE_DB if they want to reuse
    # the DB.

    if not _can_support_reuse_db(connection):
        return True

    # Notice whether the DB exists, and create it if it doesn't:
    try:
        connection.cursor()
    except Exception:  # TODO: Be more discerning but still DB agnostic.
        return True
    return not _reusing_db()


def _mysql_reset_sequences(style, connection):
    """Return a list of SQL statements needed to
    reset all sequences for Django tables."""
    tables = connection.introspection.django_table_names(only_existing=True)
    flush_statements = connection.ops.sql_flush(
            style, tables, connection.introspection.sequence_list())

    # connection.ops.sequence_reset_sql() is not implemented for MySQL,
    # and the base class just returns []. TODO: Implement it by pulling
    # the relevant bits out of sql_flush().
    return [s for s in flush_statements if s.startswith('ALTER')]
    # Being overzealous and resetting the sequences on non-empty tables
    # like django_content_type seems to be fine in MySQL: adding a row
    # afterward does find the correct sequence number rather than
    # crashing into an existing row.


def uses_mysql(connection):
    """Return whether the connection represents a MySQL DB."""
    return 'mysql' in connection.settings_dict['ENGINE']


def _skip_create_test_db(self, verbosity=1, autoclobber=False):
    """``create_test_db`` implementation that skips both creation and flushing

    The idea is to re-use the perfectly good test DB already created by an
    earlier test run, cutting the time spent before any tests run from 5-13s
    (depending on your I/O luck) down to 3.

    """
    # Notice that the DB supports transactions. Originally, this was done in
    # the method this overrides. The confirm method was added in Django v1.3
    # (https://code.djangoproject.com/ticket/12991) but removed in Django v1.5
    # (https://code.djangoproject.com/ticket/17760). In Django v1.5
    # supports_transactions is a cached property evaluated on access.
    if callable(getattr(self.connection.features, 'confirm', None)):
        # Django v1.3-4
        self.connection.features.confirm()
    elif hasattr(self, "_rollback_works"):
        # Django v1.2 and lower
        can_rollback = self._rollback_works()
        self.connection.settings_dict['SUPPORTS_TRANSACTIONS'] = can_rollback

    return self._get_test_db_name()


# monkeypatch Command.handle
_old_handle = Command.handle


def _foreign_key_ignoring_handle(self, *fixture_labels, **options):
    """Wrap the the stock loaddata to ignore foreign key
    checks so we can load circular references from fixtures.

    This is monkeypatched into place in setup_databases().

    """
    using = options.get('database', DEFAULT_DB_ALIAS)
    commit = options.get('commit', True)
    connection = connections[using]

    # MySQL stinks at loading circular references:
    if uses_mysql(connection):
        cursor = connection.cursor()
        cursor.execute('SET foreign_key_checks = 0')

    _old_handle(self, *fixture_labels, **options)

    if uses_mysql(connection):
        cursor = connection.cursor()
        cursor.execute('SET foreign_key_checks = 1')

        if commit:
            connection.close()