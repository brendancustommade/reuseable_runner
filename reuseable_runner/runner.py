from types import MethodType

from django.core.management.commands.loaddata import Command
from django.core.management.color import no_style

from django_jenkins.runner import CITestSuiteRunner

from .utils import _reusing_db, _mysql_reset_sequences, _should_create_database
from .utils import _skip_create_test_db, uses_mysql
from .utils import _foreign_key_ignoring_handle

from django.db import connections, transaction

# monkeypatch Command.handle
_old_handle = Command.handle


class ReuseDBTestRunner(CITestSuiteRunner):
    """
    Allows for re-using of a database when running tests through django_jenkins

    set your JENKINS_TEST_RUNNER in local.py to this class, and run the command:
    REUSE_DB=1 ./manage.py jenkins

    or

    export REUSE_DB=1 to always run tests with a re-used database.

    be careful and remember to tear down any data created, or brittle tests
    could break if they encounter data left over from a previous run.  you
    should be being a good testing citizen anyway and always tearing down your
    data, right?

    if you need to blow away the database, simply run your tests with
    REUSE_DB=0 ./manage.py jenkins
    and a new database will be created.


    """

    def setup_databases(self, **kwargs):
        for alias in connections:
            connection = connections[alias]
            creation = connection.creation
            test_db_name = creation._get_test_db_name()

            # Mess with the DB name so other things operate on a test DB
            # rather than the real one. This is done in create_test_db when
            # we don't monkeypatch it away with _skip_create_test_db.
            orig_db_name = connection.settings_dict['NAME']
            connection.settings_dict['NAME'] = test_db_name

            if _should_create_database(connection):
                # We're not using _skip_create_test_db, so put the DB name
                # back:
                connection.settings_dict['NAME'] = orig_db_name

                # Since we replaced the connection with the test DB, closing
                # the connection will avoid pooling issues with SQLAlchemy. The
                # issue is trying to CREATE/DROP the test database using a
                # connection to a DB that was established with that test DB.
                # MySQLdb doesn't allow it, and SQLAlchemy attempts to reuse
                # the existing connection from its pool.
                connection.close()
            else:
                # Reset auto-increment sequences. Apparently, SUMO's tests are
                # horrid and coupled to certain numbers.
                cursor = connection.cursor()
                style = no_style()

                if uses_mysql(connection):
                    reset_statements = _mysql_reset_sequences(
                        style, connection)
                else:
                    reset_statements = connection.ops.sequence_reset_sql(
                            style, self._get_models_for_connection(connection))

                for reset_statement in reset_statements:
                    cursor.execute(reset_statement)

                # Django v1.3 (https://code.djangoproject.com/ticket/9964)
                # starts using commit_unless_managed() for individual
                # connections. Backwards compatibility for Django 1.2 is to use
                # the generic transaction function.
                transaction.commit_unless_managed(using=connection.alias)

                # Each connection has its own creation object, so this affects
                # only a single connection:
                creation.create_test_db = MethodType(
                    _skip_create_test_db, creation, creation.__class__)

        Command.handle = _foreign_key_ignoring_handle

        super(ReuseDBTestRunner, self).setup_databases(**kwargs)

    def teardown_databases(self, old_config, **kwargs):
        if not _reusing_db():
            super(ReuseDBTestRunner, self).teardown_databases(old_config, **kwargs)
