'''Increment all sequences on a PostgreSQL database. Useful when promoting a logical replica to be the new primary.
'''
import psycopg2
import psycopg2.extras
import click
import colorama
from colorama import Fore
import os

VERSION = '0.1.0'

__author__ = 'Lev Kokotov <lev.kokotov@instacart.com>'
__version__ = VERSION

colorama.init()

def connect(db_url):
    '''Connect to source and replicaination DBs.'''
    conn = psycopg2.connect(db_url)
    conn.set_session(autocommit=True)

    return conn, conn.cursor(cursor_factory=psycopg2.extras.DictCursor)


def _result(text):
    print(Fore.GREEN, '\b{}'.format(text), Fore.RESET)


def _debug(cursor):
    if os.getenv('DEBUG'):
        print(Fore.BLUE, '\b{}: {}'.format(cursor.connection.dsn, cursor.query.decode('utf-8')), Fore.RESET)


def _sequence_name(column_default):
    return column_default.split("'")[1]


def _dry_run(cursor, query, params):
    query = cursor.mogrify(query, params).decode('utf-8') if len(params) > 0 else query

    if os.getenv('QUERIES_ONLY'):
        print(query + ';')
    else:
        print(Fore.YELLOW, '\b{}: {}'.format(cursor.connection.dsn, query), Fore.RESET)


def _exec(cursor, query, params=None):
    cursor.execute(query, params)
    _debug(cursor)

    return cursor


class SequenceStrategy:
    def sequences(self):
        '''Get all the sequences we want to update from the database.'''
        pass

    def sequence_value(self, sequence):
        '''Get the current sequence value given a sequence information object.'''
        pass

    def sequence_name(self, sequence):
        '''Get the sequence name given a sequence information object.'''
        pass


class PrimarySequenceStrategy(SequenceStrategy):
    def __init__(self, primary_url):
        self.conn, self.cursor = connect(primary_url)


    def sequences(self):
        return _exec(self.cursor,
            "SELECT sequencename AS sequence_name FROM pg_sequences WHERE schemaname = 'public'").fetchall()


    def sequence_value(self, sequence):
        return int(_exec(self.cursor,
            'SELECT last_value FROM pg_sequences WHERE sequencename = %s', (sequence['sequence_name'],)).fetchone()['last_value'])


    def sequence_name(self, sequence):
        return sequence['sequence_name']


class ReplicaSequenceStrategy(SequenceStrategy):
    def __init__(self, replica_url):
        self.conn, self.cursor = connect(replica_url)


    def sequences(self):
        sequences = _exec(self.cursor, """SELECT table_name, column_name, column_default::text AS column_default FROM information_schema.columns
            WHERE table_schema = 'public' AND column_default::text LIKE 'nextval(''%''::regclass)'""").fetchall()

        return sequences


    def sequence_value(self, sequence):
        return _exec(
            self.cursor, 'SELECT MAX({}) AS "max" FROM {}'.format(sequence['column_name'], sequence['table_name'])).fetchone()['max']


    def sequence_name(self, sequence):
        return _sequence_name(sequence['column_default'])


def _update_sequence(cursor, sequence_name, sequence_value, dry_run, increment_by):
    '''Update the sequence or just show what it would do, in case of a dry run.

    Parameters:
        cursor: Cursor
        sequence_name: The name of the sequence we want to update
        sequence_value: Current value of that sequence
        dry_run: Do this for real or just print the queries it will execute
        increment_by: Move the sequence forward by that amount
    '''
    query = "SELECT setval(%s, %s, true)"
    params = (sequence_name, sequence_value + increment_by)

    if dry_run:
        _dry_run(cursor, query, params)
    else:
        _exec(cursor, query, params)


@click.command()
@click.option('--replica-url', required=True, help='Connection string for the target database (replica).')
@click.option('--primary-url', required=False, help='Connection string for the current primary database.')
@click.option('--dry-run/--execute', required=False, default=True, help='Show what will be done, but do nothing else.')
@click.option('--debug/--release', required=False, default=False, help='Show queries that actually ran.')
@click.option('--increment-by', default=1000, help='Increment the sequence by this amount.')
@click.option('--strategy', default='max-id-on-replica', help='"primary-sequences" or "max-id-on-replica". "primary-sequences" gets '
    'the sequences and their values from the primary database; "max-id-on-replica" gets the sequences and their values from the replica and the'
    'MAX(id) of the corresponding column.')
@click.option('--queries-only/--run-directly', help='Only print the queries that will execute on the target database.', default=False)
def main(replica_url, primary_url, dry_run, debug, increment_by, strategy, queries_only):
    if debug:
        os.environ['DEBUG'] = 'True'

    if queries_only:
        os.environ['QUERIES_ONLY'] = 'True'

    # Connect to the replica
    conn, cursor = connect(replica_url)

    # Pick a strategy
    strategies = ['primary-sequences', 'max-id-on-replica']
    if strategy not in strategies:
        print('Only two strategies are supported: {}.'.format(', '.join(strategies)), 'You chose: {}.'.format(strategy))
        exit(1)

    if strategy == 'primary-sequences':
        strategy = PrimarySequenceStrategy(primary_url)
    elif strategy == 'max-id-on-replica':
        strategy = ReplicaSequenceStrategy(replica_url)

    for sequence in strategy.sequences():
        sequence_name = strategy.sequence_name(sequence)
        sequence_value = strategy.sequence_value(sequence)

        _update_sequence(cursor, sequence_name, sequence_value, dry_run, increment_by)


def cli():
    main(prog_name='pgseqmover')

