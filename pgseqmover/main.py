'''Increment all sequences on a PostgreSQL database. Useful when promoting a logical replica to be the new primary.
'''
import psycopg2
import psycopg2.extras
import click
import colorama
from colorama import Fore
import os

VERSION = '0.0.1'


__author__ = 'Lev Kokotov <lev.kokotov@instacart.com>'
__version__ = VERSION

colorama.init()


def connect(db_url):
    '''Connect to source and replicaination DBs.'''
    conn = psycopg2.connect(db_url)

    return conn


def _result(text):
    print(Fore.GREEN, '\b{}'.format(text), Fore.RESET)


def _debug(cursor):
    if os.getenv('DEBUG'):
        print(Fore.BLUE, '\b{}: {}'.format(cursor.connection.dsn, cursor.query.decode('utf-8')), Fore.RESET)


def _dry_run(cursor, query, params):
    query = cursor.mogrify(query, params).decode('utf-8') if len(params) > 0 else query
    print(Fore.YELLOW, '\b{}: {}'.format(cursor.connection.dsn, query), Fore.RESET)


def _exec(cursor, query, params=None):
    cursor.execute(query, params)
    _debug(cursor)

    return cursor


def _sequence_name(column_default):
    return column_default.split("'")[1]


def _sequences(cursor):
    sequences = _exec(cursor, """SELECT table_name, column_name, column_default::text AS column_default FROM information_schema.columns
        WHERE table_schema = 'public' AND column_default::text LIKE 'nextval%'""").fetchall()

    return sequences


def _max_id(cursor, sequence):
    return _exec(
        cursor, 'SELECT MAX({}) AS "max" FROM {}'.format(sequence['column_name'], sequence['table_name'])).fetchone()['max']


def _update_sequence(cursor, sequence, increment_by, dry_run):
    id_ = _max_id(cursor, sequence) + increment_by
    query = "SELECT setval(%s, %s, true)"
    params = (_sequence_name(sequence['column_default']), id_)

    if dry_run:
        _dry_run(cursor, query, params)
    else:
        _exec(cursor, query, params)


@click.command()
@click.option('--db-url', required=True, help='Connection string for the target database.')
@click.option('--dry-run/--execute', required=False, default=True, help='Show what will be done, but do nothing else.')
@click.option('--debug/--release', required=False, default=False, help='Show queries that actually ran.')
@click.option('--increment-by', default=1000, help='Increment the sequence by this amount.')
def main(db_url, dry_run, debug, increment_by):
    if debug:
        os.environ['DEBUG'] = 'True'

    conn = connect(db_url)
    conn.set_session(autocommit=True)
    cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

    sequences = _sequences(cursor)
    for sequence in sequences:
        _update_sequence(cursor, sequence, int(increment_by), dry_run)


def cli():
    main(prog_name='pgseqmover')

