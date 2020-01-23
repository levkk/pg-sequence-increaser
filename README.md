# pg-sequence-increaser
Set all sequences to a higher value when promoting a PostgreSQL logical replica.


## Requirements

1. Python3
2. `libpq-dev` (for psycopg2). On Mac OS, `brew install postgresql`, on Ubuntu, install `libpq-dev`.

## Installation

### Production

```bash
$ pip install pg-sequence-increaser
```

### Development

Using virtualenv,
```bash
$ pip install -r requirements.txt
```

### Usage

This tool implements two strategies:

1. Getting the sequences and their desired values from the logical replica alone (called "max-id-on-replica"),
2. getting the sequences and their desired values from the primary database, i.e. the publisher; this is called "primary-sequences".

#### Max Id on Replica

We get the sequences from `information_schema.default_value`. Any `default_value` that has `nextval('some_sequence_name')`, we assume that's a sequence and extract the table name, the associated column and the sequence name. Then, we run `MAX(column_name)` to get the last value inserted into that table, likely from the sequence; we then move the sequence forward to the `MAX(column_name) + increment_by`. This strategy has the advantage of using the replica as the source of truth and not requiring the publisher to be online.


```bash
$ pgseqmover --replica-url=postgres://localhost:5432/my_db --queries-only
```
will print the queries you can then run manually. Those queries will increase all sequences by 1000.

#### Primary Sequences

We get the sequences from the primary (publisher). After all, that's where we are replicating from in the first place. We use `pg_sequences` view and get both the name and the `last_value` from there as well. We then set the sequences on the replica to what they are on the primary + `increment_by`. This strategy has the advantage of using the primary database as the source of truth in case replica lag is non-zero during promotion which is bad, but possible. This requires the primary to be available during promotion of the replica.


```bash
$ pgseqmover --replica-url=postgres://replica-db:5432/my_db --primary-url=postgres://primary-db:5432/my_db --queries-only --strategy=primary-sequences
```
will print the queries you can then run manually. Those queries will increase all sequences by 1000.

#### Arguments

This also accepts optional arguments:

1. `--dry-run` which will show the queries it will run but not actually do anything; this is the default behavior,
2. `--debug` will show _all_ queries it's running; can be combined with `--dry-run`,
3. `--increment-by` overrides the default increase by value of 1000 to any value (even negative ones).
4. `--strategy` picks the strategy to use for increasing sequences. Your options are: `primary-sequences` and `max-id-on-replica`, both explained above.
5. `--queries-only` will only print the queries that will move the sequences and not do anything else. Less pretty version of `--dry-run`, but good for `pgseqmover --queries-only > ~/Desktop/queries_to_run.sql` use case.

