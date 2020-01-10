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

```bash
$ pgseqmover --db-url=postgres://localhost:5432/my_db
```

will increase all sequences by 1000.

This also accepts optional arguments:

1. `--dry-run` which will show the queries it will run but not actually do anything,
2. `--debug` will show _all_ queries it's running; can be combined with `--dry-run`,
3. `--increment-by` overrides the default increase by value of 1000 to any value (even negative ones).