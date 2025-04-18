# dummy_db_test_task

![Tests](https://github.com/GAKiknadze/dummy_db_test_task/actions/workflows/python-package.yml/badge.svg)

The test task "Interactive console application resembling a database interface" from [DrWeb](https://drweb.com)

### Task condition:

- [Russian](./docs/task_condition_ru.md)
- [English](./docs/task_condition_en.md)

## Launching the app

1. Make sure you have **Python 3.8+** installed.
2. Run in the terminal:

```bash
python main.py
```

or

```bash
python3 main.py
```

## Commands

- `SET` - saves the argument in the database.
- `GET` - returns the previously saved variable. If the variable was not saved, returns `NULL`.
- `UNSET` - removes a previously set variable. If the value was not set, does nothing.
- `COUNTS` - displays the number of times the given value occurs in the database.
- `FIND` - outputs the found set variables for the given value.
- `END` - closes the application.
- `BEGIN` - starts a transaction.
- `ROLLBACK` - rolls back the current (innermost) transaction.
- `COMMIT` - commits the changes of the current (innermost) transaction.

## Launching tests

1. Install test requirements:

```bash
pip install -r requirements.test.txt
```

2. Run tests:

```bash
pytest tests/ -v
```
