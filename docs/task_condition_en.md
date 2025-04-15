# Candidate Task

## Core Functionality

Interactive console application resembling a database interface. Data is requested from the user in a dialog mode (*keyboard input*). The entire "database" *resides in memory and is not saved between sessions*. One command — one request. Command arguments do not contain spaces. The input must recognize **EOF**, which signifies the end of input and application termination.

### Commands:
- `SET` - saves the argument in the database.
- `GET` - returns the previously saved variable. If the variable was not saved, returns `NULL`.
- `UNSET` - removes a previously set variable. If the value was not set, does nothing.
- `COUNTS` - displays the number of times the given value occurs in the database.
- `FIND` - outputs the found set variables for the given value.
- `END` - closes the application.

### Example:
```bash
> GET A
NULL
> SET A 10
> GET A
10
> COUNTS 10
1
> SET B 20
> SET C 10
> COUNTS 10
2
> UNSET B
> GET B
NULL
> END
```

## Transaction Support
The "database" must support transactions. Transactions can be nested.

### Commands:

- `BEGIN` - starts a transaction.
- `ROLLBACK` - rolls back the current (innermost) transaction.
- `COMMIT` - commits the changes of the current (innermost) transaction.

### Example:

```bash
> BEGIN
> SET A 10
> BEGIN
> SET A 20
> BEGIN
> SET A 30
> GET A
30
> ROLLBACK
> GET A
20
> COMMIT
> GET A
20
```