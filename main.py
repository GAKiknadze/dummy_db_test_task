import traceback

from database import Database


def main():
    db = Database()
    while True:
        try:
            input_line = input("> ").strip()
        except EOFError:
            break
        except (KeyboardInterrupt, Exception):
            error_msg = traceback.format_exc().splitlines()[-1]
            print(f"\nError: {error_msg}")
            continue

        if not input_line:
            continue

        parts = input_line.split()
        command = parts[0].upper()
        args = parts[1:]

        try:
            if command == "SET":
                if len(args) != 2:
                    print("Error: SET requires 2 arguments")
                    continue
                db.set(args[0], args[1])

            elif command == "GET":
                if len(args) != 1:
                    print("Error: GET requires 1 argument")
                    continue
                print(db.get(args[0]) or "NULL")

            elif command == "UNSET":
                if len(args) != 1:
                    print("Error: UNSET requires 1 argument")
                    continue
                db.unset(args[0])

            elif command == "COUNTS":
                if len(args) != 1:
                    print("Error: COUNTS requires 1 argument")
                    continue
                print(db.counts(args[0]))

            elif command == "FIND":
                if len(args) != 1:
                    print("Error: FIND requires 1 argument")
                    continue
                result = db.find(args[0])
                print(" ".join(result) if isinstance(result, list) else "NULL")

            elif command == "BEGIN":
                db.begin()

            elif command == "ROLLBACK":
                if not db.rollback():
                    print("Error: No transaction")

            elif command == "COMMIT":
                if not db.commit():
                    print("Error: No transaction")

            elif command == "END":
                break

            else:
                prepared_command = command.encode("unicode_escape").decode()
                print(f"Unknown command: {prepared_command}")

        except Exception:
            error_msg = traceback.format_exc().splitlines()[-1]
            print(f"\nError: {error_msg}")


if __name__ == "__main__":
    main()
