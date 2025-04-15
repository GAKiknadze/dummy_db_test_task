from collections import defaultdict
from typing import Dict, List, Optional, Set


class Database:
    __main_data: Dict[str, str]
    __transaction_stack: List[Dict[str, Optional[str]]]
    __value_counts: Dict[str, int]

    def __init__(self):
        self.__main_data = {}
        self.__transaction_stack = []
        self.__value_counts = defaultdict(int)

    def _get_current_transaction(self) -> Optional[Dict[str, Optional[str]]]:
        return self.__transaction_stack[-1] if self.__transaction_stack else None

    def set(self, key: str, value: str) -> None:
        current = self._get_current_transaction()
        source = current if current is not None else self.__main_data

        old_value = self.get(key)
        if old_value == value:
            return

        if old_value is not None:
            self.__value_counts[old_value] -= 1
            if self.__value_counts[old_value] == 0:
                del self.__value_counts[old_value]

        source[key] = value
        self.__value_counts[value] += 1

    def get(self, key: str) -> Optional[str]:
        for transaction in reversed(self.__transaction_stack):
            if key in transaction:
                return transaction[key] if transaction[key] is not None else None
        return self.__main_data.get(key)

    def unset(self, key: str) -> None:
        current = self._get_current_transaction()

        if current is not None:
            old_value = self.get(key)
            current[key] = None
            if old_value is not None:
                self.__value_counts[old_value] -= 1
                if self.__value_counts[old_value] == 0:
                    del self.__value_counts[old_value]
        else:
            old_value = self.__main_data.get(key)
            if old_value is not None:
                del self.__main_data[key]
                self.__value_counts[old_value] -= 1
                if self.__value_counts[old_value] == 0:
                    del self.__value_counts[old_value]

    def counts(self, value: str) -> int:
        return self.__value_counts.get(value, 0)

    def find(self, value: str) -> Optional[List[str]]:
        result = []
        seen: Set[str] = set()

        keys = set()
        for transaction in self.__transaction_stack:
            keys.update(transaction.keys())
        keys.update(self.__main_data.keys())

        for key in keys:
            current_value = self.get(key)
            if current_value == value and key not in seen:
                result.append(key)
                seen.add(key)

        return sorted(result) if result else None

    def begin(self) -> None:
        self.__transaction_stack.append({})

    def rollback(self) -> bool:
        if not self.__transaction_stack:
            return False

        transaction = self.__transaction_stack.pop()

        for key, value in transaction.items():
            if value is not None:
                self.__value_counts[value] -= 1
                if self.__value_counts[value] == 0:
                    del self.__value_counts[value]

                restored_value = self.get(key)
                if restored_value is not None:
                    self.__value_counts[restored_value] += 1
            else:
                restored_value = self.get(key)
                if restored_value is not None:
                    self.__value_counts[restored_value] += 1
        return True

    def commit(self) -> bool:
        if not self.__transaction_stack:
            return False

        current = self.__transaction_stack.pop()
        target = (
            self.__transaction_stack[-1]
            if self.__transaction_stack
            else self.__main_data
        )

        for key, value in current.items():
            if value is None:
                old_value = target.get(key)
                if old_value is not None:
                    self.__value_counts[old_value] -= 1
                    if self.__value_counts[old_value] == 0:
                        del self.__value_counts[old_value]
                    del target[key]
            else:
                old_value = target.get(key)
                if old_value == value:
                    continue

                if old_value is not None:
                    self.__value_counts[old_value] -= 1
                    if self.__value_counts[old_value] == 0:
                        del self.__value_counts[old_value]

                target[key] = value
                self.__value_counts[value] += 1

        if self.__transaction_stack:
            self.__transaction_stack[-1].update(target)

        return True
