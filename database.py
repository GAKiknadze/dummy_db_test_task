from typing import Dict, List, Optional


class Database:
    main_data: Dict[str, str]
    transaction_stack: List[Dict[str, str]]
    value_counts: Dict[str, int]
    
    def __init__(self):
        self.main_data = dict()
        self.transaction_stack = list()
        self.value_counts = dict()

    def _get_current_transaction(self) -> Optional[Dict[str, str]]:
        return self.transaction_stack[-1] if self.transaction_stack else None

    def set(self, key: str, value: str) -> None:
        current = self._get_current_transaction()
        source: Dict[str, str]

        if current is not None:
            source = current
        else:
            source = self.main_data

        old_value = source.get(key)
        if old_value is not None:
            self.value_counts[old_value] -= 1
        source[key] = value
        self.value_counts[value] = self.value_counts.get(value, 0) + 1

    def get(self, key: str) -> Optional[str]:
        for transaction in reversed(self.transaction_stack):
            if key in transaction:
                return transaction[key]
        return self.main_data.get(key)

    def unset(self, key: str) -> None:
        current = self._get_current_transaction()
        if current is not None:
            current.pop(key, None)
        else:
            if key in self.main_data:
                value = self.main_data.pop(key)
                self.value_counts[value] -= 1

    def counts(self, value: str) -> int:
        return self.value_counts.get(value, 0)

    def find(self, value: str) -> Optional[List[str]]:
        result = []
        all_data = [self.main_data] + self.transaction_stack
        for data in all_data:
            for k, v in data.items():
                if v == value and k not in result:
                    result.append(k)
        return sorted(result) if result else None

    def begin(self) -> None:
        self.transaction_stack.append(dict())

    def rollback(self) -> bool:
        if self.transaction_stack:
            transaction = self.transaction_stack.pop()
            for _, value in transaction.items():
                if value is not None:
                    self.value_counts[value] -= 1
            return True
        return False

    def commit(self) -> bool:
        if not self.transaction_stack:
            return False

        current_transaction = self.transaction_stack.pop()

        if self.transaction_stack:
            parent = self.transaction_stack[-1]
            for key, value in current_transaction.items():
                parent[key] = value
        else:
            for key, value in current_transaction.items():
                if value is None:
                    if key in self.main_data:
                        old_value = self.main_data.pop(key)
                        self.value_counts[old_value] -= 1
                else:
                    old_value = self.main_data.get(key)
                    if old_value is not None:
                        self.value_counts[old_value] -= 1
                    self.main_data[key] = value
                    self.value_counts[value] = self.value_counts.get(value, 0) + 1
        return True
