from collections import defaultdict
from typing import Dict, List, Optional, Set


class Database:
    """
    Класс для работы с ключ-значение базой данных с поддержкой транзакций.

    База данных позволяет выполнять операции set, get, unset, counts и find,
    а также управлять транзакциями через методы begin, rollback и commit.
    Все изменения в рамках транзакции являются временными, пока не будет выполнен commit.
    Поддерживаются вложенные транзакции.
    """

    __main_data: Dict[str, str]  # Основное хранилище данных вне транзакций
    __transaction_stack: List[Dict[str, Optional[str]]]  # Стек активных транзакций
    __value_counts: Dict[
        str, int
    ]  # Счетчик количества значений для оптимизации операции counts

    def __init__(self):
        """Инициализирует новую базу данных с пустым хранилищем и стеком транзакций."""
        self.__main_data = {}
        self.__transaction_stack = []
        self.__value_counts = defaultdict(int)

    def _get_current_transaction(self) -> Optional[Dict[str, Optional[str]]]:
        """Возвращает текущую активную транзакцию (последнюю в стеке) или None, если транзакций нет.

        Возвращает:
            Optional[Dict[str, Optional[str]]]: Текущая транзакция или None.
        """
        return self.__transaction_stack[-1] if self.__transaction_stack else None

    def set(self, key: str, value: str) -> None:
        """Устанавливает значение для ключа.

        Если ключ уже существует, обновляет его значение. Изменения применяются
        к текущей транзакции (если активна) или к основному хранилищу.

        Аргументы:
            key (str): Ключ для установки.
            value (str): Значение для ключа.
        """
        current = self._get_current_transaction()
        source = current if current is not None else self.__main_data

        old_value = self.get(key)
        if old_value == value:
            return

        if old_value is not None:
            self._decrement_value_count(old_value)

        source[key] = value
        self.__value_counts[value] += 1

    def get(self, key: str) -> Optional[str]:
        """Возвращает значение ключа с учетом активных транзакций.

        Проверяет транзакции от самой новой к старой, затем основное хранилище.

        Аргументы:
            key (str): Ключ для поиска.

        Возвращает:
            Optional[str]: Значение ключа или None, если ключ не существует.
        """
        for transaction in reversed(self.__transaction_stack):
            if key in transaction:
                return transaction[key] if transaction[key] is not None else None
        return self.__main_data.get(key)
    
    def _decrement_value_count(self, value: str) -> None:
        """Уменьшает значение счетчика
        
        Если количество значения равно 0, удаляет его из счетчика
        
        Аргументы:
            value (str): Значение для поиска.
        """
        self.__value_counts[value] -= 1
        if self.__value_counts[value] == 0:
            del self.__value_counts[value]

    def unset(self, key: str) -> None:
        """Удаляет ключ из базы данных.

        Если транзакция активна, помечает ключ как удаленный (None).
        В противном случае удаляет ключ из основного хранилища.

        Аргументы:
            key (str): Ключ для удаления.
        """
        current = self._get_current_transaction()

        if current is not None:
            old_value = self.get(key)
            current[key] = None
            if old_value is not None:
                self._decrement_value_count(old_value)
        else:
            old_value = self.__main_data.get(key)
            if old_value is not None:
                del self.__main_data[key]
                self._decrement_value_count(old_value)

    def counts(self, value: str) -> int:
        """Возвращает количество ключей с указанным значением.

        Аргументы:
            value (str): Значение для подсчета.

        Возвращает:
            int: Количество ключей с этим значением.
        """
        return self.__value_counts.get(value, 0)

    def find(self, value: str) -> Optional[List[str]]:
        """Возвращает список ключей с указанным значением, отсортированный в лексикографическом порядке.

        Аргументы:
            value (str): Значение для поиска.

        Возвращает:
            Optional[List[str]]: Отсортированный список ключей или None, если совпадений нет.
        """
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
        """Начинает новую транзакцию."""
        self.__transaction_stack.append({})

    def rollback(self) -> bool:
        """Откатывает последнюю транзакцию.

        Возвращает:
            bool: True, если транзакция была успешно отменена, False, если транзакций нет.
        """
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
        return True

    def commit(self) -> bool:
        """Применяет изменения из текущей транзакции.

        Если есть вложенные транзакции, объединяет изменения с предыдущей транзакцией.
        Если транзакций нет, возвращает False.

        Возвращает:
            bool: True, если коммит успешен, False, если транзакций нет.
        """
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
                    self._decrement_value_count(old_value)
                if target is self.__main_data:
                    target.pop(key, None)
                else:
                    target[key] = None
            else:
                old_value = target.get(key)
                if old_value == value:
                    continue

                if old_value is not None:
                    self._decrement_value_count(old_value)

                target[key] = value
                self.__value_counts[value] += 1

        return True
