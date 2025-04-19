import pytest

from database import Database


@pytest.fixture
def db():
    return Database()


def test_set_get_outside_transaction(db):
    """Проверка базовой установки и получения значения вне транзакции."""
    db.set("a", "foo")
    assert db.get("a") == "foo"
    assert db._Database__main_data["a"] == "foo"


def test_set_in_transaction(db):
    """Проверка установки значения внутри транзакции с последующим коммитом."""
    db.begin()
    db.set("a", "foo")
    assert db.get("a") == "foo"
    assert "a" not in db._Database__main_data
    db.commit()
    assert db._Database__main_data["a"] == "foo"


def test_unset_outside_transaction(db):
    """Проверка удаления ключа вне транзакции."""
    db.set("a", "foo")
    db.unset("a")
    assert db.get("a") is None
    assert "a" not in db._Database__main_data


def test_unset_in_transaction(db):
    """Проверка временного удаления ключа в транзакции."""
    db.set("a", "foo")
    db.begin()
    db.unset("a")
    assert db.get("a") is None
    db.commit()
    assert db.get("a") is None


def test_unset_in_sub_transaction(db):
    """Проверка удаления ключа внутри вложенной транзакции"""
    db.set("a", "foo")
    db.begin()
    assert db.get("a") == "foo"
    db.set("a", "bar")
    db.begin()
    assert db.get("a") == "bar"
    db.unset("a")
    assert db.get("a") is None
    db.commit()
    assert db.get("a") is None
    db.commit()
    assert db.get("a") is None


def test_counts_basic(db):
    """Проверка подсчета значений с обновлением счетчиков при изменениях."""
    db.set("a", "foo")
    db.set("b", "foo")
    assert db.counts("foo") == 2
    db.unset("a")
    assert db.counts("foo") == 1


def test_find_sorted_results(db):
    """Проверка получения ключей в лексикографическом порядке."""
    db.set("c", "foo")
    db.set("a", "foo")
    db.set("b", "foo")
    assert db.find("foo") == ["a", "b", "c"]


def test_transaction_rollback(db):
    """Проверка отката транзакции."""
    db.begin()
    db.set("a", "foo")
    db.rollback()
    assert db.get("a") is None


def test_nested_transactions(db):
    """Проверка вложенных транзакций с каскадным коммитом."""
    db.begin()
    db.set("a", "foo")
    db.begin()
    db.set("a", "bar")
    db.commit()
    assert db.get("a") == "bar"
    db.commit()
    assert db._Database__main_data["a"] == "bar"


def test_commit_empty_transaction(db):
    """Проверка попытки коммита без активной транзакции."""
    assert not db.commit()


def test_rollback_empty_transaction(db):
    """Проверка попытки отката без активной транзакции."""
    assert not db.rollback()


def test_value_counts_multiple_transactions(db):
    """Проверка корректности счетчиков при откате транзакции."""
    db.set("a", "foo")
    db.begin()
    db.set("a", "bar")
    db.set("b", "bar")
    assert db.counts("foo") == 0
    assert db.counts("bar") == 2
    db.rollback()
    assert db.counts("foo") == 1
    assert db.counts("bar") == 0


def test_find_with_transaction_unset(db):
    """Проверка поиска с временным удалением в транзакции."""
    db.set("a", "foo")
    db.begin()
    db.unset("a")
    db.set("b", "foo")
    assert db.find("foo") == ["b"]
    db.rollback()
    assert db.find("foo") == ["a"]


def test_commit_chain(db):
    """Проверка цепочки вложенных транзакций с каскадным коммитом."""
    db.begin()
    db.set("a", "foo")
    db.begin()
    db.set("b", "bar")
    db.commit()
    db.commit()
    assert db._Database__main_data == {"a": "foo", "b": "bar"}


def test_overwrite_value_in_transaction(db):
    """Проверка перезаписи значения и отката счетчиков."""
    db.set("a", "foo")
    db.begin()
    db.set("a", "bar")
    assert db.counts("foo") == 0
    assert db.counts("bar") == 1
    db.rollback()
    assert db.counts("foo") == 1


def test_unset_non_existing_key(db):
    """Проверка удаления несуществующего ключа."""
    db.unset("a")
    assert db.get("a") is None


def test_find_non_existing_value(db):
    """Проверка поиска несуществующего значения."""
    assert db.find("foo") is None


def test_multiple_operations_in_transaction(db):
    """Проверка комплексных операций в транзакции."""
    db.begin()
    db.set("a", "foo")
    db.set("b", "bar")
    db.unset("a")
    db.set("c", "baz")
    assert db.get("a") is None
    assert db.get("b") == "bar"
    db.commit()
    assert db._Database__main_data == {"b": "bar", "c": "baz"}
