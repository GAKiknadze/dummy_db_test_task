import pytest

from database import Database


@pytest.fixture
def db():
    return Database()


def test_set_get_outside_transaction(db):
    db.set("a", "foo")
    assert db.get("a") == "foo"
    assert db._Database__main_data["a"] == "foo"


def test_set_in_transaction(db):
    db.begin()
    db.set("a", "foo")
    assert db.get("a") == "foo"
    assert "a" not in db._Database__main_data
    db.commit()
    assert db._Database__main_data["a"] == "foo"


def test_unset_outside_transaction(db):
    db.set("a", "foo")
    db.unset("a")
    assert db.get("a") is None
    assert "a" not in db._Database__main_data


def test_unset_in_transaction(db):
    db.set("a", "foo")
    db.begin()
    db.unset("a")
    assert db.get("a") is None
    db.commit()
    assert db.get("a") is None


def test_counts_basic(db):
    db.set("a", "foo")
    db.set("b", "foo")
    assert db.counts("foo") == 2
    db.unset("a")
    assert db.counts("foo") == 1


def test_find_sorted_results(db):
    db.set("c", "foo")
    db.set("a", "foo")
    db.set("b", "foo")
    assert db.find("foo") == ["a", "b", "c"]


def test_transaction_rollback(db):
    db.begin()
    db.set("a", "foo")
    db.rollback()
    assert db.get("a") is None


def test_nested_transactions(db):
    db.begin()
    db.set("a", "foo")
    db.begin()
    db.set("a", "bar")
    db.commit()
    assert db.get("a") == "bar"
    db.commit()
    assert db._Database__main_data["a"] == "bar"


def test_commit_empty_transaction(db):
    assert not db.commit()


def test_rollback_empty_transaction(db):
    assert not db.rollback()


def test_value_counts_multiple_transactions(db):
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
    db.set("a", "foo")
    db.begin()
    db.unset("a")
    db.set("b", "foo")
    assert db.find("foo") == ["b"]
    db.rollback()
    assert db.find("foo") == ["a"]


def test_commit_chain(db):
    db.begin()
    db.set("a", "foo")
    db.begin()
    db.set("b", "bar")
    db.commit()
    db.commit()
    assert db._Database__main_data == {"a": "foo", "b": "bar"}


def test_overwrite_value_in_transaction(db):
    db.set("a", "foo")
    db.begin()
    db.set("a", "bar")
    assert db.counts("foo") == 0
    assert db.counts("bar") == 1
    db.rollback()
    assert db.counts("foo") == 1


def test_unset_non_existing_key(db):
    db.unset("a")
    assert db.get("a") is None


def test_find_non_existing_value(db):
    assert db.find("foo") is None


def test_multiple_operations_in_transaction(db):
    db.begin()
    db.set("a", "foo")
    db.set("b", "bar")
    db.unset("a")
    db.set("c", "baz")
    assert db.get("a") is None
    assert db.get("b") == "bar"
    db.commit()
    assert db._Database__main_data == {"b": "bar", "c": "baz"}
