from unittest import TestCase

from BasicDatabase.database import Database
from BasicDatabase.errors import NoOpenTransactionError


class DatabaseTest(TestCase):
    def setUp(self):
        self.db = Database()

    def test_set_and_get(self):
        # given
        key = 'foo'
        value = 'bar'

        # when
        self.db.set(key, value)

        # then
        self.assertEqual(self.db.get(key), value)

    def test_unset_and_get(self):
        # given
        key = 'foo'
        value = 'bar'
        self.db.set(key, value)

        # when
        self.db.unset(key)

        # then
        self.assertEqual(self.db.get(key), 'NULL')

    def test_num_equal_to_zero(self):
        # given
        key = 'foo'
        value = 'bar'
        expected_number = 0

        # when
        actual_number = self.db.num_equal_to(value)

        # then
        self.assertEqual(expected_number, actual_number)

    def test_num_equal_to_one(self):
        # given
        key = 'foo'
        value = 'bar'
        self.db.set(key, value)
        expected_number = 1

        # when
        actual_number = self.db.num_equal_to(value)

        # then
        self.assertEqual(expected_number, actual_number)

    def test_num_equal_to_two(self):
        # given
        keys = ['foo1', 'foo2']
        value = 'bar'
        for key in keys:
            self.db.set(key, value)
        expected_number = 2

        # when
        actual_number = self.db.num_equal_to(value)

        # then
        self.assertEqual(expected_number, actual_number)

    def test_begin_set_and_rollback(self):
        # given
        key = 'foo'
        value = 'bar'
        expected_in_transaction_value = value
        expected_after_rollback_value = 'NULL'

        # when
        self.db.begin_transaction()
        self.db.set(key, value)
        in_transaction_value = self.db.get(key)
        self.db.rollback()
        after_rollback_value = self.db.get(key)

        # then
        self.assertEqual(len(self.db.open_transactions), 0)
        self.assertEqual(in_transaction_value, expected_in_transaction_value)
        self.assertEqual(after_rollback_value, expected_after_rollback_value)

    def test_rollback_on_no_transactions(self):
        self.assertRaises(NoOpenTransactionError, lambda: self.db.rollback())

    def test_rollback_on_nested_transactions(self):
        # given
        key = 'foo'
        value1 = 'bar1'
        value2 = 'bar2'
        expected_in_transaction_value = value2
        expected_after_rollback_value = value1

        self.db.begin_transaction()
        self.db.set(key, value1)
        self.db.begin_transaction()
        self.db.set(key, value2)
        in_nested_transaction_value = self.db.get(key)

        # when
        self.db.rollback()
        after_rollback_value = self.db.get(key)

        # then
        self.assertEqual(len(self.db.open_transactions), 1)
        self.assertEqual(in_nested_transaction_value, expected_in_transaction_value)
        self.assertEqual(after_rollback_value, expected_after_rollback_value)

    def test_commit_on_single_transaction(self):
        # given
        key = 'foo'
        value = 'bar'
        expected_in_transaction_value = value
        expected_after_commit_value = value

        # when
        self.db.begin_transaction()
        self.db.set(key, value)
        in_transaction_value = self.db.get(key)
        self.db.commit()
        after_commit_value = self.db.get(key)

        # then
        self.assertEqual(len(self.db.open_transactions), 0)
        self.assertEqual(in_transaction_value, expected_in_transaction_value)
        self.assertEqual(after_commit_value, expected_after_commit_value)

    def test_commit_on_nested_transactions(self):
        # given
        key = 'foo'
        value1 = 'bar1'
        value2 = 'bar2'
        expected_in_nested_transaction_value = value2
        expected_after_commit_value = value2

        self.db.begin_transaction()
        self.db.set(key, value1)
        self.db.begin_transaction()
        self.db.set(key, value2)
        in_nested_transaction_value = self.db.get(key)

        # when
        self.db.commit()
        after_commit_value = self.db.get(key)

        # then
        self.assertEqual(len(self.db.open_transactions), 0)
        self.assertEqual(in_nested_transaction_value, expected_in_nested_transaction_value)
        self.assertEqual(after_commit_value, expected_after_commit_value)

    def test_commit_on_no_transactions(self):
        self.assertRaises(NoOpenTransactionError, lambda: self.db.commit())
