from typing import List

from BasicDatabase.db_operations import DBOperation, SetOperation, UnsetOperation


class NoOpenTransactionError(ValueError):
    pass


class Database:
    def __init__(self):
        self.persistent_operations: List[DBOperation] = []
        self.open_transactions = []

    @property
    def _db(self):
        db = {}
        for operation in self.persistent_operations:
            db = operation.run(db)

        for transaction in self.open_transactions:
            for operation in transaction:
                db = operation.run(db)
        return db

    def _add_operation(self, operation: DBOperation):
        if self.open_transactions:
            last_open_transaction = self.open_transactions[-1]
            last_open_transaction.append(operation)
        else:
            self.persistent_operations.append(operation)

    def set(self, key, value):
        self._add_operation(SetOperation(key, value))

    def get(self, key):
        return self._db.get(key, 'NULL')

    def unset(self, key):
        self._add_operation(UnsetOperation(key))

    def begin_transaction(self):
        new_transaction_operation_list = []
        self.open_transactions.append(new_transaction_operation_list)

    def num_equal_to(self, searched_value):
        valid_keys = [key for key, value in self._db.items() if value == searched_value]
        return len(valid_keys)

    def rollback(self):
        self._raise_error_if_no_transactions()
        self.open_transactions.pop()

    def commit(self):
        self._raise_error_if_no_transactions()
        for transaction in self.open_transactions:
            self.persistent_operations += transaction
        self.open_transactions = []

    def _raise_error_if_no_transactions(self):
        if len(self.open_transactions) == 0:
            raise NoOpenTransactionError
