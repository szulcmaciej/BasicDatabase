from abc import ABC, abstractmethod


class DBOperation(ABC):
    @abstractmethod
    def run(self, db: dict) -> dict:
        pass


class SetOperation(DBOperation):
    def __init__(self, key, value):
        self.key = key
        self.value = value

    def run(self, db: dict) -> dict:
        db[self.key] = self.value
        return db


class UnsetOperation(DBOperation):
    def __init__(self, key):
        self.key = key

    def run(self, db: dict) -> dict:
        db.pop(self.key)
        return db
