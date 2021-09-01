class NoOpenTransactionError(ValueError):
    pass


class EndCommandEncountered(Exception):
    pass


class UnknownCommandError(RuntimeError):
    pass