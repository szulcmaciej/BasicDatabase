from abc import abstractmethod, ABC
from typing import List

from BasicDatabase.database import Database, NoOpenTransactionError


class EndCommandEncountered(Exception):
    pass


class Command(ABC):
    def __init__(self, command_string: str):
        self.command_string = command_string

    def is_valid(self, command_string: str):
        return command_string.lower() == self.command_string

    @abstractmethod
    def run(self, db: Database, arguments):
        pass

    def return_formatted_command_and_output(self, arguments: List[str] = None, command_output=None):
        formatted_output = self.command_string.upper()
        if arguments:
            formatted_output += ' ' + ' '.join(arguments)
        if command_output is not None:
            formatted_output = formatted_output.ljust(15, ' ')
            formatted_output += ' ' + str(command_output)
        return formatted_output


class SetCommand(Command):
    def __init__(self):
        super().__init__(command_string='set')

    def run(self, db: Database,  arguments):
        key = arguments[0]
        value = arguments[1]
        db.set(key, value)
        return self.return_formatted_command_and_output(arguments)


class GetCommand(Command):
    def __init__(self):
        super().__init__(command_string='get')

    def run(self, db: Database, arguments):
        key = arguments[0]
        value = db.get(key)
        return self.return_formatted_command_and_output(arguments, value)


class UnsetCommand(Command):
    def __init__(self):
        super().__init__(command_string='unset')

    def run(self, db: Database, arguments):
        key = arguments[0]
        db.unset(key)
        return self.return_formatted_command_and_output(arguments)


class NumEqualToCommand(Command):
    def __init__(self):
        super().__init__(command_string='numequalto')

    def run(self, db: Database, arguments):
        value = arguments[0]
        quantity = db.num_equal_to(value)
        return self.return_formatted_command_and_output(arguments, quantity)


class BeginCommand(Command):
    def __init__(self):
        super().__init__(command_string='begin')

    def run(self, db: Database, arguments):
        db.begin_transaction()
        return self.return_formatted_command_and_output()


class RollbackCommand(Command):
    def __init__(self):
        super().__init__(command_string='rollback')

    def run(self, db: Database, arguments):
        try:
            db.rollback()
            output = self.return_formatted_command_and_output()
        except NoOpenTransactionError:
            output = self.return_formatted_command_and_output(command_output='NO TRANSACTION')
        return output


class CommitCommand(Command):
    def __init__(self):
        super().__init__(command_string='commit')

    def run(self, db: Database, arguments):
        try:
            db.commit()
            output = self.return_formatted_command_and_output()
        except NoOpenTransactionError:
            output = self.return_formatted_command_and_output(command_output='NO TRANSACTION')
        return output


class EndCommand(Command):
    def __init__(self):
        super().__init__(command_string='end')

    def run(self, db: Database, arguments):
        raise EndCommandEncountered


class UnknownCommandError(RuntimeError):
    pass


class CommandHandler:
    def __init__(self, db: Database):
        self.db = db
        self.commands = [SetCommand(),
                         GetCommand(),
                         UnsetCommand(),
                         NumEqualToCommand(),
                         BeginCommand(),
                         RollbackCommand(),
                         CommitCommand(),
                         EndCommand()]

    def handle(self, line) -> str:
        line_words = line.split()
        command_string = line_words[0]
        if len(line_words) > 0:
            arguments = line_words[1:]
        else:
            arguments = None
        for command in self.commands:
            if command.is_valid(command_string):
                return command.run(self.db, arguments)
        raise UnknownCommandError
