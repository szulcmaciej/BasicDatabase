import sys

from BasicDatabase.database import Database
from BasicDatabase.stdin_commands import CommandHandler, EndCommandEncountered


class StdinDBProcessor:
    def __init__(self):
        self._db = Database()
        self.command_handler = CommandHandler(self._db)
        self.read_from_stdin()

    def read_from_stdin(self):
        try:
            for line in sys.stdin.readlines():
                output = self.command_handler.handle(line)
                print(output)
        except EndCommandEncountered:
            print('END')


if __name__ == '__main__':
    StdinDBProcessor()
