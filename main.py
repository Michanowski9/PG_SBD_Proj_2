import sys

from ProgramManager import ProgramManager


def main():
    d = sys.argv[1] if len(sys.argv) > 1 else 2
    programManager = ProgramManager(d)
    programManager.run()


if __name__ == "__main__":
    main()
