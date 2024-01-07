import os
if os.name != 'nt':
    import getch

from BTree import BTree
from DataRecord import generate_random_record_data


class ProgramManager:
    def __init__(self, d) -> None:
        self.btree = BTree(d)

    def run(self) -> None:
        self.main_loop()

    def main_loop(self) -> None:
        running = True
        while running:
            self.print_menu()

            if os.name == 'nt':
                choosen_option = input()
            else:
                choosen_option = getch.getch()

            match choosen_option.lower():
                case '1':
                    self.command_insert()
                case '2':
                    self.btree.print()
                case '3':
                    self.btree.print(print_records=True)
                case '4':
                    self.command_search()
                case '5':
                    self.command_remove()
                case '6':
                    self.command_update()
                    pass
                case '7':
                    self.btree.filesHandler.print_index_file()
                case '8':
                    self.btree.filesHandler.print_data_file()
                    pass
                case 'q':
                    running = False
                    break

            if os.name == 'nt':
                pass
            else:
                print("\npress any button to continue")
                getch.getch()

    def command_insert(self) -> None:
        try:
            print("Inserting")
            user_input = int(input("Enter key: "))
            self.btree.insert(generate_random_record_data(user_input))
        except:
            pass

    def command_search(self) -> None:
        try:
            print("Searching")
            user_input = int(input("Enter key: "))
            self.btree.search(user_input)
        except:
            pass

    def command_remove(self) -> None:
        try:
            print("Removing")
            user_input = int(input("Enter key: "))
            self.btree.remove(user_input)
        except:
            pass

    def command_update(self) -> None:
        try:
            print("Updating")
            old_key = int(input("Enter key: "))
            new_key = int(input("Enter new key: "))
            self.btree.update(old_key, generate_random_record_data(new_key))
        except:
            pass

    @staticmethod
    def print_menu() -> None:
        if os.name != 'nt':
            os.system('clear')
        print("==========================")
        print("\t\tMenu:")
        print("\t[1] Insert")
        print("\t[2] Print")
        print("\t[3] Print records")
        print("\t[4] Search")
        print("\t[5] Remove")
        print("\t[6] Update")
        print("\t[7] Print index file")
        print("\t[8] Prind data file")

        print("\t[Q] Quit")
        print()