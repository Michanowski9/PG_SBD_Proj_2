from FilesHandler import FilesHandler
from IndexPage import IndexRecord, IndexPage
from DataRecord import DataRecord


class BTree:
    def __init__(self, d=2) -> None:
        self.d: int = d
        self.root_page: int | None = None  # root page address
        self.h: int = 0
        self.filesHandler = FilesHandler(2 * d)

# public:
    def insert(self, record: DataRecord) -> None:
        data_page_number = self.filesHandler.add_record_to_data_file(record)

        index_record = IndexRecord(record.key, data_page_number)

        if self.root_page is None:
            root_node = self.create_root()
        else:
            root_node = self.filesHandler.get_index_page(self.root_page)
        try:
            index_record, child_pointer = self.insert_into_node(index_record, root_node)
            if index_record and child_pointer:
                self.create_root(index_record, child_pointer)

            self.filesHandler.flush_buffers()
            self.print_reads_and_writes()
        except ValueError:
            print("Record already exists!")

    def search(self, key: int) -> None:
        self.filesHandler.reset_io_counters()

        result = self.search_by_key(key, self.root_page)
        if result:
            print(f"Key found!")
        else:
            print("Key not found!")

        self.filesHandler.flush_buffers()
        self.print_reads_and_writes()

    def remove(self, key: int) -> None:
        self.filesHandler.reset_io_counters()

        if self.root_page is None:
            print("B-Tree is empty!")
            return

        root_node = self.filesHandler.get_index_page(self.root_page)
        data_page_number = self.remove_from_node(key, root_node)

        if data_page_number:
            self.filesHandler.remove_record_from_data_file(data_page_number, key)
            self.filesHandler.flush_buffers()
            self.print_reads_and_writes()
        else:
            print(f"No record with key {key}!")

    def update(self, old_key: int, record: DataRecord) -> None:
        self.remove(old_key)
        self.insert(record)

    def print(self, print_records: bool = False) -> None:
        if self.root_page is not None:
            self.filesHandler.reset_io_counters()

            root_node = self.filesHandler.get_index_page(self.root_page)
            self.visit_node(root_node, print_records)

            print()
            self.filesHandler.flush_buffers()
            self.print_reads_and_writes()
        else:
            print("B-Tree is empty!")

# private:
    def create_root(self, record: IndexRecord | None = None, new_child_pointer: int | None = None) -> IndexPage:
        self.h += 1
        previous_root = None
        if self.root_page is not None:
            previous_root = self.filesHandler.get_index_page(self.root_page)

        root_node = self.filesHandler.create_new_index_page()
        self.root_page = root_node.page_number

        if record and new_child_pointer and previous_root:
            root_node.add_record(0, record)
            root_node.add_pointer(0, previous_root.page_number)
            root_node.add_pointer(1, new_child_pointer)
            self.update_parent([previous_root.page_number, new_child_pointer], root_node.page_number)

        return root_node

    def insert_into_node(self, record: IndexRecord, node: IndexPage) -> (IndexRecord | None, int | None):
        i = self.find_position(node, record.key)

        if i < len(node.records) and node.get_key(i) == record.key:
            raise ValueError

        if not node.is_leaf():
            node_page_number = node.page_number

            record, new_child_pointer = self.insert_into_node(record, self.filesHandler.get_index_page(node.get_pointer(i)))

            node = self.filesHandler.get_index_page(node_page_number)

            if new_child_pointer:
                if len(node.records) < 2 * self.d:
                    node.add_record(i, record)
                    node.add_pointer(i + 1, new_child_pointer)
                else:
                    can_compensation = self.try_compensation(node, record, new_child_pointer)
                    if not can_compensation:
                        record, new_child_pointer = self.split(node, i, record, new_child_pointer)
                        return record, new_child_pointer
        else:
            if len(node.records) < 2 * self.d:
                node.add_record(i, record)
            else:
                can_compensation = self.try_compensation(node, record)
                if not can_compensation:
                    record, new_child_pointer = self.split(node, i, record)
                    return record, new_child_pointer

        return None, None

    @staticmethod
    def find_position(node: IndexPage, key: int) -> int:
        i = len(node.records) - 1
        while i >= 0 and key < node.get_key(i):
            i -= 1

        if 0 <= i < len(node.records) and node.get_key(i) == key:
            return i
        else:
            return i + 1

    def try_compensation(self, node: IndexPage, record: IndexRecord, pointer: int | None = None) -> bool:
        can_compensate = False
        if node.get_parent():
            parent_node = self.filesHandler.get_index_page(node.get_parent())
            index = parent_node.pointers.index(node.page_number)
            if index - 1 >= 0:
                left_neighbour = self.filesHandler.get_index_page(parent_node.get_pointer(index - 1))
                if len(left_neighbour.records) < 2 * self.d:
                    self.compensation(left_neighbour, node, parent_node, index - 1, record, pointer)
                    can_compensate = True
                else:
                    self.filesHandler.reduce_usage(left_neighbour)
            if index + 1 < len(parent_node.pointers) and not can_compensate:
                right_neighbour = self.filesHandler.get_index_page(parent_node.get_pointer(index + 1))
                if len(right_neighbour.records) < 2 * self.d:
                    self.compensation(node, right_neighbour, parent_node, index, record, pointer)
                    can_compensate = True
                else:
                    self.filesHandler.reduce_usage(right_neighbour)

        return can_compensate

    def compensation(self, left_child: IndexPage, right_child: IndexPage, parent: IndexPage, i: int, record: IndexRecord, pointer: int | None = None) -> None:
        # Pointer parameter is necessary only for non-leaf nodes.
        # This function is called after verifying that compensation is possible.

        # Take all records from the overflown page, all records from neighbour page and the corresponding record
        # from the parent page.
        records_distribution_list = left_child.get_records() + [parent.get_record(i)] + right_child.get_records()

        # Also add the new record to be added in the appropriate place in this list.
        j = len(records_distribution_list) - 1
        while j >= 0 and record.key < records_distribution_list[j].key:
            j -= 1
        j += 1

        records_distribution_list.insert(j, record)

        middle = len(records_distribution_list) // 2

        # Distribute these records equally to the two pages and replace the record taken from parent with the
        # middle record as to the value of all these records.
        left_child.set_records(records_distribution_list[0:middle])
        right_child.set_records(records_distribution_list[middle + 1:])
        parent.set_record(i, records_distribution_list[middle])

        # If the node is not a leaf, we also need to distribute its pointers.
        if left_child.is_leaf():
            return

        pointers_distribution_list = left_child.get_pointers() + right_child.get_pointers()
        pointers_distribution_list.insert(j + 1, pointer)
        left_child.set_pointers(pointers_distribution_list[0:middle + 1])
        right_child.set_pointers(pointers_distribution_list[middle + 1:])
        self.update_parent(pointers_distribution_list[0:middle + 1], left_child.page_number)
        self.update_parent(pointers_distribution_list[middle + 1:], right_child.page_number)

    def split(self, node: IndexPage, index: int, record: IndexRecord, pointer: int | None = None) -> (IndexRecord, int):
        new_node = self.filesHandler.create_new_index_page()

        middle = self.d

        node.add_record(index, record)
        record_for_parent = node.get_record(middle)

        new_node.set_records(node.get_records(middle+1))
        new_node.set_parent(node.get_parent())
        node.set_records(node.get_records(0, middle))

        if not node.is_leaf():
            node.add_pointer(index + 1, pointer)
            pointers = node.get_pointers(middle + 1)
            new_node.set_pointers(pointers)
            node.set_pointers(node.get_pointers(0, middle + 1))
            self.update_parent(pointers, new_node.page_number)

        return record_for_parent, new_node.page_number

    def update_parent(self, children_pointers: [int], parent_page: int) -> None:
        for child_page in children_pointers:
            child_node = self.filesHandler.get_index_page(child_page)
            child_node.set_parent(parent_page)

    def visit_node(self, node: IndexPage, print_records: bool = False) -> None:
        if not print_records:
            print("( ", end="")

        for i in range(len(node.records)):
            if not node.is_leaf():
                self.visit_node(self.filesHandler.get_index_page(node.get_pointer(i)), print_records)

            if not print_records:
                print(node.get_key(i), end=" ")
            else:
                data_page = self.filesHandler.get_data_page(node.get_data_page_number(i))
                data_page.print_record(node.get_key(i))

        if not node.is_leaf():
            self.visit_node(self.filesHandler.get_index_page(node.get_pointer(len(node.records))), print_records)

        if not print_records:
            print(") ", end="")

    def search_by_key(self, key: int, page: int) -> bool:
        if page is None or self.root_page is None:
            return False

        node = self.filesHandler.get_index_page(page)

        i = 0
        while i < len(node.records) and key > node.get_key(i):
            i += 1

        if i < len(node.records) and key == node.get_key(i):
            return True

        if node.is_leaf():
            return False

        return self.search_by_key(key, node.get_pointer(i))

    def remove_from_node(self, key: int, node: IndexPage) -> int | None:
        i = self.find_position(node, key)

        if node.is_leaf() and i < len(node.records) and node.get_key(i) == key:
            data_page_number = node.get_data_page_number(i)
            self.remove_from_leaf(node, i)
            return data_page_number
        elif not node.is_leaf() and i < len(node.records) and node.get_key(i) == key:
            data_page_number = node.get_data_page_number(i)
            self.remove_from_internal_node(node, i)
            return data_page_number
        elif node.is_leaf() and (i > len(node.records) or (i < len(node.records) and node.get_key(i) != key)):
            return None
        else:
            return self.remove_from_node(key, self.filesHandler.get_index_page(node.get_pointer(i)))

    def remove_from_leaf(self, node: IndexPage, i: int) -> None:
        node.remove_record(node.get_record(i))
        self.repair_node_after_removal(self.filesHandler.get_index_page(node.page_number))

    def repair_node_after_removal(self, node: IndexPage) -> None:
        if node.page_number != self.root_page and len(node.records) < self.d:
            can_compensate = self.try_compensation_for_remove(node)
            if not can_compensate:
                parent_node = self.filesHandler.get_index_page(node.get_parent())
                i = parent_node.pointers.index(node.page_number)
                if i + 1 < len(parent_node.pointers):
                    right_neighbour = self.filesHandler.get_index_page(parent_node.get_pointer(i + 1))
                    self.merge(node, right_neighbour, parent_node, i)
                elif i - 1 >= 0:
                    left_neighbour = self.filesHandler.get_index_page(parent_node.get_pointer(i - 1))
                    self.merge(left_neighbour, node, parent_node, i - 1)
                else:
                    raise ValueError("This exception should never occur!")
        elif node.page_number == self.root_page:
            if len(node.records) == 0:
                if not node.is_leaf():
                    self.root_page = node.get_pointer(0)
                    node.set_records([])
                    node.set_pointers([])
                    node.set_parent(None)
                    self.update_parent([self.root_page], None)
                else:
                    self.root_page = None

                self.h -= 1

    def try_compensation_for_remove(self, node: IndexPage) -> bool:
        can_compensate = False
        if not node.get_parent():
            return can_compensate

        parent_node = self.filesHandler.get_index_page(node.get_parent())
        index = parent_node.pointers.index(node.page_number)
        if index - 1 >= 0:
            left_neighbour = self.filesHandler.get_index_page(parent_node.get_pointer(index - 1))
            if len(left_neighbour.records) > self.d:
                self.compensate_with_left_neighbour(node, left_neighbour, parent_node, index - 1)
                can_compensate = True
            else:
                self.filesHandler.reduce_usage(left_neighbour)

        if not can_compensate and index + 1 < len(parent_node.pointers):
            right_neighbour = self.filesHandler.get_index_page(parent_node.get_pointer(index + 1))
            if len(right_neighbour.records) > self.d:
                self.compensate_with_right_neighbour(node, right_neighbour, parent_node, index)
                can_compensate = True
            else:
                self.filesHandler.reduce_usage(right_neighbour)

        return can_compensate

    def compensate_with_left_neighbour(self, node: IndexPage, neighbour: IndexPage, parent: IndexPage, i: int) -> None:
        node.add_record(0, parent.get_record(i))

        record = neighbour.get_record(-1)

        parent.set_record(i, record)
        neighbour.remove_record(record)

        if node.is_leaf():
            return

        pointer = neighbour.get_pointer(-1)
        node.add_pointer(0, pointer)
        neighbour.remove_pointer(pointer)
        self.update_parent([pointer], node.page_number)

    def compensate_with_right_neighbour(self, node: IndexPage, neighbour: IndexPage, parent: IndexPage, i: int) -> None:
        node.add_record(len(node.records), parent.get_record(i))

        record = neighbour.get_record(0)

        parent.set_record(i, record)
        neighbour.remove_record(record)

        if node.is_leaf():
            return

        pointer = neighbour.get_pointer(0)
        node.add_pointer(len(node.pointers), pointer)
        neighbour.remove_pointer(pointer)
        self.update_parent([pointer], node.page_number)

    def remove_from_internal_node(self, node: IndexPage, i: int) -> None:
        node_page_number = node.page_number

        left_child = self.filesHandler.get_index_page(node.get_pointer(i))
        if len(left_child.records) > self.d:
            leaf_node, predecessor = self.find_predecessor(left_child)
            node = self.filesHandler.get_index_page(node_page_number)
            node.set_record(i, predecessor)
            self.remove_from_node(predecessor.key, leaf_node)
            return

        self.filesHandler.reduce_usage(left_child)
        right_child = self.filesHandler.get_index_page(node.get_pointer(i + 1))
        if len(right_child.records) > self.d:
            leaf_node, successor = self.find_successor(right_child)
            node = self.filesHandler.get_index_page(node_page_number)
            node.set_record(i, successor)
            self.remove_from_node(successor.key, leaf_node)
        else:
            self.filesHandler.reduce_usage(right_child)
            left_child = self.filesHandler.get_index_page(node.get_pointer(i))
            leaf_node, predecessor = self.find_predecessor(left_child)
            node = self.filesHandler.get_index_page(node_page_number)
            node.set_record(i, predecessor)
            self.remove_from_node(predecessor.key, leaf_node)

    def find_predecessor(self, node: IndexPage) -> (IndexPage, IndexRecord):
        predecessor = node.get_record(-1)
        while not node.is_leaf():
            node = self.filesHandler.get_index_page(node.get_pointer(-1))
            predecessor = node.get_record(-1)

        return node, predecessor

    def find_successor(self, node: IndexPage) -> (IndexPage, IndexRecord):
        successor = node.get_record(0)
        while not node.is_leaf():
            node = self.filesHandler.get_index_page(node.get_pointer(0))
            successor = node.get_record(0)

        return node, successor

    def merge(self, node: IndexPage, neighbour: IndexPage, parent: IndexPage, i: int) -> None:
        neighbour_page_number = neighbour.page_number
        parent_page_number = parent.page_number

        record_from_parent = parent.get_record(i)
        node.set_records(node.get_records() + [record_from_parent] + neighbour.get_records())

        if not node.is_leaf():
            node.set_pointers(node.get_pointers() + neighbour.get_pointers())

        parent.remove_record(record_from_parent)
        parent.remove_pointer(neighbour.page_number)

        self.update_parent(neighbour.get_pointers(), node.page_number)

        neighbour = self.filesHandler.get_index_page(neighbour_page_number)
        neighbour.set_pointers([])
        neighbour.set_records([])
        neighbour.set_parent(None)

        self.repair_node_after_removal(self.filesHandler.get_index_page(parent_page_number))

    def print_reads_and_writes(self) -> None:
        index_writes, index_reads, data_writes, data_reads = self.filesHandler.get_reads_and_writes()
        print()
        print(f"\tIndex\treads: {index_reads}\twrites: {index_writes}")
        print(f"\tData\treads: {data_reads}\twrites: {data_writes}")
        print(f"\theight: {self.h}")
        print()
