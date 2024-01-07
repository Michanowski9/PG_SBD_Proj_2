from DataPage import DataRecord


class IndexRecord:
    def __init__(self, key: int, data_page_number: int):
        self.key: int = key
        self.data_page_number: int = data_page_number


class IndexPage:
    next_page: int = 1
    max_size: int = 0

    def __init__(self, records_per_page: int, page_number: int | None = None) -> None:
        INT_SIZE = 4
        self.records_per_page: int = records_per_page
        IndexPage.max_size = records_per_page * (3 * INT_SIZE) + INT_SIZE + INT_SIZE
        self.current_size: int = 0

        if page_number is None:
            self.page_number: int = IndexPage.next_page
            IndexPage.next_page += 1
        else:
            self.page_number: int = page_number

        self.records: [IndexRecord] = []
        self.pointers: [int] = []
        self.parent_page: int | None = None
        self.dirty_bit: bool = False

    def add_record(self, position: int, record: IndexRecord):
        self.records.insert(position, record)
        self.dirty_bit = True

    def add_pointer(self, position: int, page_pointer: int) -> None:
        self.pointers.insert(position, page_pointer)
        self.dirty_bit = True

    def get_key(self, record_number: int) -> int:
        return self.records[record_number].key

    def get_data_page_number(self, record_number: int) -> int:
        return self.records[record_number].data_page_number

    def get_record(self, record_number: int) -> IndexRecord:
        return self.records[record_number]

    def get_records(self, index_from: int | None = None, index_to: int | None = None) -> [IndexRecord]:
        if not index_from and not index_to:
            return self.records
        if index_from and index_to:
            return self.records[index_from:index_to]
        if index_from and not index_to:
            return self.records[index_from:]

        return self.records[:index_to]

    def get_pointer(self, pointer_number: int) -> int:
        return self.pointers[pointer_number]

    def get_pointers(self, index_from: int | None = None, index_to: int | None = None) -> [int]:
        if not index_from and not index_to:
            return self.pointers
        if index_from and index_to:
            return self.pointers[index_from:index_to]
        if index_from and not index_to:
            return self.pointers[index_from:]

        return self.pointers[:index_to]

    def get_parent(self) -> int:
        return self.parent_page

    def set_record(self, record_number: int, new_record: IndexRecord) -> None:
        self.records[record_number] = new_record
        self.dirty_bit = True

    def set_records(self, new_records: [IndexRecord]) -> None:
        self.records = new_records
        self.dirty_bit = True

    def set_pointers(self, new_pointers: [int]) -> None:
        self.pointers = new_pointers
        self.dirty_bit = True

    def set_parent(self, new_parent_page: int | None) -> None:
        self.parent_page = new_parent_page
        self.dirty_bit = True

    def remove_record(self, record: IndexRecord) -> None:
        self.records.remove(record)
        self.dirty_bit = True

    def remove_pointer(self, pointer: int) -> None:
        self.pointers.remove(pointer)
        self.dirty_bit = True

    def serialize(self) -> [bytes]:
        int_size = 4
        byte_order = "big"

        result = []

        if self.pointers:
            result.append(self.pointers[0].to_bytes(int_size, byte_order))
        else:
            result.append(DataRecord.null_byte_key.to_bytes(int_size, byte_order))

        for index, record in enumerate(self.records):
            result.append(record.key.to_bytes(int_size, byte_order))
            result.append(record.data_page_number.to_bytes(int_size, byte_order))

            if self.pointers:
                result.append(self.pointers[index + 1].to_bytes(int_size, byte_order))
            else:
                result.append(DataRecord.null_byte_key.to_bytes(int_size, byte_order))

        for _ in range(self.records_per_page - len(self.records)):
            for _ in range(3):
                result.append(DataRecord.null_byte_key.to_bytes(int_size, byte_order))

        if self.parent_page:
            result.append(self.parent_page.to_bytes(int_size, byte_order))
        else:
            result.append(DataRecord.null_byte_key.to_bytes(int_size, byte_order))

        return result

    def is_leaf(self) -> bool:
        for pointer in self.pointers:
            if pointer is not None:
                return False
        return True

    def is_dirty(self) -> bool:
        return self.dirty_bit

    def is_empty(self) -> bool:
        if len(self.records) == 0 and len(self.pointers) == 0 and self.parent_page is None:
            return True
        return False
