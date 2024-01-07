from DataRecord import DataRecord


class DataPage:
    next_page: int = 1
    max_size: int = 0

    def __init__(self, records_per_page: int, page_number: int | None = None) -> None:
        self.records_per_page: int = records_per_page
        DataPage.max_size = records_per_page * DataRecord.max_size
        self.records: [DataRecord] = []
        self.dirty_bit: bool = False

        if page_number is None:
            self.page_number: int = DataPage.next_page
            DataPage.next_page += 1
        else:
            self.page_number: int = page_number

    def add_record(self, record: DataRecord) -> None:
        self.records.append(record)
        self.dirty_bit = True

    def is_full(self) -> bool:
        return len(self.records) == self.records_per_page

    def serialize(self) -> [bytes]:
        result = []

        for record in self.records:
            result += record.serialize()

        for _ in range(self.records_per_page - len(self.records)):
            result += DataRecord.get_empty_record_bytes()

        return result

    def print_record(self, key: int) -> None:
        for record in self.records:
            if record.key == key:
                print(record)
                break

    def remove_record(self, key: int) -> None:
        for record in self.records:
            if record.key == key:
                self.records.remove(record)
                self.dirty_bit = True
                return

    def is_dirty(self) -> bool:
        return self.dirty_bit
