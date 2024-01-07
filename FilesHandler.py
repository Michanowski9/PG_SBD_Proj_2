import os.path

from DataRecord import DataRecord
from DataPage import DataPage
from IndexPage import IndexPage, IndexRecord


class FilesHandler:
    index_buffer_size = 3
    data_buffer_size = 3

    def __init__(self, records_per_page: int, index_filename: str = "data/index.txt", data_filename: str = "data/data.txt") -> None:
        self.index_filename: str = index_filename
        self.data_filename: str = data_filename

        self.records_per_page: int = records_per_page
        self.last_data_page_number: int | None = None

        self.index_empty_pages = []
        self.data_non_full_pages = []

        self.index_reads: int = 0
        self.index_writes: int = 0
        self.data_reads: int = 0
        self.data_writes: int = 0
        self.index_buffer: [IndexPage] = []
        self.data_buffer: [DataPage] = []

        self.clean_files()

# public:
    def add_record_to_data_file(self, record: DataRecord) -> int:
        self.reset_io_counters()
        if self.last_data_page_number is None or self.get_data_page(self.last_data_page_number).is_full():
            last_data_page = self.create_new_data_page()
            self.last_data_page_number = last_data_page.page_number
        else:
            last_data_page = self.get_data_page(self.last_data_page_number)

        last_data_page.add_record(record)

        return self.last_data_page_number

    def create_new_index_page(self) -> IndexPage:
        if self.index_empty_pages:
            page_number = self.index_empty_pages[0]
            page = self.get_index_page(page_number)
            self.index_empty_pages.remove(page_number)
        else:
            page = IndexPage(self.records_per_page)

        self.add_index_page_to_buffer(page)
        return page

    def flush_buffers(self):
        self.flush_index_buffer()
        self.flush_data_buffer()

    def get_data_page(self, page_number: int) -> DataPage:
        for data_page in self.data_buffer:
            if data_page.page_number == page_number:
                self.move_to_the_beginning(self.data_buffer, data_page)
                return data_page

        return self.load_data_page(page_number)

    def get_index_page(self, page_number: int) -> IndexPage:
        for index_page in self.index_buffer:
            if index_page.page_number == page_number:
                self.move_to_the_beginning(self.index_buffer, index_page)
                return index_page

        return self.load_index_page(page_number)

    def get_reads_and_writes(self) -> (int, int, int, int):
        return self.index_writes, self.index_reads, self.data_writes, self.data_reads

    def print_index_file(self):
        print("Index file:")

        read_bytes = 0
        page_number = 1
        with open(self.index_filename, "rb") as file:
            while read_bytes < os.path.getsize(self.index_filename):
                page_bytes = 0
                print(f"Page {page_number}:\t", end=" ")
                while page_bytes < IndexPage.max_size:
                    num = int.from_bytes(file.read(DataRecord.int_size), DataRecord.byte_order)
                    if num == DataRecord.null_byte_key:
                        print(".", end=" ")
                    else:
                        print(num, end=" ")
                    page_bytes += DataRecord.int_size
                    read_bytes += DataRecord.int_size
                print()
                page_number += 1

    def print_data_file(self):
        print("Data file:")

        page_number = 1
        with open(self.data_filename, "rb") as input_file:
            record = DataRecord.deserialize(input_file)

            while record:
                print(f"Page {page_number}:\t", end="")
                page_number += 1

                for _ in range(self.records_per_page):
                    print(" ", end="")
                    if record.key != DataRecord.null_byte_key:
                        print(record.key, end=" ")
                    else:
                        print("_", end=" ")

                    for elem in record.data:
                        print(elem, end="")

                    if len(record.data) < DataRecord.max_length:
                        for _ in range(DataRecord.max_length - len(record.data)):
                            print(".", end="")

                    record = DataRecord.deserialize(input_file)
                print()
        print()

    def reduce_usage(self, index_page: IndexPage) -> None:
        self.index_buffer.remove(index_page)
        self.index_buffer.append(index_page)

    def remove_record_from_data_file(self, data_page_number: int, key: int) -> None:
        data_page = self.get_data_page(data_page_number)
        data_page.remove_record(key)

        if data_page_number != self.last_data_page_number and data_page_number not in self.data_non_full_pages:
            self.data_non_full_pages.append(data_page_number)

    def reset_io_counters(self) -> None:
        self.index_reads = 0
        self.index_writes = 0

        self.data_reads = 0
        self.data_writes = 0

        self.index_buffer = []
        self.data_buffer = []

# private
    def add_data_page_to_buffer(self, data_page: DataPage) -> None:
        if len(self.data_buffer) >= FilesHandler.data_buffer_size:
            lru_page = self.data_buffer.pop()
            self.save_data_page(lru_page)
        self.data_buffer.insert(0, data_page)

    def add_index_page_to_buffer(self, index_page: IndexPage) -> None:
        if len(self.index_buffer) >= self.index_buffer_size:
            lru_page = self.index_buffer.pop()
            self.save_index_page(lru_page)
        self.index_buffer.insert(0, index_page)

    def clean_files(self) -> None:
        open(self.index_filename, "w").close()
        open(self.data_filename, "w").close()

    def create_new_data_page(self) -> DataPage:
        # If there is any page that is not full, then use it.
        if self.data_non_full_pages:
            page_number = self.data_non_full_pages[0]
            page = self.get_data_page(page_number)  # page was added to buffer
            self.data_non_full_pages.remove(page_number)

        # Otherwise, create the new page.
        else:
            page = DataPage(self.records_per_page)
            self.add_data_page_to_buffer(page)

        return page

    def flush_data_buffer(self) -> None:
        for data_page in self.data_buffer:
            self.save_data_page(data_page)
        self.data_buffer = []

    def flush_index_buffer(self) -> None:
        for index_page in self.index_buffer:
            self.save_index_page(index_page)

        self.index_buffer = []

    def load_data_page(self, page_number: int = 1) -> DataPage:
        data_page = DataPage(self.records_per_page, page_number)
        with open(self.data_filename, "rb") as file:
            file.seek((page_number - 1) * data_page.max_size)

            for _ in range(self.records_per_page):
                record = DataRecord.deserialize(file)

                if record.key != DataRecord.null_byte_key:
                    data_page.records.append(record)

        self.add_data_page_to_buffer(data_page)
        self.data_reads += 1
        return data_page

    def load_index_page(self, page_number: int = 1) -> IndexPage:  # == load BTreeNode
        int_size = 4
        byte_order = "big"

        index_page = IndexPage(self.records_per_page, page_number)
        with open(self.index_filename, "rb") as file:
            file.seek((page_number - 1) * index_page.max_size)

            read_bytes = 0
            read_counter = 0
            while read_bytes < index_page.max_size - int_size:
                number = int.from_bytes(file.read(int_size), byte_order)
                if read_counter % 3 == 0:
                    if number != DataRecord.null_byte_key:
                        index_page.pointers.append(number)
                    read_bytes += int_size
                    read_counter += 1
                else:
                    key = number
                    page = int.from_bytes(file.read(int_size), byte_order)
                    if key != DataRecord.null_byte_key or page != DataRecord.null_byte_key:
                        index_page.records.append(IndexRecord(key, page))

                    read_bytes += 2 * int_size
                    read_counter += 2

            parent_page = int.from_bytes(file.read(int_size), byte_order)
            if parent_page != DataRecord.null_byte_key:
                index_page.parent_page = parent_page

        self.add_index_page_to_buffer(index_page)
        self.index_reads += 1
        return index_page

    @staticmethod
    def move_to_the_beginning(buffer, page: IndexPage | DataPage) -> None:
        buffer.remove(page)
        buffer.insert(0, page)

    def save_data_page(self, data_page: DataPage) -> None:
        if data_page.is_dirty():
            with open(self.data_filename, "rb+") as file:
                file.seek((data_page.page_number - 1) * data_page.max_size)
                serialized_entries = data_page.serialize()
                for entry in serialized_entries:
                    file.write(entry)
            self.data_writes += 1

    def save_index_page(self, index_page: IndexPage) -> None:
        if not index_page.is_dirty():
            return

        if index_page.is_empty() and index_page.page_number not in self.index_empty_pages:
            self.index_empty_pages.append(index_page.page_number)

        with open(self.index_filename, "rb+") as file:
            # set pointer in file
            file.seek((index_page.page_number - 1) * index_page.max_size)

            # save to file
            for entry in index_page.serialize():
                file.write(entry)

        self.index_writes += 1
