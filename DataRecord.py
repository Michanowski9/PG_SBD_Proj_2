import random
import string


class DataRecord:
    null_byte_key = 2147483647
    null_byte_data = "."
    max_length = 30
    max_size = 4 + max_length    # key + length * elem
    int_size = 4
    byte_order = "big"

    def __init__(self, key: int, data: str) -> None:
        self.key: int = key
        self.data: str = data

    def __repr__(self) -> str:
        return f"{self.key}: \"" + self.data + "\""

    def serialize(self) -> [bytes]:
        result = [self.key.to_bytes(DataRecord.int_size, DataRecord.byte_order)]

        for elem in self.data:
            result.append(elem.encode("utf-8"))

        if len(self.data) < DataRecord.max_length:
            for _ in range(DataRecord.max_length - len(self.data)):
                result.append(DataRecord.null_byte_data.encode("utf-8"))
        return result

    @staticmethod
    def deserialize(file):
        key = int.from_bytes(file.read(DataRecord.int_size), DataRecord.byte_order)
        if not key:
            return None

        data = []
        for _ in range(DataRecord.max_length):
            char = file.read(1).decode("utf-8")
            if char != DataRecord.null_byte_data:
                data.append(char)

        return DataRecord(key, "".join(data))

    @staticmethod
    def get_empty_record_bytes() -> [bytes]:
        result = [DataRecord.null_byte_key.to_bytes(DataRecord.int_size, DataRecord.byte_order)]
        for _ in range(DataRecord.max_length):  # record
            result.append(DataRecord.null_byte_data.encode("utf-8"))
        return result


def generate_random_record_data(key) -> DataRecord:
    length = random.randint(1, DataRecord.max_length)
    data = ''.join(random.choice(string.ascii_lowercase) for _ in range(length))

    print("\tGenerated: " + str(key) + " \"" + data + "\"")

    return DataRecord(key, data)



