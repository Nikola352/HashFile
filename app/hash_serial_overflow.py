from typing import Dict
import os
from app.binary_file import BinaryFile
from app.record import Record


class HashFileSerialOverflow(BinaryFile):
    def __init__(self, filename: str, record: Record, num_buckets: int, blocking_factor: int, empty_record: Dict, empty_key: int = -1):
        super().__init__(filename, record, blocking_factor, empty_record, empty_key)
        self.num_buckets = num_buckets

    def hash(self, id):
        return id % self.num_buckets

    def init_file(self):
        with open(self.filename, "wb") as f:
            for _ in range(self.num_buckets + 1): # +1 for serial overflow zone last record
                bucket = self.blocking_factor * [self.empty_record]
                self.write_block(f, bucket)

    def find_by_id(self, id):
        bucket_idx = self.hash(id)
        with open(self.filename, "rb") as f:
            f.seek(bucket_idx*self.block_size)
            bucket = self.read_block(f)
            for rec_idx, rec in enumerate(bucket):
                if rec.get('status') == 0:
                    return None
                if rec.get('id') == id:
                    if rec.get('status') == 1:
                        return bucket_idx, rec_idx
                    return None
        return self.__find_in_overflow(id)
    
    def __find_in_overflow(self, id):
        with open(self.filename, "rb") as f:
            f.seek(self.num_buckets * self.block_size)
            i = 0
            bucket = self.read_block(f)
            while bucket:
                for j, rec in enumerate(bucket):
                    if rec.get('id') == self.empty_key:
                        return None
                    if rec.get('id') == id:
                        return self.num_buckets+i, j
                bucket = self.read_block(f)
        return None

    def insert_record(self, record) -> bool:
        id = record.get('id')
        bucket_idx = self.hash(id)

        find_res = self.find_by_id(id)
        if find_res is not None:
            return False

        record['status'] = 1

        with open(self.filename, "+rb") as f:
            f.seek(bucket_idx * self.block_size)
            bucket = self.read_block(f)
            for i, rec in enumerate(bucket[:]):
                if rec.get('status') != 1:
                    bucket[i] = record
                    f.seek(-self.block_size, 1)
                    self.write_block(f, bucket)
                    return True
        
            # overflow zone
            f.seek(-self.block_size, 2)
            bucket = self.read_block(f)
            for i, rec in enumerate(bucket[:]):
                if rec.get('id') == self.empty_key:
                    bucket[i] = record
                    f.seek(-self.block_size, 1)
                    self.write_block(f, bucket)
                    if i == self.blocking_factor:
                        self.write_block(self.blocking_factor*[self.empty_record])
                    return True
                    
    def update_record(self, record):
        id = record.get('id')
        find_res = self.find_by_id(id)
        if find_res is None:
            return False
        record['status'] = 1
        block_idx, rec_idx = find_res
        with open(self.filename, "r+b") as f:
            f.seek(self.block_size*block_idx)
            block = self.read_block(f)
            block[rec_idx] = record
            f.seek(-self.block_size, 1)
            self.write_block(f, block)
                    
    def delete_by_id(self, id) -> bool:
        find_res = self.find_by_id(id)
        if find_res is None:
            return False
        block_idx, rec_idx = find_res

        with open(self.filename, "rb+") as f:
            f.seek(block_idx * self.block_size)
            if block_idx < self.num_buckets:
                # primary zone
                bucket = self.read_block(f)
                bucket[rec_idx]['status'] = 2
                f.seek(-self.block_size, 1)
                self.write_block(f, bucket)
            else:
                # overflow zone
                block = self.read_block(f)
                while block:
                    for i in range(rec_idx, self.blocking_factor-1):
                        block[i] = block[i+1]
                        if block[i].get('id') == self.empty_key:
                            break
                    next_block = self.read_block(f)
                    if next_block:
                        block[-1] = next_block[0]
                        if block[-1].get('id') == self.empty_key:
                            os.ftruncate(f.fileno(), block_idx*self.block_size)

                    f.seek(block_idx*self.block_size)
                    self.write_block(f, block)
                    f.seek(self.block_size, 1)

                    block = next_block
                    block_idx += 1
                    rec_idx = 0
        return True
    
    def print_file(self):
        with open(self.filename, "rb") as f:
            for i in range(self.num_buckets):
                print(f"BUCKET {i+1}:")
                bucket = self.read_block(f)
                for j, rec in enumerate(bucket):
                    print(f"Record {j}:\t{rec}")
            print("\nOverflow zone:")
            i = 0
            while True:
                block = self.read_block(f)
                if not block:
                    break
                i += 1
                print(f"BLOCK {i}:")
                for j, rec in enumerate(block):
                    print(f"Record {j}:\t{rec}")
            print()