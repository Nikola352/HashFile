from typing import Dict, Tuple
from app.binary_file import BinaryFile
from app.record import Record


class HashFileLinear(BinaryFile):
    def __init__(self, filename: str, record: Record, num_buckets: int, blocking_factor: int, empty_record: Dict, empty_key: int=-1, step:int=1):
        super().__init__(filename, record, blocking_factor, empty_record, empty_key)
        self.num_buckets = num_buckets
        self.step = step

    def hash(self, id: int) -> int:
        return id % self.num_buckets
    
    def init_file(self):
        with open(self.filename, "wb") as f:
            for _ in range(self.num_buckets):
                bucket = self.blocking_factor * [self.empty_record]
                self.write_block(f, bucket)

    def find_by_id(self, id) -> Tuple[bool, int, int]:
        bucket_idx = self.hash(id)
        curr_idx = bucket_idx
        with open(self.filename, "rb") as f:
            while True:
                f.seek(curr_idx * self.block_size)
                bucket = self.read_block(f)
                for rec_idx, rec in enumerate(bucket):
                    if rec.get('status') == 0:
                        return False, curr_idx, rec_idx
                    if rec.get('id') == id:
                        if rec.get('status') == 1:
                            return True, curr_idx, rec_idx
                        return False, curr_idx, rec_idx
                curr_idx = (curr_idx + self.step) % self.num_buckets
                if curr_idx == bucket_idx:
                    break
        return False, bucket_idx, self.blocking_factor
    
    def insert_record(self, record: Dict) -> bool:
        id = record.get('id')
        found, bucket_idx, rec_idx = self.find_by_id(id)
        if found:
            return False
        if rec_idx == self.blocking_factor: # completely filled file
            return False
        record['status'] = 1
        with open(self.filename, "+rb") as f:
            f.seek(bucket_idx * self.block_size)
            bucket = self.read_block(f)
            bucket[rec_idx] = record
            f.seek(-self.block_size, 1)
            self.write_block(f, bucket)
        return True
    
    def update_record(self, record: Dict) -> bool:
        id = record.get('id')
        found, bucket_idx, rec_idx = self.find_by_id(id)
        if not found:
            return False
        record['status'] = 1
        with open(self.filename, "+rb") as f:
            f.seek(bucket_idx * self.block_size)
            bucket = self.read_block(f)
            bucket[rec_idx] = record
            f.seek(-self.block_size, 1)
            self.write_block(f, bucket)
        return True
    
    def logical_delete_by_id(self, id: int) -> bool:
        found, bucket_idx, rec_idx = self.find_by_id(id)
        if not found:
            return False
        with open(self.filename, "+rb") as f:
            f.seek(bucket_idx * self.block_size)
            bucket = self.read_block(f)
            bucket[rec_idx]['status'] = 2
            f.seek(-self.block_size, 1)
            self.write_block(f, bucket)
        return True
    
    def delete_by_id(self, id: int) -> bool:
        found, block_idx, rec_idx = self.find_by_id(id)
        if not found:
            return False
        
        with open(self.filename, "rb+") as f:
            curr_blk_idx, curr_rec_idx = block_idx, rec_idx
            done = False
            while not done:
                f.seek(curr_blk_idx * self.block_size)
                block = self.read_block(f)
                block[curr_rec_idx] = self.empty_record
                for i in range(curr_rec_idx, self.blocking_factor-1):
                    block[i] = block[i+1]
                    if block[i].get('id') == self.empty_key:
                        done = True
                block[-1] = self.empty_record
                if done:
                    break
                seen_blocks = set()
                seen_blocks.add(curr_blk_idx)
                next_idx = (curr_blk_idx + self.step) % self.num_buckets
                while next_idx != block_idx:
                    seen_blocks.add(next_idx)
                    f.seek(next_idx * self.block_size)
                    next_block = self.read_block(f)
                    next_rec_idx = None
                    for i, rec in enumerate(next_block):
                        if rec.get('status') == 0:
                            done = True
                            break
                        if self.hash(rec.get('id')) not in seen_blocks:
                            next_rec_idx = i
                            break
                    if done:
                        break
                    if next_rec_idx != None:
                        block[-1] = next_block[next_rec_idx]
                        f.seek(curr_blk_idx * self.block_size)
                        self.write_block(f, block)
                        curr_blk_idx = next_idx
                        curr_rec_idx = next_rec_idx
                        break
                    next_idx = (next_idx + self.step) % self.num_buckets
                if next_idx == block_idx:
                    break
            f.seek(curr_blk_idx * self.block_size)
            self.write_block(f, block)
    
    def print_file(self):
        with open(self.filename, "rb") as f:
            for i in range(self.num_buckets):
                print(f"BUCKET {i+1}:")
                bucket = self.read_block(f)
                for j, rec in enumerate(bucket):
                    print(f"Record {j}:\t{rec}")
                print()