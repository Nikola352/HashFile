from typing import Dict, BinaryIO, Tuple, Union
from app.binary_file import BinaryFile
from app.record import Record
import struct

class Bucket:
    def __init__(self, header: Dict, block: list[Dict]):
        self.header = header
        self.block = block

class HashFileLinkedOverflow(BinaryFile):
    def __init__(self, filename: str, record: Record, num_buckets: int, blocking_factor: int, empty_record: Dict, empty_key: int = -1):
        super().__init__(filename, record, blocking_factor, empty_record, empty_key)
        self.num_buckets = num_buckets
        self.header_record = Record(['u'], 'i', 'ascii')
        self.header_record_size = struct.calcsize(self.header_record.format)
        self.primary_bucket_size = self.header_record_size + self.block_size
        self.overflow_bucket_size = self.header_record_size + self.record_size # use blocking factor 1 for overflow zone

    def hash(self, id):
        return id % self.num_buckets

    def _write_header(self, file: BinaryIO, header: Dict):
        binary_data = self.header_record.dict_to_encoded_values(header)
        file.write(binary_data)

    def _read_header(self, file: BinaryIO) -> Dict:
        binary_data = file.read(self.header_record_size)
        if len(binary_data) == 0:
            return None
        return self.header_record.encoded_tuple_to_dict(binary_data)

    def _write_bucket(self, file: BinaryIO, bucket: Bucket):
        self._write_header(file, bucket.header)
        self.write_block(file, bucket.block)

    def _read_primary_bucket(self, file: BinaryIO) -> Bucket:
        header = self._read_header(file)
        if header is None:
            return None
        block = self.read_block(file)
        return Bucket(header, block)
    
    def _read_overflow_bucket(self, file: BinaryIO) -> Bucket:
        header = self._read_header(file)
        if header is None:
            return None
        binary_data = file.read(self.record_size)
        record = self.record.encoded_tuple_to_dict(binary_data)
        return Bucket(header, [record])
    
    def __calc_overflow_bucket_position(self, bucket_idx: int) -> int:
        return self.num_buckets*self.primary_bucket_size + self.header_record_size + (bucket_idx-self.num_buckets)*self.overflow_bucket_size

    def init_file(self):
        with open(self.filename, "wb") as f:
            # primary zone
            for _ in range(self.num_buckets):
                bucket = Bucket({'u':-1}, self.blocking_factor*[self.empty_record])
                self._write_bucket(f, bucket)
            # overflow zone
            self._write_header(f, {'u': -1}) # help struct E (pointer to first free location in overflow zone)

    def find_by_id(self, id) -> Union[Tuple[int, int], None]:
        bucket_idx = self.hash(id)
        with open(self.filename, "rb") as f:
            # primary bucket
            f.seek(bucket_idx * self.primary_bucket_size)
            bucket = self._read_primary_bucket(f)
            for rec_idx, rec in enumerate(bucket.block):
                if rec.get('id') == id and rec.get('status') == 1:
                    return bucket_idx, rec_idx
                if rec.get('id') == self.empty_key:
                    return None
            # overflow zone
            while True:
                bucket_idx = bucket.header.get('u')
                if bucket_idx == -1:
                    break
                f.seek(self.__calc_overflow_bucket_position(bucket_idx))
                bucket = self._read_overflow_bucket(f)
                if bucket.block[0].get('id') == id:
                    return bucket_idx, 0
                
    def insert_record(self, record) -> bool:
        id = record.get('id')
        find_res = self.find_by_id(id)
        if find_res is not None:
            return False
        bucket_idx = self.hash(id)
        record['status'] = 1

        with open(self.filename, "rb+") as f:
            # primary zone
            f.seek(bucket_idx * self.primary_bucket_size)
            bucket = self._read_primary_bucket(f)
            for rec_idx, rec in enumerate(bucket.block[:]):
                if rec.get('id') == self.empty_key:
                    bucket.block[rec_idx] = record
                    f.seek(bucket_idx * self.primary_bucket_size)
                    self._write_bucket(f, bucket)
                    return True

            # overflow zone
            f.seek(self.num_buckets * self.primary_bucket_size)
            overflow_header = self._read_header(f)
            if overflow_header['u'] == -1:
                # no free buckets in overflow zone: add new bucket
                f.seek(0, 2) # end of file

                pos_in_overflow = f.tell() - self.num_buckets*self.primary_bucket_size - self.header_record_size
                new_idx = self.num_buckets + pos_in_overflow // self.overflow_bucket_size

                new_bucket = Bucket({'u': bucket.header['u']}, [record])
                self._write_bucket(f, new_bucket)

                bucket.header['u'] = new_idx
            else:
                # add to first free bucket in overflow zone
                new_idx = overflow_header['u']
                f.seek((new_idx-self.num_buckets) * self.overflow_bucket_size, 1)
                new_bucket = self._read_overflow_bucket(f)

                overflow_header['u'] = new_bucket.header.get('u')

                new_bucket.header['u'] = bucket.header['u']
                new_bucket.block[0] = record
                f.seek(-self.overflow_bucket_size, 1)
                self._write_bucket(f, new_bucket)

                f.seek(self.num_buckets * self.primary_bucket_size)
                self._write_header(f, overflow_header)

                bucket.header['u'] = new_idx

            f.seek(bucket_idx * self.primary_bucket_size)
            self._write_bucket(f, bucket)
            return True
        
    def update_record(self, record) -> bool:
        id = record.get('id')
        find_res = self.find_by_id(id)
        if find_res is None:
            return False
        bucket_idx, rec_idx = find_res
        record['status'] = 1
        with open(self.filename, "rb+") as f:
            if bucket_idx < self.num_buckets: # primary
                f.seek(bucket_idx * self.primary_bucket_size)
                bucket = self._read_primary_bucket(f)
                bucket.block[rec_idx] = record
                f.seek(-self.primary_bucket_size, 1)
                self._write_bucket(f, bucket)
            else: # overflow
                f.seek(self.__calc_overflow_bucket_position(bucket_idx))
                bucket = self._read_overflow_bucket(f)
                bucket.block[0] = record
                f.seek(-self.overflow_bucket_size, 1)
                self._write_bucket(f, bucket)
        return True
    
    def __delete_primary(self, bucket_idx: int, rec_idx: int):
        with open(self.filename, "rb+") as f:
            f.seek(bucket_idx * self.primary_bucket_size)
            bucket = self._read_primary_bucket(f)
            for i, rec in enumerate(bucket.block[rec_idx+1:]):
                bucket.block[rec_idx+i] = bucket.block[rec_idx+i+1]
                if rec.get('id') == self.empty_key:
                    break
            overflow_bucket_idx = bucket.header.get('u')
            if overflow_bucket_idx == -1: # no overflow records
                bucket.block[-1] = self.empty_record
            else:
                f.seek(self.__calc_overflow_bucket_position(overflow_bucket_idx))
                overflow_bucket = self._read_overflow_bucket(f)
                f.seek(self.num_buckets * self.primary_bucket_size)
                overflow_header = self._read_header(f)

                bucket.header['u'] = overflow_bucket.header.get('u')
                overflow_bucket.header['u'] = overflow_header.get('u')
                overflow_header['u'] = overflow_bucket_idx

                bucket.block[-1] = overflow_bucket.block[0]
                overflow_bucket.block[0] = self.empty_record

                f.seek(self.__calc_overflow_bucket_position(overflow_bucket_idx))
                self._write_bucket(f, overflow_bucket)
                f.seek(self.num_buckets * self.primary_bucket_size)
                self._write_header(f, overflow_header)
            f.seek(bucket_idx * self.primary_bucket_size)
            self._write_bucket(f, bucket)

    def __delete_overflow(self, id, bucket_idx):
        with open(self.filename, "rb+") as f:
            f.seek(self.hash(id) * self.primary_bucket_size)
            primary_bucket = self._read_primary_bucket(f)
            f.seek(self.__calc_overflow_bucket_position(bucket_idx))
            bucket = self._read_overflow_bucket(f)
            f.seek(self.num_buckets * self.primary_bucket_size)
            overflow_header = self._read_header(f)
            first_idx = primary_bucket.header.get('u')
            f.seek(self.__calc_overflow_bucket_position(first_idx))
            first_bucket = self._read_overflow_bucket(f)

            # swap bucket contents with first bucket contents and remove first bucket
            bucket.block[0] = first_bucket.block[0]
            first_bucket.block[0] = self.empty_record

            primary_bucket.header['u'] = first_bucket.header.get('u')
            first_bucket.header['u'] = overflow_header.get('u')
            overflow_header['u'] = first_idx

            f.seek(self.hash(id) * self.primary_bucket_size)
            self._write_bucket(f, primary_bucket)
            f.seek(self.__calc_overflow_bucket_position(bucket_idx))
            self._write_bucket(f, bucket)
            f.seek(self.num_buckets * self.primary_bucket_size)
            self._write_header(f, overflow_header)
            f.seek(self.__calc_overflow_bucket_position(first_idx))
            self._write_bucket(f, first_bucket)

    def delete_by_id(self, id) -> bool:
        find_res = self.find_by_id(id)
        if find_res is None:
            return False
        bucket_idx, rec_idx = find_res
        if bucket_idx < self.num_buckets:
            self.__delete_primary(bucket_idx, rec_idx)
        else: 
            self.__delete_overflow(id, bucket_idx)
        return True
            
    def print_file(self):
        with open(self.filename, "rb") as f:
            print("Primary zone:")
            for i in range(self.num_buckets):
                bucket = self._read_primary_bucket(f)
                print(f"\nBUCKET {i+1}:")
                print(f"Header: {bucket.header.get('u')+1}")
                for j, rec in enumerate(bucket.block):
                    print(f"Record {j}:\t{rec}")
            print("\nOverflow zone:")
            overflow_header = self._read_header(f)
            print(f"Overflow zone header: {overflow_header.get('u')+1}\n")
            i = self.num_buckets
            while True:
                bucket = self._read_overflow_bucket(f)
                if bucket is None:
                    break
                i += 1
                print(f"BLOCK {i}:")
                print(f"Record:\t{bucket.block[0]}")
                print(f"Link: {bucket.header.get('u')+1}\n")
            print()