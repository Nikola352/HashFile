#!/usr/bin/python

from app.hash_serial_overflow import HashFileSerialOverflow
from app.hash_linked_overflow import HashFileLinkedOverflow
from app.hash_linear import HashFileLinear
from app.record import Record
from app.constants import *

def main():
    file = HashFileLinear(FILENAME, Record(ATTRIBUTES, FMT, CODING), B, b, EMPTY_REC, EMPTY_KEY)
    file.init_file()

    file.insert_record({'id': 1, 'number': 1, 'string': 'prvi'})

    file.insert_record({'id': 8, 'number': 2, 'string': 'drugi'})
    file.insert_record({'id': 15, 'number': 3, 'string': 'treci'})
    file.insert_record({'id': 22, 'number': 4, 'string': 'cetvrti'})
    file.insert_record({'id': 5, 'number': 5, 'string': 'peti'})
    file.insert_record({'id': 29, 'number': 6, 'string': 'sesti'})

    file.update_record({'id': 15, 'number': 3, 'string': 'TRECI ALO'})
    file.update_record({'id': 29, 'number': 1000000, 'string': 'kraj'})

    # file.logical_delete_by_id(8)
    file.delete_by_id(22)

    file.print_file()

if __name__ == "__main__":
    main()
