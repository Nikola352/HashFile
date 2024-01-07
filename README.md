# HashFile
Python implementation of utility classes used for managing different types of hash files.

Example usage is demonstrated in `main.py`.

Try modifying values in `constants.py` to try different file structures.

The hash function used for every file type is division remainder.

# Hash File Types
Each of the supported file types has its own management class. Every class inherits BinaryFile which is a class that provides general binary file utilities such as file initialization, reading or writing blocks of records etc.

The parameters for constructing any hash file are:
- `filename`: path to the file on the disk
- `record`: an instance of `Record` class that defines the structure of a record
- `num_buckets`: number of buckets in the file
- `blocking_factor`: number of records in a bucket (block)
- `empty_record`: a dictionary that defines the structure of an empty record
- `empty_key`: a value that represents an empty key

## HashFileSerialOverflow
Hash file with serial overflow zone. 

Primary zone is comprised of serially organized buckets of synonym records. Overflow records are placed into serially organized overflow zone.

## HashFileLinkedOverflow
Hash file with linked overflow zone.

Primary zone is comprised of serially organized (primary) buckets of synonym records. Each primary bucket has a header with a link to the start of the overflow chain for that bucket. Each overflow chain is located in a seperate overflow zone and consits of overflow records from its respective bucket.

## HashFileLinear
Hash file with linear probing of overflow records with constant step.

The file is comprised of serially organized buckets of synonym records. Overflow records are places into the "next" available bucket. Here, "next" means located `k` buckets after the primary bucket (modulo the number of buckets to prevent overflow), where k is the fixed step which can be provided when constructing the file.

### Terminology note
Two records are considered synonyms if they have the same hash value.