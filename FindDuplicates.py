import argparse
import hashlib
import os
from collections import defaultdict

from tqdm import tqdm


def chunk_reader(file, chunk_size):
    """
    Reads a file's content by chunks

    :param file:  file object. The opened descriptor of the desired file
    :param chunk_size: int. Chunk size / how many bytes to read during the call of a read operation
    :return: generator. Yields the available read chunk
    """
    while True:
        chunk = file.read(chunk_size)

        if not chunk:
            return

        yield chunk

def get_hash(filename, first_chunk_only, chunk_size):
    """
    Creates the hash of the content of a file by reading either first chunk_size bytes or the content. The behavior
    is controlled by the first_chunk_only variable.

    :param filename: string. absolute path to the file
    :param first_chunk_only: boolean. Whether to consider the first chunk_size bytes
    :param chunk_size: int. How many bytes to read during a read operation
    :return: _Hash. The hash of the read bytes
    """
    hash_object = hashlib.new(name='sha512')

    with open(filename, 'rb') as f:

        if first_chunk_only:
            hash_object.update(f.read(chunk_size))
        else:
            for chunk in chunk_reader(f, chunk_size=chunk_size):
                hash_object.update(chunk)

        return hash_object.digest()

def build_hash_table(root_dir):
    # Dict[int, str]: size in bytes is the key and the value is a list of full paths of files having same size
    hash_table = defaultdict(list)

    # Get the absolute path to avoid confusion
    abs_path = os.path.abspath(root_dir)

    for dir_path, dir_names, file_names in tqdm(os.walk(abs_path), desc='Traversing Directory Structure'):

        for file_name in file_names:
            # Skip hidden files
            if file_name.startswith('.'):
                continue

            file_absolute_path = os.path.join(dir_path, file_name)

            try:
                # If the target is a soft symlink, this will change the value to the actual target file
                file_absolute_path = os.path.realpath(file_absolute_path)
                file_size = os.path.getsize(file_absolute_path)
                hash_table[file_size].append(file_absolute_path)
            except:
                pass

    return hash_table

def check_for_duplicates(hash_table, chunk_size):
    """
    Finds the duplicated files by analyzing files with the same size only. It compares the hash of the first chunk
    of bytes, and then if those hashes are equal, ity proceeds to compare the hash of the full content. Number of
    comparisons on the full content depends on chunk size and the hash function used as they need tyo minimize
    collisions.

    :param hash_table: Dict[int, list]. Key is size in bytes and value is the list of files having the same size of the content
    :param chunk_size: int. How many bytes to read during a read call
    :return: Dict[bytes, list]. Dictionary where the key is the hash and the value is a list that contains all the files that have same content
    """

    # Dictionary from hash to list. Contains all the absolute paths to the files having same hash
    duplicates = defaultdict(list)

    for file_list in tqdm(hash_table.values(), desc='Iterating Through Files'):
        for candidate_path in file_list:
            try:
                # Compute hash of the full content
                full_hash = get_hash(candidate_path, first_chunk_only=False, chunk_size=chunk_size)

                # Append the file to the corresponding bucket indexed by its hash
                # If more contents have the same hash -> duplicates[full_hash] will have more than one element
                duplicates[full_hash].append(candidate_path)
            except OSError:
                pass

    # Return the dictionary indexing: hash -> list of files whose all content has same hash
    return duplicates


def validate_chunk_size(value):
    try:
        value = int(value)

        if value <= 0:
            raise argparse.ArgumentError(f'Variable chunk_size needs to be greater than zero')

        return value
    except ValueError:
        raise argparse.ArgumentTypeError(f'{value} is not of type int')

def build_argparse():
    parser = argparse.ArgumentParser(description='Duplicate Finder')

    parser.add_argument('--root_dir', type=str, required=True,
                        help='The root directory that contains all the files and other directories.')

    return parser.parse_args()


if __name__ == '__main__':
    args = build_argparse()

    # Build the table that has all the files indexed by size (initial set of candidates)
    table = build_hash_table(args.root_dir)

    # Get the final candidates
    duplicates_result = check_for_duplicates(table, chunk_size=65000)

    for file_list in duplicates_result.values():
        try:
            # Check if the list has size greater than 2 vby addressing the 2nd element
            # We do this tyo avoid using the length which may vbe expensive if there are many duplicates
            _ = file_list[1]
            print('\n' + '* duplicate files', ', '.join(file_list) + '\n')
        except IndexError:
            pass
