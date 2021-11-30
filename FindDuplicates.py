
import argparse
import hashlib
import os
from collections import defaultdict

from tqdm import tqdm


# Calculates MD5 hash of file
# Returns HEX digest of file
def Hash_File(path):

    # Opening file in afile
    afile = open(path, 'rb')
    hasher = hashlib.md5()
    blocksize=65536
    buf = afile.read(blocksize)

    while len(buf) > 0:
        hasher.update(buf)
        buf = afile.read(blocksize)
    afile.close()
    return hasher.hexdigest()

def build_hash_table(root_dir):
    print('Building hash table')

    # Dict[str, list]: size in bytes is the key and the value is a list of full paths of files having same size
    hash_table = defaultdict(list)

    # Get the absolute path to avoid confusion (If there are any name duplicates as well)
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
                hash = Hash_File(file_absolute_path)
                hash_table[hash].append(file_absolute_path)
            except:
                pass

    return hash_table

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Duplicate Finder')

    parser.add_argument('--root_dir', type=str, required=True,
                        help='The root directory that contains all the files and the sub-directories.')
    args = parser.parse_args()

    duplicates_result = build_hash_table(args.root_dir)

    for file_list in duplicates_result.values():
        try:
            # Check if the list has size greater than 2 by addressing the 2nd element
            # We do this to avoid using the length which may be expensive if there are many duplicates
            _ = file_list[1]
            print('* duplicate files', ', '.join(file_list))
        except IndexError:
            pass
