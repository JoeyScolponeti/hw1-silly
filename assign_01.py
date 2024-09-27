
import json
from indexer.abstract_index import AbstractIndex
from indexer.trees.bst_node import BSTNode
from indexer.trees.avl_tree import AVLTreeIndex
from indexer.trees.bst_index import BinarySearchTreeIndex
from indexer.maps.hash_map import HashMapIndex
from indexer.util.timer import timer
from zipfile import ZipFile, Path

def index_files(path, filename, index: AbstractIndex) -> None:
    # path should contain the location of the news articles you want to parse
    if path is not None:
        print(f'path = {path}')

    words = path["preprocessed_text"]

    for word in words:
        index.insert(word, filename)
        
@timer
def loopy_loop():
    total = sum((x for x in range(0, 1000000)))

def index_creation():
    # You'll need to change this to be the absolute path to the root folder
    # of the dataset
    data_directory = "USFinancialNewsArticles-preprocessed.zip"

    zipfile = ZipFile(data_directory, 'r')

    file_list = zipfile.namelist()
    jsons = [x for x in file_list if '.json' in x]
    april = [x for x in jsons if 'April2018' in x]
    # print(len(april))

    x = 0
    sets = {}
    while x < 6:
        sets[x] = (april[0:10 ** x])
        x += 1

    # print(len(sets[5]))

    sets[6] = april + [x for x in jsons if 'May2018' in x]
    sets[7] = jsons

    index_dict = {}

    for key, set in sets.items():

        bst_index = BinarySearchTreeIndex()
        avl_index = AVLTreeIndex()
        hash_index = HashMapIndex(size=10000)

        for file in set:
            f = json.loads(zipfile.read(file).decode('utf-8'))

            # Here, we are creating a sample binary search tree index
            # and sending it to the index_files function

            index_files(f, file, bst_index)
            print(f'BST {key}')
            index_files(f, file, avl_index)
            print(f'AVL {key}')
            print(kmlml)
            index_files(f, file, hash_index)
            print(f'Hash {key}')
            list_index = f['preprocessed_text']
            print(f'List {key}')

            index_dict[key] = [bst_index, avl_index, hash_index, list_index]

    return index_dict


"""
    # quick demo of how to use the timing decorator included
    # in indexer.util
def search_index(index: AbstractIndex, term: str) -> None:
    index.search(term)

@timer
def run_searches(index: AbstractIndex, dataset: list) -> None:
    for file in dataset:
        f = json.loads(zipfile.read(file).decode('utf-8'))
        words = f["preprocessed_text"]
        for word in words:
            search_index(index, word)

search_times = {}

for key, dataset in sets.items():
    print(f"Indexing dataset size {len(dataset)}...")

    search_time_bst = run_searches(bst_index, dataset)
    search_times[f'BST Size {len(dataset)}'] = search_time_bst

    search_time_avl = run_searches(avl_index, dataset)
    search_times[f'AVL Size {len(dataset)}'] = search_time_avl

for key, value in search_times.items():
    print(f"{key}: {value} seconds")

loopy_loop()
"""

def main():
    index_creation()

if __name__ == "__main__":
    main()
