from indexer.abstract_index import AbstractIndex
from collections import defaultdict

class HashMapIndex():
    
    def __init__(self, size):
        super().__init__()
        self.hash_map = [[]]*size

    def insert(self, term, document_id):

        h = hash(term) % len(self.hash_map)
        self.hash_map[h].append((term, document_id))

    def search(self, term):

        h = hash(term) % len(self.hash_map)

        for i in range(len(self.hash_map[h])):
            if self.hash_map[h][i][0] == term:
                return self.hash_map[h][i][1]

    def remove(self, term):

        h = hash(term) % len(self.hash_map)

        if term in self.hash_map[h]:
            self.hash_map[h].remove((term, 0))




