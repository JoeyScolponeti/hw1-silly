from typing import Optional, Any

from dask.dataframe.io.parquet.core import NONE_LABEL
from indexer.trees.avl_node import AVLNode
from indexer.trees.bst_index import BinarySearchTreeIndex


class AVLTreeIndex(BinarySearchTreeIndex):
    """
    An AVL Tree implementation of an index that maps a key to a list of values.
    AVLTreeIndex inherits from BinarySearchTreeIndex meaning it automatically
    contains all the data and functionality of BinarySearchTree.  Any
    functions below that have the same name and param list as one in 
    BinarySearchTreeIndex overrides (replaces) the BSTIndex functionality. 

    Methods:
        insert(key: Any, value: Any) -> None:
            Inserts a new node with key and value into the AVL Tree
    """
    
    def __init__(self):
       super().__init__()
       self.root: Optional[AVLNode] = None
       self.help = 0
    
    def _height(self, node: Optional[AVLNode]) -> int:
        """
        Calculate the height of the given AVLNode.

        Parameters:
        - node: The AVLNode for which to calculate the height.

        Returns:
        - int: The height of the AVLNode. If the node is None, returns 0.
        """
        
        # TODO: make sure to update height appropriately in the
        # recursive insert function.

        if not node:
            return 0

        else:

            print(node.key)

            if node.left:
                print(f'LEFT {node.left.key}')

            if node.right:
                print(f'RIGHT {node.right.key}')

            print('---------')

            node.height = max(self._height(node.left), self._height(node.right)) + 1

        return node.height

    def _find_parent(self, node: AVLNode, root) -> Optional[AVLNode]:

        #print(f'Search Root: {root.key}')

        if root is None or root == node:
            return None

        if node is None:
            return None

        if node.key < root.key:
            if root.left == node:
                return root
            else:
                return self._find_parent(node, root.left)
        else:
            if root.right == node:
                return root
            else:
                return self._find_parent(node, root.right)

    def get_subtrees(self, node):

        if node.left is not None:
            LST = self._height(node.left)
        else:
            LST = 0

        if node.right is not None:
            RST = self._height(node.right)
        else:
            RST = 0

        return LST, RST

    def _rotate_right(self, z: AVLNode, a: AVLNode) -> AVLNode:
        """
        Performs a right rotation on the AVL tree.

        Args:
            y (AVLNode): The node to be rotated.

        Returns:
            AVLNode: The new root of the rotated subtree.
        """
        
        # TODO: implement the right rotation for AVL Tree

        y = z.left
        x = y.left

        #print(y.right.key)
        z.left = None
        #print(z.left.key)
        y.right = z

        if not a:
            self.root = y
        else:
            if a.left == z:
                a.left = y
            else:
                a.right = y

        if z.left:
            print(f'Z LST {z.left.key}')
        if z.right:
            print(f'Z RST {z.right.key}')
        if a:
            if a.left:
                print(f'A LST {a.left.key}')
            if a.right:
                print(f'A RST {a.right.key}')
        if y.left:
            print(f'Y LST {y.left.key}')
        if y.right:
            print(f'Y RST {y.right.key}')

        print('done')
        #print(h)

        return y

    def _rotate_left(self, z: AVLNode, a:AVLNode) -> AVLNode:
        """
        Rotate the given node `x` to the left.
        Args:
            x (AVLNode): The node to be rotated.
        Returns:
            AVLNode: The new root of the subtree after rotation.
        """
        
        # TODO: implement the left rotation for AVL Tree

        T1 = z.left
        y = z.right
        T2 = y.left
        x = y.right
        T3 = x.left
        T4 = x.right




        # print(y.right.key)
        z.right = None
        # print(z.left.key)
        y.left = z

        if not a:
            self.root = y
        else:
            if a.right == z:
                a.right = y
            else:
                a.left = y

        #print(f'a = {a.key}')
        print(f'Z = {z.key}')
        print(f'Y = {y.key}')
        print(f'X = {x.key}')

        if a:
            print(f'A = {a.key}')

        if z.left:
            print(f'Z LST {z.left.key}')
        if z.right:
            print(f'Z RST {z.right.key}')

        if a:
            if a.left:
                print(f'A LST {a.left.key}')
            if a.right:
                print(f'A RST {a.right.key}')
        if y.left:
            print(f'Y LST {y.left.key}')
        if y.right:
            print(f'Y RST {y.right.key}')

        self.help = 1

        #print(h)

        pass

    def _height_logic(self, node: Optional[AVLNode]) -> int:

        if self._height(node) > 2:

            LST, RST = self.get_subtrees(node)

            if abs(LST - RST) > 1:
                print(f'BAD NODE: {node.key}')

                if LST > RST:
                    LST_y, RST_y = self.get_subtrees(node.left)

                    if LST_y > RST_y:
                        # left left
                        print('LEFT LEFT')
                        # print(node.key)
                        self._rotate_right(node, self._find_parent(node, self.root))
                        return 1

                    else:

                        # left right
                        print('LEFT RIGHT')
                        # print(node.key)

                        y = node.left
                        x = y.right

                        node.left = x
                        y.left = 0
                        x.left = y

                        print(f'z {node.key}')
                        print(f'x {node.left.key}')
                        print(f'y {node.left.left.key}')

                        # self._rotate_left(node, self._find_parent(node, self.root))
                        self._rotate_right(node, self._find_parent(node, self.root))
                        return 1

                else:

                    LST_y, RST_y = self.get_subtrees(node.right)

                    if LST_y > RST_y:

                        # right left
                        print('RIGHT LEFT')

                        y = node.right
                        x = y.left

                        node.right = x
                        y.right = 0
                        x.right = y

                        print(f'z {node.key}')
                        print(f'x {node.right.key}')
                        print(f'y {node.right.right.key}')

                        # self._rotate_right(node, self._find_parent(node, self.root))
                        self._rotate_left(node, self._find_parent(node, self.root))
                        return 1

                    else:

                     # right right
                     print('RIGHT RIGHT')
                     # print(node.key)
                     self._rotate_left(node, self._find_parent(node, self.root))
                     print('RIGHT RIGHT')



            else:

                rotate = 0

                if node.left is not None:
                    rotate = self._height_logic(node.left)

                if node.right is not None:
                    if rotate != 1:
                        self._height_logic(node.right)




    def _insert_recursive(self, current: Optional[AVLNode], key: Any, value: Any) -> AVLNode:
        """
        Recursively inserts a new node with the given key and value into the AVL tree.
        Args:
            current (Optional[AVLNode]): The current node being considered during the recursive insertion.
            key (Any): The key of the new node.
            value (Any): The value of the new node.
        Returns:
            AVLNode: The updated AVL tree with the new node inserted.
        """
        # TODO: Implement a proper recursive insert function for an
        # AVL tree including updating height and balancing if a
        # new node is inserted.

        #print(f'start loop current: {current}')

        if not current:
            node = AVLNode(key)
            node.add_value(value)

            return node

        elif key < current.key:
            #print(f'{key} < {current.key}')
            print('left')
            current.left = self._insert_recursive(current.left, key, value)
            #print('<')

        elif key > current.key:
            #print(f'{key} > {current.key}')
            print('right')
            current.right = self._insert_recursive(current.right, key, value)
            #print('>')

        elif key == current.key:
            #print(f'{key} == {current.key}')
            current.add_value(value)

        #print(f'current {current.key}')
        return current
        
        # TODO: Remove or comment out this line once you've implemented
        # the AVL insert functionality 
        # current = super()._insert_recursive(current, key, value)

    def insert(self, key: Any, value: Any) -> None:
        """
        Inserts a key-value pair into the AVL tree. If the key exists, the
         value will be appended to the list of values in the node. 

        Parameters:
            key (Any): The key to be inserted.
            value (Any): The value associated with the key.

        Returns:
            None
        """

        #print(key)

        if self.root is None:
            self.root = AVLNode(key)
            self.root.add_value(value)
        else:
            print(f'INSERTING {key}')
            self.root = self._insert_recursive(self.root, key, value)
            print(f'INSERTED {key}')

            self._height_logic(self.root)

            print('DONE')

    #def _inorder_traversal(self, current: Optional[AVLNode], result: List[Any]) -> None:
    #   if current is None:
    #       return
        
    #   self._inorder_traversal(current.left, result)
    #   result.append(current.key)
    #   self._inorder_traversal(current.right, result)
   
    # def get_keys(self) -> List[Any]:
    #     keys: List[Any] = []
    #     self._inorder_traversal(self.root, keys)
    #     return keys