'''
Created on Aug 7, 2014

@author: davide
'''


class Tree(object):
    """A Tree is a collection of linked nodes.
    It always has a root (if undefined it creates one).
    Each node is unique. A Tree must be acyclic.
    """
    def __init__(self):
        self.nodes = dict()
    
    def __str__(self):
        return str(self.nodes)
    
    def __repr__(self):
        return self.__str__()
    
    def __hash__(self):
        return hash(self.__repr__())
    
    def __eq__(self, other):
        return isinstance(other, Tree) and other.nodes == self.nodes
        
    def get_root(self):
        """Returns the Tree root name
        """
        all_sons = self.__get_all_sons()
        all_nodes = set(self.nodes.keys())
        possible_roots = all_nodes.difference(all_sons) # a root is not son by definition
        if len(possible_roots) == 1:
            return possible_roots.pop()
        else:
            self.nodes['DEFAULT_NO_NAME_ROOT'] = list(possible_roots)
            return 'DEFAULT_NO_NAME_ROOT'
    
    def __get_all_sons(self):
        """Return all nodes that are dependent on at least another node
        """
        all_sons = set()
        for sons in self.nodes.values():
            if sons:
                all_sons.update(set(sons))
        return all_sons
    
    def reverse(self):
        """Returns the inverse tree, a son becomes a parent of its parents
        """
        rev_tree = Tree()
        for node, sons in self.nodes.items():
            if sons:
                for son in sons:
                    rev_tree.nodes.setdefault(son, []).append(node)
            rev_tree.nodes.setdefault(node, [])
        return rev_tree
        
    def add(self, node, sons=None):
        """Adds a new node and its sons to the Tree
        """
        if sons is None:
            self.nodes[node] = []
        else:
            self.__check_cycles(node, sons)
            self.nodes[node] = sons
            if sons:
                for son in sons:
                    self.nodes.setdefault(son, [])
    
    def __check_cycles(self, node, sons):
        """Raises an exception if we are trying to insert
        a node that's depending on a node which is already 
        depending from him
        """
        node_ancestors = self.get_ancestors(node)
        for son in sons:
            if son in node_ancestors or son == node:
                raise Exception('%s IS ALREADY AN ANCESTOR OF %s' % (son, node))

    def get_ancestors(self, node):
        """Returns all parents of the node (deeply)
        which are nothing else that all sons of the inverse tree ;)
        """
        rev_tree = self.reverse()
        progeny_set = rev_tree.get_progeny(node, set())
        return list(progeny_set)

    def get_progeny(self, node, progeny=None):
        """Returns all sons of the node (deeply)
        @warning: recursive function
        """
        if not progeny:
            progeny = set()
        if self.nodes.get(node, []) == []:
            return set()
        for son in self.nodes[node]:
            progeny.add(son)
            progeny.union(self.get_progeny(son, progeny))
        return progeny
        
    def remove(self, node_to_remove):
        """Removes a node and all its sons from the tree
        """
        reverse_tree = self.reverse()
        nodes_cleaned = dict()
        for node, sons in reverse_tree.nodes.items():
            if node == node_to_remove or node_to_remove in sons:
                continue
            nodes_cleaned.setdefault(node, []).extend(sons)
        reverse_tree.nodes = nodes_cleaned
        cleaned_tree = reverse_tree.reverse()
        self.nodes = cleaned_tree.nodes
    
    def walk(self):
        """Walks through the whole tree following dependencies order
        """
        root = self.get_root()
        reverse_tree = self.reverse()
        walk = [root]
        while len(walk) < len(self.nodes):
            for node, sons in self.nodes.items():
                if reverse_tree.can_add_node_to_walk(node, walk):
                    walk.append(node)
                for son in sons:
                    if reverse_tree.can_add_node_to_walk(son, walk):
                        walk.append(son)
        if 'DEFAULT_NO_NAME_ROOT' in walk:
            walk.remove('DEFAULT_NO_NAME_ROOT')
        return list(walk)
    
    def can_add_node_to_walk(self, node, walk):
        """Checks if all parents of the node have been parsed and that
        the node is not already in the walk
        """
        return node not in walk and all([parent in walk for parent in self.nodes[node]])
        
    
    def get_path_from_node(self, node):
        """Walks through tree starting from the input node
        following dependencies, touching each node, only once. Returns a list
        with the node names ordered
        """
        tree_from_node = self.copy_from_node(node)
        return tree_from_node.walk()
    
    def copy_from_node(self, new_root_node):
        """Returns a Tree starting from the node specified
        with all dependencies still connected.
        """
        new_tree = Tree()
        if new_root_node not in self.nodes:
            return new_tree
        new_tree.nodes = self.__copy_rec(new_root_node)
        return new_tree
    
    def __copy_rec(self, new_root, nodes=None):
        """Recursive call to copy all nodes after the one selected
        with dependencies
        """
        if not nodes:
            nodes = dict()
        nodes.update({new_root: self.nodes[new_root]})
        for son in self.nodes[new_root]:
            nodes.update(self.__copy_rec(son, nodes))
        return nodes
    
    def parse_dict(self, in_dict):
        """Given a dictionary, it initializes a Tree
        if the dict is valid
        """
        for node, sons in in_dict.items():
            self.add(node, sons)
