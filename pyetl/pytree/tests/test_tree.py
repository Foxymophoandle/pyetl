"""
Created on Aug 7, 2014

@author: davide
"""
import unittest
from pyetl.pytree.tree import Tree


class TestTree(unittest.TestCase):
    
    def setUp(self):
        self.empty_tree = Tree()
        
        self.one_node_tree = Tree()
        self.one_node_tree.add('A')
        
        self.simple_tree = Tree()
        self.simple_tree.add('A', ['B'])
        
        self.simple_tree_more_sons = Tree()
        self.simple_tree_more_sons.add('A', ['B', 'C', 'D'])
        
        self.non_connected_multi_root_tree = Tree()
        self.non_connected_multi_root_tree.add('A', ['B', 'C'])
        self.non_connected_multi_root_tree.add('D', ['E', 'F'])

        self.connected_multi_root_tree = Tree()
        self.connected_multi_root_tree.add('A', ['B'])
        self.connected_multi_root_tree.add('B', ['C'])
        self.connected_multi_root_tree.add('D', ['E'])
        self.connected_multi_root_tree.add('E', ['C'])
        
        self.simple_deep_tree = Tree()
        self.simple_deep_tree.add('A', ['B'])
        self.simple_deep_tree.add('B', ['C'])
        
        self.deep_tree = Tree()
        self.deep_tree.add('A', ['B', 'C'])
        self.deep_tree.add('B', ['D', 'E'])
        self.deep_tree.add('F', ['B', 'G'])
        self.deep_tree.add('D', ['E'])
        
        self.jump_level_tree = Tree()
        self.jump_level_tree.parse_dict({'A': ['B', 'C', 'G'], 'B': ['D', 'E'], 'C': ['F'], 'D': ['E'], 'F': ['G']})
        
    def test_add_node_to_tree_adds_node(self):
        self.assertEquals(len(self.one_node_tree.nodes), 1)

    def test_add_node_to_tree_adds_node_with_no_sons(self):
        self.assertEquals(len(self.one_node_tree.nodes['A']), 0)
 
    def test_add_node_with_sons_adds_all_nodes(self):
        self.assertEquals(len(self.simple_tree_more_sons.nodes), 4)

    def test_add_node_already_in_with_sons_adds_only_new_nodes(self):
        self.assertEquals(len(self.deep_tree.nodes), 7)
        self.assertEquals(len(self.deep_tree.nodes['B']), 2)
 
    def test_add_node_does_not_inject_cycles_simple(self):
        self.assertRaises(Exception, self.simple_tree.add, 'B', ['A'])

    def test_add_node_does_not_inject_cycles_self(self):
        self.assertRaises(Exception, self.simple_tree.add, 'A', ['A'])
 
    def test_add_node_does_not_inject_cycles_deep(self):
        self.assertRaises(Exception, self.deep_tree.add, 'D', ['A', 'B'])
    
    def test_get_ancestors_empty(self):
        parents = self.empty_tree.get_ancestors('A')
        self.assertEquals(parents, [])
 
    def test_get_ancestors_simple(self):
        parents = self.simple_tree_more_sons.get_ancestors('B')
        self.assertEquals(parents, ['A'])
    
    def test_get_ancestors_simple_multi(self):
        parents = self.simple_tree_more_sons.get_ancestors('B')
        self.assertEquals(parents, ['A'])
        parents = self.simple_tree_more_sons.get_ancestors('C')
        self.assertEquals(parents, ['A'])

    def test_get_ancestors_deep(self):
        parents = self.simple_deep_tree.get_ancestors('C')
        self.assertEquals(parents, ['A', 'B'])

    def test_get_ancestors_deep_multi(self):
        parents = self.deep_tree.get_ancestors('D')
        self.assertEquals(parents, ['A', 'B', 'F'])
        parents = self.deep_tree.get_ancestors('E')
        self.assertEquals(parents, ['A', 'B', 'D', 'F'])

    def test_get_progeny_empty(self):
        progeny = self.empty_tree.get_progeny('A')
        self.assertEquals(progeny, set())

    def test_get_progeny_simple(self):
        progeny = self.simple_tree.get_progeny('A')
        self.assertEquals(progeny, set(['B']))

    def test_get_progeny_simple_multi(self):
        progeny = self.simple_tree_more_sons.get_progeny('A')
        self.assertEquals(progeny, set(['B', 'C', 'D']))

    def test_get_progeny_deep(self):
        progeny = self.simple_deep_tree.get_progeny('A')
        self.assertEquals(progeny, set(['B', 'C']))

    def test_get_progeny_deep_multi(self):
        progeny = self.deep_tree.get_progeny('A')
        self.assertEquals(progeny, set(['B', 'C', 'D', 'E']))
        
    def test_new_tree_has_root(self):
        root = self.empty_tree.get_root()
        self.assertIsNotNone(root)
        self.assertEquals(len(self.empty_tree.nodes), 1)
     
    def test_tree_with_one_node_has_node_as_root(self):
        root = self.one_node_tree.get_root()
        self.assertEquals(len(self.one_node_tree.nodes), 1)
        self.assertEquals(root, 'A')
         
    def test_tree_with_unconnected_nodes_creates_root(self):
        root = self.non_connected_multi_root_tree.get_root()
        self.assertIsNotNone(root)
 
    def test_tree_with_unconnected_nodes_creates_unique_root(self):
        root = self.non_connected_multi_root_tree.get_root()
        self.assertNotEquals(root, 'A')
        self.assertNotEquals(root, 'B')
        self.assertEquals(len(self.non_connected_multi_root_tree.nodes), 7)

    def test_tree_with_connected_nodes_has_root_node_as_root(self):
        root = self.simple_tree_more_sons.get_root()
        self.assertEquals(root, 'A')
        self.assertEquals(len(self.simple_tree_more_sons.nodes), 4)

    def test_tree_with_multiple_connected_nodes_creates_root(self):
        root = self.connected_multi_root_tree.get_root()
        self.assertNotEquals(root, 'A')
        self.assertNotEquals(root, 'D')
        self.assertIsNotNone(root)
        self.assertEquals(len(self.connected_multi_root_tree.nodes), 6)
    
    def test_remove_non_existent_node_ignores_command(self):
        self.empty_tree.remove('A')
        self.assertEqual(len(self.empty_tree.nodes), 0)

    def test_remove_leaf_removes_only_that_node(self):
        self.simple_tree_more_sons.remove('C')
        self.assertEqual(len(self.simple_tree_more_sons.nodes), 3)
        self.assertNotIn('C', self.simple_tree_more_sons.nodes)
    
    def test_remove_branch_removes_all_dependencies_deep_simple(self):
        self.simple_deep_tree.remove('B')
        self.assertEqual(len(self.simple_deep_tree.nodes), 1)
        self.assertNotIn('B', self.simple_deep_tree.nodes)
        self.assertNotIn('C', self.simple_deep_tree.nodes)

    def test_remove_branch_removes_all_dependencies_deep(self):
        self.deep_tree.remove('B')
        self.assertEqual(len(self.deep_tree.nodes), 4)
        self.assertNotIn('B', self.deep_tree.nodes)
        self.assertNotIn('D', self.deep_tree.nodes)
        self.assertNotIn('E', self.deep_tree.nodes)

    def test_remove_branch_removes_all_refs_from_other_nodes(self):
        self.deep_tree.remove('D')
        self.assertEqual(len(self.deep_tree.nodes), 5)
        self.assertEqual(len(self.deep_tree.nodes['B']), 0)
        self.assertNotIn('E', self.deep_tree.nodes)

    def test_reverse_single_node_tree_is_equal(self):
        rev_tree = self.one_node_tree.reverse()
        self.assertEquals(self.one_node_tree, rev_tree)

    def test_reverse_simple_tree(self):
        rev_tree = Tree()
        rev_tree.add('A')
        rev_tree.add('B', ['A'])
        rev_tree.add('C', ['A'])
        rev_tree.add('D', ['A'])
        test_rev_tree = self.simple_tree_more_sons.reverse()
        self.assertEquals(test_rev_tree, rev_tree)

    def test_reverse_with_multiple_roots(self):
        rev_tree = Tree()
        rev_tree.add('A')
        rev_tree.add('B', ['A'])
        rev_tree.add('C', ['A'])
        rev_tree.add('D')
        rev_tree.add('E', ['D'])
        rev_tree.add('F', ['D'])
        test_rev_tree = self.non_connected_multi_root_tree.reverse()
        self.assertEquals(rev_tree, test_rev_tree)

    def test_revert_deep_simple(self):
        rev_tree = Tree()
        rev_tree.add('C', ['B'])
        rev_tree.add('B', ['A'])
        test_rev_tree = self.simple_deep_tree.reverse()
        self.assertEquals(rev_tree, test_rev_tree)

    def test_revert_deep(self):
        rev_tree = Tree()
        rev_tree.add('E', ['B', 'D'])
        rev_tree.add('B', ['A', 'F'])
        rev_tree.add('D', ['B'])
        rev_tree.add('C', ['A'])
        rev_tree.add('G', ['F'])
        test_rev_tree = self.deep_tree.reverse()
        self.assertEquals(rev_tree, test_rev_tree)

    def test_copy_from_node_one_node_tree(self):
        self.assertEquals(self.one_node_tree.copy_from_node('A'), self.one_node_tree)

    def test_copy_from_root_simple_tree(self):
        copy_tree = self.simple_tree.copy_from_node('A')
        self.assertEquals(copy_tree, self.simple_tree)

    def test_copy_from_node_simple_tree(self):
        copy_tree = self.simple_tree.copy_from_node('B')
        self.assertIsInstance(copy_tree, Tree)
        self.assertEquals(len(copy_tree.nodes), 1)
        self.assertIn('B', copy_tree.nodes)

    def test_copy_from_root_simple_tree_with_sons(self):
        copy_tree = self.simple_tree_more_sons.copy_from_node('A')
        self.assertEquals(copy_tree, self.simple_tree_more_sons)

    def test_copy_from_root_simple_deep(self):
        copy_tree = self.simple_deep_tree.copy_from_node('A')
        self.assertEquals(copy_tree, self.simple_deep_tree)
        
    def test_copy_from_node_simple_deep(self):
        copy_tree = self.simple_deep_tree.copy_from_node('B')
        self.assertIsInstance(copy_tree, Tree)
        self.assertEquals(len(copy_tree.nodes), 2)
        self.assertIn('C', copy_tree.nodes)
    
    def test_copy_from_root_unconnected_multi_root_deep(self):
        copy_tree = self.non_connected_multi_root_tree.copy_from_node('A')
        self.assertIsInstance(copy_tree, Tree)
        self.assertEquals(len(copy_tree.nodes), 3)
        self.assertIn('B', copy_tree.nodes)
        self.assertIn('C', copy_tree.nodes)

    def test_copy_from_root_deep(self):
        copy_tree = self.deep_tree.copy_from_node('A')
        self.assertIsInstance(copy_tree, Tree)
        self.assertEquals(len(copy_tree.nodes), 5)
        self.assertIn('B', copy_tree.nodes)
        self.assertNotIn('F', copy_tree.nodes)

    def test_copy_from_node_deep(self):
        copy_tree = self.deep_tree.copy_from_node('B')
        self.assertIsInstance(copy_tree, Tree)
        self.assertEquals(len(copy_tree.nodes), 3)
        self.assertIn('B', copy_tree.nodes)
        self.assertNotIn('F', copy_tree.nodes)

    def test_walk_empty(self):
        self.assertEqual(self.empty_tree.walk(), ['DEFAULT_NO_NAME_ROOT'])
 
    def test_walk_one_node(self):
        self.assertEqual(self.one_node_tree.walk(), ['A'])

    def test_walk_simple(self):
        self.assertEqual(self.simple_tree.walk(), ['A', 'B'])
 
    def test_walk_simple_with_sons(self):
        walk = self.simple_tree_more_sons.walk()
        self.assertEqual(walk[0], 'A')
        self.assertIn('B', walk[1:])
        self.assertIn('C', walk[1:])
        self.assertIn('D', walk[1:])
    
    def test_walk_non_connected_multi_root(self):
        walk = self.non_connected_multi_root_tree.walk()
        self.assertEqual(walk[0], 'DEFAULT_NO_NAME_ROOT')
        self.assertGreater(walk.index('E', ), walk.index('D', ))
        self.assertGreater(walk.index('B', ), walk.index('A', ))
    
    def test_walk_connected_multi_root(self):
        walk = self.connected_multi_root_tree.walk()
        self.assertEqual(walk[0], 'DEFAULT_NO_NAME_ROOT')
        self.assertGreater(walk.index('E', ), walk.index('D', ))
        self.assertGreater(walk.index('B', ), walk.index('A', ))
        self.assertIn('C', walk[-1])

    def test_walk_simple_deep(self):
        walk = self.simple_deep_tree.walk()
        self.assertEqual(walk[0], 'A')
        self.assertEqual(walk[1], 'B')
        self.assertEqual(walk[2], 'C')

    def test_walk_deep(self):
        walk = self.deep_tree.walk()
        self.assertEqual(walk[0], 'DEFAULT_NO_NAME_ROOT')
        self.assertGreater(walk.index('G', ), walk.index('F', ))
        self.assertGreater(walk.index('B', ), walk.index('F', ))
        self.assertGreater(walk.index('E', ), walk.index('B', ))
        self.assertGreater(walk.index('E', ), walk.index('D', ))
        
    def test_walk_jump_tree(self):
        walk = self.jump_level_tree.walk()
        self.assertEqual(walk[0], 'A')
        self.assertIn('B', walk[1:3])
        self.assertIn('C', walk[1:3])
        self.assertGreater(walk.index('G', ), walk.index('F', ))
        self.assertGreater(walk.index('G', ), walk.index('C', ))

    def test_parse_empty_dict(self):
        self.empty_tree.parse_dict(dict())
        self.assertIsInstance(self.empty_tree, Tree)
        self.assertEquals(len(self.empty_tree.nodes), 0)
    
    def test_parse_one_node_dict(self):
        self.empty_tree.parse_dict({'A': []})
        self.assertIsInstance(self.empty_tree, Tree)
        self.assertEquals(self.empty_tree, self.one_node_tree)
    
    def test_parse_simple(self):
        self.empty_tree.parse_dict({'A': ['B']})
        self.assertIsInstance(self.empty_tree, Tree)
        self.assertEquals(self.empty_tree, self.simple_tree)

    def test_parse_simple_more_sons(self):
        self.empty_tree.parse_dict({'A': ['B', 'C', 'D']})
        self.assertIsInstance(self.empty_tree, Tree)
        self.assertEquals(self.empty_tree, self.simple_tree_more_sons)

    def test_parse_simple_deep(self):
        self.empty_tree.parse_dict({'A': ['B'], 'B': ['C']})
        self.assertIsInstance(self.empty_tree, Tree)
        self.assertEquals(self.empty_tree, self.simple_deep_tree)

    def test_parse_deep(self):
        self.empty_tree.parse_dict({'A': ['B', 'C'], 'B': ['D', 'E'], 'F': ['B', 'G'], 'D': ['E']})
        self.assertIsInstance(self.empty_tree, Tree)
        self.assertEquals(self.empty_tree, self.deep_tree)
        
if __name__ == "__main__":
    unittest.main()
