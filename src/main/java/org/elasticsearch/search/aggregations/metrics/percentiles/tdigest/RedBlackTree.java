/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.search.aggregations.metrics.percentiles.tdigest;

import com.carrotsearch.hppc.IntArrayDeque;
import com.carrotsearch.hppc.cursors.IntCursor;
import com.google.common.collect.UnmodifiableIterator;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.OpenBitSet;
import org.apache.lucene.util.RamUsageEstimator;

import java.util.Arrays;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * A red-black tree that identifies every node with a unique dense integer ID to make it easy to store node-specific data into
 * parallel arrays. This implementation doesn't support more than 2B nodes.
 */
public abstract class RedBlackTree implements Iterable<IntCursor> {

    protected static final int NIL = 0;
    private static final boolean BLACK = false, RED = true;

    private int size;
    private int maxNode;
    private int root;
    private final IntArrayDeque unusedSlots;
    private int[] leftNodes, rightNodes, parentNodes;
    private final OpenBitSet colors;

    /** Create a new instance able to store <code>capacity</code> without resizing. */
    protected RedBlackTree(int capacity) {
        size = 0;
        maxNode = 1;
        root = NIL;
        leftNodes = new int[1+capacity];
        rightNodes = new int[1+capacity];
        parentNodes = new int[1+capacity];
        colors = new OpenBitSet(1+capacity);
        unusedSlots = new IntArrayDeque();
    }

    /** Return the identifier of the root of the tree. */
    protected final int root() {
        assert size > 0 || root == NIL;
        return root;
    }

    /** Release a node */
    protected void releaseNode(int node) {
        unusedSlots.addLast(node);
        parent(node, NIL);
        left(node, NIL);
        right(node, NIL);
    }

    /** Create a new node in this tree. */
    protected int newNode() {
        if (unusedSlots.isEmpty()) {
            final int slot = maxNode++;
            if (maxNode > leftNodes.length) {
                final int minSize = ArrayUtil.oversize(maxNode, RamUsageEstimator.NUM_BYTES_INT);
                leftNodes = Arrays.copyOf(leftNodes, minSize);
                rightNodes = Arrays.copyOf(rightNodes, minSize);
                parentNodes = Arrays.copyOf(parentNodes, minSize);
                colors.ensureCapacity(leftNodes.length);
            }
            return slot;
        } else {
            return unusedSlots.removeLast();
        }
    }

    /** Return the number of nodes in this tree. */
    public int size() {
        assert size == maxNode - unusedSlots.size() - 1 : size + " != " + (maxNode - unusedSlots.size() - 1);
        return size;
    }

    private boolean color(int node) {
        return colors.get(node);
    }

    private void color(int node, boolean color) {
        assert node != NIL;
        if (color) {
            colors.fastSet(node);
        } else {
            colors.fastClear(node);
        }
    }

    /** Return the parent of the given node. */
    protected final int parent(int node) {
        return parentNodes[node];
    }

    private void parent(int node, int parent) {
        assert node != NIL;
        parentNodes[node] = parent;
    }

    /** Return the left child of the given node. */
    protected final int left(int node) {
        assert node != NIL;
        return leftNodes[node];
    }

    protected void left(int node, int leftNode) {
        assert node != NIL;
        leftNodes[node] = leftNode;
    }

    /** Return the right child of the given node. */
    protected final int right(int node) {
        assert node != NIL;
        return rightNodes[node];
    }

    private void right(int node, int rightNode) {
        assert node != NIL;
        rightNodes[node] = rightNode;
    }

    // return the number of black nodes to go through up to leaves
    // to use within assertions only
    private int numBlackNode(int node) {
        assert assertConsistent(node);
        if (node == NIL) {
            return 1;
        } else {
            final int numLeft = numBlackNode(left(node));
            final int numRight = numBlackNode(right(node));
            assert numLeft == numRight : numLeft + " " + numRight;
            int num = numLeft;
            if (color(node) == BLACK) {
                ++num;
            }
            return num;
        }
    }

    // check consistency of parent/left/right
    private boolean assertConsistent(int node) {
        if (node == NIL) {
            return true;
        }
        final int parent = parent(node);
        if (parent == NIL) {
            assert node == root;
        } else {
            assert node == left(parent) || node == right(parent);
        }
        final int left = left(node);
        if (left != NIL) {
            assert parent(left) == node;
        }
        final int right = right(node);
        if (right != NIL) {
            assert parent(right) == node;
        }
        return true;
    }

    // for testing
    public void assertConsistent() {
        numBlackNode(root);
    }

    /** Rotate left the subtree under <code>n</code> */
    protected void rotateLeft(int n) {
        final int r = right(n);
        final int lr = left(r);
        right(n, left(r));
        if (lr != NIL) {
            parent(lr, n);
        }
        final int p = parent(n);
        parent(r, p);
        if (p == NIL) {
            root = r;
        } else if (left(p) == n) {
            left(p, r);
        } else {
            assert right(p) == n;
            right(p, r);
        }
        left(r, n);
        parent(n, r);
    }

    /** Rotate right the subtree under <code>n</code> */
    protected void rotateRight(int n) {
        final int l = left(n);
        final int rl = right(l);
        left(n, rl);
        if (rl != NIL) {
            parent(rl, n);
        }
        final int p = parent(n);
        parent(l, p);
        if (p == NIL) {
            root = l;
        } else if (right(p) == n) {
            right(p, l);
        } else {
            assert left(p) == n;
            left(p, l);
        }
        right(l, n);
        parent(n, l);
    }

    /** Called after insertions. Base implementation just balances the tree,
     *  see http://en.wikipedia.org/wiki/Red%E2%80%93black_tree#Insertion */
    protected void afterInsertion(int node) {
        color(node, RED);
        insertCase1(node);
    }

    private void insertCase1(int node) {
        final int parent = parent(node);
        if (parent == NIL) {
            assert node == root;
            color(node, BLACK);
        } else {
            insertCase2(node, parent);
        }
    }

    private void insertCase2(int node, int parent) {
        if (color(parent) != BLACK) {
            insertCase3(node, parent);
        }
    }

    private void insertCase3(int node, int parent) {
        final int grandParent = parent(parent);
        assert grandParent != NIL;
        final int uncle;
        if (parent == left(grandParent)) {
            uncle = right(grandParent);
        } else {
            assert parent == right(grandParent);
            uncle = left(grandParent);
        }
        if (uncle != NIL && color(uncle) == RED) {
            color(parent, BLACK);
            color(uncle, BLACK);
            color(grandParent, RED);
            insertCase1(grandParent);
        } else {
            insertCase4(node, parent, grandParent);
        }
    }

    private void insertCase4(int node, int parent, int grandParent) {
        if (node == right(parent) && parent == left(grandParent)) {
            rotateLeft(parent);
            node = left(node);
        } else if (node == left(parent) && parent == right(grandParent)) {
            rotateRight(parent);
            node = right(node);
        }
        insertCase5(node);
    }

    private void insertCase5(int node) {
        final int parent = parent(node);
        final int grandParent = parent(parent);
        color(parent, BLACK);
        color(grandParent, RED);
        if (node == left(parent)) {
            rotateRight(grandParent);
        } else {
            assert node == right(parent);
            rotateLeft(grandParent);
        }
    }

    /** Called before the removal of a node. */
    protected void beforeRemoval(int node) {}

    // see http://en.wikipedia.org/wiki/Red%E2%80%93black_tree#Removal
    /** Called after removal. Base implementation just balances the tree,
     *  see http://en.wikipedia.org/wiki/Red%E2%80%93black_tree#Removal */
    protected void afterRemoval(int node) {
        assert node != NIL;
        if (color(node) == BLACK) {
            removeCase1(node);
        }
    }

    private int sibling(int node, int parent) {
        final int left = left(parent);
        if (node == left) {
            return right(parent);
        } else {
            assert node == right(parent);
            return left;
        }
    }

    private void removeCase1(int node) {
        if (parent(node) != NIL) {
            removeCase2(node);
        }
    }

    private void removeCase2(int node) {
        final int parent = parent(node);
        final int sibling = sibling(node, parent);

        if (color(sibling) == RED) {
            color(sibling, BLACK);
            color(parent, RED);
            if (node == left(parent)) {
                rotateLeft(parent);
            } else {
                assert node == right(parent);
                rotateRight(parent);
            }
        }
        removeCase3(node);
    }

    private void removeCase3(int node) {
        final int parent = parent(node);
        final int sibling = sibling(node, parent);

        if (color(parent) == BLACK && sibling != NIL && color(sibling) == BLACK
                && color(left(sibling)) == BLACK && color(right(sibling)) == BLACK) {
            color(sibling, RED);
            removeCase1(parent);
        } else {
            removeCase4(node, parent, sibling);
        }
    }

    private void removeCase4(int node, int parent, int sibling) {
        if (color(parent) == RED && sibling != NIL && color(sibling) == BLACK
                && color(left(sibling)) == BLACK && color(right(sibling)) == BLACK) {
            color(sibling, RED);
            color(parent, BLACK);
        } else {
            removeCase5(node, parent, sibling);
        }
    }

    private void removeCase5(int node, int parent, int sibling) {
        if (color(sibling) == BLACK) {
            if (node == left(parent) && sibling != NIL && color(left(sibling)) == RED && color(right(sibling)) == BLACK) {
                color(sibling, RED);
                color(left(sibling), BLACK);
                rotateRight(sibling);
            } else if (node == right(parent) && sibling != NIL && color(left(sibling)) == BLACK && color(right(sibling)) == RED) {
                color(sibling, RED);
                color(right(sibling), BLACK);
                rotateLeft(sibling);
            }
        }
        removeCase6(node);
    }

    private void removeCase6(int node) {
        final int parent = parent(node);
        final int sibling = sibling(node, parent);
        color(sibling, color(parent));
        color(parent, BLACK);
        if (node == left(parent)) {
            color(right(sibling), BLACK);
            rotateLeft(parent);
        } else {
            assert node == right(parent);
            color(left(sibling), BLACK);
            rotateRight(parent);
        }
    }

    /** Compare to <code>node</code>. */
    protected abstract int compare(int node);

    /** Copy data used for comparison into <code>node</code>. */
    protected abstract void copy(int node);

    /** Merge data used for comparison into <code>node</code>. */
    protected abstract void merge(int node);

    /** Add a node to the tree. */
    public boolean addNode() {
        int newNode = NIL;
        if (size == 0) {
            newNode = root = newNode();
            copy(root);
        } else {
            int parent = NIL, node = root;
            int cmp;
            do {
                cmp = compare(node);
                if (cmp < 0) {
                    parent = node;
                    node = left(node);
                } else if (cmp > 0) {
                    parent = node;
                    node = right(node);
                } else {
                    merge(node);
                    return false;
                }
            } while (node != NIL);

            newNode = newNode();
            copy(newNode);
            if (cmp < 0) {
                left(parent, newNode);
            } else {
                assert cmp > 0;
                right(parent, newNode);
            }
            parent(newNode, parent);
        }
        ++size;
        afterInsertion(newNode);
        return true;
    }

    public int getNode() {
        if (size() == 0) {
            return NIL;
        }
        int node = root;
        do {
            final int cmp = compare(node);
            if (cmp < 0) {
                node = left(node);
            } else if (cmp > 0) {
                node = right(node);
            } else {
                return node;
            }
        } while (node != NIL);

        return NIL;
    }

    /** Swap two nodes. */
    protected void swap(int node1, int node2) {
        final int parent1 = parent(node1);
        final int parent2 = parent(node2);
        if (parent1 != NIL) {
            if (node1 == left(parent1)) {
                left(parent1, node2);
            } else {
                assert node1 == right(parent1);
                right(parent1, node2);
            }
        } else {
            assert root == node1;
            root = node2;
        }
        if (parent2 != NIL) {
            if (node2 == left(parent2)) {
                left(parent2, node1);
            } else {
                assert node2 == right(parent2);
                right(parent2, node1);
            }
        } else {
            assert root == node2;
            root = node1;
        }
        parent(node1, parent2);
        parent(node2, parent1);
        final int left1 = left(node1);
        final int left2 = left(node2);
        left(node1, left2);
        if (left2 != NIL) {
            parent(left2, node1);
        }
        left(node2, left1);
        if (left1 != NIL) {
            parent(left1, node2);
        }
        final int right1 = right(node1);
        final int right2 = right(node2);
        right(node1, right2);
        if (right2 != NIL) {
            parent(right2, node1);
        }
        right(node2, right1);
        if (right1 != NIL) {
            parent(right1, node2);
        }
        final boolean color1 = color(node1);
        final boolean color2 = color(node2);
        color(node1, color2);
        color(node2, color1);

        assertConsistent(node1);
        assertConsistent(node2);
    }

    /** Remove a node from the tree. */
    public void removeNode(int nodeToRemove) {
        assert nodeToRemove != NIL;
        if (left(nodeToRemove) != NIL && right(nodeToRemove) != NIL) {
            final int next = nextNode(nodeToRemove);
            if (next != NIL) {
                swap(nodeToRemove, next);
                //copy(next, nodeToRemove);
                //nodeToRemove = next;
            }
        }

        assert left(nodeToRemove) == NIL || right(nodeToRemove) == NIL;

        final int left = left(nodeToRemove);
        int child = left != NIL ? left : right(nodeToRemove);

        beforeRemoval(nodeToRemove);

        if (child != NIL) {
            final int parent = parent(nodeToRemove);
            parent(child, parent);
            if (parent != NIL) {
                if (nodeToRemove == left(parent)) {
                    left(parent, child);
                } else {
                    assert nodeToRemove == right(parent);
                    right(parent, child);
                }
            } else {
                assert nodeToRemove == root;
                root = child;
            }
            if (color(nodeToRemove) == BLACK) {
                color(child, BLACK);
            } else {
                afterRemoval(child);
            }
        } else {
            // no children
            final int parent = parent(nodeToRemove);
            if (parent == NIL) {
                assert nodeToRemove == root;
                root = NIL;
            } else {
                afterRemoval(nodeToRemove);
                if (nodeToRemove == left(parent)) {
                    left(parent, NIL);
                } else {
                    assert nodeToRemove == right(parent);
                    right(parent, NIL);
                }
            }
        }

        releaseNode(nodeToRemove);
        --size;
    }

    /** Return the least node under <code>node</code>. */
    protected final int first(int node) {
        if (node == NIL) {
            return NIL;
        }
        while (true) {
            final int left = left(node);
            if (left == NIL) {
                break;
            }
            node = left;
        }
        return node;
    }

    /** Return the largest node under <code>node</code>. */
    protected final int last(int node) {
        while (true) {
            final int right = right(node);
            if (right == NIL) {
                break;
            }
            node = right;
        }
        return node;
    }

    /** Return the previous node. */
    public final int prevNode(int node) {
        final int left = left(node);
        if (left != NIL) {
            return last(left);
        } else {
            int parent = parent(node);
            while (parent != NIL && node == left(parent)) {
                node = parent;
                parent = parent(parent);
            }
            return parent;
        }
    }

    /** Return the next node. */
    public final int nextNode(int node) {
        final int right = right(node);
        if (right != NIL) {
            return first(right);
        } else {
            int parent = parent(node);
            while (parent != NIL && node == right(parent)) {
                node = parent;
                parent = parent(parent);
            }
            return parent;
        }
    }

    @Override
    public Iterator<IntCursor> iterator() {
        return iterator(first(root));
    }

    private Iterator<IntCursor> iterator(final int startNode) {
        return new UnmodifiableIterator<IntCursor>() {

            boolean nextSet;
            final IntCursor cursor;

            {
                cursor = new IntCursor();
                cursor.index = -1;
                cursor.value = startNode;
                nextSet = cursor.value != NIL;
            }

            boolean computeNext() {
                if (cursor.value != NIL) {
                    cursor.value = RedBlackTree.this.nextNode(cursor.value);
                }
                return nextSet = (cursor.value != NIL);
            }

            @Override
            public boolean hasNext() {
                return nextSet || computeNext();
            }

            @Override
            public IntCursor next() {
                if (!hasNext()) {
                    throw new NoSuchElementException();
                }
                nextSet = false;
                return cursor;
            }
        };
    }

    public Iterator<IntCursor> reverseIterator() {
        return reverseIterator(last(root));
    }

    private Iterator<IntCursor> reverseIterator(final int startNode) {
        return new UnmodifiableIterator<IntCursor>() {

            boolean nextSet;
            final IntCursor cursor;

            {
                cursor = new IntCursor();
                cursor.index = -1;
                cursor.value = startNode;
                nextSet = cursor.value != NIL;
            }

            boolean computeNext() {
                if (cursor.value != NIL) {
                    cursor.value = RedBlackTree.this.prevNode(cursor.value);
                }
                return nextSet = (cursor.value != NIL);
            }

            @Override
            public boolean hasNext() {
                return nextSet || computeNext();
            }

            @Override
            public IntCursor next() {
                if (!hasNext()) {
                    throw new NoSuchElementException();
                }
                nextSet = false;
                return cursor;
            }
        };
    }

    /** Return a view over the nodes that are stored after <code>startNode</code> in the tree. */
    public Iterable<IntCursor> tailSet(final int startNode) {
        return new Iterable<IntCursor>() {
            @Override
            public Iterator<IntCursor> iterator() {
                return RedBlackTree.this.iterator(startNode);
            }
        };
    }

    /** Return a reversed view over the elements of this tree. */
    public Iterable<IntCursor> reverseSet() {
        return new Iterable<IntCursor>() {
            @Override
            public Iterator<IntCursor> iterator() {
                return RedBlackTree.this.reverseIterator();
            }
        };
    }
}
