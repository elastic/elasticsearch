/*
 * Licensed to Elasticsearch B.V. under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch B.V. licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 * This project is based on a modification of https://github.com/tdunning/t-digest which is licensed under the Apache 2.0 License.
 */

package org.elasticsearch.tdigest;

import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.tdigest.arrays.TDigestArrays;
import org.elasticsearch.tdigest.arrays.TDigestByteArray;
import org.elasticsearch.tdigest.arrays.TDigestIntArray;

/**
 * An AVL-tree structure stored in parallel arrays.
 * This class only stores the tree structure, so you need to extend it if you
 * want to add data to the nodes, typically by using arrays and node
 * identifiers as indices.
 */
abstract class IntAVLTree implements Releasable, Accountable {
    static final long SHALLOW_SIZE = RamUsageEstimator.shallowSizeOfInstance(IntAVLTree.class);
    /**
     * We use <code>0</code> instead of <code>-1</code> so that left(NIL) works without
     * condition.
     */
    protected static final int NIL = 0;

    /** Grow a size by 1/8. */
    static int oversize(int size) {
        return size + (size >>> 3);
    }

    private final TDigestArrays arrays;
    private boolean closed = false;

    private final NodeAllocator nodeAllocator;
    private int root;
    private final TDigestIntArray parent;
    private final TDigestIntArray left;
    private final TDigestIntArray right;
    private final TDigestByteArray depth;

    IntAVLTree(TDigestArrays arrays, int initialCapacity) {
        this.arrays = arrays;
        root = NIL;

        NodeAllocator nodeAllocator = null;
        TDigestIntArray parent = null;
        TDigestIntArray left = null;
        TDigestIntArray right = null;
        TDigestByteArray depth = null;

        try {
            this.nodeAllocator = nodeAllocator = NodeAllocator.create(arrays);
            this.parent = parent = arrays.newIntArray(initialCapacity);
            this.left = left = arrays.newIntArray(initialCapacity);
            this.right = right = arrays.newIntArray(initialCapacity);
            this.depth = depth = arrays.newByteArray(initialCapacity);

            nodeAllocator = null;
            parent = null;
            left = null;
            right = null;
            depth = null;
        } finally {
            Releasables.close(nodeAllocator, parent, left, right, depth);
        }
    }

    IntAVLTree(TDigestArrays arrays) {
        this(arrays, 16);
    }

    @Override
    public long ramBytesUsed() {
        return SHALLOW_SIZE + nodeAllocator.ramBytesUsed() + parent.ramBytesUsed() + left.ramBytesUsed() + right.ramBytesUsed() + depth
            .ramBytesUsed();
    }

    /**
     * Return the current root of the tree.
     */
    public int root() {
        return root;
    }

    /**
     * Return the current capacity, which is the number of nodes that this tree
     * can hold.
     */
    public int capacity() {
        return parent.size();
    }

    /**
     * Resize internal storage in order to be able to store data for nodes up to
     * <code>newCapacity</code> (excluded).
     */
    protected void resize(int newCapacity) {
        parent.resize(newCapacity);
        left.resize(newCapacity);
        right.resize(newCapacity);
        depth.resize(newCapacity);
    }

    /**
     * Return the size of this tree.
     */
    public int size() {
        return nodeAllocator.size();
    }

    /**
     * Return the parent of the provided node.
     */
    public int parent(int node) {
        return parent.get(node);
    }

    /**
     * Return the left child of the provided node.
     */
    public int left(int node) {
        return left.get(node);
    }

    /**
     * Return the right child of the provided node.
     */
    public int right(int node) {
        return right.get(node);
    }

    /**
     * Return the depth nodes that are stored below <code>node</code> including itself.
     */
    public int depth(int node) {
        return depth.get(node);
    }

    /**
     * Return the least node under <code>node</code>.
     */
    public int first(int node) {
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

    /**
     * Return the largest node under <code>node</code>.
     */
    public int last(int node) {
        while (true) {
            final int right = right(node);
            if (right == NIL) {
                break;
            }
            node = right;
        }
        return node;
    }

    /**
     * Return the least node that is strictly greater than <code>node</code>.
     */
    public final int next(int node) {
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

    /**
     * Return the highest node that is strictly less than <code>node</code>.
     */
    public final int prev(int node) {
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

    /**
     * Compare data against data which is stored in <code>node</code>.
     */
    protected abstract int compare(int node);

    /**
     * Compare data into <code>node</code>.
     */
    protected abstract void copy(int node);

    /**
     * Merge data into <code>node</code>.
     */
    protected abstract void merge(int node);

    /**
     * Add current data to the tree and return <code>true</code> if a new node was added
     * to the tree or <code>false</code> if the node was merged into an existing node.
     */
    public boolean add() {
        if (root == NIL) {
            root = nodeAllocator.newNode();
            copy(root);
            fixAggregates(root);
            return true;
        } else {
            int node = root;
            assert parent(root) == NIL;
            int parent;
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

            node = nodeAllocator.newNode();
            if (node >= capacity()) {
                resize(oversize(node + 1));
            }
            copy(node);
            parent(node, parent);
            if (cmp < 0) {
                left(parent, node);
            } else {
                right(parent, node);
            }

            rebalance(node);

            return true;
        }
    }

    /**
     * Find a node in this tree.
     */
    public int find() {
        for (int node = root; node != NIL;) {
            final int cmp = compare(node);
            if (cmp < 0) {
                node = left(node);
            } else if (cmp > 0) {
                node = right(node);
            } else {
                return node;
            }
        }
        return NIL;
    }

    /**
     * Update <code>node</code> with the current data.
     */
    public void update(int node) {
        final int prev = prev(node);
        final int next = next(node);
        if ((prev == NIL || compare(prev) > 0) && (next == NIL || compare(next) < 0)) {
            // Update can be done in-place
            copy(node);
            for (int n = node; n != NIL; n = parent(n)) {
                fixAggregates(n);
            }
        } else {
            // TODO: it should be possible to find the new node position without
            // starting from scratch
            remove(node);
            add();
        }
    }

    /**
     * Remove the specified node from the tree.
     */
    public void remove(int node) {
        if (node == NIL) {
            throw new IllegalArgumentException();
        }
        if (left(node) != NIL && right(node) != NIL) {
            // inner node
            final int next = next(node);
            assert next != NIL;
            swap(node, next);
        }
        assert left(node) == NIL || right(node) == NIL;

        final int parent = parent(node);
        int child = left(node);
        if (child == NIL) {
            child = right(node);
        }

        if (child == NIL) {
            // no children
            if (node == root) {
                assert size() == 1 : size();
                root = NIL;
            } else {
                if (node == left(parent)) {
                    left(parent, NIL);
                } else {
                    assert node == right(parent);
                    right(parent, NIL);
                }
            }
        } else {
            // one single child
            if (node == root) {
                assert size() == 2;
                root = child;
            } else if (node == left(parent)) {
                left(parent, child);
            } else {
                assert node == right(parent);
                right(parent, child);
            }
            parent(child, parent);
        }

        release(node);
        rebalance(parent);
    }

    private void release(int node) {
        left(node, NIL);
        right(node, NIL);
        parent(node, NIL);
        nodeAllocator.release(node);
    }

    private void swap(int node1, int node2) {
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

        final int depth1 = depth(node1);
        final int depth2 = depth(node2);
        depth(node1, depth2);
        depth(node2, depth1);
    }

    private int balanceFactor(int node) {
        return depth(left(node)) - depth(right(node));
    }

    private void rebalance(int node) {
        for (int n = node; n != NIL;) {
            final int p = parent(n);

            fixAggregates(n);

            switch (balanceFactor(n)) {
                case -2:
                    final int right = right(n);
                    if (balanceFactor(right) == 1) {
                        rotateRight(right);
                    }
                    rotateLeft(n);
                    break;
                case 2:
                    final int left = left(n);
                    if (balanceFactor(left) == -1) {
                        rotateLeft(left);
                    }
                    rotateRight(n);
                    break;
                case -1:
                case 0:
                case 1:
                    break; // ok
                default:
                    throw new AssertionError();
            }

            n = p;
        }
    }

    protected void fixAggregates(int node) {
        depth(node, 1 + Math.max(depth(left(node)), depth(right(node))));
    }

    /** Rotate left the subtree under <code>n</code> */
    private void rotateLeft(int n) {
        final int r = right(n);
        final int lr = left(r);
        right(n, lr);
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
        fixAggregates(n);
        fixAggregates(parent(n));
    }

    /** Rotate right the subtree under <code>n</code> */
    private void rotateRight(int n) {
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
        fixAggregates(n);
        fixAggregates(parent(n));
    }

    private void parent(int node, int parent) {
        assert node != NIL;
        this.parent.set(node, parent);
    }

    private void left(int node, int left) {
        assert node != NIL;
        this.left.set(node, left);
    }

    private void right(int node, int right) {
        assert node != NIL;
        this.right.set(node, right);
    }

    private void depth(int node, int depth) {
        assert node != NIL;
        assert depth >= 0 && depth <= Byte.MAX_VALUE;
        this.depth.set(node, (byte) depth);
    }

    void checkBalance(int node) {
        if (node == NIL) {
            assert depth(node) == 0;
        } else {
            assert depth(node) == 1 + Math.max(depth(left(node)), depth(right(node)));
            assert Math.abs(depth(left(node)) - depth(right(node))) <= 1;
            checkBalance(left(node));
            checkBalance(right(node));
        }
    }

    /**
     * A stack of int values.
     */
    private static class IntStack implements Releasable, Accountable {
        private static final long SHALLOW_SIZE = RamUsageEstimator.shallowSizeOfInstance(IntStack.class);

        private final TDigestArrays arrays;
        private boolean closed = false;

        private final TDigestIntArray stack;
        private int size;

        IntStack(TDigestArrays arrays) {
            this.arrays = arrays;
            stack = arrays.newIntArray(0);
            size = 0;
        }

        @Override
        public long ramBytesUsed() {
            return SHALLOW_SIZE + stack.ramBytesUsed();
        }

        int size() {
            return size;
        }

        int pop() {
            int value = stack.get(--size);
            stack.resize(size);
            return value;
        }

        void push(int v) {
            stack.resize(++size);
            stack.set(size - 1, v);
        }

        @Override
        public void close() {
            if (closed == false) {
                closed = true;
                arrays.adjustBreaker(-SHALLOW_SIZE);
                stack.close();
            }
        }
    }

    private static class NodeAllocator implements Releasable, Accountable {
        private static final long SHALLOW_SIZE = RamUsageEstimator.shallowSizeOfInstance(NodeAllocator.class);

        private final TDigestArrays arrays;
        private boolean closed = false;

        private int nextNode;
        private final IntStack releasedNodes;

        static NodeAllocator create(TDigestArrays arrays) {
            arrays.adjustBreaker(SHALLOW_SIZE);
            try {
                return new NodeAllocator(arrays);
            } catch (Exception e) {
                arrays.adjustBreaker(-SHALLOW_SIZE);
                throw e;
            }
        }

        private NodeAllocator(TDigestArrays arrays) {
            this.arrays = arrays;
            nextNode = NIL + 1;
            arrays.adjustBreaker(IntStack.SHALLOW_SIZE);
            try {
                releasedNodes = new IntStack(arrays);
            } catch (Exception e) {
                arrays.adjustBreaker(-IntStack.SHALLOW_SIZE);
                throw e;
            }
        }

        @Override
        public long ramBytesUsed() {
            return SHALLOW_SIZE + releasedNodes.ramBytesUsed();
        }

        int newNode() {
            if (releasedNodes.size() > 0) {
                return releasedNodes.pop();
            } else {
                return nextNode++;
            }
        }

        void release(int node) {
            assert node < nextNode;
            releasedNodes.push(node);
        }

        int size() {
            return nextNode - releasedNodes.size() - 1;
        }

        @Override
        public void close() {
            if (closed == false) {
                closed = true;
                arrays.adjustBreaker(-SHALLOW_SIZE);
                releasedNodes.close();
            }
        }
    }

    @Override
    public void close() {
        if (closed == false) {
            closed = true;
            arrays.adjustBreaker(-SHALLOW_SIZE);
            Releasables.close(nodeAllocator, parent, left, right, depth);
        }
    }
}
