/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.lucene.analysis.miscellaneous;

import org.apache.lucene.util.RamUsageEstimator;

/**
 * A Trie structure for analysing byte streams for duplicate sequences. Bytes
 * from a stream are added one at a time using the addByte method and the number
 * of times it has been seen as part of a sequence is returned.
 * <p>
 * The minimum required length for a duplicate sequence detected is 6 bytes.
 * <p>
 * The design goals are to maximize speed of lookup while minimizing the space
 * required to do so. This has led to a hybrid solution for representing the
 * bytes that make up a sequence in the trie.
 * <p>
 * If we have 6 bytes in sequence e.g. abcdef then they are represented as
 * object nodes in the tree as follows:
 * <p>
 * (a)-(b)-(c)-(def as an int)
 * <p>
 * {@link RootTreeNode} objects are used for the first two levels of the tree
 * (representing bytes a and b in the example sequence). The combinations of
 * objects at these 2 levels are few so internally these objects allocate an
 * array of 256 child node objects to quickly address children by indexing
 * directly into the densely packed array using a byte value. The third level in
 * the tree holds {@link LightweightTreeNode} nodes that have few children
 * (typically much less than 256) and so use a dynamically-grown array to hold
 * child nodes as simple int primitives. These ints represent the final 3 bytes
 * of a sequence and also hold a count of the number of times the entire sequence
 * path has been visited (count is a single byte).
 * <p>
 * The Trie grows indefinitely as more content is added and while theoretically
 * it could be massive (a 6-depth tree could produce 256^6 nodes) non-random
 * content e.g English text contains fewer variations.
 * <p>
 * In future we may look at using one of these strategies when memory is tight:
 * <ol>
 * <li>auto-pruning methods to remove less-visited parts of the tree
 * <li>auto-reset to wipe the whole tree and restart when a memory threshold is
 * reached
 * <li>halting any growth of the tree
 * </ol>
 * Tests on real-world-text show that the size of the tree is a multiple of the
 * input text where that multiplier varies between 10 and 5 times as the content
 * size increased from 10 to 100 megabytes of content.
 */
public class DuplicateByteSequenceSpotter {
    public static final int TREE_DEPTH = 6;
    // The maximum number of repetitions that are counted
    public static final int MAX_HIT_COUNT = 255;
    private final TreeNode root;
    private boolean sequenceBufferFilled = false;
    private final byte[] sequenceBuffer = new byte[TREE_DEPTH];
    private int nextFreePos = 0;

    // ==Performance info
    private final int[] nodesAllocatedByDepth;
    private int nodesResizedByDepth;
    // ==== RAM usage estimation settings ====
    private long bytesAllocated;
    // Root node object plus inner-class reference to containing "this"
    // (profiler suggested this was a cost)
    static final long TREE_NODE_OBJECT_SIZE = RamUsageEstimator.NUM_BYTES_OBJECT_HEADER + RamUsageEstimator.NUM_BYTES_OBJECT_REF;
    // A TreeNode specialization with an array ref (dynamically allocated and
    // fixed-size)
    static final long ROOT_TREE_NODE_OBJECT_SIZE = TREE_NODE_OBJECT_SIZE + RamUsageEstimator.NUM_BYTES_OBJECT_REF;
    // A KeyedTreeNode specialization with an array ref (dynamically allocated
    // and grown)
    static final long LIGHTWEIGHT_TREE_NODE_OBJECT_SIZE = TREE_NODE_OBJECT_SIZE + RamUsageEstimator.NUM_BYTES_OBJECT_REF;
    // A KeyedTreeNode specialization with a short-based hit count and a
    // sequence of bytes encoded as an int
    static final long LEAF_NODE_OBJECT_SIZE = TREE_NODE_OBJECT_SIZE + Short.BYTES + Integer.BYTES;

    public DuplicateByteSequenceSpotter() {
        this.nodesAllocatedByDepth = new int[4];
        this.bytesAllocated = 0;
        root = new RootTreeNode((byte) 1, null, 0);
    }

    /**
     * Reset the sequence detection logic to avoid any continuation of the
     * immediately previous bytes. A minimum of dupSequenceSize bytes need to be
     * added before any new duplicate sequences will be reported.
     * Hit counts are not reset by calling this method.
     */
    public void startNewSequence() {
        sequenceBufferFilled = false;
        nextFreePos = 0;
    }

    /**
     * Add a byte to the sequence.
     * @param b
     *            the next byte in a sequence
     * @return number of times this byte and the preceding 6 bytes have been
     *         seen before as a sequence (only counts up to 255)
     *
     */
    public short addByte(byte b) {
        // Add latest byte to circular buffer
        sequenceBuffer[nextFreePos] = b;
        nextFreePos++;
        if (nextFreePos >= sequenceBuffer.length) {
            nextFreePos = 0;
            sequenceBufferFilled = true;
        }
        if (sequenceBufferFilled == false) {
            return 0;
        }
        TreeNode node = root;
        // replay updated sequence of bytes represented in the circular
        // buffer starting from the tail
        int p = nextFreePos;

        // The first tier of nodes are addressed using individual bytes from the
        // sequence
        node = node.add(sequenceBuffer[p], 0);
        p = nextBufferPos(p);
        node = node.add(sequenceBuffer[p], 1);
        p = nextBufferPos(p);
        node = node.add(sequenceBuffer[p], 2);

        // The final 3 bytes in the sequence are represented in an int
        // where the 4th byte will contain a hit count.

        p = nextBufferPos(p);
        int sequence = 0xFF & sequenceBuffer[p];
        p = nextBufferPos(p);
        sequence = sequence << 8 | (0xFF & sequenceBuffer[p]);
        p = nextBufferPos(p);
        sequence = sequence << 8 | (0xFF & sequenceBuffer[p]);
        return (short) (node.add(sequence << 8) - 1);
    }

    private int nextBufferPos(int p) {
        p++;
        if (p >= sequenceBuffer.length) {
            p = 0;
        }
        return p;
    }

    /**
     * Base class for nodes in the tree. Subclasses are optimised for use at
     * different locations in the tree - speed-optimized nodes represent
     * branches near the root while space-optimized nodes are used for deeper
     * leaves/branches.
     */
    abstract class TreeNode {

        TreeNode(byte key, TreeNode parentNode, int depth) {
            nodesAllocatedByDepth[depth]++;
        }

        public abstract TreeNode add(byte b, int depth);

        /**
         *
         * @param byteSequence
         *            a sequence of bytes encoded as an int
         * @return the number of times the full sequence has been seen (counting
         *         up to a maximum of 32767).
         */
        public abstract short add(int byteSequence);
    }

    // Node implementation for use at the root of the tree that sacrifices space
    // for speed.
    class RootTreeNode extends TreeNode {

        // A null-or-256 sized array that can be indexed into using a byte for
        // fast access.
        // Being near the root of the tree it is expected that this is a
        // non-sparse array.
        TreeNode[] children;

        RootTreeNode(byte key, TreeNode parentNode, int depth) {
            super(key, parentNode, depth);
            bytesAllocated += ROOT_TREE_NODE_OBJECT_SIZE;
        }

        public TreeNode add(byte b, int depth) {
            if (children == null) {
                children = new TreeNode[256];
                bytesAllocated += (RamUsageEstimator.NUM_BYTES_OBJECT_REF * 256);
            }
            int bIndex = 0xFF & b;
            TreeNode node = children[bIndex];
            if (node == null) {
                if (depth <= 1) {
                    // Depths 0 and 1 use RootTreeNode impl and create
                    // RootTreeNodeImpl children
                    node = new RootTreeNode(b, this, depth);
                } else {
                    // Deeper-level nodes are less visited but more numerous
                    // so use a more space-friendly data structure
                    node = new LightweightTreeNode(b, this, depth);
                }
                children[bIndex] = node;
            }
            return node;
        }

        @Override
        public short add(int byteSequence) {
            throw new UnsupportedOperationException("Root nodes do not support byte sequences encoded as integers");
        }

    }

    // Node implementation for use by the depth 3 branches of the tree that
    // sacrifices speed for space.
    final class LightweightTreeNode extends TreeNode {

        // An array dynamically resized but frequently only sized 1 as most
        // sequences leading to end leaves are one-off paths.
        // It is scanned for matches sequentially and benchmarks showed
        // that sorting contents on insertion didn't improve performance.
        int[] children = null;

        LightweightTreeNode(byte key, TreeNode parentNode, int depth) {
            super(key, parentNode, depth);
            bytesAllocated += LIGHTWEIGHT_TREE_NODE_OBJECT_SIZE;

        }

        @Override
        public short add(int byteSequence) {
            if (children == null) {
                // Create array adding new child with the byte sequence combined with hitcount of 1.
                // Most nodes at this level we expect to have only 1 child so we start with the
                // smallest possible child array.
                children = new int[1];
                bytesAllocated += RamUsageEstimator.NUM_BYTES_ARRAY_HEADER + Integer.BYTES;
                children[0] = byteSequence + 1;
                return 1;
            }
            // Find existing child and if discovered increment count
            for (int i = 0; i < children.length; i++) {
                int child = children[i];
                if (byteSequence == (child & 0xFFFFFF00)) {
                    int hitCount = child & 0xFF;
                    if (hitCount < MAX_HIT_COUNT) {
                        children[i]++;
                    }
                    return (short) (hitCount + 1);
                }
            }
            // Grow array adding new child
            int[] newChildren = new int[children.length + 1];
            bytesAllocated += Integer.BYTES;

            System.arraycopy(children, 0, newChildren, 0, children.length);
            children = newChildren;
            // Combine the byte sequence with a hit count of 1 into an int.
            children[newChildren.length - 1] = byteSequence + 1;
            nodesResizedByDepth++;
            return 1;
        }

        @Override
        public TreeNode add(byte b, int depth) {
            throw new UnsupportedOperationException("Leaf nodes do not take byte sequences");
        }

    }

    public final long getEstimatedSizeInBytes() {
        return bytesAllocated;
    }

    /**
     * @return Performance info - the number of nodes allocated at each depth
     */
    public int[] getNodesAllocatedByDepth() {
        return nodesAllocatedByDepth.clone();
    }

    /**
     * @return Performance info - the number of resizing of children arrays, at
     *         each depth
     */
    public int getNodesResizedByDepth() {
        return nodesResizedByDepth;
    }

}
