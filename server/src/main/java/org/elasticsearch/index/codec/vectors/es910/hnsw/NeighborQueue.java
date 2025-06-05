/*
 * @notice
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * Modifications copyright (C) 2025 Elasticsearch B.V.
 */

package org.elasticsearch.index.codec.vectors.es910.hnsw;

import org.apache.lucene.util.LongHeap;
import org.apache.lucene.util.NumericUtils;

/**
 * NeighborQueue uses a {@link LongHeap} to store lists of arcs in an HNSW graph, represented as a
 * neighbor node id with an associated score packed together as a sortable long, which is sorted
 * primarily by score. The queue provides both fixed-size and unbounded operations via {@link
 * #insertWithOverflow(int, float)} and {@link #add(int, float)}, and provides MIN and MAX heap
 * subclasses.
 */
public class NeighborQueue {

    private enum Order {
        MIN_HEAP {
            @Override
            long apply(long v) {
                return v;
            }
        },
        MAX_HEAP {
            @Override
            long apply(long v) {
                // This cannot be just `-v` since Long.MIN_VALUE doesn't have a positive counterpart. It
                // needs a function that returns MAX_VALUE for MIN_VALUE and vice-versa.
                return -1 - v;
            }
        };

        abstract long apply(long v);
    }

    private final LongHeap heap;
    private final Order order;

    // Used to track the number of neighbors visited during a single graph traversal
    private int visitedCount;
    // Whether the search stopped early because it reached the visited nodes limit
    private boolean incomplete;

    public NeighborQueue(int initialSize, boolean maxHeap) {
        this.heap = new LongHeap(initialSize);
        this.order = maxHeap ? Order.MAX_HEAP : Order.MIN_HEAP;
    }

    /**
     * @return the number of elements in the heap
     */
    public int size() {
        return heap.size();
    }

    /**
     * Adds a new graph arc, extending the storage as needed.
     *
     * @param newNode the neighbor node id
     * @param newScore the score of the neighbor, relative to some other node
     */
    public void add(int newNode, float newScore) {
        heap.push(encode(newNode, newScore));
    }

    /**
     * If the heap is not full (size is less than the initialSize provided to the constructor), adds a
     * new node-and-score element. If the heap is full, compares the score against the current top
     * score, and replaces the top element if newScore is better than (greater than unless the heap is
     * reversed), the current top score.
     *
     * @param newNode the neighbor node id
     * @param newScore the score of the neighbor, relative to some other node
     */
    public boolean insertWithOverflow(int newNode, float newScore) {
        return heap.insertWithOverflow(encode(newNode, newScore));
    }

    /**
     * Encodes the node ID and its similarity score as long, preserving the Lucene tie-breaking rule
     * that when two scores are equal, the smaller node ID must win.
     *
     * <p>The most significant 32 bits represent the float score, encoded as a sortable int.
     *
     * <p>The least significant 32 bits represent the node ID.
     *
     * <p>The bits representing the node ID are complemented to guarantee the win for the smaller node
     * Id.
     *
     * <p>The AND with 0xFFFFFFFFL (a long with first 32 bits as 1) is necessary to obtain a long that
     * has:
     * - The most significant 32 bits set to 0.
     * - The least significant 32 bits represent the node ID.
     *
     * @param node the node ID
     * @param score the node score
     * @return the encoded score, node ID
     */
    private long encode(int node, float score) {
        return order.apply((((long) NumericUtils.floatToSortableInt(score)) << 32) | (0xFFFFFFFFL & ~node));
    }

    private float decodeScore(long heapValue) {
        return NumericUtils.sortableIntToFloat((int) (order.apply(heapValue) >> 32));
    }

    private int decodeNodeId(long heapValue) {
        return (int) ~(order.apply(heapValue));
    }

    /** Removes the top element and returns its node id. */
    public int pop() {
        return decodeNodeId(heap.pop());
    }

    public int[] nodes() {
        int size = size();
        int[] nodes = new int[size];
        for (int i = 0; i < size; i++) {
            nodes[i] = decodeNodeId(heap.get(i + 1));
        }
        return nodes;
    }

    /** Returns the top element's node id. */
    public int topNode() {
        return decodeNodeId(heap.top());
    }

    /**
     * Returns the top element's node score. For the min heap this is the minimum score. For the max
     * heap this is the maximum score.
     */
    public float topScore() {
        return decodeScore(heap.top());
    }

    public void clear() {
        heap.clear();
        visitedCount = 0;
        incomplete = false;
    }

    public int visitedCount() {
        return visitedCount;
    }

    public void setVisitedCount(int visitedCount) {
        this.visitedCount = visitedCount;
    }

    public boolean incomplete() {
        return incomplete;
    }

    public void markIncomplete() {
        this.incomplete = true;
    }

    @Override
    public String toString() {
        return "Neighbors[" + heap.size() + "]";
    }
}
