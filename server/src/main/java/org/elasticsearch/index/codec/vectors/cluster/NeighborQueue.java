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
package org.elasticsearch.index.codec.vectors.cluster;

import org.apache.lucene.util.LongHeap;
import org.apache.lucene.util.NumericUtils;

/**
 * Copied from and modified from Apache Lucene.
 */
class NeighborQueue {

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

    NeighborQueue(int initialSize, boolean maxHeap) {
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
     * @param node the node ID
     * @param score the node score
     * @return the encoded score, node ID
     */
    private long encode(int node, float score) {
        return order.apply((((long) NumericUtils.floatToSortableInt(score)) << 32) | (0xFFFFFFFFL & ~node));
    }

    /** Returns the top element's node id. */
    int topNode() {
        return decodeNodeId(heap.top());
    }

    /**
     * Returns the top element's node score. For the min heap this is the minimum score. For the max
     * heap this is the maximum score.
     */
    float topScore() {
        return decodeScore(heap.top());
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

    public void consumeNodes(int[] dest) {
        if (dest.length < size()) {
            throw new IllegalArgumentException("Destination array is too small. Expected at least " + size() + " elements.");
        }
        for (int i = 0; i < size(); i++) {
            dest[i] = decodeNodeId(heap.get(i + 1));
        }
    }

    public int consumeNodesAndScoresMin(int[] dest, float[] scores) {
        if (dest.length < size() || scores.length < size()) {
            throw new IllegalArgumentException("Destination array is too small. Expected at least " + size() + " elements.");
        }
        float bestScore = Float.POSITIVE_INFINITY;
        int bestIdx = 0;
        for (int i = 0; i < size(); i++) {
            long heapValue = heap.get(i + 1);
            scores[i] = decodeScore(heapValue);
            dest[i] = decodeNodeId(heapValue);
            if (scores[i] < bestScore) {
                bestScore = scores[i];
                bestIdx = i;
            }
        }
        return bestIdx;
    }

    public void clear() {
        heap.clear();
    }

    @Override
    public String toString() {
        return "Neighbors[" + heap.size() + "]";
    }
}
