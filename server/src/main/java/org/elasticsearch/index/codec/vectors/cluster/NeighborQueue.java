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

    public long peek() {
        return heap.top();
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

    public boolean insertWithOverflow(long encoded) {
        return heap.insertWithOverflow(encoded);
    }

    /**
     * Encodes the node ID and its similarity score as long, preserving the Lucene tie-breaking rule
     * that when two scores are equal, the smaller node ID must win.
     * @param node the node ID
     * @param score the node score
     * @return the encoded score, node ID
     */
    public long encode(int node, float score) {
        return order.apply(encodeRaw(node, score));
    }

    /** Returns the top element's node id. */
    public int topNode() {
        return decodeNodeId(heap.top());
    }

    public static long encodeRaw(int node, float score) {
        return (((long) NumericUtils.floatToSortableInt(score)) << 32) | (0xFFFFFFFFL & ~node);
    }

    public static float decodeScoreRaw(long heapValue) {
        return NumericUtils.sortableIntToFloat((int) (heapValue >> 32));
    }

    /**
     * Returns the top element's node score. For the min heap this is the minimum score. For the max
     * heap this is the maximum score.
     */
    public float topScore() {
        return decodeScore(heap.top());
    }

    public float decodeScore(long heapValue) {
        return NumericUtils.sortableIntToFloat((int) (order.apply(heapValue) >> 32));
    }

    private int decodeNodeId(long heapValue) {
        return (int) ~(order.apply(heapValue));
    }

    /** Removes the top element and returns its node id. */
    public int pop() {
        return decodeNodeId(heap.pop());
    }

    /** Removes the top element and returns it */
    public long popRaw() {
        return heap.pop();
    }

    /**
     * if the new element is the new top then return its node id. Otherwise,
     * removes the current top element, returns its node id and adds the new element
     * to the queue.
     * */
    public int popAndAddRaw(long raw) {
        long top = heap.top();
        if (raw < top) {
            return decodeNodeId(raw);
        }
        heap.updateTop(raw);
        return decodeNodeId(top);
    }

    public void clear() {
        heap.clear();
    }

    @Override
    public String toString() {
        return "Neighbors[" + heap.size() + "]";
    }
}
