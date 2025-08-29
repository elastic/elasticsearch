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

import org.apache.lucene.util.ArrayUtil;
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

    public void bulkTransfer(int limit, long[] buffer, NeighborQueue neighborQueue) {
        assert buffer.length >= limit;
        assert this.order == neighborQueue.order;
        if (limit <= 0) {
            return;
        }
        int toTransfer = Math.min(limit, neighborQueue.size());
        for (int i = 0; i < toTransfer; i++) {
            buffer[i] = neighborQueue.rawPop();
        }
        heap.bulkPushOrderedValues(buffer, 0, toTransfer);
    }

    public void addRaw(long value) {
        heap.push(value);
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

    public void clear() {
        heap.clear();
    }

    public long rawPop() {
        return heap.pop();
    }

    @Override
    public String toString() {
        return "Neighbors[" + heap.size() + "]";
    }

    // copied and modified from Lucene
    private static final class LongHeap {

        private final int maxSize;

        private long[] heap;
        private int size = 0;

        private LongHeap(int maxSize) {
            final int heapSize;
            if (maxSize < 1 || maxSize >= ArrayUtil.MAX_ARRAY_LENGTH) {
                // Throw exception to prevent confusing OOME:
                throw new IllegalArgumentException("maxSize must be > 0 and < " + (ArrayUtil.MAX_ARRAY_LENGTH - 1) + "; got: " + maxSize);
            }
            // NOTE: we add +1 because all access to heap is 1-based not 0-based. heap[0] is unused.
            heapSize = maxSize + 1;
            this.maxSize = maxSize;
            this.heap = new long[heapSize];
        }

        private long bulkPushOrderedValues(long[] values, int offset, int length) {
            if (length < 0 || length > values.length - offset) {
                throw new IllegalArgumentException("Illegal length: " + length);
            }
            // we know that values are ordered, bulk push them optimally, increasing heap size as needed
            int newSize = size + length;
            if (newSize >= heap.length) {
                heap = ArrayUtil.grow(heap, (newSize * 3 + 1) / 2);
            }
            System.arraycopy(values, offset, heap, size + 1, length);
            if (size == 0) {
                // no need to heapify, just set the size
                size = newSize;
                return heap[1];
            }
            size = newSize;
            // heapify the new elements
            for (int i = size - length; i < size; i++) {
                upHeap(i);
            }
            return heap[1];
        }

        private long push(long element) {
            size++;
            if (size == heap.length) {
                heap = ArrayUtil.grow(heap, (size * 3 + 1) / 2);
            }
            heap[size] = element;
            upHeap(size);
            return heap[1];
        }

        private boolean insertWithOverflow(long value) {
            if (size >= maxSize) {
                if (value < heap[1]) {
                    return false;
                }
                updateTop(value);
                return true;
            }
            push(value);
            return true;
        }

        private long top() {
            return heap[1];
        }

        private long pop() {
            if (size > 0) {
                long result = heap[1]; // save first value
                heap[1] = heap[size]; // move last to first
                size--;
                downHeap(1); // adjust heap
                return result;
            } else {
                throw new IllegalStateException("The heap is empty");
            }
        }

        private long updateTop(long value) {
            heap[1] = value;
            downHeap(1);
            return heap[1];
        }

        private int size() {
            return size;
        }

        private void clear() {
            size = 0;
        }

        private void upHeap(int origPos) {
            int i = origPos;
            long value = heap[i]; // save bottom value
            int j = i >>> 1;
            while (j > 0 && value < heap[j]) {
                heap[i] = heap[j]; // shift parents down
                i = j;
                j = j >>> 1;
            }
            heap[i] = value; // install saved value
        }

        private void downHeap(int i) {
            long value = heap[i]; // save top value
            int j = i << 1; // find smaller child
            int k = j + 1;
            if (k <= size && heap[k] < heap[j]) {
                j = k;
            }
            while (j <= size && heap[j] < value) {
                heap[i] = heap[j]; // shift up child
                i = j;
                j = i << 1;
                k = j + 1;
                if (k <= size && heap[k] < heap[j]) {
                    j = k;
                }
            }
            heap[i] = value; // install saved value
        }
    }
}
