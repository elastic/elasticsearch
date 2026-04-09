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
 * Modifications copyright (C) 2026 Elasticsearch B.V.
 */
package org.elasticsearch.index.codec.vectors.cluster;

import org.apache.lucene.util.NumericUtils;

import java.util.function.LongConsumer;

/**
 * A bulk-only neighbor queue that supports insert-with-overflow from arrays.
 */
public class BulkNeighborQueue {

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

    public enum Strategy {
        BINARY,
        QUICKSELECT
    }

    private final Strategy strategy;
    private final BulkLongHeap heap;
    private final Order order;
    private final int maxSize;
    private final long sentinelWorst;
    private long[] encodedScratch;

    public BulkNeighborQueue(int maxSize, boolean maxHeap) {
        this(maxSize, maxHeap, Strategy.BINARY);
    }

    public BulkNeighborQueue(int maxSize, boolean maxHeap, Strategy strategy) {
        if (maxSize < 1) {
            throw new IllegalArgumentException("maxSize must be >= 1");
        }
        if (strategy != Strategy.BINARY && maxHeap) {
            throw new IllegalArgumentException("Quickselect strategy requires min-heap");
        }
        this.strategy = strategy;
        this.order = maxHeap ? Order.MAX_HEAP : Order.MIN_HEAP;
        this.maxSize = maxSize;
        this.heap = (strategy == Strategy.BINARY || strategy == Strategy.QUICKSELECT) ? new BulkLongHeap(maxSize, order) : null;
        float worstScore = order == Order.MIN_HEAP ? Float.NEGATIVE_INFINITY : Float.POSITIVE_INFINITY;
        this.sentinelWorst = order.apply(encodeRaw(0, worstScore));
    }

    public int size() {
        return heap.size();
    }

    /**
     * Returns the top element if the heap is full. Otherwise returns a sentinel
     * worst element that signals the heap is not yet built.
     */
    public long peek() {
        if (size() < maxSize) {
            return sentinelWorst;
        }
        return heap.top();
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

    public static long encodeRaw(int node, float score) {
        return (((long) NumericUtils.floatToSortableInt(score)) << 32) | (0xFFFFFFFFL & ~node);
    }

    public static float decodeScoreRaw(long heapValue) {
        return NumericUtils.sortableIntToFloat((int) (heapValue >> 32));
    }

    public float decodeScore(long heapValue) {
        return NumericUtils.sortableIntToFloat((int) (order.apply(heapValue) >> 32));
    }

    public int decodeNodeId(long heapValue) {
        return (int) ~(order.apply(heapValue));
    }

    /**
     * Adds a batch of node-and-score elements using insert-with-overflow semantics.
     * @param docs node ids
     * @param scores node scores
     * @param count number of entries to read from docs/scores
     * @param bestScore best score in the batch, used for fast rejection
     * @return the number of elements that were accepted (added or replaced).
     */
    public int insertWithOverflowBulk(int[] docs, float[] scores, int count, float bestScore) {
        return switch (strategy) {
            case BINARY -> heap.insertWithOverflowBulk(docs, scores, count, bestScore);
            case QUICKSELECT -> insertWithOverflowBulkQuickselect(docs, scores, count, bestScore);
        };
    }

    /**
     * Drains the heap in sorted order (best to worst), providing encoded values.
     */
    public void drain(LongConsumer consumer) {
        int count = size();
        if (count == 0) {
            return;
        }
        long[] buffer = new long[count];
        for (int i = 0; i < count; i++) {
            buffer[i] = pop();
        }
        for (int i = count - 1; i >= 0; i--) {
            consumer.accept(buffer[i]);
        }
    }

    private long pop() {
        return heap.pop();
    }

    private int insertWithOverflowBulkQuickselect(int[] docs, float[] scores, int count, float bestScore) {
        if (count <= 0) {
            return 0;
        }
        if (heap.size() >= maxSize && bestScoreDoesNotBeatTop(bestScore)) {
            return 0;
        }
        if (heap.size() == 0 && count >= maxSize) {
            ensureEncodedScratch(count);
            for (int i = 0; i < count; i++) {
                encodedScratch[i] = encode(docs[i], scores[i]);
            }
            int start = selectTopK(encodedScratch, count, maxSize);
            heap.resetWithEncoded(encodedScratch, start, maxSize);
            return maxSize;
        }
        return heap.insertWithOverflowBulk(docs, scores, count, bestScore);
    }

    private boolean bestScoreDoesNotBeatTop(float bestScore) {
        float topScore = decodeScore(peek());
        if (order == Order.MIN_HEAP) {
            return bestScore <= topScore;
        }
        return bestScore >= topScore;
    }

    /**
     * Partitions the array so that the top {@code k} elements (largest values) are in the tail.
     * @return the start index of the top-k region.
     */
    private static int selectTopK(long[] values, int count, int k) {
        int target = count - k;
        int left = 0;
        int right = count - 1;
        while (left < right) {
            int pivotIndex = partition(values, left, right);
            if (pivotIndex == target) {
                return target;
            } else if (pivotIndex < target) {
                left = pivotIndex + 1;
            } else {
                right = pivotIndex - 1;
            }
        }
        return target;
    }

    private static int partition(long[] values, int left, int right) {
        int mid = left + ((right - left) >>> 1);
        long pivot = values[mid];
        swap(values, mid, right);
        int store = left;
        for (int i = left; i < right; i++) {
            if (values[i] < pivot) {
                swap(values, store++, i);
            }
        }
        swap(values, store, right);
        return store;
    }

    private static void swap(long[] values, int i, int j) {
        long tmp = values[i];
        values[i] = values[j];
        values[j] = tmp;
    }

    private void ensureEncodedScratch(int count) {
        if (encodedScratch == null || encodedScratch.length < count) {
            encodedScratch = new long[count];
        }
    }

    private static class BulkLongHeap {

        private long[] heap;
        private int size;
        private final int maxSize;
        private final Order order;
        private boolean heapified = true;

        private BulkLongHeap(int maxSize, Order order) {
            this.maxSize = maxSize;
            this.order = order;
            this.heap = new long[Math.max(2, maxSize + 1)];
        }

        private int size() {
            return size;
        }

        private long top() {
            ensureHeapified();
            return heap[1];
        }

        private long pop() {
            ensureHeapified();
            long result = heap[1];
            heap[1] = heap[size];
            size--;
            if (size > 0) {
                downHeap(1);
            }
            return result;
        }

        private boolean insertWithOverflow(long value) {
            if (size < maxSize) {
                push(value);
                return true;
            }
            ensureHeapified();
            if (size > 0 && value > heap[1]) {
                heap[1] = value;
                downHeap(1);
                return true;
            }
            return false;
        }

        private int insertWithOverflowBulk(int[] docs, float[] scores, int count, float bestScore) {
            if (count <= 0) {
                return 0;
            }
            if (size >= maxSize && bestScoreDoesNotBeatTop(bestScore)) {
                return 0;
            }
            if (size < maxSize) {
                return insertWithOverflowBulkWhileGrowing(docs, scores, count);
            }
            int accepted = 0;
            for (int i = 0; i < count; i++) {
                long value = encode(docs[i], scores[i]);
                if (insertWithOverflow(value)) {
                    accepted++;
                }
            }
            return accepted;
        }

        private int insertWithOverflowBulkWhileGrowing(int[] docs, float[] scores, int count) {
            int fill = Math.min(count, maxSize - size);
            for (int i = 0; i < fill; i++) {
                heap[size + 1 + i] = encode(docs[i], scores[i]);
            }
            size += fill;
            heapified = false;
            if (count <= fill) {
                return fill;
            }
            ensureHeapified();
            int accepted = fill;
            for (int i = fill; i < count; i++) {
                long value = encode(docs[i], scores[i]);
                if (value > heap[1]) {
                    heap[1] = value;
                    downHeap(1);
                    accepted++;
                }
            }
            return accepted;
        }

        private boolean bestScoreDoesNotBeatTop(float bestScore) {
            float topScore = decodeScore(heap[1]);
            if (order == Order.MIN_HEAP) {
                return bestScore <= topScore;
            }
            return bestScore >= topScore;
        }

        private long encode(int doc, float score) {
            return order.apply(encodeRaw(doc, score));
        }

        private float decodeScore(long heapValue) {
            return NumericUtils.sortableIntToFloat((int) (order.apply(heapValue) >> 32));
        }

        private void push(long value) {
            assert size < maxSize : "heap size exceeded maxSize";
            size++;
            heap[size] = value;
            if (heapified) {
                upHeap(size);
            }
        }

        private void heapify() {
            for (int i = size >>> 1; i >= 1; i--) {
                downHeap(i);
            }
            heapified = true;
        }

        private void upHeap(int i) {
            long value = heap[i];
            int j = i >>> 1;
            while (j > 0 && value < heap[j]) {
                heap[i] = heap[j];
                i = j;
                j = i >>> 1;
            }
            heap[i] = value;
        }

        private void downHeap(int i) {
            long value = heap[i];
            int j = i << 1;
            while (j <= size) {
                if (j < size && heap[j + 1] < heap[j]) {
                    j++;
                }
                if (heap[j] >= value) {
                    break;
                }
                heap[i] = heap[j];
                i = j;
                j = i << 1;
            }
            heap[i] = value;
        }

        private void ensureCapacity(int targetSize) {
            assert targetSize <= maxSize : "heap size exceeded maxSize";
        }

        private void grow() {
            throw new IllegalStateException("heap growth is not supported");
        }

        private void resetWithEncoded(long[] encoded, int offset, int length) {
            assert length <= maxSize : "heap size exceeded maxSize";
            System.arraycopy(encoded, offset, heap, 1, length);
            size = length;
            heapify();
        }

        private void ensureHeapified() {
            if (heapified == false) {
                heapify();
            }
        }
    }

}
