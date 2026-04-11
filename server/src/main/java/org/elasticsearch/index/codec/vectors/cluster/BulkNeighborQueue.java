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

import java.util.Arrays;
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
        QUICKSELECT,
        FAISS_RESERVOIR,
        SCANN_FAST,
        AUTO_V2
    }

    private final BulkLongHeap heap;
    private final FaissReservoirTopK faissQueue;
    private final ScannFastTopK scannQueue;
    private final Strategy strategy;
    private final Order order;
    private final int maxSize;
    private final long sentinelWorst;

    public BulkNeighborQueue(int maxSize, boolean maxHeap, Strategy strategy) {
        this(maxSize, maxHeap, strategy, Integer.MAX_VALUE);
    }

    public BulkNeighborQueue(int maxSize, boolean maxHeap, Strategy strategy, int totalVectorsHint) {
        if (maxSize < 1) {
            throw new IllegalArgumentException("maxSize must be >= 1");
        }
        if (totalVectorsHint < 1) {
            throw new IllegalArgumentException("totalVectorsHint must be >= 1");
        }
        this.order = maxHeap ? Order.MAX_HEAP : Order.MIN_HEAP;
        this.maxSize = maxSize;
        this.strategy = resolveStrategy(strategy, maxHeap, maxSize, totalVectorsHint);
        this.heap = (this.strategy == Strategy.BINARY || this.strategy == Strategy.QUICKSELECT) ? new BulkLongHeap(maxSize, order) : null;
        this.faissQueue = this.strategy == Strategy.FAISS_RESERVOIR ? new FaissReservoirTopK(maxSize, order) : null;
        this.scannQueue = this.strategy == Strategy.SCANN_FAST ? new ScannFastTopK(maxSize, order) : null;
        float worstScore = order == Order.MIN_HEAP ? Float.NEGATIVE_INFINITY : Float.POSITIVE_INFINITY;
        this.sentinelWorst = order.apply(encodeRaw(0, worstScore));
    }

    private static Strategy resolveStrategy(Strategy strategy, boolean maxHeap, int maxSize, int totalVectorsHint) {
        if (strategy == Strategy.BINARY) {
            return Strategy.BINARY;
        }
        if (strategy == Strategy.QUICKSELECT) {
            if (maxHeap) {
                throw new IllegalArgumentException("Quickselect strategy requires min-heap");
            }
            return Strategy.QUICKSELECT;
        }
        if (strategy == Strategy.FAISS_RESERVOIR || strategy == Strategy.SCANN_FAST || strategy == Strategy.AUTO_V2) {
            if (maxHeap) {
                throw new IllegalArgumentException("Reservoir strategies require min-heap");
            }
            if (strategy == Strategy.AUTO_V2) {
                if (totalVectorsHint <= maxSize) {
                    return Strategy.BINARY;
                }
                return maxSize <= 5 ? Strategy.SCANN_FAST : Strategy.FAISS_RESERVOIR;
            }
            return strategy;
        }
        throw new IllegalArgumentException("Unknown strategy " + strategy);
    }

    public int size() {
        return switch (strategy) {
            case BINARY, QUICKSELECT -> heap.size();
            case FAISS_RESERVOIR -> faissQueue.size();
            case SCANN_FAST -> scannQueue.size();
            case AUTO_V2 -> throw new IllegalStateException("AUTO_V2 must be resolved in constructor");
        };
    }

    /**
     * Returns the top element if the heap is full. Otherwise returns a sentinel
     * worst element that signals the heap is not yet built.
     */
    public long peek() {
        if (size() < maxSize) {
            return sentinelWorst;
        }
        return switch (strategy) {
            case BINARY, QUICKSELECT -> heap.top();
            case FAISS_RESERVOIR -> faissQueue.peek();
            case SCANN_FAST -> scannQueue.peek();
            case AUTO_V2 -> throw new IllegalStateException("AUTO_V2 must be resolved in constructor");
        };
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
            case BINARY, QUICKSELECT -> heap.insertWithOverflowBulk(docs, scores, count, bestScore);
            case FAISS_RESERVOIR -> faissQueue.insertWithOverflowBulk(docs, scores, count, bestScore);
            case SCANN_FAST -> scannQueue.insertWithOverflowBulk(docs, scores, count, bestScore);
            case AUTO_V2 -> throw new IllegalStateException("AUTO_V2 must be resolved in constructor");
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
        switch (strategy) {
            case BINARY, QUICKSELECT -> {
                for (int i = 1; i <= count; i++) {
                    consumer.accept(pop());
                }
            }
            case FAISS_RESERVOIR -> faissQueue.drain(consumer);
            case SCANN_FAST -> scannQueue.drain(consumer);
            case AUTO_V2 -> throw new IllegalStateException("AUTO_V2 must be resolved in constructor");
        }
    }

    private long pop() {
        return heap.pop();
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
            assert size == maxSize;
            if (size >= maxSize) {
                if (value < heap[1]) {
                    return false;
                }
                updateTop(value);
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
                    updateTop(value);
                    accepted++;
                }
            }
            return accepted;
        }

        private long updateTop(long value) {
            heap[1] = value;
            downHeap(1);
            return heap[1];
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

        private void heapify() {
            for (int i = size >>> 1; i >= 1; i--) {
                downHeap(i);
            }
            heapified = true;
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

        private void ensureHeapified() {
            if (heapified == false) {
                heapify();
            }
        }
    }

    private static class FaissReservoirTopK {
        private final int maxSize;
        private final int bufferSize;
        private final long[] values;
        private final Order order;
        private int size;
        private long threshold = Long.MIN_VALUE;

        private FaissReservoirTopK(int maxSize, Order order) {
            this.maxSize = maxSize;
            this.bufferSize = Math.max(maxSize, maxSize * 2);
            this.values = new long[bufferSize];
            this.order = order;
        }

        private int size() {
            return Math.min(size, maxSize);
        }

        private long peek() {
            return threshold;
        }

        private int insertWithOverflowBulk(int[] docs, float[] scores, int count, float bestScore) {
            if (count <= 0) {
                return 0;
            }
            if (size >= maxSize && bestScoreDoesNotBeatThreshold(bestScore)) {
                return 0;
            }
            int accepted = 0;
            for (int i = 0; i < count; i++) {
                long encoded = order.apply(encodeRaw(docs[i], scores[i]));
                if (encoded <= threshold) {
                    continue;
                }
                values[size++] = encoded;
                accepted++;
                if (size == maxSize) {
                    threshold = minValue(values, size);
                }
                if (size == bufferSize) {
                    compactToMaxSize();
                }
            }
            return accepted;
        }

        private void drain(LongConsumer consumer) {
            compactToMaxSize();
            Arrays.sort(values, 0, size);
            for (int i = 0; i < size; i++) {
                consumer.accept(values[i]);
            }
            size = 0;
            threshold = Long.MIN_VALUE;
        }

        private void compactToMaxSize() {
            if (size <= maxSize) {
                if (size == maxSize) {
                    threshold = minValue(values, size);
                }
                return;
            }
            int start = size - maxSize;
            quickSelect(values, 0, size - 1, start);
            long min = Long.MAX_VALUE;
            for (int i = 0; i < maxSize; i++) {
                long value = values[start + i];
                values[i] = value;
                if (value < min) {
                    min = value;
                }
            }
            size = maxSize;
            threshold = min;
        }

        private boolean bestScoreDoesNotBeatThreshold(float bestScore) {
            long bestEncoded = order.apply(encodeRaw(0, bestScore));
            return bestEncoded <= threshold;
        }
    }

    private static class ScannFastTopK {
        private final int maxSize;
        private final int capacity;
        private final long[] values;
        private final Order order;
        private int size;
        private long threshold = Long.MIN_VALUE;

        private ScannFastTopK(int maxSize, Order order) {
            this.maxSize = maxSize;
            this.capacity = Math.max(maxSize, maxSize * 2);
            this.values = new long[capacity];
            this.order = order;
        }

        private int size() {
            return Math.min(size, maxSize);
        }

        private long peek() {
            return threshold;
        }

        private int insertWithOverflowBulk(int[] docs, float[] scores, int count, float bestScore) {
            if (count <= 0) {
                return 0;
            }
            if (size >= maxSize && bestScoreDoesNotBeatThreshold(bestScore)) {
                return 0;
            }
            int accepted = 0;
            for (int i = 0; i < count; i++) {
                long encoded = order.apply(encodeRaw(docs[i], scores[i]));
                if (encoded <= threshold) {
                    continue;
                }
                values[size++] = encoded;
                accepted++;
                if (size == maxSize) {
                    threshold = minValue(values, size);
                }
                if (size == capacity) {
                    garbageCollect();
                }
            }
            return accepted;
        }

        private void drain(LongConsumer consumer) {
            compactToMaxSize();
            Arrays.sort(values, 0, size);
            for (int i = 0; i < size; i++) {
                consumer.accept(values[i]);
            }
            size = 0;
            threshold = Long.MIN_VALUE;
        }

        private void garbageCollect() {
            if (size <= maxSize) {
                if (size == maxSize) {
                    threshold = minValue(values, size);
                }
                return;
            }
            int keepMax = (maxSize + capacity) / 2 - 1;
            if (keepMax < maxSize) {
                keepMax = maxSize;
            }
            int start = size - keepMax;
            quickSelect(values, 0, size - 1, start);
            long min = Long.MAX_VALUE;
            for (int i = 0; i < keepMax; i++) {
                long value = values[start + i];
                values[i] = value;
                if (value < min) {
                    min = value;
                }
            }
            size = keepMax;
            threshold = min;
        }

        private void compactToMaxSize() {
            if (size <= maxSize) {
                if (size == maxSize) {
                    threshold = minValue(values, size);
                }
                return;
            }
            int start = size - maxSize;
            quickSelect(values, 0, size - 1, start);
            long min = Long.MAX_VALUE;
            for (int i = 0; i < maxSize; i++) {
                long value = values[start + i];
                values[i] = value;
                if (value < min) {
                    min = value;
                }
            }
            size = maxSize;
            threshold = min;
        }

        private boolean bestScoreDoesNotBeatThreshold(float bestScore) {
            long bestEncoded = order.apply(encodeRaw(0, bestScore));
            return bestEncoded <= threshold;
        }
    }

    private static void quickSelect(long[] values, int left, int right, int k) {
        while (left < right) {
            long pivot = values[left + ((right - left) >>> 1)];
            int i = left;
            int j = right;
            while (i <= j) {
                while (values[i] < pivot) {
                    i++;
                }
                while (values[j] > pivot) {
                    j--;
                }
                if (i <= j) {
                    long tmp = values[i];
                    values[i] = values[j];
                    values[j] = tmp;
                    i++;
                    j--;
                }
            }
            if (k <= j) {
                right = j;
            } else if (k >= i) {
                left = i;
            } else {
                return;
            }
        }
    }

    private static long minValue(long[] values, int size) {
        long min = Long.MAX_VALUE;
        for (int i = 0; i < size; i++) {
            long value = values[i];
            if (value < min) {
                min = value;
            }
        }
        return min;
    }

}
