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

import org.apache.lucene.util.LongHeap;
import org.apache.lucene.util.NumericUtils;

import java.util.Arrays;
import java.util.function.LongConsumer;

/**
 * A bulk-only neighbor queue that supports insert-with-overflow from arrays.
 */
public class BulkNeighborQueue {
    private static final int TINY_K_BINARY_THRESHOLD = 10;
    private static final long SENTINEL_WORST = encodeRaw(Integer.MAX_VALUE, 0f);

    private final int maxSize;
    private final LongHeap tinyHeap;
    private final ReservoirTopK collector;

    public BulkNeighborQueue(int maxSize) {
        this(maxSize, true);
    }

    /**
     * Creates a merge-focused queue that always uses the bulk pivot collector.
     */
    public static BulkNeighborQueue forMerging(int maxSize) {
        return new BulkNeighborQueue(maxSize, false);
    }

    private BulkNeighborQueue(int maxSize, boolean useTinyHeapWhenEligible) {
        if (maxSize < 1) {
            throw new IllegalArgumentException("maxSize must be >= 1");
        }
        this.maxSize = maxSize;
        if (useTinyHeapWhenEligible && maxSize <= TINY_K_BINARY_THRESHOLD) {
            this.tinyHeap = new LongHeap(maxSize);
            this.collector = null;
        } else {
            this.tinyHeap = null;
            this.collector = new ReservoirTopK(maxSize);
        }
    }

    public int size() {
        if (tinyHeap != null) {
            return tinyHeap.size();
        }
        assert collector != null;
        return collector.size();
    }

    /**
     * Returns the top element if the queue is full. Otherwise returns a sentinel
     * worst element that signals the queue is not yet built. The sentinel score is
     * always {@code 0f} so encoded values remain non-negative.
     */
    public long peek() {
        if (size() < maxSize) {
            return SENTINEL_WORST;
        }
        if (tinyHeap != null) {
            return tinyHeap.top();
        }
        assert collector != null;
        return collector.peek();
    }

    /**
     * Encodes the node ID and its similarity score as long, preserving the Lucene tie-breaking rule
     * that when two scores are equal, the smaller node ID must win.
     * @param node the node ID
     * @param score the node score
     * @return the encoded score, node ID
     */
    public long encode(int node, float score) {
        return encodeRaw(node, score);
    }

    public static long encodeRaw(int node, float score) {
        return (((long) NumericUtils.floatToSortableInt(score)) << 32) | (0xFFFFFFFFL & ~node);
    }

    public static float decodeScoreRaw(long heapValue) {
        return NumericUtils.sortableIntToFloat((int) (heapValue >> 32));
    }

    public float decodeScore(long heapValue) {
        return decodeScoreRaw(heapValue);
    }

    public int decodeNodeId(long heapValue) {
        return (int) ~heapValue;
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
        if (tinyHeap != null) {
            return insertWithOverflowTiny(docs, scores, count, bestScore);
        }
        assert collector != null;
        return collector.insertWithOverflowBulk(docs, scores, count, bestScore);
    }

    /**
     * Drains the queue in sorted order (worst to best), providing encoded values.
     */
    public void drain(LongConsumer consumer) {
        if (tinyHeap != null) {
            while (tinyHeap.size() > 0) {
                consumer.accept(tinyHeap.pop());
            }
            return;
        }
        assert collector != null;
        collector.drain(consumer);
    }

    /**
     * Gathers the top k, but may not be sorted
     */
    public void drainUnsorted(LongConsumer consumer) {
        if (tinyHeap != null) {
            int size = tinyHeap.size();
            for (int i = 1; i <= size; i++) {
                consumer.accept(tinyHeap.get(i));
            }
            tinyHeap.clear();
            return;
        }
        assert collector != null;
        collector.drainUnsorted(consumer);
    }

    private int insertWithOverflowTiny(int[] docs, float[] scores, int count, float bestScore) {
        assert tinyHeap != null;
        if (count <= 0) {
            return 0;
        }
        int accepted = 0;
        int i = 0;
        while (i < count && tinyHeap.size() < maxSize) {
            if (tinyHeap.insertWithOverflow(encodeRaw(docs[i], scores[i]))) {
                accepted++;
            }
            i++;
        }
        if (i == count) {
            return accepted;
        }
        if (bestScore < decodeScoreRaw(tinyHeap.top())) {
            return accepted;
        }
        for (; i < count; i++) {
            if (tinyHeap.insertWithOverflow(encodeRaw(docs[i], scores[i]))) {
                accepted++;
            }
        }
        return accepted;
    }

    private static final class ReservoirTopK {

        private static final int SINGLE_MEDIAN_THRESHOLD = 40;

        private final int maxSize;
        private final int capacity;
        private final long[] values;
        private int size;
        private long threshold = Long.MIN_VALUE;
        private float thresholdScore = Float.NEGATIVE_INFINITY;

        private ReservoirTopK(int maxSize) {
            this.maxSize = maxSize;
            this.capacity = maxSize * 2;
            this.values = new long[capacity];
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
            int accepted = 0;
            int i = 0;
            if (size < maxSize) {
                int fill = Math.min(count, maxSize - size);
                for (; i < fill; i++) {
                    values[size++] = encodeRaw(docs[i], scores[i]);
                }
                accepted += fill;
                if (size == maxSize) {
                    initializeThresholdOnFirstFull();
                }
                if (i == count) {
                    return accepted;
                }
            }
            if (bestScore < thresholdScore) {
                return accepted;
            }
            return accepted + insertBranchless(docs, scores, i, count);
        }

        private int insertBranchless(int[] docs, float[] scores, int start, int count) {
            int accepted = 0;
            for (int i = start; i < count; i++) {
                float score = scores[i];
                int doc = docs[i];
                long encoded = encodeRaw(doc, score);
                values[size] = encoded;
                int acceptedDelta = encoded > threshold ? 1 : 0;
                size += acceptedDelta;
                accepted += acceptedDelta;
                if (size == capacity) {
                    onCapacityReached();
                }
            }
            return accepted;
        }

        private void drain(LongConsumer consumer) {
            compactTo(maxSize);
            sortValues(size);
            for (int i = 0; i < size; i++) {
                consumer.accept(values[i]);
            }
            reset();
        }

        private void drainUnsorted(LongConsumer consumer) {
            compactTo(maxSize);
            for (int i = 0; i < size; i++) {
                consumer.accept(values[i]);
            }
            reset();
        }

        private void sortValues(int length) {
            if (length <= 1) {
                return;
            }
            Arrays.sort(values, 0, length);
        }

        private void onCapacityReached() {
            compactTo(maxSize);
        }

        private void initializeThresholdOnFirstFull() {
            threshold = values[minValueIndex(values, size)];
            refreshThresholdCache();
        }

        private void compactTo(int keepMax) {
            if (size <= keepMax) {
                if (size == maxSize) {
                    refreshThresholdFromValues();
                }
                return;
            }
            int start = size - keepMax;
            select(start);
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
            refreshThresholdCache();
        }

        private void select(int k) {
            quickSelectIntro(values, 0, size - 1, k);
        }

        private void refreshThresholdFromValues() {
            threshold = values[minValueIndex(values, size)];
            refreshThresholdCache();
        }

        private void refreshThresholdCache() {
            thresholdScore = decodeScoreRaw(threshold);
        }

        private void reset() {
            size = 0;
            threshold = Long.MIN_VALUE;
            thresholdScore = Float.NEGATIVE_INFINITY;
        }

        /**
         * This is copied and mutated from Apache Lucene's IntroSelector logic.
         */
        private static void quickSelectIntro(long[] values, int left, int right, int k) {
            int from = left;
            int to = right + 1; // Convert to an exclusive upper bound.
            int maxDepth = 2 * log2(to - from);

            int size;
            while ((size = to - from) > 3) {
                if (--maxDepth == -1) {
                    Arrays.sort(values, from, to);
                    return;
                }

                int last = to - 1;
                int mid = (from + last) >>> 1;
                int pivot;
                if (size <= SINGLE_MEDIAN_THRESHOLD) {
                    int range = size >>> 2;
                    pivot = medianIndex(values, mid - range, mid, mid + range);
                } else {
                    int range = size >>> 3;
                    int doubleRange = range << 1;
                    int medianFirst = medianIndex(values, from, from + range, from + doubleRange);
                    int medianMiddle = medianIndex(values, mid - range, mid, mid + range);
                    int medianLast = medianIndex(values, last - doubleRange, last - range, last);
                    if (k - from < range) {
                        pivot = minIndex(values, medianFirst, medianMiddle, medianLast);
                    } else if (to - k <= range) {
                        pivot = maxIndex(values, medianFirst, medianMiddle, medianLast);
                    } else {
                        pivot = medianIndex(values, medianFirst, medianMiddle, medianLast);
                    }
                }

                long pivotValue = values[pivot];
                swap(values, from, pivot);
                int i = from;
                int j = to;
                int p = from + 1;
                int q = last;
                while (true) {
                    long leftValue;
                    long rightValue;
                    while ((leftValue = values[++i]) < pivotValue) {
                    }
                    while ((rightValue = values[--j]) > pivotValue) {
                    }
                    if (i >= j) {
                        if (i == j && rightValue == pivotValue) {
                            swap(values, i, p);
                        }
                        break;
                    }
                    swap(values, i, j);
                    if (rightValue == pivotValue) {
                        swap(values, i, p++);
                    }
                    if (leftValue == pivotValue) {
                        swap(values, j, q--);
                    }
                }
                i = j + 1;
                for (int l = from; l < p;) {
                    swap(values, l++, j--);
                }
                for (int l = last; l > q;) {
                    swap(values, l--, i++);
                }

                if (k <= j) {
                    to = j + 1;
                } else if (k >= i) {
                    from = i;
                } else {
                    return;
                }
            }

            switch (size) {
                case 2:
                    if (values[from] > values[from + 1]) {
                        swap(values, from, from + 1);
                    }
                    break;
                case 3:
                    sort3(values, from);
                    break;
                default:
                    break;
            }
        }

        private static int minIndex(long[] values, int i, int j, int k) {
            if (values[i] <= values[j]) {
                return values[i] <= values[k] ? i : k;
            }
            return values[j] <= values[k] ? j : k;
        }

        private static int maxIndex(long[] values, int i, int j, int k) {
            if (values[i] <= values[j]) {
                return values[j] < values[k] ? k : j;
            }
            return values[i] < values[k] ? k : i;
        }

        private static int medianIndex(long[] values, int i, int j, int k) {
            if (values[i] < values[j]) {
                if (values[j] <= values[k]) {
                    return j;
                }
                return values[i] < values[k] ? k : i;
            }
            if (values[j] >= values[k]) {
                return j;
            }
            return values[i] < values[k] ? i : k;
        }

        private static void sort3(long[] values, int from) {
            int mid = from + 1;
            int last = from + 2;
            if (values[from] <= values[mid]) {
                if (values[mid] > values[last]) {
                    swap(values, mid, last);
                    if (values[from] > values[mid]) {
                        swap(values, from, mid);
                    }
                }
            } else if (values[mid] >= values[last]) {
                swap(values, from, last);
            } else {
                swap(values, from, mid);
                if (values[mid] > values[last]) {
                    swap(values, mid, last);
                }
            }
        }

        private static int log2(int value) {
            return 31 - Integer.numberOfLeadingZeros(value);
        }

        private static void swap(long[] values, int i, int j) {
            long tmp = values[i];
            values[i] = values[j];
            values[j] = tmp;
        }

        private static int minValueIndex(long[] values, int size) {
            int minIndex = 0;
            long minValue = values[0];
            for (int i = 1; i < size; i++) {
                if (values[i] < minValue) {
                    minValue = values[i];
                    minIndex = i;
                }
            }
            return minIndex;
        }
    }

}
