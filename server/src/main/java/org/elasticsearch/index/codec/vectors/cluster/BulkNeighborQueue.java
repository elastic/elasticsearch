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
    public enum Strategy {
        AUTO_V2,
        FAISS_RESERVOIR,
        SCANN_FAST
    }

    private final ReservoirTopK collector;
    private final int maxSize;
    private final long sentinelWorst;

    public BulkNeighborQueue(int maxSize, boolean maxHeap) {
        this(maxSize, maxHeap, Strategy.AUTO_V2, Integer.MAX_VALUE);
    }

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
        if (maxHeap) {
            throw new IllegalArgumentException("BulkNeighborQueue only supports min-heap mode");
        }
        this.maxSize = maxSize;
        Strategy resolved = resolveStrategy(strategy, maxSize);
        this.collector = new ReservoirTopK(maxSize, resolved == Strategy.SCANN_FAST);
        this.sentinelWorst = encodeRaw(0, Float.NEGATIVE_INFINITY);
    }

    private static Strategy resolveStrategy(Strategy strategy, int maxSize) {
        if (strategy == Strategy.AUTO_V2) {
            return maxSize <= 5 ? Strategy.SCANN_FAST : Strategy.FAISS_RESERVOIR;
        }
        return strategy;
    }

    public int size() {
        return collector.size();
    }

    /**
     * Returns the top element if the queue is full. Otherwise returns a sentinel
     * worst element that signals the queue is not yet built.
     */
    public long peek() {
        if (size() < maxSize) {
            return sentinelWorst;
        }
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
        return collector.insertWithOverflowBulk(docs, scores, count, bestScore);
    }

    /**
     * Drains the queue in sorted order (worst to best), providing encoded values.
     */
    public void drain(LongConsumer consumer) {
        collector.drain(consumer);
    }

    private static final class ReservoirTopK {
        private final int maxSize;
        private final boolean scannFast;
        private final int capacity;
        private final long[] values;
        private int size;
        private long threshold = Long.MIN_VALUE;
        private float thresholdScore = Float.NEGATIVE_INFINITY;
        private int thresholdNode = Integer.MAX_VALUE;

        private ReservoirTopK(int maxSize, boolean scannFast) {
            this.maxSize = maxSize;
            this.scannFast = scannFast;
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
                boolean beatsThreshold = score > thresholdScore || (score == thresholdScore && doc < thresholdNode);
                int acceptedDelta = beatsThreshold && encoded > threshold ? 1 : 0;
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
            size = 0;
            threshold = Long.MIN_VALUE;
            thresholdScore = Float.NEGATIVE_INFINITY;
            thresholdNode = Integer.MAX_VALUE;
        }

        private void sortValues(int length) {
            if (length <= 1) {
                return;
            }
            if (length < 64) {
                Arrays.sort(values, 0, length);
                return;
            }
            radixSortPositive(values, new long[length], new int[256], length);
        }

        private void onCapacityReached() {
            if (scannFast) {
                int keepMax = (maxSize + capacity) / 2 - 1;
                if (keepMax < maxSize) {
                    keepMax = maxSize;
                }
                compactTo(keepMax);
            } else {
                compactTo(maxSize);
            }
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
            refreshThresholdCache();
        }

        private void refreshThresholdFromValues() {
            threshold = values[minValueIndex(values, size)];
            refreshThresholdCache();
        }

        private void refreshThresholdCache() {
            thresholdScore = decodeScoreRaw(threshold);
            thresholdNode = (int) ~threshold;
        }
    }

    private static final int SINGLE_MEDIAN_THRESHOLD = 40;
    private static final int RADIX_SELECT_MIN_SIZE = 128;
    private static final int RADIX_SELECT_FALLBACK_SIZE = 32;

    /**
     * This is copied and mutated from Apache Lucene's IntroSelector logic.
     */
    private static void quickSelect(long[] values, int left, int right, int k) {
        int length = right - left + 1;
        if (length >= RADIX_SELECT_MIN_SIZE) {
            radixSelectNonNegative(values, left, right, k);
            return;
        }
        quickSelectIntro(values, left, right, k);
    }

    private static void radixSelectNonNegative(long[] values, int left, int right, int k) {
        int from = left;
        int to = right + 1;
        long[] scratch = new long[to - from];
        int[] counts = new int[256];
        int[] offsets = new int[256];

        for (int shift = Long.SIZE - Byte.SIZE; shift >= 0 && to - from > RADIX_SELECT_FALLBACK_SIZE; shift -= Byte.SIZE) {
            Arrays.fill(counts, 0);
            for (int i = from; i < to; i++) {
                counts[(int) ((values[i] >>> shift) & 0xFFL)]++;
            }

            int targetRank = k - from;
            int cumulative = 0;
            int bucketStart = 0;
            int bucketEnd = 0;
            int targetBucket = -1;
            for (int bucket = 0; bucket < counts.length; bucket++) {
                int next = cumulative + counts[bucket];
                if (targetRank < next) {
                    targetBucket = bucket;
                    bucketStart = cumulative;
                    bucketEnd = next;
                    break;
                }
                cumulative = next;
            }
            if (targetBucket == -1) {
                break;
            }
            if (counts[targetBucket] == to - from) {
                continue;
            }

            int running = 0;
            for (int bucket = 0; bucket < counts.length; bucket++) {
                offsets[bucket] = running;
                running += counts[bucket];
            }
            for (int i = from; i < to; i++) {
                int bucket = (int) ((values[i] >>> shift) & 0xFFL);
                scratch[offsets[bucket]++] = values[i];
            }

            int range = to - from;
            System.arraycopy(scratch, 0, values, from, range);
            int newFrom = from + bucketStart;
            int newTo = from + bucketEnd;
            from = newFrom;
            to = newTo;
        }

        if (to - from > 1) {
            quickSelectIntro(values, from, to - 1, k);
        }
    }

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

    /**
     * Radix sort for encoded values that are always non-negative.
     */
    private static void radixSortPositive(long[] values, long[] scratch, int[] counts, int length) {
        long[] src = values;
        long[] dst = scratch;
        for (int shift = 0; shift < Long.SIZE; shift += Byte.SIZE) {
            Arrays.fill(counts, 0);
            for (int i = 0; i < length; i++) {
                int bucket = (int) ((src[i] >>> shift) & 0xFFL);
                counts[bucket]++;
            }
            int sum = 0;
            for (int i = 0; i < counts.length; i++) {
                int count = counts[i];
                counts[i] = sum;
                sum += count;
            }
            for (int i = 0; i < length; i++) {
                long value = src[i];
                int bucket = (int) ((value >>> shift) & 0xFFL);
                dst[counts[bucket]++] = value;
            }
            long[] tmp = src;
            src = dst;
            dst = tmp;
        }
        if (src != values) {
            System.arraycopy(src, 0, values, 0, length);
        }
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
