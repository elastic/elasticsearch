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
        this.collector = resolved == Strategy.SCANN_FAST ? new ScannFastTopK(maxSize) : new FaissReservoirTopK(maxSize);
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

    private sealed abstract static class ReservoirTopK permits FaissReservoirTopK, ScannFastTopK {
        protected final int maxSize;
        protected final long[] values;
        protected int size;
        private long threshold = Long.MIN_VALUE;
        private float thresholdScore = Float.NEGATIVE_INFINITY;
        private int thresholdNode = Integer.MAX_VALUE;

        private ReservoirTopK(int maxSize, int capacityMultiplier) {
            this.maxSize = maxSize;
            this.values = new long[Math.max(maxSize, maxSize * capacityMultiplier)];
        }

        protected abstract void onCapacityReached();

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
            for (; i < count; i++) {
                if (scoreDoesNotBeatThreshold(docs[i], scores[i])) {
                    continue;
                }
                long encoded = encodeRaw(docs[i], scores[i]);
                values[size++] = encoded;
                accepted++;
                if (size == values.length) {
                    onCapacityReached();
                }
            }
            return accepted;
        }

        private boolean scoreDoesNotBeatThreshold(int doc, float score) {
            if (score < thresholdScore) {
                return true;
            }
            if (score > thresholdScore) {
                return false;
            }
            return doc >= thresholdNode;
        }

        private void initializeThresholdOnFirstFull() {
            threshold = values[minValueIndex(values, size)];
            refreshThresholdCache();
        }

        protected final void compactTo(int keepMax) {
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

        private void drain(LongConsumer consumer) {
            compactTo(maxSize);
            Arrays.sort(values, 0, size);
            for (int i = 0; i < size; i++) {
                consumer.accept(values[i]);
            }
            size = 0;
            threshold = Long.MIN_VALUE;
            thresholdScore = Float.NEGATIVE_INFINITY;
            thresholdNode = Integer.MAX_VALUE;
        }
    }

    private static final class FaissReservoirTopK extends ReservoirTopK {

        private FaissReservoirTopK(int maxSize) {
            super(maxSize, 2);
        }

        @Override
        protected void onCapacityReached() {
            compactTo(maxSize);
        }
    }

    private static final class ScannFastTopK extends ReservoirTopK {

        private final int maxSize;
        private final int capacity;

        private ScannFastTopK(int maxSize) {
            super(maxSize, 2);
            this.maxSize = maxSize;
            this.capacity = Math.max(maxSize, maxSize * 2);
        }

        @Override
        protected void onCapacityReached() {
            int keepMax = (maxSize + capacity) / 2 - 1;
            if (keepMax < maxSize) {
                keepMax = maxSize;
            }
            compactTo(keepMax);
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
