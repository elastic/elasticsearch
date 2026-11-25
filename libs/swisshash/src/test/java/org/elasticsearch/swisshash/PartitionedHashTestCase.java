/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.swisshash;

import org.apache.lucene.util.ArrayUtil;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.util.PartitionedHashTable;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.test.ESTestCase;

import java.util.Arrays;

/**
 * Base class for {@link PartitionedHashTable} tests
 */
public abstract class PartitionedHashTestCase extends ESTestCase {

    record KeyAndSum(long k1, long k2, long sum) {

    }

    record PartitionedKeyAndAggs(PartitionedHashTable.PartitionedHashKeys keys, PartitionedSum aggs) {
        public void release(CircuitBreaker breaker) {
            keys.releaseAll(breaker);
            aggs.releaseAll(breaker);
        }
    }

    PartitionedKeyAndAggs partition(CircuitBreaker breaker, PartitionedHashTable hash, int hashSize, SumAgg agg) {
        int estimatePartitionSize = ArrayUtil.oversize(Math.ceilDiv(hashSize, PartitionedHashTable.NUM_PARTITIONS), Long.BYTES);
        var partitionSum = new PartitionedSum(breaker, estimatePartitionSize);
        var aggSplitter = agg.splitter(breaker, partitionSum);
        boolean success = false;
        try {
            var partitionedHash = hash.splitPartition(breaker, aggSplitter);
            success = true;
            return new PartitionedKeyAndAggs(partitionedHash, partitionSum);
        } finally {
            aggSplitter.release(breaker);
            if (success == false) {
                partitionSum.releaseAll(breaker);
            }
        }
    }

    static final class PartitionedSum {
        final int[][] values;

        PartitionedSum(CircuitBreaker breaker, int estimatePartitionSize) {
            long bytes = (long) PartitionedHashTable.NUM_PARTITIONS * estimatePartitionSize * Integer.BYTES;
            breaker.addEstimateBytesAndMaybeBreak(bytes, "SumAgg#partition");
            values = new int[PartitionedHashTable.NUM_PARTITIONS][];
            for (int p = 0; p < values.length; p++) {
                values[p] = new int[estimatePartitionSize];
            }
        }

        void releasePartition(CircuitBreaker breaker, int partition) {
            int[] sub = values[partition];
            if (sub != null) {
                values[partition] = null;
                breaker.addWithoutBreaking(-(long) sub.length * Integer.BYTES);
            }
        }

        void releaseAll(CircuitBreaker breaker) {
            for (int p = 0; p < values.length; p++) {
                releasePartition(breaker, p);
            }
        }
    }

    /**
     * A minimal test aggregator that keeps one running {@code int} sum per group id.
     */
    static final class SumAgg implements Releasable {
        final CircuitBreaker breaker;

        int[] sums;

        SumAgg(CircuitBreaker breaker) {
            this.breaker = breaker;
            breaker.addEstimateBytesAndMaybeBreak(1024L * Integer.BYTES, "SumAgg");
            this.sums = new int[1024];
        }

        void ensureCapacity(int size) {
            if (sums.length < size) {
                int oldLength = sums.length;
                int newLength = ArrayUtil.oversize(size, Integer.BYTES);
                breaker.addEstimateBytesAndMaybeBreak((long) newLength * Integer.BYTES, "SumAgg");
                sums = ArrayUtil.growExact(sums, newLength);
                breaker.addWithoutBreaking(-(long) oldLength * Integer.BYTES);
            }
        }

        void add(int groupId, int value) {
            sums[groupId] += value;
        }

        void clear() {
            Arrays.fill(sums, 0);
        }

        @Override
        public void close() {
            breaker.addWithoutBreaking(-(long) sums.length * Integer.BYTES);
        }

        PartitionedHashTable.PartitionSplitter splitter(CircuitBreaker breaker, PartitionedSum out) {
            return new PartitionedHashTable.PartitionSplitter() {
                @Override
                public void split(int firstId, short[] shiftedIds, int batchSize, int[] batchPartitionCounts, int[] partitionOffsets) {
                    int[] src = sums;
                    for (int p = 0; p < batchPartitionCounts.length; p++) {
                        int c = batchPartitionCounts[p];
                        if (c == 0) {
                            continue;
                        }
                        int[] dst = out.values[p];
                        int dstOffset = partitionOffsets[p];
                        if (dst.length < dstOffset + c) {
                            int oldLength = dst.length;
                            int newLength = ArrayUtil.oversize(dstOffset + c, Integer.BYTES);
                            breaker.addEstimateBytesAndMaybeBreak((long) newLength * Integer.BYTES, "SumAgg#partition");
                            dst = out.values[p] = ArrayUtil.growExact(dst, newLength);
                            breaker.addWithoutBreaking(-(long) oldLength * Integer.BYTES);
                        }
                        int base = PartitionedHashTable.PARTITION_WRITE_BATCH * p;
                        for (int v = 0; v < c; v++) {
                            int id = firstId + shiftedIds[base + v];
                            dst[dstOffset + v] = src[id];
                        }
                    }
                }

                @Override
                public void release(CircuitBreaker breaker) {

                }
            };
        }

        void combinePartition(PartitionedSum partitioned, int partition, int[] mergedIds, int numKeys, boolean appendOnly) {
            int[] from = partitioned.values[partition];
            if (appendOnly && numKeys > 0) {
                for (int i = 0; i < numKeys; i++) {
                    assert mergedIds[i] == mergedIds[0] + i : "append-only ids must be consecutive and in input order";
                }
                System.arraycopy(from, 0, sums, mergedIds[0], numKeys);
            } else {
                for (int i = 0; i < numKeys; i++) {
                    add(mergedIds[i], from[i]);
                }
            }
        }
    }
}
