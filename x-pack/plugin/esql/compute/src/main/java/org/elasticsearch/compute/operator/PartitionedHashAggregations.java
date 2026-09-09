/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.apache.lucene.util.ArrayUtil;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.util.PartitionedHashTable;
import org.elasticsearch.compute.aggregation.blockhash.PartitionedBlockHash;
import org.elasticsearch.core.AbstractRefCounted;
import org.elasticsearch.core.Releasable;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Helper class for {@link ParallelHashAggregationOperator} to hold partitioned aggregation states.
 */
final class PartitionedHashAggregations extends AbstractRefCounted implements Releasable {
    private final CircuitBreaker globalBreaker;
    private final List<PartitionedKeyAndAggs> generations = new ArrayList<>();

    private final AtomicInteger nextPartition = new AtomicInteger();
    private final AtomicInteger completedPartitions = new AtomicInteger();

    /** Returned by {@link #claimPartition()} when every partition has been claimed. */
    static final int NO_MORE_PARTITION = Integer.MAX_VALUE;

    PartitionedHashAggregations(CircuitBreaker globalBreaker) {
        this.globalBreaker = globalBreaker;
    }

    /**
     * Claims the next partition to emit, or returns {@link #NO_MORE_PARTITION} if every partition has been claimed.
     */
    int claimPartition() {
        int p = nextPartition.getAndIncrement();
        return p < PartitionedHashTable.NUM_PARTITIONS ? p : NO_MORE_PARTITION;
    }

    boolean hasUncombinedPartitions() {
        return nextPartition.get() < PartitionedHashTable.NUM_PARTITIONS;
    }

    /**
     * Records that one partition has been fully emitted; returns true when this was the last one.
     */
    boolean completePartition() {
        return completedPartitions.incrementAndGet() == PartitionedHashTable.NUM_PARTITIONS;
    }

    void split(CircuitBreaker breaker, HashAggregationOperator op) {
        if (op.blockHash.numKeys() == 0) {
            return;
        }
        PartitionedKeyAndAggs partitioned = splitKeysAndAggs(breaker, op);
        synchronized (generations) {
            generations.add(partitioned);
        }
        op.clearCurrentBatch();
    }

    @Override
    public void close() {
        decRef();
    }

    @Override
    protected void closeInternal() {
        for (var gen : generations) {
            gen.release(globalBreaker);
        }
    }

    Combiner newCombiner(HashAggregationOperator op) {
        return new Combiner(op);
    }

    private static PartitionedKeyAndAggs splitKeysAndAggs(CircuitBreaker breaker, HashAggregationOperator op) {
        PartitionedHashTable.PartitionSplitter splitter = new PartitionedHashTable.PartitionSplitter() {
            @Override
            public void split(int firstId, short[] shiftedIds, int batchSize, int[] batchPartitionCounts, int[] partitionOffsets) {

            }

            @Override
            public void release(CircuitBreaker breaker) {

            }
        };
        PartitionedHashTable.PartitionedHashKeys partitionKeys = null;
        try {
            partitionKeys = ((PartitionedHashTable) op.blockHash).splitPartition(breaker, splitter);
            PartitionedKeyAndAggs result = new PartitionedKeyAndAggs(partitionKeys, null);
            partitionKeys = null;
            return result;
        } finally {
            if (partitionKeys != null) {
                partitionKeys.releaseAll(breaker);
            }
            splitter.release(breaker);
        }
    }

    /**
     * Combines one partition from every generation
     */
    final class Combiner implements Releasable {
        private final HashAggregationOperator op;
        private final CircuitBreaker breaker;
        private int[] mergedIds = null;

        Combiner(HashAggregationOperator op) {
            this.op = op;
            this.breaker = op.driverContext.breaker();
        }

        boolean combine(int p) {
            final int numGens = generations.size();
            PartitionedBlockHash blockHash = (PartitionedBlockHash) op.blockHash;
            blockHash.clear();
            op.rowsAddedInCurrentBatch = 0;
            for (int g = 0; g < numGens; g++) {
                PartitionedKeyAndAggs partitioned = generations.get(g);
                var partitionedKeys = partitioned.keys;
                int numKeys = partitionedKeys.keysInPartition(p);
                if (numKeys > 0) {
                    ensureMergedIdsForGen(numKeys);
                    blockHash.combinePartition(partitionedKeys, p, mergedIds);
                    op.rowsAddedInCurrentBatch += numKeys;
                }
                partitionedKeys.releasePartition(breaker, p);
            }
            return true;
        }

        private void ensureMergedIdsForGen(int numKeys) {
            final int oldLength = mergedIds == null ? 0 : mergedIds.length;
            if (oldLength < numKeys) {
                final int newLength = ArrayUtil.oversize(numKeys, Integer.BYTES);
                breaker.addEstimateBytesAndMaybeBreak((long) (newLength - oldLength) * Integer.BYTES, "PartitionedHashAggregations");
                mergedIds = new int[newLength];
            }
        }

        @Override
        public void close() {
            if (mergedIds != null) {
                breaker.addWithoutBreaking(-(long) (mergedIds.length) * Integer.BYTES, "PartitionedHashAggregations");
            }
        }
    }

    private record PartitionedKeyAndAggs(PartitionedHashTable.PartitionedHashKeys keys, MultiAggsPartitionedState aggs) {
        void release(CircuitBreaker breaker) {
            keys.releaseAll(breaker);
        }
    }

    // TODO: for the follow-up
    private record MultiAggsPartitionedState() {

    }
}
