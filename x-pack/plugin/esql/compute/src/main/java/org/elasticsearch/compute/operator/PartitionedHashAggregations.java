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
import org.elasticsearch.compute.aggregation.CountGroupingAggregatorFunction;
import org.elasticsearch.compute.aggregation.GroupingAggregator;
import org.elasticsearch.compute.aggregation.GroupingAggregatorFunction;
import org.elasticsearch.compute.aggregation.blockhash.BlockHash;
import org.elasticsearch.core.AbstractRefCounted;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Holds the partitioned state
 */
final class PartitionedHashAggregations extends AbstractRefCounted implements Releasable {
    private final List<PartitionedKeyAndAggs> generations = new ArrayList<>();
    private final AtomicInteger nextPartition = new AtomicInteger();
    private final AtomicInteger completedPartitions = new AtomicInteger();

    /** Returned by {@link #claimPartition()} when every partition has been claimed. */
    static final int NO_MORE_PARTITION = Integer.MAX_VALUE;

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

    void split(HashAggregationOperator op) {
        if (op.blockHash.numKeys() == 0) {
            return;
        }
        PartitionedKeyAndAggs partitioned = splitKeysAndAggs(op.driverContext.globalBreaker(), op);
        synchronized (generations) {
            generations.add(partitioned);
        }
        op.blockHash.clear();
        resetAggs(op);
    }

    @Override
    public void close() {
        decRef();
    }

    @Override
    protected void closeInternal() {
        Releasables.close(generations);
    }

    Combiner newCombiner(HashAggregationOperator op) {
        return new Combiner(op);
    }

    private static PartitionedKeyAndAggs splitKeysAndAggs(CircuitBreaker breaker, HashAggregationOperator op) {
        int partitionSize = Math.ceilDiv(op.blockHash.numKeys(), PartitionedHashTable.NUM_PARTITIONS);
        List<GroupingAggregatorFunction.GroupingStatePartitioner> splitters = new ArrayList<>(op.aggregators.size());
        boolean success = false;
        try {
            for (GroupingAggregator aggregator : op.aggregators) {
                var splitter = aggregator.aggregatorFunction().splitPartition(breaker, partitionSize);
                splitters.add(splitter);
            }
            success = true;
        } finally {
            if (success == false) {
                Releasables.close(splitters);
            }
        }
        GroupingAggregatorFunction.GroupingStatePartitioner splitter = new GroupingAggregatorFunction.GroupingStatePartitioner() {
            @Override
            public GroupingAggregatorFunction.PartitionedGroupingState finish() {
                int n = splitters.size();
                GroupingAggregatorFunction.PartitionedGroupingState[] subs = new GroupingAggregatorFunction.PartitionedGroupingState[n];
                boolean success = false;
                try {
                    for (int i = 0; i < n; i++) {
                        subs[i] = splitters.get(i).finish();
                    }
                    success = true;
                    return new MultiPartitionedState(subs);
                } finally {
                    if (success == false) {
                        Releasables.close(subs);
                    }
                }
            }

            @Override
            public void split(int firstId, short[] shiftedIds, int batchSize, int[] partitionCounts, int[] partitionOffsets) {
                for (GroupingAggregatorFunction.GroupingStatePartitioner splitter : splitters) {
                    splitter.split(firstId, shiftedIds, batchSize, partitionCounts, partitionOffsets);
                }
            }

            @Override
            public void close() {
                Releasables.close(splitters);
            }
        };

        PartitionedHashTable.PartitionedHashKeys partitionKeys = null;
        try {
            partitionKeys = op.blockHash.splitPartition(breaker, splitter);
            PartitionedKeyAndAggs result = new PartitionedKeyAndAggs(partitionKeys, (MultiPartitionedState) splitter.finish());
            partitionKeys = null;
            return result;
        } finally {
            Releasables.close(partitionKeys, splitter);
        }
    }

    private static void resetAggs(HashAggregationOperator op) {
        for (int i = 0; i < op.aggregators.size(); i++) {
            GroupingAggregator aggregator = op.aggregators.get(i);
            if (aggregator.aggregatorFunction() instanceof CountGroupingAggregatorFunction count) {
                count.clear();
            } else {
                Releasables.close(op.aggregators.set(i, op.aggregatorFactories.get(i).apply(op.driverContext)));
            }
        }
    }

    /**
     * Combines one partition of every generations
     */
    final class Combiner {
        private final HashAggregationOperator op;
        private int[][] allGenIds = null;

        Combiner(HashAggregationOperator op) {
            this.op = op;
        }

        boolean combine(int p) {
            final int numGens = generations.size();
            int totalKeys = 0;
            for (PartitionedKeyAndAggs partitioned : generations) {
                totalKeys += partitioned.keys.keysInPartition(p);
            }
            if (totalKeys == 0) {
                return false;
            }
            BlockHash blockHash = op.blockHash;
            blockHash.clear();
            if (allGenIds == null) {
                allGenIds = new int[numGens][];
            }
            for (int g = 0; g < numGens; g++) {
                PartitionedKeyAndAggs partitioned = generations.get(g);
                var partitionedKeys = partitioned.keys;
                int numKeys = partitionedKeys.keysInPartition(p);
                int[] genIds = this.allGenIds[g];
                if (genIds == null || genIds.length < numKeys) {
                    this.allGenIds[g] = genIds = new int[ArrayUtil.oversize(numKeys, Integer.BYTES)];
                }
                // TODO: use append-only
                blockHash.combinePartition(partitionedKeys, p, totalKeys, genIds);
                op.rowsAddedInCurrentBatch += numKeys;
                partitionedKeys.releasePartition(p);
            }
            resetAggs(op);
            for (int i = 0; i < op.aggregators.size(); i++) {
                GroupingAggregatorFunction af = op.aggregators.get(i).aggregatorFunction();
                for (int g = 0; g < numGens; g++) {
                    var gen = generations.get(g);
                    var partitioned = gen.aggs;
                    af.combinePartition(partitioned.subs[i], p, allGenIds[g], gen.keys.keysInPartition(p), blockHash.numKeys());
                    partitioned.subs[i].releasePartition(p);
                }
            }
            return true;
        }
    }

    private record PartitionedKeyAndAggs(PartitionedHashTable.PartitionedHashKeys keys, MultiPartitionedState aggs) implements Releasable {
        @Override
        public void close() {
            Releasables.close(keys, aggs);
        }
    }

    private record MultiPartitionedState(GroupingAggregatorFunction.PartitionedGroupingState[] subs)
        implements
            GroupingAggregatorFunction.PartitionedGroupingState {

        @Override
        public void releasePartition(int p) {
            for (GroupingAggregatorFunction.PartitionedGroupingState sub : subs) {
                if (sub != null) {
                    sub.releasePartition(p);
                }
            }
        }

        @Override
        public void close() {
            Releasables.close(subs);
            Arrays.fill(subs, null);
        }
    }
}
