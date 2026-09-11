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
import org.elasticsearch.compute.aggregation.GroupingAggregator;
import org.elasticsearch.compute.aggregation.GroupingAggregatorFunction;
import org.elasticsearch.compute.aggregation.SeenGroupIds;
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
            gen.releaseAll(globalBreaker);
        }
    }

    Combiner newCombiner(HashAggregationOperator op) {
        return new Combiner(op);
    }

    private static PartitionedKeyAndAggs splitKeysAndAggs(CircuitBreaker breaker, HashAggregationOperator op) {
        PartitionedHashTable.PartitionedHashKeys partitionedKeys = null;
        MultiAggsPartitionSplitter aggSplitter = new MultiAggsPartitionSplitter(breaker, op.aggregators);
        try {
            partitionedKeys = ((PartitionedHashTable) op.blockHash).splitPartition(breaker, aggSplitter);
            PartitionedKeyAndAggs result = new PartitionedKeyAndAggs(partitionedKeys, aggSplitter.finishAll(breaker));
            partitionedKeys = null;
            return result;
        } finally {
            aggSplitter.release(breaker);
            if (partitionedKeys != null) {
                partitionedKeys.releaseAll(breaker);
            }
        }
    }

    /**
     * Combines one partition from every generation
     */
    final class Combiner implements Releasable {
        private final HashAggregationOperator op;
        private final CircuitBreaker breaker;
        private int[][] allGenIds = null;
        private boolean[] appendOnly;

        Combiner(HashAggregationOperator op) {
            this.op = op;
            this.breaker = op.driverContext.breaker();
        }

        boolean combine(int p) {
            final int numGens = generations.size();
            op.clearCurrentBatch();
            PartitionedBlockHash blockHash = (PartitionedBlockHash) op.blockHash;
            if (allGenIds == null) {
                allGenIds = new int[numGens][];
            }
            if (appendOnly == null) {
                appendOnly = new boolean[numGens];
            }
            // Combine keys from every generation first, then combine each aggregation across all generations.
            // This keeps accesses to the hash table and aggregation state cache-friendly.
            for (int g = 0; g < numGens; g++) {
                PartitionedKeyAndAggs partitioned = generations.get(g);
                var partitionedKeys = partitioned.keys;
                int numKeys = partitionedKeys.keysInPartition(p);
                if (numKeys > 0) {
                    ensureGenIds(g, numKeys);
                    appendOnly[g] = blockHash.combinePartition(partitionedKeys, p, allGenIds[g]);
                    op.rowsAddedInCurrentBatch += numKeys;
                }
                partitionedKeys.releasePartition(breaker, p);
            }
            // now combine aggregations
            List<GroupingAggregator> aggregators = op.aggregators;
            for (int i = 0; i < aggregators.size(); i++) {
                final var aggregator = aggregators.get(i).aggregatorFunction();
                // If some group in any generation is missing a value we need to track groupIds
                for (int g = 0; g < numGens; g++) {
                    if (generations.get(g).aggs.states[i].hasAllValues(p) == false) {
                        aggregator.selectedMayContainUnseenGroups(new SeenGroupIds.Empty());
                        break;
                    }
                }
                aggregator.maybeEnsureCapacity(blockHash.numKeys() + 1);
                for (int g = 0; g < numGens; g++) {
                    PartitionedKeyAndAggs keysAndAggs = generations.get(g);
                    GroupingAggregatorFunction.PartitionedState agg = keysAndAggs.aggs.states[i];
                    aggregator.combinePartition(agg, p, appendOnly[g], allGenIds[g], keysAndAggs.keys.keysInPartition(p));
                    agg.releasePartition(breaker, p);
                }
            }
            return true;
        }

        private void ensureGenIds(int g, int numKeys) {
            int[] genIds = allGenIds[g];
            final int oldLength = genIds == null ? 0 : genIds.length;
            if (oldLength < numKeys) {
                final int newLength = ArrayUtil.oversize(numKeys, Integer.BYTES);
                breaker.addEstimateBytesAndMaybeBreak((long) (newLength - oldLength) * Integer.BYTES, "PartitionedHashAggregations");
                allGenIds[g] = new int[newLength];
            }
        }

        @Override
        public void close() {
            if (allGenIds != null) {
                long bytes = 0;
                for (int[] genIds : allGenIds) {
                    if (genIds != null) {
                        bytes += (long) genIds.length * Integer.BYTES;
                    }
                }
                breaker.addWithoutBreaking(-bytes, "PartitionedHashAggregations");
            }
        }
    }

    private record PartitionedKeyAndAggs(PartitionedHashTable.PartitionedHashKeys keys, MultiAggsPartitionedState aggs) {
        void releaseAll(CircuitBreaker breaker) {
            keys.releaseAll(breaker);
            aggs.releaseAll(breaker);
        }
    }

    private record MultiAggsPartitionedState(GroupingAggregatorFunction.PartitionedState[] states) {
        void releaseAll(CircuitBreaker breaker) {
            for (var state : states) {
                state.releaseAll(breaker);
            }
        }
    }

    private static class MultiAggsPartitionSplitter implements PartitionedHashTable.PartitionSplitter {
        final GroupingAggregatorFunction.PartitionSplitter[] splitters;

        MultiAggsPartitionSplitter(CircuitBreaker breaker, List<GroupingAggregator> aggregators) {
            this.splitters = new GroupingAggregatorFunction.PartitionSplitter[aggregators.size()];
            boolean success = false;
            try {
                for (int i = 0; i < splitters.length; i++) {
                    splitters[i] = aggregators.get(i).aggregatorFunction().createPartitioningSplitter(breaker);
                }
                success = true;
            } finally {
                if (success == false) {
                    release(breaker);
                }
            }
        }

        public MultiAggsPartitionedState finishAll(CircuitBreaker breaker) {
            GroupingAggregatorFunction.PartitionedState[] states = new GroupingAggregatorFunction.PartitionedState[splitters.length];
            boolean success = false;
            try {
                for (int i = 0; i < splitters.length; i++) {
                    states[i] = splitters[i].finish();
                }
                success = true;
            } finally {
                if (success == false) {
                    for (var state : states) {
                        if (state != null) {
                            state.releaseAll(breaker);
                        }
                    }
                }
            }
            return new MultiAggsPartitionedState(states);
        }

        @Override
        public void split(int firstId, short[] shiftedIds, int batchSize, int[] batchPartitionCounts, int[] partitionOffsets) {
            for (var splitter : splitters) {
                splitter.split(firstId, shiftedIds, batchSize, batchPartitionCounts, partitionOffsets);
            }
        }

        @Override
        public void release(CircuitBreaker breaker) {
            for (int i = 0; i < splitters.length; i++) {
                var splitter = splitters[i];
                if (splitter != null) {
                    splitters[i] = null;
                    splitter.release(breaker);
                }
            }
        }
    }
}
