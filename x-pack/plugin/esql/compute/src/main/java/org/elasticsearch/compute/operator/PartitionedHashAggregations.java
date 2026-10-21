/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.util.PartitionedHashTable;
import org.elasticsearch.compute.aggregation.GroupingAggregator;
import org.elasticsearch.compute.aggregation.GroupingAggregatorFunction;
import org.elasticsearch.compute.aggregation.SeenGroupIds;
import org.elasticsearch.compute.aggregation.blockhash.PartitionedBlockHash;
import org.elasticsearch.compute.aggregation.blockhash.PartitionedGenerations;
import org.elasticsearch.compute.data.PartitionedAggregationBlock;
import org.elasticsearch.core.AbstractRefCounted;
import org.elasticsearch.core.Releasable;

import java.util.List;

/**
 * Helper class for {@link ParallelHashAggregationOperator} to hold partitioned aggregation states.
 */
final class PartitionedHashAggregations extends AbstractRefCounted implements Releasable {
    private final PartitionedGenerations<MultiAggsPartitionedState> generations;

    /** Returned by {@link #claimPartition()} when every partition has been claimed. */
    static final int NO_MORE_PARTITION = PartitionedGenerations.NO_MORE_PARTITION;

    PartitionedHashAggregations(CircuitBreaker globalBreaker) {
        this.generations = new PartitionedGenerations<>(globalBreaker);
    }

    /**
     * Claims the next partition to emit, or returns {@link #NO_MORE_PARTITION} if every partition has been claimed.
     */
    int claimPartition() {
        return generations.claimPartition();
    }

    boolean hasUncombinedPartitions() {
        return generations.hasUnclaimedPartitions();
    }

    /**
     * Records that one partition has been fully emitted; returns true when this was the last one.
     */
    boolean completePartition() {
        return generations.completePartition();
    }

    void split(CircuitBreaker breaker, HashAggregationOperator op) {
        if (op.blockHash.numKeys() == 0) {
            return;
        }
        PartitionedKeyAndAggs partitioned = splitKeysAndAggs(breaker, op);
        generations.add(partitioned.keys, partitioned.aggs);
        op.clearCurrentBatch();
    }

    @Override
    public void close() {
        decRef();
    }

    @Override
    protected void closeInternal() {
        generations.close();
    }

    Combiner newCombiner(HashAggregationOperator op) {
        return new Combiner(op);
    }

    static PartitionedAggregationBlock splitToPartitionedBlock(CircuitBreaker breaker, HashAggregationOperator op) {
        int numKeys = op.blockHash.numKeys();
        PartitionedKeyAndAggs keysAndAggs = splitKeysAndAggs(breaker, op);
        try {
            var block = new PartitionedAggregationBlock(
                op.driverContext.blockFactory(),
                numKeys,
                keysAndAggs.keys,
                keysAndAggs.aggs.states
            );
            keysAndAggs = null;
            return block;
        } finally {
            if (keysAndAggs != null) {
                keysAndAggs.releaseAll(breaker);
            }
        }
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
     * Receive partitioned output blocks from initial operators
     */
    void addPartitionedBlocks(List<PartitionedAggregationBlock> blocks) {
        for (PartitionedAggregationBlock block : blocks) {
            var keys = block.takeKeys();
            generations.add(keys, new MultiAggsPartitionedState(block.takeAggs()));
        }
    }

    /**
     * Combines one partition from every generation
     */
    final class Combiner implements Releasable {
        private final HashAggregationOperator op;
        private final CircuitBreaker breaker;
        private final PartitionedGenerations<MultiAggsPartitionedState>.KeyCombiner keyCombiner;

        Combiner(HashAggregationOperator op) {
            this.op = op;
            this.breaker = op.driverContext.breaker();
            this.keyCombiner = generations.new KeyCombiner(breaker);
        }

        boolean combine(int p) {
            final int numGens = generations.size();
            op.clearCurrentBatch();
            PartitionedBlockHash blockHash = (PartitionedBlockHash) op.blockHash;
            // Combine keys from every generation first, then combine each aggregation across all generations.
            // This keeps accesses to the hash table and aggregation state cache-friendly.
            keyCombiner.combineKeys(blockHash, p);
            for (int g = 0; g < numGens; g++) {
                op.rowsAddedInCurrentBatch += generations.get(g).keys().keysInPartition(p);
            }
            // now combine aggregations
            List<GroupingAggregator> aggregators = op.aggregators;
            for (int i = 0; i < aggregators.size(); i++) {
                final var aggregator = aggregators.get(i).aggregatorFunction();
                // If some group in any generation is missing a value we need to track groupIds
                for (int g = 0; g < numGens; g++) {
                    if (generations.get(g).state().states[i].hasAllValues(p) == false) {
                        aggregator.selectedMayContainUnseenGroups(new SeenGroupIds.Empty());
                        break;
                    }
                }
                aggregator.maybeEnsureCapacity(blockHash.numKeys() + 1);
                for (int g = 0; g < numGens; g++) {
                    var generation = generations.get(g);
                    GroupingAggregatorFunction.PartitionedState agg = generation.state().states[i];
                    aggregator.combinePartition(
                        agg,
                        p,
                        keyCombiner.appendOnly(g),
                        keyCombiner.ids(g),
                        generation.keys().keysInPartition(p)
                    );
                    agg.releasePartition(breaker, p);
                }
            }
            return true;
        }

        @Override
        public void close() {
            keyCombiner.close();
        }
    }

    private record PartitionedKeyAndAggs(PartitionedHashTable.PartitionedHashKeys keys, MultiAggsPartitionedState aggs) {
        void releaseAll(CircuitBreaker breaker) {
            keys.releaseAll(breaker);
            aggs.releaseAll(breaker);
        }
    }

    private record MultiAggsPartitionedState(GroupingAggregatorFunction.PartitionedState[] states) implements PartitionedGenerations.State {
        @Override
        public void releaseAll(CircuitBreaker breaker) {
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
