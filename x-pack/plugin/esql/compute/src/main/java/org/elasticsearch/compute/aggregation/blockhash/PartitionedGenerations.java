/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation.blockhash;

import org.apache.lucene.util.ArrayUtil;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.util.PartitionedHashTable;
import org.elasticsearch.core.Releasable;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Hash tables that were built independently, each split into {@link PartitionedHashTable#NUM_PARTITIONS} partitions
 * along with some per-key state, waiting to be combined one partition at a time. Each split table is a generation.
 * Partitions are claimed with {@link #claimPartition()}, so several threads can combine different partitions at once:
 * a partition's keys from every generation go into one {@link PartitionedBlockHash}, and the caller then merges each
 * generation's per-key state using the ids that hash assigned.
 * <p>
 *     Generations are added with {@link #add} before any partition is combined. Partitions of keys are released as
 *     they are combined; the caller releases partitions of its state. {@link #close()} releases whatever is left.
 * </p>
 */
public final class PartitionedGenerations<S extends PartitionedGenerations.State> implements Releasable {
    /**
     * Per-key state split alongside the keys of one generation.
     */
    public interface State {
        /**
         * Releases every partition that hasn't been released yet.
         */
        void releaseAll(CircuitBreaker breaker);
    }

    /**
     * The keys of one split hash table and the state split alongside them.
     */
    public record Generation<S>(PartitionedHashTable.PartitionedHashKeys keys, S state) {}

    /** Returned by {@link #claimPartition()} when every partition has been claimed. */
    public static final int NO_MORE_PARTITION = Integer.MAX_VALUE;

    private final CircuitBreaker breaker;
    private final List<Generation<S>> generations = new ArrayList<>();
    private final AtomicInteger nextPartition = new AtomicInteger();
    private final AtomicInteger completedPartitions = new AtomicInteger();

    /**
     * @param breaker the breaker the generations' memory is returned to on {@link #close()}
     */
    public PartitionedGenerations(CircuitBreaker breaker) {
        this.breaker = breaker;
    }

    /**
     * Adds a generation, taking ownership of {@code keys} and {@code state}. May be called from several threads,
     * but every generation must be added before the first partition is combined.
     */
    public void add(PartitionedHashTable.PartitionedHashKeys keys, S state) {
        synchronized (generations) {
            generations.add(new Generation<>(keys, state));
        }
    }

    public int size() {
        return generations.size();
    }

    public Generation<S> get(int generation) {
        return generations.get(generation);
    }

    /**
     * Claims the next partition to combine, or returns {@link #NO_MORE_PARTITION} if every partition has been claimed.
     */
    public int claimPartition() {
        int p = nextPartition.getAndIncrement();
        return p < PartitionedHashTable.NUM_PARTITIONS ? p : NO_MORE_PARTITION;
    }

    public boolean hasUnclaimedPartitions() {
        return nextPartition.get() < PartitionedHashTable.NUM_PARTITIONS;
    }

    /**
     * Records that one claimed partition has been fully combined; returns true when this was the last one. The thread
     * that gets true sees everything written while combining any partition.
     */
    public boolean completePartition() {
        return completedPartitions.incrementAndGet() == PartitionedHashTable.NUM_PARTITIONS;
    }

    @Override
    public void close() {
        for (Generation<S> generation : generations) {
            generation.keys.releaseAll(breaker);
            generation.state.releaseAll(breaker);
        }
    }

    /**
     * Combines the keys of one partition from every generation into a hash, remembering the id the hash assigned
     * to each key so the caller can merge the per-key state. Holds scratch space reused across partitions, so use
     * one per thread.
     */
    public final class KeyCombiner implements Releasable {
        private final CircuitBreaker breaker;
        private int[][] ids;
        private boolean[] appendOnly;

        /**
         * @param breaker charged for this combiner's scratch space and for releasing partitions of keys
         */
        public KeyCombiner(CircuitBreaker breaker) {
            this.breaker = breaker;
        }

        /**
         * Combines partition {@code p} of every generation's keys into {@code target}, then releases that partition of
         * the keys. Afterwards {@link #ids} and {@link #appendOnly} describe each generation's keys in that partition.
         */
        public void combineKeys(PartitionedBlockHash target, int p) {
            final int numGens = generations.size();
            if (ids == null) {
                ids = new int[numGens][];
                appendOnly = new boolean[numGens];
            }
            for (int g = 0; g < numGens; g++) {
                PartitionedHashTable.PartitionedHashKeys keys = generations.get(g).keys;
                int numKeys = keys.keysInPartition(p);
                if (numKeys > 0) {
                    ensureIds(g, numKeys);
                    appendOnly[g] = target.combinePartition(keys, p, ids[g]);
                }
                keys.releasePartition(breaker, p);
            }
        }

        /**
         * The ids {@code target} assigned to generation {@code g}'s keys in the last combined partition, in the order of
         * that generation's split. Only the first {@code keysInPartition} entries are meaningful.
         */
        public int[] ids(int g) {
            return ids[g];
        }

        /**
         * Whether every key of generation {@code g} in the last combined partition was new to the target, see
         * {@link PartitionedHashTable#combinePartition}.
         */
        public boolean appendOnly(int g) {
            return appendOnly[g];
        }

        private void ensureIds(int g, int numKeys) {
            int[] genIds = ids[g];
            final int oldLength = genIds == null ? 0 : genIds.length;
            if (oldLength < numKeys) {
                final int newLength = ArrayUtil.oversize(numKeys, Integer.BYTES);
                breaker.addEstimateBytesAndMaybeBreak((long) (newLength - oldLength) * Integer.BYTES, "PartitionedGenerations");
                ids[g] = new int[newLength];
            }
        }

        @Override
        public void close() {
            if (ids != null) {
                long bytes = 0;
                for (int[] genIds : ids) {
                    if (genIds != null) {
                        bytes += (long) genIds.length * Integer.BYTES;
                    }
                }
                breaker.addWithoutBreaking(-bytes, "PartitionedGenerations");
            }
        }
    }
}
