/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.util;

import org.elasticsearch.common.breaker.CircuitBreaker;

/**
 * A hash table whose keys can be redistributed into partitions by hash, so that several tables can be merged partition by
 * partition via {@link #splitPartition} and {@link #combinePartition} instead of all at once. A key always falls into the
 * same partition regardless of which table it came from, so partitions can be combined independently, concurrently and
 * released as soon as they have been combined.
 */
public interface PartitionedHashTable {
    /**
     * The number of partitions a hash table is split into.
     * Ideally, we should choose the number of partitions dynamically depending on the hash table size.
     */
    int NUM_PARTITIONS = 256;

    /**
     * The mask applied to a key's hash to determine its partition.
     */
    int PARTITION_MASK = NUM_PARTITIONS - 1;

    /**
     * The maximum number of entries buffered in any partition before flushing. When any partition is about to exceed
     * this limit, all buffered entries are flushed regardless of the sizes of the other partitions. Since entries are
     * partitioned from a hash table with a good hash function, the other partitions will likely be similarly full.
     * <p>
     * Entries are accumulated in a buffer of {@code NUM_PARTITIONS * PARTITION_WRITE_BATCH} entries. This value must fit in
     * a {@code short}, since {@link #splitPartition(CircuitBreaker, PartitionSplitter)} passes ids to the callback as shorts.
     */
    int PARTITION_WRITE_BATCH = 64;

    /**
     * The keys of a hash table, split into partitions. Returned by {@link #splitPartition} and consumed by
     * {@link #combinePartition}. Partitions can be released individually once combined.
     * The {@link PartitionedHashKeys} is constructed and released by a single thread, but partitions can be
     * accessed and released by multiple threads.
     * <p>
     * All memory is accounted against the breaker passed to {@link #splitPartition}. The breakers passed to
     * {@link #releasePartition} and {@link #releaseAll} do not have to be the same instance, but they must belong
     * to the same breaker hierarchy (e.g. local breakers sharing the same parent), so that the bytes reserved
     * during the split are returned to the same accounting.
     */
    interface PartitionedHashKeys {
        /**
         * Returns the number of keys in the given partition.
         */
        int keysInPartition(int partition);

        /**
         * Releases the given partition without waiting for the remaining ones.
         * One partition index must be released by one thread at a time,
         * but different partitions can be released by different threads.
         */
        void releasePartition(CircuitBreaker breaker, int partition);

        /**
         * Releases all remaining partitions of this partitioned keys.
         * This must happen after all {@link #releasePartition} calls have completed.
         */
        void releaseAll(CircuitBreaker breaker);
    }

    /**
     * Callback invoked by {@link #splitPartition} whenever a partition has {@link #PARTITION_WRITE_BATCH} buffered entries,
     * so the caller can split per-key state (e.g. aggregation states) using the same partitioning. Partition {@code p}
     * receives {@code partitionCounts[p]} entries from this batch, to be written to its state at positions
     * {@code [partitionOffsets[p], partitionOffsets[p] + partitionCounts[p])}. Any single partition reaching
     * {@link #PARTITION_WRITE_BATCH} buffered entries triggers the callback, but since keys are partitioned by hash, a good
     * hash function fills all partitions at roughly the same rate, so each batch should carry a reasonable number of entries
     * for every partition.
     */
    interface PartitionSplitter {
        /**
         * @param firstId              the id of the first key in this batch; the absolute id of an entry is
         *                             {@code firstId + shiftedIds[i]}
         * @param shiftedIds           the ids of the keys, relative to {@code firstId}; partition {@code p}'s ids are at
         *                             {@code [p * PARTITION_WRITE_BATCH, p * PARTITION_WRITE_BATCH + partitionCounts[p])}
         * @param batchSize            the total number of ids in this batch
         * @param batchPartitionCounts the number of this batch's ids that fall into each partition
         * @param partitionOffsets     the number of ids each partition received in previous batches, i.e. the position
         *                             within the partition where this batch's entries start
         */
        void split(int firstId, short[] shiftedIds, int batchSize, int[] batchPartitionCounts, int[] partitionOffsets);

        /**
         * Releases the state held by this splitter.
         */
        void release(CircuitBreaker breaker);
    }

    /**
     * Partitions the keys of this hash table into {@link #NUM_PARTITIONS} partitions by hash, invoking
     * {@code partitionSplitter} periodically so the caller can split associated per-key state the same way.
     *
     * @return the keys of every partition, to be passed to {@link #combinePartition}
     */
    PartitionedHashKeys splitPartition(CircuitBreaker breaker, PartitionSplitter partitionSplitter);

    /**
     * Combines one partition of keys into this hash table, writing the id assigned to each key to {@code resultIds} so the
     * caller can merge the associated per-key state. This table must only contain keys of the same partition.
     *
     * @param resultIds                 the combined ids will be written to this array - must be at least the size of the
     *                                  {@link PartitionedHashKeys#keysInPartition(int)} for the partition index
     * @return {@code true} if every key in the partition was new to this table. In that case the assigned ids are consecutive
     *                      and in input order, starting at the size of this table before the call, so callers can bulk-copy the
     *                      associated per-key state instead of scattering it by id.
     */
    boolean combinePartition(PartitionedHashKeys keys, int partitionIndex, int[] resultIds);
}
