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
import org.elasticsearch.core.Releasable;

/**
 * A hash table whose keys can be redistributed into partitions by hash, so that several tables can be merged partition by
 * partition via {@link #splitPartition} and {@link #combinePartition} instead of all at once. A key always falls into the
 * same partition regardless of which table it came from, so partitions can be combined independently and released as soon
 * as they have been combined.
 */
public interface PartitionedHashTable {
    /**
     * The number of partitions a hash table is split into.
     * Ideally, we should choose the number of partitions dynamically depending on the hash table size.
     */
    int NUM_PARTITIONS = 256;

    /**
     * Mask applied to a key's hash to determine its partition.
     */
    int PARTITION_MASK = NUM_PARTITIONS - 1;

    /**
     * The number of entries buffered per partition before flushing. Entries are staged in a buffer of
     * {@code NUM_PARTITIONS * PARTITION_WRITE_BATCH} entries and flushed one partition at a time, rather than
     * scattering individual writes across all partitions.
     */
    int PARTITION_WRITE_BATCH = 64;

    /**
     * The keys of a hash table, split into partitions. Returned by {@link #splitPartition} and consumed by
     * {@link #combinePartition}. Partitions can be released individually once combined.
     */
    interface PartitionedHashKeys extends Releasable {
        /**
         * Releases the given partition without waiting for the remaining ones.
         */
        void releasePartition(int partition);

        /**
         * Returns the number of keys in the given partition.
         */
        int keysInPartition(int partition);
    }

    /**
     * Callback invoked by {@link #splitPartition} whenever a partition has {@link #PARTITION_WRITE_BATCH} buffered entries,
     * so the caller can split per-key state (e.g. aggregation states) using the same partitioning. Partition {@code p}
     * receives {@code partitionCounts[p]} entries from this batch, to be written to its state at positions
     * {@code [partitionOffsets[p], partitionOffsets[p] + partitionCounts[p])}.
     */
    interface PartitionSplitter extends Releasable {
        /**
         * @param firstId          the id of the first key in this batch; the absolute id of an entry is
         *                         {@code firstId + shiftedIds[i]}
         * @param shiftedIds       the ids of the keys, relative to {@code firstId}; partition {@code p}'s ids are at
         *                         {@code [p * PARTITION_WRITE_BATCH, p * PARTITION_WRITE_BATCH + partitionCounts[p])}
         * @param batchSize        the total number of ids in this batch
         * @param partitionCounts  the number of this batch's ids that fall into each partition
         * @param partitionOffsets the number of ids each partition received in previous batches, i.e. the position
         *                         within the partition where this batch's entries start
         */
        void split(int firstId, short[] shiftedIds, int batchSize, int[] partitionCounts, int[] partitionOffsets);
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
     * caller can merge the associated per-key state. The caller must clear this table before combining a new partition and
     * must supply {@code resultIds} with capacity of at least {@link PartitionedHashKeys#keysInPartition}.
     *
     * @param totalSizeAcrossPartitions the total number of keys across all partitions, used to size this table
     * @return {@code true} if the entire partition was append-only. The caller can take advantage of this to enable optimizations.
     */
    boolean combinePartition(PartitionedHashKeys keys, int partitionIndex, int totalSizeAcrossPartitions, int[] resultIds);
}
