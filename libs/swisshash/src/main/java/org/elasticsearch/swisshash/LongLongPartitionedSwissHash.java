/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.swisshash;

import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.util.LongLongHashTable;
import org.elasticsearch.common.util.PageCacheRecycler;

import java.util.Arrays;

/**
 * Two-level (long, long) -> id hash. Wraps N {@link TinyLongLongHash} sub-tables and routes keys
 * by the top bits of a Fibonacci hash. Sub-tables use simple linear-probe open addressing, no
 * SIMD control bytes, no prefetching -- the goal is for each sub-table to fit in L2 cache so the
 * hot loop runs cache-resident.
 *
 * <p>The wrapper presents a dense global id space (0..size-1) so it can be a drop-in replacement
 * for {@link LongLongSwissHash} in the aggregator state arrays.
 *
 * <p>{@link #bulkAdd} radix-partitions the input batch, then processes each partition's slice
 * against its own sub-table. While a partition's slice runs, only that sub-table's memory is
 * touched.
 */
public class LongLongPartitionedSwissHash implements LongLongHashTable {

    private static final int N_PARTITIONS = Integer.getInteger("esql.partitioned_longlong_hash.n", 64);
    private static final int PARTITION_BITS = Integer.numberOfTrailingZeros(N_PARTITIONS);
    /** Partition + local id are packed into 32 bits. */
    private static final int LOCAL_ID_BITS = Integer.SIZE - PARTITION_BITS;
    private static final int LOCAL_ID_MASK = (int) ((1L << LOCAL_ID_BITS) - 1);

    private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(LongLongPartitionedSwissHash.class);

    @SuppressWarnings("unused")
    private final PageCacheRecycler recycler;
    @SuppressWarnings("unused")
    private final CircuitBreaker breaker;

    private final TinyLongLongHash[] subHashes;

    /** Per-partition local-id -> dense global-id mapping. Grown lazily as new local ids appear. */
    private final int[][] partitionLocalToGlobal;

    /** Global-id to packed {@code (partition shifted-left LOCAL_ID_BITS) | localId}. */
    private int[] globalToPacked;

    private int totalSize = 0;

    // Scratch buffers reused across bulkAdd calls.
    private long[] scratchKey1;
    private long[] scratchKey2;
    private int[] scratchOrigIdx;
    private int[] scratchPartitions;
    private final int[] partitionStarts = new int[N_PARTITIONS + 1];
    private final int[] partitionOffsets = new int[N_PARTITIONS];

    public LongLongPartitionedSwissHash(PageCacheRecycler recycler, CircuitBreaker breaker) {
        this.recycler = recycler;
        this.breaker = breaker;
        this.subHashes = new TinyLongLongHash[N_PARTITIONS];
        this.partitionLocalToGlobal = new int[N_PARTITIONS][];
        for (int i = 0; i < N_PARTITIONS; i++) {
            subHashes[i] = new TinyLongLongHash();
            partitionLocalToGlobal[i] = new int[16];
        }
        globalToPacked = new int[1024];
    }

    /**
     * Routes (k1, k2) to a partition using the TOP {@link #PARTITION_BITS} bits of {@code k1}
     * (range-partitioning). When {@code k1} is sorted (as in a Lucene index with k1 in the
     * sort key, e.g. WatchID), rows arrive in partition order, so each partition's sub-table
     * stays cache-resident during its run.
     */
    private static int partition(long k1, long k2) {
        return (int) (k1 >>> (Long.SIZE - PARTITION_BITS)) & ((1 << PARTITION_BITS) - 1);
    }

    private int assignGlobalId(int p, int localId) {
        int globalId = totalSize++;
        int[] p2g = partitionLocalToGlobal[p];
        if (localId >= p2g.length) {
            partitionLocalToGlobal[p] = Arrays.copyOf(p2g, Math.max(localId + 1, p2g.length * 2));
            p2g = partitionLocalToGlobal[p];
        }
        p2g[localId] = globalId;
        if (globalId >= globalToPacked.length) {
            globalToPacked = Arrays.copyOf(globalToPacked, Math.max(globalId + 1, globalToPacked.length * 2));
        }
        globalToPacked[globalId] = (p << LOCAL_ID_BITS) | localId;
        return globalId;
    }

    @Override
    public long add(long key1, long key2) {
        int p = partition(key1, key2);
        long subResult = subHashes[p].add(key1, key2);
        if (subResult >= 0) {
            return assignGlobalId(p, (int) subResult);
        } else {
            int localId = (int) (-1L - subResult);
            return -1L - partitionLocalToGlobal[p][localId];
        }
    }

    @Override
    public long find(long key1, long key2) {
        int p = partition(key1, key2);
        long localId = subHashes[p].find(key1, key2);
        if (localId < 0) {
            return -1;
        }
        return partitionLocalToGlobal[p][(int) localId];
    }

    @Override
    public long getKey1(long id) {
        int packed = globalToPacked[(int) id];
        int p = packed >>> LOCAL_ID_BITS;
        int localId = packed & LOCAL_ID_MASK;
        return subHashes[p].key1AtId(localId);
    }

    @Override
    public long getKey2(long id) {
        int packed = globalToPacked[(int) id];
        int p = packed >>> LOCAL_ID_BITS;
        int localId = packed & LOCAL_ID_MASK;
        return subHashes[p].key2AtId(localId);
    }

    @Override
    public long size() {
        return totalSize;
    }

    @Override
    public boolean supportBulkAdd() {
        return true;
    }

    @Override
    public void bulkAdd(long[] key1s, long[] key2s, int[] ids, int length) {
        // Fast path: walk the batch detecting runs of consecutive same-partition rows. With
        // range-partitioning and a {@code k1}-sorted input (the common case for ESQL queries
        // grouping by a Lucene sort-key column), the entire batch is typically one or a few runs,
        // so we avoid the scatter pass entirely and keep each sub-table cache-resident during its
        // run. Falls back gracefully (one row at a time) when partitions are interleaved.
        int i = 0;
        long lastK1 = Long.MIN_VALUE;
        long lastK2 = Long.MIN_VALUE;
        int lastGlobalId = -1;
        boolean cachedKey = false;
        while (i < length) {
            int p = partition(key1s[i], key2s[i]);
            TinyLongLongHash subHash = subHashes[p];
            int[] p2g = partitionLocalToGlobal[p];
            // Process consecutive rows that fall in the same partition together.
            while (i < length && partition(key1s[i], key2s[i]) == p) {
                long k1 = key1s[i];
                long k2 = key2s[i];
                int globalId;
                if (cachedKey && k1 == lastK1 && k2 == lastK2) {
                    // Sort-order fast path: same (k1, k2) as previous row -> reuse the groupId
                    // without re-hashing. For sorted input this catches all consecutive duplicates.
                    globalId = lastGlobalId;
                } else {
                    long subResult = subHash.add(k1, k2);
                    if (subResult >= 0) {
                        globalId = assignGlobalId(p, (int) subResult);
                        p2g = partitionLocalToGlobal[p];
                    } else {
                        int localId = (int) (-1L - subResult);
                        globalId = p2g[localId];
                    }
                    lastK1 = k1;
                    lastK2 = k2;
                    lastGlobalId = globalId;
                    cachedKey = true;
                }
                ids[i] = globalId;
                i++;
            }
        }
    }

    @Override
    public long ramBytesUsed() {
        long bytes = BASE_RAM_BYTES_USED;
        for (TinyLongLongHash sub : subHashes) {
            if (sub != null) {
                bytes += sub.ramBytesUsed();
            }
        }
        for (int[] arr : partitionLocalToGlobal) {
            if (arr != null) {
                bytes += RamUsageEstimator.sizeOf(arr);
            }
        }
        bytes += RamUsageEstimator.sizeOf(globalToPacked);
        return bytes;
    }

    @Override
    public void close() {
        if (subHashes != null) {
            for (int i = 0; i < N_PARTITIONS; i++) {
                if (subHashes[i] != null) {
                    subHashes[i].close();
                    subHashes[i] = null;
                }
            }
        }
    }
}
