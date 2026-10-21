/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation.table;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.IntArray;
import org.elasticsearch.common.util.PartitionedHashTable;
import org.elasticsearch.compute.aggregation.blockhash.BlockHash;
import org.elasticsearch.compute.aggregation.blockhash.LongBytesRefBlockHash;
import org.elasticsearch.compute.aggregation.blockhash.PartitionedBlockHash;
import org.elasticsearch.compute.aggregation.blockhash.PartitionedGenerations;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.ReleasableIterator;
import org.elasticsearch.core.Releasables;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.common.util.PartitionedHashTable.NUM_PARTITIONS;
import static org.elasticsearch.common.util.PartitionedHashTable.PARTITION_WRITE_BATCH;

/**
 * Builds the same table as {@code RowInTableLookup.build(blockFactory, keys, true)} using every thread that calls
 * {@link #help()}, so the drivers probing the table build it together instead of waiting on one of them.
 * <p>
 *     The build has two phases, each split into units of work that callers claim one at a time:
 * </p>
 * <ol>
 *     <li>Each slice of build rows is hashed on its own and the hash is split into
 *     {@link PartitionedHashTable#NUM_PARTITIONS} partitions, along with the rows of each key.</li>
 *     <li>Each partition of keys from every slice is combined into one hash per partition, the same way
 *     {@link PartitionedGenerations} combines aggregation states, and the rows of every key are written to their
 *     final place in the table.</li>
 * </ol>
 * <p>
 *     Probes find a key's partition from its hash and look it up in that partition's hash. Every partition gets a
 *     disjoint range of group ids, so the table keeps one array of row offsets, indexed by group id, like a table
 *     built by one thread. A key's rows stay in ascending order.
 * </p>
 * <p>
 *     A caller that claims a unit of work finishes it before returning, so waiting on {@link #slicesDone()} or on
 *     the listener can't deadlock. The keys are only read, never retained or ref counted, so they can be shared
 *     with other threads. The first failure stops claiming new work and completes the listener and
 *     {@link #slicesDone()} exceptionally.
 * </p>
 */
public final class PartitionedRowInTableLookupBuilder implements Releasable {
    /**
     * Fewer build rows than this aren't worth splitting: a single thread builds them faster than threads can
     * coordinate, and each of the {@link PartitionedHashTable#NUM_PARTITIONS} partition hashes costs some memory.
     */
    static final int MIN_ROWS = 1 << 17;
    static final int MIN_SLICE_ROWS = 1 << 16;
    /**
     * Bounds the number of slices so combining a partition doesn't visit too many slices.
     */
    static final int MAX_SLICES = 64;
    private static final String LABEL = "PartitionedRowInTableLookupBuilder";

    /**
     * Whether a repeated-key table on {@code keys} is worth building with this builder.
     */
    public static boolean shouldPartition(Block[] keys) {
        return keys[0].getPositionCount() >= MIN_ROWS
            && PartitionedBlockHash.supportsPartitionedLookup(RepeatedKeyBlockHashRowInTableLookup.groupSpecs(keys));
    }

    private final BlockFactory blockFactory;
    private final CircuitBreaker breaker;
    private final Block[] keys;
    private final List<BlockHash.GroupSpec> groupSpecs;
    private final int rowCount;
    private final int sliceRows;
    private final ActionListener<RowInTableLookup> listener;
    private final AtomicBoolean listenerCompleted = new AtomicBoolean();
    private final AtomicReference<Exception> failure = new AtomicReference<>();
    private final SubscribableListener<Void> slicesDone = new SubscribableListener<>();

    private final Slice[] slices;
    private final AtomicInteger nextSlice = new AtomicInteger();
    private final AtomicInteger completedSlices = new AtomicInteger();

    private final PartitionedGenerations<Slice> generations;
    private final PartitionedBlockHash[] partitions = new PartitionedBlockHash[NUM_PARTITIONS];
    /**
     * Written by the thread that finishes the last slice before it sets {@link #partitionsReady}.
     */
    private int[] groupBase;
    private int[] rowBase;
    private IntArray rowOffsets;
    private IntArray rowIds;
    private volatile boolean partitionsReady;
    private boolean generationsReleased;

    /**
     * @param blockFactory a factory that can be used from any thread; the table is built with it
     * @param keys non-null, single-valued build keys, read but never retained or ref counted
     * @param listener completed once with the built table or the first failure
     */
    public PartitionedRowInTableLookupBuilder(BlockFactory blockFactory, Block[] keys, ActionListener<RowInTableLookup> listener) {
        this(blockFactory, keys, defaultSliceRows(keys[0].getPositionCount()), listener);
    }

    PartitionedRowInTableLookupBuilder(BlockFactory blockFactory, Block[] keys, int sliceRows, ActionListener<RowInTableLookup> listener) {
        if (keys[0].getPositionCount() == 0 || sliceRows < 1) {
            throw new IllegalArgumentException("expected build rows and a positive slice size");
        }
        this.blockFactory = blockFactory;
        this.breaker = blockFactory.breaker();
        this.keys = keys;
        this.groupSpecs = RepeatedKeyBlockHashRowInTableLookup.groupSpecs(keys);
        if (PartitionedBlockHash.supportsPartitionedLookup(groupSpecs) == false) {
            throw new IllegalArgumentException("lookups on " + groupSpecs + " don't support partitioning");
        }
        this.rowCount = keys[0].getPositionCount();
        this.sliceRows = sliceRows;
        this.slices = new Slice[(rowCount + sliceRows - 1) / sliceRows];
        this.listener = listener;
        this.generations = new PartitionedGenerations<>(breaker);
    }

    private static int defaultSliceRows(int rowCount) {
        int sliceCount = Math.clamp((rowCount + MIN_SLICE_ROWS - 1) / MIN_SLICE_ROWS, 1, MAX_SLICES);
        return Math.max(1, (rowCount + sliceCount - 1) / sliceCount);
    }

    /**
     * Completed once every slice is split, so threads waiting for work to claim can wait on this and then help
     * combine partitions.
     */
    public SubscribableListener<Void> slicesDone() {
        return slicesDone;
    }

    /**
     * Claims and does units of work until none is left to claim. Rethrows a failure of the work this thread did.
     */
    public void help() {
        try {
            int s;
            while (failure.get() == null && (s = nextSlice.getAndIncrement()) < slices.length) {
                buildSlice(s);
                if (completedSlices.incrementAndGet() == slices.length) {
                    finishSlices();
                }
            }
            if (partitionsReady == false) {
                return;
            }
            try (var combiner = generations.new KeyCombiner(breaker)) {
                int p;
                while (failure.get() == null && (p = generations.claimPartition()) != PartitionedGenerations.NO_MORE_PARTITION) {
                    combinePartition(p, combiner);
                    if (generations.completePartition()) {
                        finishPartitions();
                    }
                }
            }
        } catch (Exception e) {
            fail(e);
            throw e;
        } catch (Error e) {
            // Wake the waiting threads rather than leave them blocked forever.
            fail(new IllegalStateException("failed to build the lookup table", e));
            throw e;
        }
    }

    /**
     * Hashes the rows of slice {@code s} and splits the hash into partitions, recording which rows have each key.
     */
    private void buildSlice(int s) {
        final int start = s * sliceRows;
        final int end = Math.min(rowCount, start + sliceRows);
        final int n = end - start;
        for (Block key : keys) {
            for (int row = start; row < end; row++) {
                if (key.getValueCount(row) != 1) {
                    throw new IllegalArgumentException("repeated-key lookup requires non-null, single-valued build keys");
                }
            }
        }
        Block[] copies = new Block[keys.length];
        PartitionedBlockHash hash = null;
        PartitionedHashTable.PartitionedHashKeys splitKeys = null;
        long tempBytes = 0;
        long sliceBytes = 0;
        boolean success = false;
        try {
            for (int k = 0; k < keys.length; k++) {
                try (Block.Builder builder = keys[k].elementType().newBlockBuilder(n, blockFactory)) {
                    builder.copyFrom(keys[k], start, end);
                    copies[k] = builder.build();
                }
            }
            hash = PartitionedBlockHash.buildPartitionedForLookup(groupSpecs, blockFactory);
            tempBytes += reserve((long) n * Integer.BYTES);
            final int[] groupOfRow = new int[n];
            final int[] maxGroup = new int[] { -1 };
            hash.add(new Page(copies), new BlockHashGroupIdsAddInput() {
                @Override
                protected void processGroupId(int row, int groupId) {
                    if (groupId < 0) {
                        throw new IllegalStateException("build key produced an invalid group id [" + groupId + "]");
                    }
                    groupOfRow[row] = groupId;
                    maxGroup[0] = Math.max(maxGroup[0], groupId);
                }
            });
            Releasables.closeExpectNoException(copies);
            Arrays.fill(copies, null);

            final int groupCount = maxGroup[0] + 1;
            tempBytes += reserve(2L * groupCount * Integer.BYTES);
            final int[] groupPartition = new int[groupCount];
            Arrays.fill(groupPartition, -1);
            // The index of each group's key within its partition, later within the whole slice.
            final int[] groupKey = new int[groupCount];
            splitKeys = hash.splitPartition(breaker, new PartitionedHashTable.PartitionSplitter() {
                @Override
                public void split(int firstId, short[] shiftedIds, int batchSize, int[] batchPartitionCounts, int[] partitionOffsets) {
                    for (int p = 0; p < NUM_PARTITIONS; p++) {
                        final int base = p * PARTITION_WRITE_BATCH;
                        for (int i = 0; i < batchPartitionCounts[p]; i++) {
                            final int group = firstId + shiftedIds[base + i];
                            groupPartition[group] = p;
                            groupKey[group] = partitionOffsets[p] + i;
                        }
                    }
                }

                @Override
                public void release(CircuitBreaker breaker) {}
            });
            hash.close();
            hash = null;

            sliceBytes += reserve((NUM_PARTITIONS + 1L) * Integer.BYTES);
            final int[] partitionKeyStart = new int[NUM_PARTITIONS + 1];
            for (int p = 0; p < NUM_PARTITIONS; p++) {
                partitionKeyStart[p + 1] = partitionKeyStart[p] + splitKeys.keysInPartition(p);
            }
            final int numKeys = partitionKeyStart[NUM_PARTITIONS];
            for (int group = 0; group < groupCount; group++) {
                if (groupPartition[group] >= 0) {
                    groupKey[group] += partitionKeyStart[groupPartition[group]];
                }
            }
            sliceBytes += reserve((numKeys + 1L + n) * Integer.BYTES);
            final int[] keyRowStart = new int[numKeys + 1];
            final int[] rowsByKey = new int[n];
            for (int row = 0; row < n; row++) {
                assert groupPartition[groupOfRow[row]] >= 0 : "group [" + groupOfRow[row] + "] wasn't split";
                keyRowStart[groupKey[groupOfRow[row]] + 1]++;
            }
            for (int k = 0; k < numKeys; k++) {
                keyRowStart[k + 1] += keyRowStart[k];
            }
            // Use keyRowStart[k] as the cursor of key k, which leaves it at the start of key k + 1, then shift it back.
            for (int row = 0; row < n; row++) {
                rowsByKey[keyRowStart[groupKey[groupOfRow[row]]]++] = start + row;
            }
            System.arraycopy(keyRowStart, 0, keyRowStart, 1, numKeys);
            keyRowStart[0] = 0;

            slices[s] = new Slice(splitKeys, partitionKeyStart, keyRowStart, rowsByKey, sliceBytes);
            success = true;
        } finally {
            release(tempBytes);
            Releasables.closeExpectNoException(copies);
            Releasables.closeExpectNoException(hash);
            if (success == false) {
                release(sliceBytes);
                if (splitKeys != null) {
                    splitKeys.releaseAll(breaker);
                }
            }
        }
    }

    /**
     * Gives every partition a range of group ids and of rows, then opens the partitions to be combined.
     */
    private void finishSlices() {
        if (failure.get() != null) {
            return;
        }
        int[] groupBase = new int[NUM_PARTITIONS + 1];
        int[] rowBase = new int[NUM_PARTITIONS + 1];
        for (int p = 0; p < NUM_PARTITIONS; p++) {
            int keysInPartition = 0;
            int rowsInPartition = 0;
            for (Slice slice : slices) {
                keysInPartition += slice.keys.keysInPartition(p);
                rowsInPartition += slice.rowsInPartition(p);
            }
            // One more than the keys, so the ids of both kinds of partition hash fit: some reserve 0 for null.
            groupBase[p + 1] = groupBase[p] + keysInPartition + 1;
            rowBase[p + 1] = rowBase[p] + rowsInPartition;
        }
        assert rowBase[NUM_PARTITIONS] == rowCount : rowBase[NUM_PARTITIONS] + " != " + rowCount;
        this.groupBase = groupBase;
        this.rowBase = rowBase;
        this.rowOffsets = blockFactory.bigArrays().newIntArray(groupBase[NUM_PARTITIONS] + 1L, false);
        this.rowIds = blockFactory.bigArrays().newIntArray(rowCount, false);
        /*
         * Big arrays allocate a page on its first write, which is not thread-safe, so allocate every page here,
         * before the partitions write disjoint ranges of the arrays from several threads.
         */
        rowOffsets.fill(0, rowOffsets.size(), 0);
        rowIds.fill(0, rowIds.size(), 0);
        rowOffsets.set(groupBase[NUM_PARTITIONS], rowCount);
        // Slice order is row order, so combining the slices in this order keeps each key's rows ascending.
        for (int s = 0; s < slices.length; s++) {
            generations.add(slices[s].keys, slices[s]);
            slices[s] = null;
        }
        partitionsReady = true;
        slicesDone.onResponse(null);
    }

    /**
     * Combines partition {@code p} of every slice into one hash and writes the rows of each of its keys.
     */
    private void combinePartition(int p, PartitionedGenerations<Slice>.KeyCombiner combiner) {
        PartitionedBlockHash target = PartitionedBlockHash.buildPartitionedForLookup(groupSpecs, blockFactory);
        partitions[p] = target;
        final int base = groupBase[p];
        final int width = groupBase[p + 1] - base;
        if (target instanceof LongBytesRefBlockHash == false) {
            // A LongBytesRefBlockHash would size its long and int hash, which combining replaces with a packed keys hash.
            target.ensureCapacity(width - 1);
        }
        combiner.combineKeys(target, p);

        long cursorBytes = reserve((long) width * Integer.BYTES);
        try {
            // First the number of rows of each group, then the position of its next row.
            int[] cursor = new int[width];
            for (int g = 0; g < generations.size(); g++) {
                var generation = generations.get(g);
                int keysInPartition = generation.keys().keysInPartition(p);
                if (keysInPartition == 0) {
                    continue;
                }
                int[] ids = combiner.ids(g);
                Slice slice = generation.state();
                int firstKey = slice.partitionKeyStart[p];
                for (int i = 0; i < keysInPartition; i++) {
                    assert ids[i] >= 0 && ids[i] < width : ids[i] + " outside of [0, " + width + ")";
                    cursor[ids[i]] += slice.keyRowStart[firstKey + i + 1] - slice.keyRowStart[firstKey + i];
                }
            }
            int position = rowBase[p];
            for (int local = 0; local < width; local++) {
                rowOffsets.set(base + local, position);
                int rows = cursor[local];
                cursor[local] = position;
                position += rows;
            }
            assert position == rowBase[p + 1] : position + " != " + rowBase[p + 1];
            for (int g = 0; g < generations.size(); g++) {
                var generation = generations.get(g);
                int keysInPartition = generation.keys().keysInPartition(p);
                if (keysInPartition == 0) {
                    continue;
                }
                int[] ids = combiner.ids(g);
                Slice slice = generation.state();
                int firstKey = slice.partitionKeyStart[p];
                for (int i = 0; i < keysInPartition; i++) {
                    int local = ids[i];
                    for (int r = slice.keyRowStart[firstKey + i]; r < slice.keyRowStart[firstKey + i + 1]; r++) {
                        rowIds.set(cursor[local]++, slice.rowsByKey[r]);
                    }
                }
            }
        } finally {
            release(cursorBytes);
        }
    }

    /**
     * Hands the partition hashes and row arrays to the table and releases what the build no longer needs.
     */
    private void finishPartitions() {
        RowInTableLookup table = new RepeatedKeyBlockHashRowInTableLookup(
            blockFactory,
            new PartitionedGroupLookup(partitions.clone(), groupBase),
            groupBase[NUM_PARTITIONS],
            rowOffsets,
            rowIds
        );
        Arrays.fill(partitions, null);
        rowOffsets = null;
        rowIds = null;
        releaseGenerations();
        if (listenerCompleted.compareAndSet(false, true)) {
            listener.onResponse(table);
        } else {
            table.close();
        }
    }

    private void fail(Exception e) {
        if (failure.compareAndSet(null, e)) {
            slicesDone.onFailure(e);
            if (listenerCompleted.compareAndSet(false, true)) {
                listener.onFailure(e);
            }
        }
    }

    private long reserve(long bytes) {
        breaker.addEstimateBytesAndMaybeBreak(bytes, LABEL);
        return bytes;
    }

    private void release(long bytes) {
        breaker.addWithoutBreaking(-bytes);
    }

    private void releaseGenerations() {
        if (generationsReleased == false) {
            generationsReleased = true;
            generations.close();
        }
    }

    /**
     * Releases whatever the build holds that it hasn't handed to the table. Call only once no thread is in
     * {@link #help()}.
     */
    @Override
    public void close() {
        for (int s = 0; s < slices.length; s++) {
            if (slices[s] != null) {
                slices[s].keys.releaseAll(breaker);
                slices[s].releaseAll(breaker);
                slices[s] = null;
            }
        }
        releaseGenerations();
        Releasables.closeExpectNoException(partitions);
        Arrays.fill(partitions, null);
        Releasables.closeExpectNoException(rowOffsets, rowIds);
        rowOffsets = null;
        rowIds = null;
    }

    /**
     * The keys of one slice split into partitions, and the rows of each key. Keys are numbered by partition, then
     * by their index in the partition: partition {@code p}'s keys are {@code [partitionKeyStart[p], partitionKeyStart[p + 1])}.
     * The rows of key {@code k} are {@code rowsByKey[keyRowStart[k]]} up to {@code rowsByKey[keyRowStart[k + 1]]},
     * ascending.
     */
    private static final class Slice implements PartitionedGenerations.State {
        private final PartitionedHashTable.PartitionedHashKeys keys;
        private final int[] partitionKeyStart;
        private final int[] keyRowStart;
        private final int[] rowsByKey;
        private final long bytes;

        private Slice(
            PartitionedHashTable.PartitionedHashKeys keys,
            int[] partitionKeyStart,
            int[] keyRowStart,
            int[] rowsByKey,
            long bytes
        ) {
            this.keys = keys;
            this.partitionKeyStart = partitionKeyStart;
            this.keyRowStart = keyRowStart;
            this.rowsByKey = rowsByKey;
            this.bytes = bytes;
        }

        int rowsInPartition(int p) {
            return keyRowStart[partitionKeyStart[p + 1]] - keyRowStart[partitionKeyStart[p]];
        }

        @Override
        public void releaseAll(CircuitBreaker breaker) {
            breaker.addWithoutBreaking(-bytes);
        }
    }

    private record PartitionedGroupLookup(PartitionedBlockHash[] partitions, int[] groupBase)
        implements
            RepeatedKeyBlockHashRowInTableLookup.GroupLookup {
        @Override
        public ReleasableIterator<IntBlock> lookup(Page page, ByteSizeValue targetBlockSize) {
            return PartitionedBlockHash.lookupPartitioned(page, targetBlockSize, partitions, groupBase);
        }

        @Override
        public void close() {
            Releasables.closeExpectNoException(partitions);
        }

        @Override
        public String toString() {
            return "Partitioned[" + partitions[0] + "]";
        }
    }
}
