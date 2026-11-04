/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation.blockhash;

import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.BitArray;
import org.elasticsearch.common.util.LongHashTable;
import org.elasticsearch.common.util.PartitionedHashTable;
import org.elasticsearch.compute.aggregation.GroupingAggregatorFunction;
import org.elasticsearch.compute.aggregation.SeenGroupIds;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.mvdedupe.MultivalueDedupeInt;
import org.elasticsearch.core.ReleasableIterator;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.swisshash.LongSwissHash;

/**
 * Maps two {@link IntBlock} columns to group ids by packing the two ints of a key into one {@code long}.
 * <p>
 *     This hash does not support nulls: two ints use all 64 bits of the {@code long}, so there is no room to tell a null from a
 *     value. Only use it for groups that the planner proved to have no null, see {@link GroupSpec#nonNull()}. A null throws an
 *     {@link IllegalStateException}, because it means that the proof was wrong, and hashing it as a value would give a wrong result.
 *     Multivalued positions are supported.
 * </p>
 * <p>
 *     Group ids are the ordinals of the keys, with no id reserved for null. It does not support {@link #lookup}, which only the
 *     hashes used for joins need.
 * </p>
 */
final class IntIntBlockHash extends PartitionedBlockHash {
    private static final int PREFETCH_BATCH = 128;

    private final int channel1;
    private final int channel2;
    private final int emitBatchSize;
    final LongHashTable hash;

    private final PrefetchBarrier prefetchBarrier = new PrefetchBarrier();
    private final int[] batchHashes = new int[PREFETCH_BATCH];
    private final long[] batchKeys = new long[PREFETCH_BATCH];

    IntIntBlockHash(BlockFactory blockFactory, int channel1, int channel2, int emitBatchSize) {
        super(blockFactory);
        this.channel1 = channel1;
        this.channel2 = channel2;
        this.emitBatchSize = emitBatchSize;
        this.hash = HashImplFactory.newLongHash(blockFactory);
    }

    /** The key of two ints: the first one in the high 32 bits. */
    static long pack(int first, int second) {
        return ((long) first << 32) | (second & 0xFFFFFFFFL);
    }

    static int first(long key) {
        return (int) (key >>> 32);
    }

    static int second(long key) {
        return (int) key;
    }

    @Override
    public void add(Page page, GroupingAggregatorFunction.AddInput addInput) {
        IntBlock block1 = page.getBlock(channel1);
        IntBlock block2 = page.getBlock(channel2);
        IntVector vector1 = block1.asVector();
        IntVector vector2 = block2.asVector();
        if (vector1 != null && vector2 != null) {
            try (IntVector groupIds = add(vector1, vector2)) {
                addInput.add(0, groupIds);
            }
        } else {
            addBlock(block1, block2, addInput);
        }
    }

    IntVector add(IntVector vector1, IntVector vector2) {
        if (hash instanceof LongSwissHash swiss && swiss.shouldPrefetch()) {
            return addWithPrefetch(vector1, vector2, swiss);
        }
        final int positions = vector1.getPositionCount();
        try (var builder = blockFactory.newIntVectorFixedBuilder(positions)) {
            for (int i = 0; i < positions; i++) {
                long key = pack(vector1.getInt(i), vector2.getInt(i));
                builder.appendInt(i, Math.toIntExact(hashOrdToGroup(hash.add(key))));
            }
            return builder.build();
        }
    }

    private IntVector addWithPrefetch(IntVector vector1, IntVector vector2, LongSwissHash swiss) {
        final int positions = vector1.getPositionCount();
        int dummy = 0;
        try (var builder = blockFactory.newIntVectorFixedBuilder(positions)) {
            for (int offset = 0; offset < positions; offset += PREFETCH_BATCH) {
                final int batchSize = Math.min(PREFETCH_BATCH, positions - offset);
                for (int i = 0; i < batchSize; i++) {
                    batchKeys[i] = pack(vector1.getInt(offset + i), vector2.getInt(offset + i));
                    batchHashes[i] = LongSwissHash.hash(batchKeys[i]);
                    dummy ^= swiss.prefetch(batchHashes[i]);
                }
                for (int i = 0; i < batchSize; i++) {
                    final long id = swiss.addWithHash(batchKeys[i], batchHashes[i]);
                    builder.appendInt(offset + i, Math.toIntExact(hashOrdToGroup(id)));
                }
            }
            prefetchBarrier.consume(dummy);
            return builder.build();
        }
    }

    private void addBlock(IntBlock block1, IntBlock block2, GroupingAggregatorFunction.AddInput addInput) {
        try (
            IntBlock deduped1 = new MultivalueDedupeInt(block1).dedupeToBlockAdaptive(blockFactory);
            IntBlock deduped2 = new MultivalueDedupeInt(block2).dedupeToBlockAdaptive(blockFactory);
            AddBlockWork work = new AddBlockWork(deduped1, deduped2, addInput, emitBatchSize)
        ) {
            work.add();
        }
    }

    private class AddBlockWork extends AddPage {
        final IntBlock block1;
        final IntBlock block2;

        AddBlockWork(IntBlock block1, IntBlock block2, GroupingAggregatorFunction.AddInput addInput, int batchSize) {
            super(blockFactory, batchSize, addInput);
            this.block1 = block1;
            this.block2 = block2;
        }

        void add() {
            final int positionCount = block1.getPositionCount();
            for (int p = 0; p < positionCount; p++) {
                final int count1 = block1.getValueCount(p);
                final int count2 = block2.getValueCount(p);
                if (count1 == 0 || count2 == 0) {
                    throw new IllegalStateException("a group has a null, but the planner said that no group does");
                }
                final int start1 = block1.getFirstValueIndex(p);
                final int start2 = block2.getFirstValueIndex(p);
                if (count1 == 1 && count2 == 1) {
                    long key = pack(block1.getInt(start1), block2.getInt(start2));
                    appendOrdSv(p, Math.toIntExact(hashOrdToGroup(hash.add(key))));
                    continue;
                }
                final int end1 = start1 + count1;
                final int end2 = start2 + count2;
                for (int v1 = start1; v1 < end1; v1++) {
                    final int first = block1.getInt(v1);
                    for (int v2 = start2; v2 < end2; v2++) {
                        appendOrdInMv(p, Math.toIntExact(hashOrdToGroup(hash.add(pack(first, block2.getInt(v2))))));
                    }
                }
                finishMv();
            }
            flushRemaining();
        }
    }

    @Override
    public ReleasableIterator<IntBlock> lookup(Page page, ByteSizeValue targetBlockSize) {
        throw new UnsupportedOperationException("IntIntBlockHash is only for aggregations");
    }

    @Override
    public Block[] getKeys(IntVector selected) {
        final int positions = selected.getPositionCount();
        IntVector keys1 = null;
        IntVector keys2 = null;
        try (
            IntVector.FixedBuilder builder1 = blockFactory.newIntVectorFixedBuilder(positions);
            IntVector.FixedBuilder builder2 = blockFactory.newIntVectorFixedBuilder(positions)
        ) {
            for (int i = 0; i < positions; i++) {
                long key = hash.get(selected.getInt(i));
                builder1.appendInt(i, first(key));
                builder2.appendInt(i, second(key));
            }
            keys1 = builder1.build();
            keys2 = builder2.build();
            return new Block[] { keys1.asBlock(), keys2.asBlock() };
        } finally {
            if (keys2 == null) {
                Releasables.close(keys1);
            }
        }
    }

    @Override
    public IntVector nonEmpty() {
        return blockFactory.newIntRangeVector(0, Math.toIntExact(hash.size()));
    }

    @Override
    public int numKeys() {
        return Math.toIntExact(hash.size());
    }

    @Override
    public long estimatedBytesForPartitioning() {
        return hash.size() * Long.BYTES;
    }

    @Override
    public BitArray seenGroupIds(BigArrays bigArrays) {
        return new SeenGroupIds.Range(0, Math.toIntExact(hash.size())).seenGroupIds(bigArrays);
    }

    @Override
    public PartitionedHashTable.PartitionedHashKeys splitPartition(
        CircuitBreaker breaker,
        PartitionedHashTable.PartitionSplitter partitionSplitter
    ) {
        if (hash instanceof LongSwissHash swiss) {
            return swiss.splitPartition(breaker, partitionSplitter);
        }
        throw new UnsupportedOperationException(getClass().getSimpleName() + " doesn't support partitioning");
    }

    @Override
    public boolean combinePartition(PartitionedHashTable.PartitionedHashKeys keys, int partitionIndex, int[] resultIds) {
        if (hash instanceof LongSwissHash swiss) {
            return swiss.combinePartition(keys, partitionIndex, resultIds);
        }
        throw new UnsupportedOperationException(getClass().getSimpleName() + " doesn't support partitioning");
    }

    @Override
    public void ensureCapacity(int size) {
        if (hash instanceof LongSwissHash swiss) {
            swiss.ensureCapacity(size);
        }
    }

    @Override
    public void clear() {
        hash.clear();
    }

    @Override
    public void close() {
        prefetchBarrier.flush();
        hash.close();
    }

    @Override
    public String toString() {
        return "IntIntBlockHash{channels=[" + channel1 + "," + channel2 + "], entries=" + hash.size() + "}";
    }
}
