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
import org.elasticsearch.common.util.LongLongHashTable;
import org.elasticsearch.common.util.PartitionedHashTable;
import org.elasticsearch.compute.aggregation.GroupingAggregatorFunction;
import org.elasticsearch.compute.aggregation.SeenGroupIds;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.LongVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.mvdedupe.LongLongBlockAdd;
import org.elasticsearch.core.ReleasableIterator;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.swisshash.LongLongSwissHash;

/**
 * Maps two {@link LongBlock} columns to group ids.
 * <p>
 *     This hash does not handle nulls: the key has no room for a flag, and a null position gets a null group id, which the
 *     aggregations skip. That is wrong for a {@code GROUP BY}, which is why {@link BlockHash#build} only picks it in production
 *     for groups that the planner proved to have no null, see {@link GroupSpec#nonNull()}. For those, a null means that the proof
 *     was wrong, so it throws, and the vectors are added in batches, like {@link LongIntBlockHash} does.
 * </p>
 */
final class LongLongBlockHash extends PartitionedBlockHash {
    private final int channel1;
    private final int channel2;
    private final int emitBatchSize;
    private final LongLongHashTable hash;
    private final boolean nonNull;

    // Only for groups with no null, which add the vectors in batches.
    private final long batchUsedBytes;
    private final long[] batchKeys1;
    private final long[] batchKeys2;
    private final int[] batchIds;

    LongLongBlockHash(BlockFactory blockFactory, int channel1, int channel2, int emitBatchSize) {
        this(blockFactory, channel1, channel2, emitBatchSize, false);
    }

    LongLongBlockHash(BlockFactory blockFactory, int channel1, int channel2, int emitBatchSize, boolean nonNull) {
        super(blockFactory);
        this.channel1 = channel1;
        this.channel2 = channel2;
        this.emitBatchSize = emitBatchSize;
        this.nonNull = nonNull;
        if (nonNull) {
            // The emit size should be at least 256 so that prefetch takes effect.
            final int batchSize = Math.max(emitBatchSize, 256);
            this.batchUsedBytes = (Integer.BYTES + Long.BYTES * 2) * (long) batchSize;
            blockFactory.adjustBreaker(batchUsedBytes);
            boolean success = false;
            try {
                this.batchKeys1 = new long[batchSize];
                this.batchKeys2 = new long[batchSize];
                this.batchIds = new int[batchSize];
                this.hash = HashImplFactory.newLongLongHash(blockFactory);
                success = true;
            } finally {
                if (success == false) {
                    blockFactory.adjustBreaker(-batchUsedBytes);
                }
            }
        } else {
            this.batchUsedBytes = 0;
            this.batchKeys1 = null;
            this.batchKeys2 = null;
            this.batchIds = null;
            this.hash = HashImplFactory.newLongLongHash(blockFactory);
        }
    }

    @Override
    public void close() {
        blockFactory.adjustBreaker(-batchUsedBytes);
        Releasables.close(hash);
    }

    @Override
    public void add(Page page, GroupingAggregatorFunction.AddInput addInput) {
        LongBlock block1 = page.getBlock(channel1);
        LongBlock block2 = page.getBlock(channel2);
        LongVector vector1 = block1.asVector();
        LongVector vector2 = block2.asVector();
        if (vector1 != null && vector2 != null) {
            if (nonNull && hash.supportBulkAdd()) {
                addBatch(vector1, vector2, addInput);
                return;
            }
            try (IntBlock groupIds = add(vector1, vector2).asBlock()) {
                addInput.add(0, groupIds.asVector());
            }
        } else {
            if (nonNull) {
                requireNoNull(block1, block2);
            }
            try (var addBlock = new LongLongBlockAdd(blockFactory, emitBatchSize, addInput, hash, block1, block2)) {
                addBlock.add();
            }
        }
    }

    private void addBatch(LongVector vector1, LongVector vector2, GroupingAggregatorFunction.AddInput addInput) {
        final int positions = vector1.getPositionCount();
        int offset = 0;
        while (offset < positions) {
            final int batchSize = Math.min(batchIds.length, positions - offset);
            vector1.copyTo(offset, batchKeys1, 0, batchSize);
            vector2.copyTo(offset, batchKeys2, 0, batchSize);
            hash.bulkAdd(batchKeys1, batchKeys2, batchIds, batchSize);
            try (var groupIds = blockFactory.newIntArrayVector(batchIds, batchSize)) {
                addInput.add(offset, groupIds);
            }
            offset += batchSize;
        }
    }

    private static void requireNoNull(LongBlock block1, LongBlock block2) {
        for (int p = 0; p < block1.getPositionCount(); p++) {
            if (block1.getValueCount(p) == 0 || block2.getValueCount(p) == 0) {
                throw new IllegalStateException("a group has a null, but the planner said that no group does");
            }
        }
    }

    IntVector add(LongVector vector1, LongVector vector2) {
        int positions = vector1.getPositionCount();
        try (var builder = blockFactory.newIntVectorFixedBuilder(positions)) {
            for (int i = 0; i < positions; i++) {
                builder.appendInt(i, Math.toIntExact(hashOrdToGroup(hash.add(vector1.getLong(i), vector2.getLong(i)))));
            }
            return builder.build();
        }
    }

    @Override
    public ReleasableIterator<IntBlock> lookup(Page page, ByteSizeValue targetBlockSize) {
        throw new UnsupportedOperationException("TODO");
    }

    @Override
    public Block[] getKeys(IntVector selected) {
        int positions = selected.getPositionCount();
        LongVector k1 = null;
        LongVector k2 = null;
        try (
            LongVector.Builder keys1 = blockFactory.newLongVectorBuilder(positions);
            LongVector.Builder keys2 = blockFactory.newLongVectorBuilder(positions)
        ) {
            for (int i = 0; i < positions; i++) {
                int groupId = selected.getInt(i);
                keys1.appendLong(hash.getKey1(groupId));
                keys2.appendLong(hash.getKey2(groupId));
            }
            k1 = keys1.build();
            k2 = keys2.build();
        } finally {
            if (k2 == null) {
                Releasables.close(k1);
            }
        }
        return new Block[] { k1.asBlock(), k2.asBlock() };
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
    public BitArray seenGroupIds(BigArrays bigArrays) {
        return new SeenGroupIds.Range(0, Math.toIntExact(hash.size())).seenGroupIds(bigArrays);
    }

    @Override
    public long estimatedBytesForPartitioning() {
        return hash.size() * (Long.BYTES * 2);
    }

    @Override
    public PartitionedHashTable.PartitionedHashKeys splitPartition(
        CircuitBreaker breaker,
        PartitionedHashTable.PartitionSplitter partitionSplitter
    ) {
        if (hash instanceof LongLongSwissHash swiss) {
            return swiss.splitPartition(breaker, partitionSplitter);
        }
        throw new UnsupportedOperationException(getClass().getSimpleName() + " doesn't support partitioning");
    }

    @Override
    public boolean combinePartition(PartitionedHashTable.PartitionedHashKeys keys, int partitionIndex, int[] resultIds) {
        if (hash instanceof LongLongSwissHash swiss) {
            return swiss.combinePartition(keys, partitionIndex, resultIds);
        }
        throw new UnsupportedOperationException(getClass().getSimpleName() + " doesn't support partitioning");
    }

    @Override
    public void ensureCapacity(int size) {
        if (hash instanceof LongLongSwissHash swiss) {
            swiss.ensureCapacity(size);
        }
    }

    @Override
    public void clear() {
        hash.clear();
    }

    @Override
    public String toString() {
        return "LongLongBlockHash{channels=[" + channel1 + "," + channel2 + "], entries=" + hash.size() + "}";
    }
}
