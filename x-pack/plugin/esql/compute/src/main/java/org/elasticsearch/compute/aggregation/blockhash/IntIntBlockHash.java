/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation.blockhash;

import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.BitArray;
import org.elasticsearch.common.util.LongHashTable;
import org.elasticsearch.compute.aggregation.GroupingAggregatorFunction;
import org.elasticsearch.compute.aggregation.SeenGroupIds;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.core.ReleasableIterator;
import org.elasticsearch.core.Releasables;

/**
 * Maps two {@link IntBlock} columns to group ids by packing both ints into a single long
 * key and using a {@link LongHashTable}. Avoids the BytesRef-packed slow path used by
 * {@link PackedValuesBlockHash} for multi-column integer GROUP BYs.
 */
final class IntIntBlockHash extends BlockHash {
    private final int channel1;
    private final int channel2;
    private final LongHashTable hash;

    IntIntBlockHash(BlockFactory blockFactory, int channel1, int channel2) {
        super(blockFactory);
        this.channel1 = channel1;
        this.channel2 = channel2;
        this.hash = HashImplFactory.newLongHash(blockFactory);
    }

    private static long packKey(int k1, int k2) {
        return (((long) k1) << 32) | (k2 & 0xFFFFFFFFL);
    }

    private static int unpackHi(long key) {
        return (int) (key >>> 32);
    }

    private static int unpackLo(long key) {
        return (int) key;
    }

    @Override
    public void close() {
        Releasables.close(hash);
    }

    @Override
    public void add(Page page, GroupingAggregatorFunction.AddInput addInput) {
        IntBlock block1 = page.getBlock(channel1);
        IntBlock block2 = page.getBlock(channel2);
        IntVector vector1 = block1.asVector();
        IntVector vector2 = block2.asVector();
        if (vector1 != null && vector2 != null) {
            try (IntBlock groupIds = add(vector1, vector2).asBlock()) {
                addInput.add(0, groupIds.asVector());
            }
        } else {
            // Fall through to slow path - multi-valued / nullable handling not implemented here.
            // Callers should select PackedValuesBlockHash for such inputs.
            throw new UnsupportedOperationException("IntIntBlockHash requires non-null, single-valued vector inputs");
        }
    }

    IntVector add(IntVector vector1, IntVector vector2) {
        int positions = vector1.getPositionCount();
        try (var builder = blockFactory.newIntVectorFixedBuilder(positions)) {
            for (int i = 0; i < positions; i++) {
                long packed = packKey(vector1.getInt(i), vector2.getInt(i));
                builder.appendInt(i, Math.toIntExact(hashOrdToGroup(hash.add(packed))));
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
        IntVector k1 = null;
        IntVector k2 = null;
        try (
            IntVector.Builder keys1 = blockFactory.newIntVectorBuilder(positions);
            IntVector.Builder keys2 = blockFactory.newIntVectorBuilder(positions)
        ) {
            for (int i = 0; i < positions; i++) {
                int groupId = selected.getInt(i);
                long packed = hash.get(groupId);
                keys1.appendInt(unpackHi(packed));
                keys2.appendInt(unpackLo(packed));
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
    public String toString() {
        return "IntIntBlockHash{channels=[" + channel1 + "," + channel2 + "], entries=" + hash.size() + "}";
    }
}
