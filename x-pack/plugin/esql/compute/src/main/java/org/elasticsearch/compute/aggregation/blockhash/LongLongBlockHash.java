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
import org.elasticsearch.common.util.LongLongHash;
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

/**
 * Maps two {@link LongBlock} columns to group ids.
 */
final class LongLongBlockHash extends BlockHash {
    private final int channel1;
    private final int channel2;
    private final int emitBatchSize;
    private final LongLongHash hash;

    LongLongBlockHash(BlockFactory blockFactory, int channel1, int channel2, int emitBatchSize) {
        super(blockFactory);
        this.channel1 = channel1;
        this.channel2 = channel2;
        this.emitBatchSize = emitBatchSize;
        this.hash = new LongLongHash(1, blockFactory.bigArrays());
    }

    @Override
    public void close() {
        Releasables.close(hash);
    }

    @Override
    public void add(Page page, GroupingAggregatorFunction.AddInput addInput) {
        LongBlock block1 = page.getBlock(channel1);
        LongBlock block2 = page.getBlock(channel2);
        LongVector vector1 = block1.asVector();
        LongVector vector2 = block2.asVector();
        if (vector1 != null && vector2 != null) {
            try (IntBlock groupIds = add(vector1, vector2).asBlock()) {
                addInput.add(0, groupIds.asVector());
            }
        } else {
            try (var addBlock = new LongLongBlockAdd(blockFactory, emitBatchSize, addInput, hash, block1, block2)) {
                addBlock.add();
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
    public Block[] getKeys() {
        int positions = (int) hash.size();
        LongVector k1 = null;
        LongVector k2 = null;
        try (
            LongVector.Builder keys1 = blockFactory.newLongVectorBuilder(positions);
            LongVector.Builder keys2 = blockFactory.newLongVectorBuilder(positions)
        ) {
            for (long i = 0; i < positions; i++) {
                keys1.appendLong(hash.getKey1(i));
                keys2.appendLong(hash.getKey2(i));
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
        return IntVector.range(0, Math.toIntExact(hash.size()), blockFactory);
    }

    @Override
    public BitArray seenGroupIds(BigArrays bigArrays) {
        return new SeenGroupIds.Range(0, Math.toIntExact(hash.size())).seenGroupIds(bigArrays);
    }

    @Override
    public String toString() {
        return "LongLongBlockHash{channels=[" + channel1 + "," + channel2 + "], entries=" + hash.size() + "}";
    }
}
