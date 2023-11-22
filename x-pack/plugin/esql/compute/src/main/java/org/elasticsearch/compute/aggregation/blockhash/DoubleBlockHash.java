/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation.blockhash;

import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.BitArray;
import org.elasticsearch.common.util.LongHash;
import org.elasticsearch.compute.aggregation.GroupingAggregatorFunction;
import org.elasticsearch.compute.aggregation.SeenGroupIds;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.DoubleVector;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.MultivalueDedupe;
import org.elasticsearch.compute.operator.MultivalueDedupeDouble;

import java.util.BitSet;

/**
 * Maps a {@link DoubleBlock} column to group ids.
 */
final class DoubleBlockHash extends BlockHash {
    private final int channel;
    private final LongHash longHash;

    /**
     * Have we seen any {@code null} values?
     * <p>
     *     We reserve the 0 ordinal for the {@code null} key so methods like
     *     {@link #nonEmpty} need to skip 0 if we haven't seen any null values.
     * </p>
     */
    private boolean seenNull;

    DoubleBlockHash(int channel, DriverContext driverContext) {
        super(driverContext);
        this.channel = channel;
        this.longHash = new LongHash(1, bigArrays);
    }

    @Override
    public void add(Page page, GroupingAggregatorFunction.AddInput addInput) {
        var block = page.getBlock(channel);
        if (block.areAllValuesNull()) {
            seenNull = true;
            try (IntVector groupIds = blockFactory.newConstantIntVector(0, block.getPositionCount())) {
                addInput.add(0, groupIds);
            }
        } else {
            DoubleBlock doubleBlock = (DoubleBlock) block;
            DoubleVector doubleVector = doubleBlock.asVector();
            if (doubleVector == null) {
                try (IntBlock groupIds = add(doubleBlock)) {
                    addInput.add(0, groupIds);
                }
            } else {
                try (IntVector groupIds = add(doubleVector)) {
                    addInput.add(0, groupIds);
                }
            }
        }
    }

    private IntVector add(DoubleVector vector) {
        int positions = vector.getPositionCount();
        try (var builder = blockFactory.newIntVectorFixedBuilder(positions)) {
            for (int i = 0; i < positions; i++) {
                builder.appendInt(Math.toIntExact(hashOrdToGroupNullReserved(longHash.add(Double.doubleToLongBits(vector.getDouble(i))))));
            }
            return builder.build();
        }
    }

    private IntBlock add(DoubleBlock block) {
        MultivalueDedupe.HashResult result = new MultivalueDedupeDouble(block).hash(longHash); // TODO: block factory
        seenNull |= result.sawNull();
        return result.ords();
    }

    @Override
    public DoubleBlock[] getKeys() {
        if (seenNull) {
            final int size = Math.toIntExact(longHash.size() + 1);
            final double[] keys = new double[size];
            for (int i = 1; i < size; i++) {
                keys[i] = Double.longBitsToDouble(longHash.get(i - 1));
            }
            BitSet nulls = new BitSet(1);
            nulls.set(0);
            return new DoubleBlock[] {
                blockFactory.newDoubleArrayBlock(keys, keys.length, null, nulls, Block.MvOrdering.DEDUPLICATED_AND_SORTED_ASCENDING) };
        }

        final int size = Math.toIntExact(longHash.size());
        final double[] keys = new double[size];
        for (int i = 0; i < size; i++) {
            keys[i] = Double.longBitsToDouble(longHash.get(i));
        }

        // TODO claim the array and wrap?
        return new DoubleBlock[] { blockFactory.newDoubleArrayVector(keys, keys.length).asBlock() };
    }

    @Override
    public IntVector nonEmpty() {
        return IntVector.range(seenNull ? 0 : 1, Math.toIntExact(longHash.size() + 1), blockFactory);
    }

    @Override
    public BitArray seenGroupIds(BigArrays bigArrays) {
        return new SeenGroupIds.Range(seenNull ? 0 : 1, Math.toIntExact(longHash.size() + 1)).seenGroupIds(bigArrays);
    }

    @Override
    public void close() {
        longHash.close();
    }

    @Override
    public String toString() {
        return "DoubleBlockHash{channel=" + channel + ", entries=" + longHash.size() + ", seenNull=" + seenNull + '}';
    }
}
