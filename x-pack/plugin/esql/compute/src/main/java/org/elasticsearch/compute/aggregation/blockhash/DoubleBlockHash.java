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
import org.elasticsearch.compute.data.DoubleArrayBlock;
import org.elasticsearch.compute.data.DoubleArrayVector;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.DoubleVector;
import org.elasticsearch.compute.data.IntArrayVector;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.Page;
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

    DoubleBlockHash(int channel, BigArrays bigArrays) {
        this.channel = channel;
        this.longHash = new LongHash(1, bigArrays);
    }

    @Override
    public void add(Page page, GroupingAggregatorFunction.AddInput addInput) {
        DoubleBlock block = page.getBlock(channel);
        DoubleVector vector = block.asVector();
        if (vector == null) {
            addInput.add(0, add(block));
        } else {
            addInput.add(0, add(vector));
        }
    }

    private IntVector add(DoubleVector vector) {
        int[] groups = new int[vector.getPositionCount()];
        for (int i = 0; i < vector.getPositionCount(); i++) {
            groups[i] = Math.toIntExact(hashOrdToGroupNullReserved(longHash.add(Double.doubleToLongBits(vector.getDouble(i)))));
        }
        return new IntArrayVector(groups, groups.length);
    }

    private IntBlock add(DoubleBlock block) {
        MultivalueDedupe.HashResult result = new MultivalueDedupeDouble(block).hash(longHash);
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
                new DoubleArrayBlock(keys, keys.length, null, nulls, Block.MvOrdering.DEDUPLICATED_AND_SORTED_ASCENDING) };
        }

        final int size = Math.toIntExact(longHash.size());
        final double[] keys = new double[size];
        for (int i = 0; i < size; i++) {
            keys[i] = Double.longBitsToDouble(longHash.get(i));
        }

        // TODO claim the array and wrap?
        return new DoubleBlock[] { new DoubleArrayVector(keys, keys.length).asBlock() };
    }

    @Override
    public IntVector nonEmpty() {
        return IntVector.range(seenNull ? 0 : 1, Math.toIntExact(longHash.size() + 1));
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
