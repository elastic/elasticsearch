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
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.MultivalueDedupe;
import org.elasticsearch.compute.operator.MultivalueDedupeInt;

import java.util.BitSet;

/**
 * Maps a {@link IntBlock} column to group ids.
 */
final class IntBlockHash extends BlockHash {
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

    IntBlockHash(int channel, DriverContext driverContext) {
        super(driverContext);
        this.channel = channel;
        this.longHash = new LongHash(1, bigArrays);
    }

    @Override
    public void add(Page page, GroupingAggregatorFunction.AddInput addInput) {
        IntBlock block = page.getBlock(channel);
        IntVector vector = block.asVector();
        if (vector == null) {
            try (IntBlock groupIds = add(block)) {
                addInput.add(0, groupIds);
            }
        } else {
            try (IntVector groupIds = add(vector)) {
                addInput.add(0, add(groupIds));
            }
        }
    }

    private IntVector add(IntVector vector) {
        long preAdjustBytes = blockFactory.preAdjustBreakerForInt(vector.getPositionCount());
        int[] groups = new int[vector.getPositionCount()];
        for (int i = 0; i < vector.getPositionCount(); i++) {
            groups[i] = Math.toIntExact(hashOrdToGroupNullReserved(longHash.add(vector.getInt(i))));
        }
        return blockFactory.newIntArrayVector(groups, groups.length, preAdjustBytes);
    }

    private IntBlock add(IntBlock block) {
        MultivalueDedupe.HashResult result = new MultivalueDedupeInt(block).hash(longHash);
        seenNull |= result.sawNull();
        return result.ords();
    }

    @Override
    public IntBlock[] getKeys() {
        if (seenNull) {
            final int size = Math.toIntExact(longHash.size() + 1);
            final int[] keys = new int[size];
            for (int i = 1; i < size; i++) {
                keys[i] = (int) longHash.get(i - 1);
            }
            BitSet nulls = new BitSet(1);
            nulls.set(0);
            return new IntBlock[] { blockFactory.newIntArrayBlock(keys, keys.length, null, nulls, Block.MvOrdering.DEDUPLICATED_AND_SORTED_ASCENDING) };
        }
        final int size = Math.toIntExact(longHash.size());
        final int[] keys = new int[size];
        for (int i = 0; i < size; i++) {
            keys[i] = (int) longHash.get(i);
        }
        return new IntBlock[] { blockFactory.newIntArrayVector(keys, keys.length).asBlock() };
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
        return "IntBlockHash{channel=" + channel + ", entries=" + longHash.size() + ", seenNull=" + seenNull + '}';
    }
}
