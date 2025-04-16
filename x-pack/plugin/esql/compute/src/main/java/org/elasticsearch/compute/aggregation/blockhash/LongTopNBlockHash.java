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
import org.elasticsearch.common.util.LongHash;
import org.elasticsearch.compute.aggregation.GroupingAggregatorFunction;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.LongVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.mvdedupe.MultivalueDedupe;
import org.elasticsearch.compute.operator.mvdedupe.MultivalueDedupeLong;
import org.elasticsearch.compute.operator.mvdedupe.TopNMultivalueDedupeLong;
import org.elasticsearch.core.ReleasableIterator;

import java.util.BitSet;
import java.util.Comparator;
import java.util.TreeSet;

/**
 * Maps a {@link LongBlock} column to group ids, keeping only the top N values.
 */
// TODO: package-private instead of public?
public final class LongTopNBlockHash extends TopNBlockHash {
    private final int channel;
    private final boolean asc;
    private final boolean nullsFirst;
    private final int limit;
    private final LongHash hash;
    private final TreeSet<Long> topValues;

    /**
     * Have we seen any {@code null} values?
     * <p>
     *     We reserve the 0 ordinal for the {@code null} key so methods like
     *     {@link #nonEmpty} need to skip 0 if we haven't seen any null values.
     * </p>
     */
    private boolean hasNull;

    // TODO: package-private instead of public?
    public LongTopNBlockHash(int channel, boolean asc, boolean nullsFirst, int limit, BlockFactory blockFactory) {
        super(blockFactory);
        this.channel = channel;
        this.asc = asc;
        this.nullsFirst = nullsFirst;
        this.limit = limit;
        this.hash = new LongHash(1, blockFactory.bigArrays());
        this.topValues = new TreeSet<>(asc ? Comparator.<Long>naturalOrder() : Comparator.<Long>reverseOrder());

        assert limit > 0 : "LongTopNBlockHash requires a limit greater than 0";
    }

    @Override
    public void add(Page page, GroupingAggregatorFunction.AddInput addInput) {
        // TODO track raw counts and which implementation we pick for the profiler - #114008
        var block = page.getBlock(channel);

        if (block.areAllValuesNull() && acceptNull()) {
            hasNull = true;
            try (IntVector groupIds = blockFactory.newConstantIntVector(0, block.getPositionCount())) {
                addInput.add(0, groupIds);
            }
            return;
        }
        LongBlock castBlock = (LongBlock) block;
        LongVector vector = castBlock.asVector();
        if (vector == null) {
            try (IntBlock groupIds = add(castBlock)) {
                addInput.add(0, groupIds);
            }
            return;
        }
        try (IntVector groupIds = add(vector)) {
            addInput.add(0, groupIds);
        }
    }

    /**
     * Tries to add null to the top values, and returns true if it was successful.
     */
    private boolean acceptNull() {
        if (hasNull) {
            return true;
        }

        if (nullsFirst) {
            hasNull = true;
            if (topValues.size() == limit) {
                topValues.remove(topValues.last());
            }
            return true;
        }

        if (topValues.size() < limit) {
            hasNull = true;
            return true;
        }

        return false;
    }

    /**
     * Tries to add the value to the top values, and returns true if it was successful.
     */
    private boolean acceptValue(long value) {
        if (isAcceptable(value) == false && isTopComplete()) {
            return false;
        }

        topValues.add(value);

        if (topValues.size() > limit - (hasNull ? 1 : 0)) {
            if (hasNull && nullsFirst == false) {
                hasNull = false;
            } else {
                topValues.remove(topValues.last());
            }
        }

        return true;
    }

    /**
     * Returns true if the value is in, or can be added to the top; false otherwise.
     */
    private boolean isAcceptable(long value) {
        return isTopComplete() == false || isInTop(value);
    }

    /**
     * Returns true if the value is in the top; false otherwise.
     */
    private boolean isInTop(long value) {
        return asc ? value <= topValues.last() : value >= topValues.last();
    }

    /**
     * Returns true if there are {@code limit} values in the blockhash; false otherwise.
     */
    private boolean isTopComplete() {
        return topValues.size() >= limit - (hasNull ? 1 : 0);
    }

    /**
     *  Adds the vector values to the hash, and returns a new vector with the group IDs for those positions.
     */
    IntVector add(LongVector vector) {
        int positions = vector.getPositionCount();

        // Add all values to the top set, so we don't end up sending invalid values later
        for (int i = 0; i < positions; i++) {
            long v = vector.getLong(i);
            acceptValue(v);
        }

        // Create a vector with the groups
        try (var builder = blockFactory.newIntVectorFixedBuilder(positions)) {
            for (int i = 0; i < positions; i++) {
                long v = vector.getLong(i);
                if (isAcceptable(v)) {
                    builder.appendInt(Math.toIntExact(hashOrdToGroupNullReserved(hash.add(v))));
                }
            }
            return builder.build();
        }
    }

    /**
     *  Adds the block values to the hash, and returns a new vector with the group IDs for those positions.
     * <p>
     *     For nulls, a 0 group ID is used. For multivalues, a multivalue is used with all the group IDs.
     * </p>
     */
    IntBlock add(LongBlock block) {
        // Add all the values to the top set, so we don't end up sending invalid values later
        for (int p = 0; p < block.getPositionCount(); p++) {
            int count = block.getValueCount(p);
            if (count == 0) {
                acceptNull();
                continue;
            }
            int first = block.getFirstValueIndex(p);

            for (int i = 0; i < count; i++) {
                long value = block.getLong(first + i);
                acceptValue(value);
            }
        }

        // TODO: Make the custom dedupe *less custom*
        MultivalueDedupe.HashResult result = new TopNMultivalueDedupeLong(block, hasNull, this::isAcceptable).hashAdd(blockFactory, hash);

        return result.ords();
    }

    @Override
    public ReleasableIterator<IntBlock> lookup(Page page, ByteSizeValue targetBlockSize) {
        var block = page.getBlock(channel);
        if (block.areAllValuesNull()) {
            return ReleasableIterator.single(blockFactory.newConstantIntVector(0, block.getPositionCount()).asBlock());
        }

        LongBlock castBlock = (LongBlock) block;
        LongVector vector = castBlock.asVector();
        // TODO honor targetBlockSize and chunk the pages if requested.
        if (vector == null) {
            return ReleasableIterator.single(lookup(castBlock));
        }
        return ReleasableIterator.single(lookup(vector));
    }

    private IntBlock lookup(LongVector vector) {
        int positions = vector.getPositionCount();
        try (var builder = blockFactory.newIntBlockBuilder(positions)) {
            for (int i = 0; i < positions; i++) {
                long v = vector.getLong(i);
                long found = hash.find(v);
                if (found < 0) {
                    builder.appendNull();
                } else {
                    builder.appendInt(Math.toIntExact(hashOrdToGroupNullReserved(found)));
                }
            }
            return builder.build();
        }
    }

    private IntBlock lookup(LongBlock block) {
        return new MultivalueDedupeLong(block).hashLookup(blockFactory, hash);
    }

    @Override
    public LongBlock[] getKeys() {
        if (hasNull) {
            final long[] keys = new long[topValues.size() + 1];
            int keysIndex = 1;
            for (int i = 1; i < hash.size() + 1; i++) {
                long value = hash.get(i - 1);
                if (isInTop(value)) {
                    keys[keysIndex++] = value;
                }
            }
            BitSet nulls = new BitSet(1);
            nulls.set(0);
            return new LongBlock[] {
                blockFactory.newLongArrayBlock(keys, keys.length, null, nulls, Block.MvOrdering.DEDUPLICATED_AND_SORTED_ASCENDING) };
        }
        final long[] keys = new long[topValues.size()];
        int keysIndex = 0;
        for (int i = 0; i < hash.size(); i++) {
            long value = hash.get(i);
            if (isInTop(value)) {
                keys[keysIndex++] = value;
            }
        }
        return new LongBlock[] { blockFactory.newLongArrayVector(keys, keys.length).asBlock() };
    }

    @Override
    public IntVector nonEmpty() {
        int nullOffset = hasNull ? 1 : 0;
        final int[] ids = new int[topValues.size() + nullOffset];
        int idsIndex = nullOffset;
        for (int i = 1; i < hash.size() + 1; i++) {
            long value = hash.get(i - 1);
            if (isInTop(value)) {
                ids[idsIndex++] = i;
            }
        }
        return blockFactory.newIntArrayVector(ids, ids.length);
    }

    @Override
    public BitArray seenGroupIds(BigArrays bigArrays) {
        return new Range(hasNull ? 0 : 1, Math.toIntExact(hash.size() + 1)).seenGroupIds(bigArrays);
    }

    @Override
    public void close() {
        hash.close();
    }

    @Override
    public String toString() {
        StringBuilder b = new StringBuilder();
        b.append("LongBlockHash{channel=").append(channel);
        b.append(", entries=").append(hash.size());
        b.append(", seenNull=").append(hasNull);
        return b.append('}').toString();
    }
}
