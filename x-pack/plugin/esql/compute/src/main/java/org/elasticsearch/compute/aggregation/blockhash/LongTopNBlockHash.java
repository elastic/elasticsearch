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
import org.elasticsearch.compute.data.sort.LongTopNUniqueSort;
import org.elasticsearch.compute.operator.mvdedupe.MultivalueDedupe;
import org.elasticsearch.compute.operator.mvdedupe.TopNMultivalueDedupeLong;
import org.elasticsearch.core.ReleasableIterator;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.search.sort.SortOrder;

import java.util.BitSet;

/**
 * Maps a {@link LongBlock} column to group ids, keeping only the top N values.
 */
final class LongTopNBlockHash extends BlockHash {
    private final int channel;
    private final boolean asc;
    private final boolean nullsFirst;
    private final int limit;
    private final LongHash hash;
    private final LongTopNUniqueSort topValues;

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
        this.topValues = new LongTopNUniqueSort(blockFactory.bigArrays(), asc ? SortOrder.ASC : SortOrder.DESC, limit);

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
        try (IntBlock groupIds = add(vector)) {
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
            migrateToSmallTop();
            return true;
        }

        if (topValues.getCount() < limit) {
            hasNull = true;
            return true;
        }

        return false;
    }

    /**
     * Tries to add the value to the top values, and returns true if it was successful.
     */
    private boolean acceptValue(long value) {
        if (topValues.collect(value) == false) {
            return false;
        }

        // Full top and null, there's an extra value/null we must remove
        if (topValues.getCount() == limit && hasNull && nullsFirst == false) {
            hasNull = false;
        }

        return true;
    }

    /**
     * Converts the current BucketedSort to one with {@code limit - 1} values, as one value is a null.
     */
    private void migrateToSmallTop() {
        assert nullsFirst : "The small top is only used when nulls are first";
        assert topValues.getLimit() == limit : "The top values can't be migrated twice";

        topValues.reduceLimitByOne();
    }

    /**
     * Returns true if the value is in, or can be added to the top; false otherwise.
     */
    private boolean isAcceptable(long value) {
        return isTopComplete() == false || (hasNull && nullsFirst == false) || isInTop(value);
    }

    /**
     * Returns true if the value is in the top; false otherwise.
     * <p>
     *     This method does not check if the value is currently part of the top; only if it's better or equal than the current worst value.
     * </p>
     */
    private boolean isInTop(long value) {
        return asc ? value <= topValues.getWorstValue() : value >= topValues.getWorstValue();
    }

    /**
     * Returns true if there are {@code limit} values in the blockhash; false otherwise.
     */
    private boolean isTopComplete() {
        return topValues.getCount() >= limit - (hasNull ? 1 : 0);
    }

    /**
     *  Adds the vector values to the hash, and returns a new vector with the group IDs for those positions.
     */
    IntBlock add(LongVector vector) {
        int positions = vector.getPositionCount();

        // Add all values to the top set, so we don't end up sending invalid values later
        for (int i = 0; i < positions; i++) {
            long v = vector.getLong(i);
            acceptValue(v);
        }

        // Create a block with the groups
        try (var builder = blockFactory.newIntBlockBuilder(positions)) {
            for (int i = 0; i < positions; i++) {
                long v = vector.getLong(i);
                if (isAcceptable(v)) {
                    builder.appendInt(Math.toIntExact(hashOrdToGroupNullReserved(hash.add(v))));
                } else {
                    builder.appendNull();
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
                if (found < 0 || isAcceptable(v) == false) {
                    builder.appendNull();
                } else {
                    builder.appendInt(Math.toIntExact(hashOrdToGroupNullReserved(found)));
                }
            }
            return builder.build();
        }
    }

    private IntBlock lookup(LongBlock block) {
        return new TopNMultivalueDedupeLong(block, hasNull, this::isAcceptable).hashLookup(blockFactory, hash);
    }

    @Override
    public LongBlock[] getKeys() {
        if (hasNull) {
            final long[] keys = new long[topValues.getCount() + 1];
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
        final long[] keys = new long[topValues.getCount()];
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
        final int[] ids = new int[topValues.getCount() + nullOffset];
        int idsIndex = nullOffset;
        // TODO: Can we instead iterate the top and take the ids from the hash? To avoid checking unused values
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
        BitArray seenGroups = new BitArray(111, bigArrays);
        if (hasNull) {
            seenGroups.set(0);
        }
        // TODO: Can we instead iterate the top and take the ids from the hash? To avoid checking unused values
        for (int i = 1; i < hash.size() + 1; i++) {
            long value = hash.get(i - 1);
            if (isInTop(value)) {
                seenGroups.set(i);
            }
        }
        return seenGroups;
    }

    @Override
    public void close() {
        Releasables.close(hash, topValues);
    }

    @Override
    public String toString() {
        StringBuilder b = new StringBuilder();
        b.append("LongTopNBlockHash{channel=").append(channel);
        b.append(", asc=").append(asc);
        b.append(", nullsFirst=").append(nullsFirst);
        b.append(", limit=").append(limit);
        b.append(", entries=").append(hash.size());
        b.append(", hasNull=").append(hasNull);
        return b.append('}').toString();
    }
}
