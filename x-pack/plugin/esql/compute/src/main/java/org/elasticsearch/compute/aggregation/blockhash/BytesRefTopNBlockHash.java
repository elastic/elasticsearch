/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation.blockhash;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.BitArray;
import org.elasticsearch.common.util.BytesRefHashTable;
import org.elasticsearch.compute.aggregation.GroupingAggregatorFunction;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.BytesRefVector;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.data.sort.BytesRefTopNSet;
import org.elasticsearch.compute.operator.mvdedupe.TopNMultivalueDedupeBytesRef;
import org.elasticsearch.core.ReleasableIterator;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.search.sort.SortOrder;

/**
 * Maps a {@link BytesRefBlock} column to group ids, keeping only the top N values.
 * <p>
 *     Values that are not in the top-N at insertion time produce {@code null} group ids and are not stored
 *     in the hash table; their aggregator state is therefore never allocated. {@link #nonEmpty()} filters
 *     out group ids whose key has been evicted while the hash was being filled.
 * </p>
 * <p>
 *     <b>Sort collation:</b> ordering uses {@link BytesRef#compareTo}, i.e. unsigned lexicographic byte
 *     comparison. For {@code KEYWORD} fields encoded as valid UTF-8 this is equivalent to Unicode
 *     codepoint order, matching how Lucene sorts these values. ICU/locale-aware collations are not honored
 *     here; if a query needs them, the caller must perform the sort outside of this hash.
 * </p>
 */
final class BytesRefTopNBlockHash extends BlockHash {
    private final int channel;
    private final boolean asc;
    private final boolean nullsFirst;
    private final int limit;
    final BytesRefHashTable hash;
    private final BytesRefTopNSet topValues;

    /**
     * Have we seen any {@code null} values?
     * <p>
     *     We reserve the 0 ordinal for the {@code null} key so methods like
     *     {@link #nonEmpty} need to skip 0 if we haven't seen any null values.
     * </p>
     */
    private boolean hasNull;

    BytesRefTopNBlockHash(int channel, boolean asc, boolean nullsFirst, int limit, BlockFactory blockFactory) {
        super(blockFactory);
        assert limit > 0 : "BytesRefTopNBlockHash requires a limit greater than 0";
        this.channel = channel;
        this.asc = asc;
        this.nullsFirst = nullsFirst;
        this.limit = limit;

        boolean success = false;
        try {
            this.hash = HashImplFactory.newBytesRefHash(blockFactory);
            this.topValues = new BytesRefTopNSet(blockFactory.bigArrays(), asc ? SortOrder.ASC : SortOrder.DESC, limit);
            success = true;
        } finally {
            if (success == false) {
                close();
            }
        }
    }

    @Override
    public void add(Page page, GroupingAggregatorFunction.AddInput addInput) {
        var block = page.getBlock(channel);

        if (block.areAllValuesNull() && acceptNull()) {
            hasNull = true;
            try (IntVector groupIds = blockFactory.newConstantIntVector(0, block.getPositionCount())) {
                addInput.add(0, groupIds);
            }
            return;
        }
        BytesRefBlock castBlock = (BytesRefBlock) block;
        BytesRefVector vector = castBlock.asVector();
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
            // Reduce the limit of the sort by one, as it's not filled with a null.
            assert topValues.getLimit() == limit : "The top values can't be reduced twice";
            topValues.reduceLimitByOne();
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
    private boolean acceptValue(BytesRef value) {
        if (topValues.collect(value) == false) {
            return false;
        }

        // Full top and null, there's an extra value/null we must remove.
        if (topValues.getCount() == limit && hasNull && nullsFirst == false) {
            hasNull = false;
        }

        return true;
    }

    /**
     * Returns true if the value is in, or can be added to the top; false otherwise.
     */
    private boolean isAcceptable(BytesRef value) {
        return isTopComplete() == false || (hasNull && nullsFirst == false) || isInTop(value);
    }

    /**
     * Returns true if the value is in the top; false otherwise.
     * <p>
     *     This method does not check if the value is currently part of the top; only if it's better or equal
     *     than the current worst value.
     * </p>
     */
    private boolean isInTop(BytesRef value) {
        // Safe to use the view here: callers of isInTop never mutate topValues in-between.
        // When the set is empty (e.g. limit was reduced to 0 by nullsFirst on a limit-1 hash) no real value
        // can be in the top; the null slot is handled separately by isAcceptable/nonEmpty.
        if (topValues.getCount() == 0) {
            return false;
        }
        BytesRef worst = topValues.getWorstValueView();
        return asc ? value.compareTo(worst) <= 0 : value.compareTo(worst) >= 0;
    }

    /**
     * Returns true if there are {@code limit} values in the blockhash; false otherwise.
     */
    private boolean isTopComplete() {
        return topValues.getCount() >= limit - (hasNull ? 1 : 0);
    }

    /**
     * Adds the vector values to the hash, and returns a new vector with the group IDs for those positions.
     * <p>
     *     TODO: when an {@code OrdinalBytesRefVector} reaches this method (e.g. once a Parquet reader emits
     *     dictionary-encoded blocks), short-circuit by running the two-pass logic over the dictionary entries
     *     and looking up rows via the ordinal index, mirroring {@code BytesRefBlockHash#addOrdinalsVector}.
     *     The current implementation correctly resolves ordinals to bytes per row, but loses the dictionary
     *     speed-up that the upstream block already paid for.
     * </p>
     */
    IntBlock add(BytesRefVector vector) {
        int positions = vector.getPositionCount();
        BytesRef scratch = new BytesRef();

        // Add all values to the top set, so we don't end up sending invalid values later.
        for (int i = 0; i < positions; i++) {
            BytesRef v = vector.getBytesRef(i, scratch);
            acceptValue(v);
        }

        try (var builder = blockFactory.newIntBlockBuilder(positions)) {
            for (int i = 0; i < positions; i++) {
                BytesRef v = vector.getBytesRef(i, scratch);
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
     * Adds the block values to the hash, and returns a new vector with the group IDs for those positions.
     * <p>
     *     For nulls, a 0 group ID is used. For multivalues, a multivalue is used with all the group IDs.
     * </p>
     */
    IntBlock add(BytesRefBlock block) {
        BytesRef scratch = new BytesRef();
        // Add all the values to the top set first, so we don't end up sending invalid values later.
        for (int p = 0; p < block.getPositionCount(); p++) {
            int count = block.getValueCount(p);
            if (count == 0) {
                acceptNull();
                continue;
            }
            int first = block.getFirstValueIndex(p);

            for (int i = 0; i < count; i++) {
                BytesRef value = block.getBytesRef(first + i, scratch);
                acceptValue(value);
            }
        }

        return new TopNMultivalueDedupeBytesRef(block, hasNull, this::isAcceptable).hashAdd(blockFactory, hash).ords();
    }

    @Override
    public ReleasableIterator<IntBlock> lookup(Page page, ByteSizeValue targetBlockSize) {
        var block = page.getBlock(channel);
        if (block.areAllValuesNull()) {
            return ReleasableIterator.single(blockFactory.newConstantIntVector(0, block.getPositionCount()).asBlock());
        }

        BytesRefBlock castBlock = (BytesRefBlock) block;
        BytesRefVector vector = castBlock.asVector();
        // TODO honor targetBlockSize and chunk the pages if requested.
        if (vector == null) {
            return ReleasableIterator.single(lookup(castBlock));
        }
        return ReleasableIterator.single(lookup(vector));
    }

    private IntBlock lookup(BytesRefVector vector) {
        int positions = vector.getPositionCount();
        BytesRef scratch = new BytesRef();
        try (var builder = blockFactory.newIntBlockBuilder(positions)) {
            for (int i = 0; i < positions; i++) {
                BytesRef v = vector.getBytesRef(i, scratch);
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

    private IntBlock lookup(BytesRefBlock block) {
        return new TopNMultivalueDedupeBytesRef(block, hasNull, this::isAcceptable).hashLookup(blockFactory, hash);
    }

    @Override
    public BytesRefBlock[] getKeys(IntVector selected) {
        int positions = selected.getPositionCount();
        BytesRef spare = new BytesRef();
        try (BytesRefBlock.Builder builder = blockFactory.newBytesRefBlockBuilder(positions)) {
            for (int i = 0; i < positions; i++) {
                int groupId = selected.getInt(i);
                if (groupId == 0) {
                    builder.appendNull();
                } else {
                    builder.appendBytesRef(hash.get(groupId - 1, spare));
                }
            }
            return new BytesRefBlock[] { builder.build() };
        }
    }

    @Override
    public IntVector nonEmpty() {
        BytesRef spare = new BytesRef();
        int nullOffset = hasNull ? 1 : 0;
        // Every entry in topValues was inserted into hash, and any hash entry not currently in topValues
        // has been evicted. So the number of "in top" hash entries equals topValues.getCount().
        final int[] ids = new int[topValues.getCount() + nullOffset];
        int idsIndex = nullOffset;
        long hashSize = hash.size();
        for (int i = 1; i <= hashSize; i++) {
            BytesRef value = hash.get(i - 1, spare);
            if (isInTop(value)) {
                ids[idsIndex++] = i;
            }
        }
        return blockFactory.newIntArrayVector(ids, ids.length);
    }

    @Override
    public int numKeys() {
        if (hasNull) {
            return Math.toIntExact(hash.size()) + 1;
        } else {
            return Math.toIntExact(hash.size());
        }
    }

    @Override
    public BitArray seenGroupIds(BigArrays bigArrays) {
        BitArray seenGroups = new BitArray(1, bigArrays);
        if (hasNull) {
            seenGroups.set(0);
        }
        BytesRef spare = new BytesRef();
        long hashSize = hash.size();
        for (int i = 1; i <= hashSize; i++) {
            BytesRef value = hash.get(i - 1, spare);
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
        b.append("BytesRefTopNBlockHash{channel=").append(channel);
        b.append(", asc=").append(asc);
        b.append(", nullsFirst=").append(nullsFirst);
        b.append(", limit=").append(limit);
        b.append(", entries=").append(hash.size());
        b.append(", hasNull=").append(hasNull);
        return b.append('}').toString();
    }
}
