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
import org.elasticsearch.compute.data.OrdinalBytesRefBlock;
import org.elasticsearch.compute.data.OrdinalBytesRefVector;
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
     *     For {@link OrdinalBytesRefVector} inputs we short-circuit and run the two-pass logic over the
     *     {@code k} dictionary entries instead of the {@code N} row positions, mirroring
     *     {@code BytesRefBlockHash#addOrdinalsVector}. Non-competitive dictionary entries are rejected via a
     *     simple {@code int[]} lookup at row time, so the dictionary speed-up that the upstream block already
     *     paid for composes with the TopN pruning instead of being undone by per-row string resolution.
     * </p>
     */
    IntBlock add(BytesRefVector vector) {
        OrdinalBytesRefVector ordinals = vector.asOrdinals();
        if (ordinals != null) {
            return addOrdinalsVector(ordinals);
        }
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
     * Ordinal fast path for {@link #add(BytesRefVector)}. Walks the dictionary instead of every row, so the
     * TopN comparator and the hash table only see each distinct value once per page. Unreferenced dictionary
     * entries (which can appear after {@link OrdinalBytesRefVector#filter}) are skipped to avoid corrupting
     * the TopN with values that don't actually appear in this page.
     */
    private IntBlock addOrdinalsVector(OrdinalBytesRefVector ordinals) {
        IntVector ordinalIndices = ordinals.getOrdinalsVector();
        BytesRefVector dict = ordinals.getDictionaryVector();
        int positions = ordinalIndices.getPositionCount();
        int dictSize = dict.getPositionCount();

        // Mark dictionary entries actually referenced by this page; only those participate in the TopN.
        boolean[] referenced = new boolean[dictSize];
        for (int p = 0; p < positions; p++) {
            referenced[ordinalIndices.getInt(p)] = true;
        }

        // Pass 1: feed each referenced dictionary entry to the TopN set. Pass 2 (below) hashes only
        // the survivors, so transient winners that get evicted later in this loop never reach the hash.
        BytesRef scratch = new BytesRef();
        for (int d = 0; d < dictSize; d++) {
            if (referenced[d]) {
                acceptValue(dict.getBytesRef(d, scratch));
            }
        }

        // Per-dictionary-entry mapping to the final group id; -1 means non-competitive (or not referenced).
        // Built once and reused for every row, so the row-time work shrinks to two int loads + a branch.
        int[] groupIds = new int[dictSize];
        for (int d = 0; d < dictSize; d++) {
            if (referenced[d]) {
                BytesRef value = dict.getBytesRef(d, scratch);
                groupIds[d] = isAcceptable(value) ? Math.toIntExact(hashOrdToGroupNullReserved(hash.add(value))) : -1;
            } else {
                groupIds[d] = -1;
            }
        }

        try (var builder = blockFactory.newIntBlockBuilder(positions)) {
            for (int p = 0; p < positions; p++) {
                int g = groupIds[ordinalIndices.getInt(p)];
                if (g < 0) {
                    builder.appendNull();
                } else {
                    builder.appendInt(g);
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
     * <p>
     *     {@link OrdinalBytesRefBlock} inputs without multivalues take the same dictionary fast path as
     *     {@link #addOrdinalsVector(OrdinalBytesRefVector)}; multivalue ordinal blocks fall through to the
     *     general path because the cross-row dedupe in {@link TopNMultivalueDedupeBytesRef} would have to be
     *     re-implemented over ordinals, which is not worth the complexity for the parquet workloads we care
     *     about today (single-valued KEYWORD columns).
     * </p>
     */
    IntBlock add(BytesRefBlock block) {
        OrdinalBytesRefBlock ordinals = block.asOrdinals();
        if (ordinals != null && ordinals.mayHaveMultivaluedFields() == false) {
            return addOrdinalsBlock(ordinals);
        }
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

    /**
     * Ordinal fast path for {@link #add(BytesRefBlock)} on single-valued blocks (with optional nulls). Mirrors
     * {@link #addOrdinalsVector(OrdinalBytesRefVector)} but processes the per-position null markers from the
     * ordinals {@link IntBlock}. Multivalue positions are not supported here — see the precondition in
     * {@link #add(BytesRefBlock)}.
     */
    private IntBlock addOrdinalsBlock(OrdinalBytesRefBlock ordinals) {
        IntBlock ordinalIndices = ordinals.getOrdinalsBlock();
        BytesRefVector dict = ordinals.getDictionaryVector();
        int positions = ordinalIndices.getPositionCount();
        int dictSize = dict.getPositionCount();

        // Single pass in row order: feed null positions into the TopN's null accounting (matching the
        // interleaving the non-ordinal {@link #add(BytesRefBlock)} does) and mark which dictionary entries
        // this page actually references so the dictionary sweep below ignores phantom entries.
        boolean[] referenced = new boolean[dictSize];
        for (int p = 0; p < positions; p++) {
            if (ordinalIndices.isNull(p)) {
                acceptNull();
            } else {
                // Single-valued precondition (mayHaveMultivaluedFields() == false) means each non-null
                // position has exactly one value; we read it via getFirstValueIndex to be safe across
                // block layouts where the value array is not 1:1 with positions (e.g. compacted nullable
                // IntArrayBlock).
                referenced[ordinalIndices.getInt(ordinalIndices.getFirstValueIndex(p))] = true;
            }
        }

        // Pass over the dictionary: feed referenced entries to the TopN. As with the vector path, hashing is
        // deferred so transient winners that get evicted later never enter the hash table.
        BytesRef scratch = new BytesRef();
        for (int d = 0; d < dictSize; d++) {
            if (referenced[d]) {
                acceptValue(dict.getBytesRef(d, scratch));
            }
        }

        // Final mapping from dictionary ord to group id (or -1 for non-competitive / unreferenced).
        int[] groupIds = new int[dictSize];
        for (int d = 0; d < dictSize; d++) {
            if (referenced[d]) {
                BytesRef value = dict.getBytesRef(d, scratch);
                groupIds[d] = isAcceptable(value) ? Math.toIntExact(hashOrdToGroupNullReserved(hash.add(value))) : -1;
            } else {
                groupIds[d] = -1;
            }
        }

        try (var builder = blockFactory.newIntBlockBuilder(positions)) {
            for (int p = 0; p < positions; p++) {
                if (ordinalIndices.isNull(p)) {
                    // Mirrors the non-ordinal block path: nulls land on group id 0 only when the TopN has
                    // accepted at least one null; otherwise the position is excluded entirely.
                    if (hasNull) {
                        builder.appendInt(0);
                    } else {
                        builder.appendNull();
                    }
                } else {
                    int g = groupIds[ordinalIndices.getInt(ordinalIndices.getFirstValueIndex(p))];
                    if (g < 0) {
                        builder.appendNull();
                    } else {
                        builder.appendInt(g);
                    }
                }
            }
            return builder.build();
        }
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
        OrdinalBytesRefVector ordinals = vector.asOrdinals();
        if (ordinals != null) {
            return lookupOrdinalsVector(ordinals);
        }
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

    /**
     * Ordinal fast path for {@link #lookup(BytesRefVector)}. Resolves the dictionary entries to group ids
     * once per page and then services every row with two int loads. We do not build a {@code referenced[]}
     * mask here: phantom dictionary entries are never actually used as a row index, so the at-most-extra
     * {@code hash.find} per unreferenced entry is wasted work but cannot produce a wrong result for any
     * row in this page.
     */
    private IntBlock lookupOrdinalsVector(OrdinalBytesRefVector ordinals) {
        IntVector ordinalIndices = ordinals.getOrdinalsVector();
        BytesRefVector dict = ordinals.getDictionaryVector();
        int positions = ordinalIndices.getPositionCount();
        int dictSize = dict.getPositionCount();

        BytesRef scratch = new BytesRef();
        int[] groupIds = new int[dictSize];
        for (int d = 0; d < dictSize; d++) {
            BytesRef v = dict.getBytesRef(d, scratch);
            long found = hash.find(v);
            if (found < 0 || isAcceptable(v) == false) {
                groupIds[d] = -1;
            } else {
                groupIds[d] = Math.toIntExact(hashOrdToGroupNullReserved(found));
            }
        }

        try (var builder = blockFactory.newIntBlockBuilder(positions)) {
            for (int p = 0; p < positions; p++) {
                int g = groupIds[ordinalIndices.getInt(p)];
                if (g < 0) {
                    builder.appendNull();
                } else {
                    builder.appendInt(g);
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
