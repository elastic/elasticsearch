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
import org.elasticsearch.compute.aggregation.GroupingAggregatorFunction;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.IntArrayBlock;
import org.elasticsearch.compute.data.IntBigArrayBlock;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.data.sort.BytesRefTopNSet;
import org.elasticsearch.compute.data.sort.LongTopNSet;
import org.elasticsearch.core.ReleasableIterator;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.search.sort.SortOrder;

import java.util.List;

/**
 * A {@link BlockHash} for multi-key group-by that applies Top-N pruning based on the primary sort key.
 *
 * <p>Internally delegates actual group tracking to a {@link PackedValuesBlockHash}. The composite Top-N
 * pruning operates by tracking the top-K primary key values seen across all pages. During each {@link #add},
 * positions whose primary key is definitively outside the top-K are nulled out before the group ids reach
 * downstream aggregators.
 *
 * <p>This is a conservative optimization: a primary key value that ties at the boundary (equal to the K-th
 * best value) is always kept, even if the secondary key would place the composite group outside the top-K.
 * Correctness is guaranteed because the {@code TopNExec} consuming this aggregation's output always performs
 * the exact final sort.
 */
final class CompositeTopNBlockHash extends BlockHash {

    private final BlockHash inner;
    private final int primaryChannel;
    private final ElementType primaryType;
    private final boolean primaryAsc;
    private final int primaryGroupingIndex;
    private final int limit;

    /** Non-null when the primary sort key is {@link ElementType#LONG}. */
    private final LongTopNSet longTopValues;
    /** Non-null when the primary sort key is {@link ElementType#BYTES_REF}. */
    private final BytesRefTopNSet bytesRefTopValues;

    CompositeTopNBlockHash(List<BlockHash.GroupSpec> groups, BlockHash.TopNDef topNDef, BlockFactory blockFactory, int emitBatchSize) {
        super(blockFactory);
        this.limit = topNDef.limit();
        BlockHash.SortKey primary = topNDef.primaryKey();
        this.primaryGroupingIndex = primary.groupingIndex();
        this.primaryAsc = primary.asc();
        // primary.nullsFirst() is intentionally not stored: null primary key positions are always kept
        // conservatively (see buildCompetitiveMask), so the enclosing TopNExec handles exact null ordering.
        this.primaryChannel = groups.get(primaryGroupingIndex).channel();
        this.primaryType = groups.get(primaryGroupingIndex).elementType();
        if (primaryType != ElementType.LONG && primaryType != ElementType.BYTES_REF) {
            throw new IllegalArgumentException(
                "CompositeTopNBlockHash only supports LONG and BYTES_REF primary sort keys, got: " + primaryType
            );
        }

        boolean success = false;
        try {
            // Strip the topNDef annotations before passing to inner: PackedValuesBlockHash doesn't use them.
            List<BlockHash.GroupSpec> strippedGroups = groups.stream()
                .map(g -> g.topNDef() == null ? g : new BlockHash.GroupSpec(g.channel(), g.elementType(), g.categorizeDef(), null))
                .toList();
            this.inner = new PackedValuesBlockHash(strippedGroups, blockFactory, emitBatchSize);
            if (primaryType == ElementType.LONG) {
                this.longTopValues = new LongTopNSet(blockFactory.bigArrays(), primaryAsc ? SortOrder.ASC : SortOrder.DESC, limit);
                this.bytesRefTopValues = null;
            } else {
                this.longTopValues = null;
                this.bytesRefTopValues = new BytesRefTopNSet(blockFactory.bigArrays(), primaryAsc ? SortOrder.ASC : SortOrder.DESC, limit);
            }
            success = true;
        } finally {
            if (success == false) {
                close();
            }
        }
    }

    @Override
    public void add(Page page, GroupingAggregatorFunction.AddInput addInput) {
        Block primaryKeyBlock = page.getBlock(primaryChannel);
        // Phase 1: feed all primary key values into the topN set so the boundary is as tight as possible.
        updatePrimaryTopValues(primaryKeyBlock);
        // Phase 2: build the per-position competitive mask using the updated topN set.
        boolean[] competitive = buildCompetitiveMask(primaryKeyBlock);
        // Phase 3: let the inner hash do group assignment, but filter out non-competitive positions.
        inner.add(page, new FilteringAddInput(addInput, competitive, blockFactory));
    }

    private void updatePrimaryTopValues(Block block) {
        if (primaryType == ElementType.LONG) {
            LongBlock longBlock = (LongBlock) block;
            for (int p = 0; p < longBlock.getPositionCount(); p++) {
                if (longBlock.isNull(p) == false) {
                    int first = longBlock.getFirstValueIndex(p);
                    int count = longBlock.getValueCount(p);
                    for (int i = 0; i < count; i++) {
                        longTopValues.collect(longBlock.getLong(first + i));
                    }
                }
            }
        } else {
            BytesRefBlock bytesBlock = (BytesRefBlock) block;
            BytesRef scratch = new BytesRef();
            for (int p = 0; p < bytesBlock.getPositionCount(); p++) {
                if (bytesBlock.isNull(p) == false) {
                    int first = bytesBlock.getFirstValueIndex(p);
                    int count = bytesBlock.getValueCount(p);
                    for (int i = 0; i < count; i++) {
                        bytesRefTopValues.collect(bytesBlock.getBytesRef(first + i, scratch));
                    }
                }
            }
        }
    }

    private boolean[] buildCompetitiveMask(Block block) {
        boolean[] competitive = new boolean[block.getPositionCount()];
        if (primaryType == ElementType.LONG) {
            LongBlock longBlock = (LongBlock) block;
            boolean topComplete = longTopValues.getCount() >= limit;
            for (int p = 0; p < longBlock.getPositionCount(); p++) {
                if (longBlock.isNull(p)) {
                    competitive[p] = true; // keep nulls conservatively
                } else if (topComplete == false) {
                    competitive[p] = true;
                } else {
                    // Multi-valued: competitive if ANY value is within the top-K range.
                    int first = longBlock.getFirstValueIndex(p);
                    int count = longBlock.getValueCount(p);
                    for (int i = 0; i < count; i++) {
                        if (isLongInTop(longBlock.getLong(first + i))) {
                            competitive[p] = true;
                            break;
                        }
                    }
                }
            }
        } else {
            BytesRefBlock bytesBlock = (BytesRefBlock) block;
            boolean topComplete = bytesRefTopValues.getCount() >= limit;
            BytesRef scratch = new BytesRef();
            // Cache the view once — no mutations occur during mask building (updatePrimaryTopValues finished).
            BytesRef worstView = topComplete ? bytesRefTopValues.getWorstValueView() : null;
            for (int p = 0; p < bytesBlock.getPositionCount(); p++) {
                if (bytesBlock.isNull(p)) {
                    competitive[p] = true;
                } else if (topComplete == false) {
                    competitive[p] = true;
                } else {
                    int first = bytesBlock.getFirstValueIndex(p);
                    int count = bytesBlock.getValueCount(p);
                    for (int i = 0; i < count; i++) {
                        if (isBytesRefInTop(bytesBlock.getBytesRef(first + i, scratch), worstView)) {
                            competitive[p] = true;
                            break;
                        }
                    }
                }
            }
        }
        return competitive;
    }

    private boolean isLongInTop(long value) {
        return primaryAsc ? value <= longTopValues.getWorstValue() : value >= longTopValues.getWorstValue();
    }

    private boolean isBytesRefInTop(BytesRef value, BytesRef worstView) {
        int cmp = value.compareTo(worstView);
        return primaryAsc ? cmp <= 0 : cmp >= 0;
    }

    @Override
    public ReleasableIterator<IntBlock> lookup(Page page, ByteSizeValue targetBlockSize) {
        return inner.lookup(page, targetBlockSize);
    }

    @Override
    public Block[] getKeys(IntVector selected) {
        return inner.getKeys(selected);
    }

    @Override
    public IntVector nonEmpty() {
        try (IntVector allGroups = inner.nonEmpty()) {
            if (allGroups.getPositionCount() == 0) {
                return blockFactory.newIntArrayVector(new int[0], 0);
            }
            Block[] keys = inner.getKeys(allGroups);
            try {
                return filterGroupsByPrimaryKey(allGroups, keys[primaryGroupingIndex]);
            } finally {
                Releasables.close(keys);
            }
        }
    }

    /**
     * Filters a vector of group IDs to retain only those whose primary sort key is within the top-K range.
     * Null primary key positions are always kept (conservative).
     */
    private IntVector filterGroupsByPrimaryKey(IntVector groups, Block primaryKeys) {
        int[] result = new int[groups.getPositionCount()];
        int count = 0;
        if (primaryType == ElementType.LONG) {
            LongBlock longKeys = (LongBlock) primaryKeys;
            boolean topComplete = longTopValues.getCount() >= limit;
            for (int i = 0; i < groups.getPositionCount(); i++) {
                if (longKeys.isNull(i) || topComplete == false || isLongInTop(longKeys.getLong(longKeys.getFirstValueIndex(i)))) {
                    result[count++] = groups.getInt(i);
                }
            }
        } else {
            BytesRefBlock bytesKeys = (BytesRefBlock) primaryKeys;
            boolean topComplete = bytesRefTopValues.getCount() >= limit;
            BytesRef scratch = new BytesRef();
            // Cache the view once — no mutations occur during group filtering.
            BytesRef worstView = topComplete ? bytesRefTopValues.getWorstValueView() : null;
            for (int i = 0; i < groups.getPositionCount(); i++) {
                if (bytesKeys.isNull(i)
                    || topComplete == false
                    || isBytesRefInTop(bytesKeys.getBytesRef(bytesKeys.getFirstValueIndex(i), scratch), worstView)) {
                    result[count++] = groups.getInt(i);
                }
            }
        }
        return blockFactory.newIntArrayVector(result, count);
    }

    @Override
    public int numKeys() {
        // Delegate to inner for an O(1) upper-bound count rather than materialising all key blocks
        // via nonEmpty() on every addPage() call (which is where shouldEmitPartialResultsPeriodically
        // reads this). The inner count may be higher than the competitive group count, but emitting
        // partial results earlier is always safe — the TopNExec above produces the correct final answer.
        return inner.numKeys();
    }

    @Override
    public BitArray seenGroupIds(BigArrays bigArrays) {
        try (IntVector nonEmpty = nonEmpty()) {
            BitArray seen = new BitArray(1, bigArrays);
            for (int i = 0; i < nonEmpty.getPositionCount(); i++) {
                seen.set(nonEmpty.getInt(i));
            }
            return seen;
        }
    }

    @Override
    public void close() {
        Releasables.close(inner, longTopValues, bytesRefTopValues);
    }

    @Override
    public String toString() {
        return "CompositeTopNBlockHash{primaryChannel=" + primaryChannel + ", primaryType=" + primaryType + ", limit=" + limit + "}";
    }

    /**
     * Wraps an outer {@link GroupingAggregatorFunction.AddInput} and nulls out positions where the
     * primary sort key is non-competitive. This ensures that non-competitive rows never accumulate
     * aggregator state and that their group ids are not surfaced to downstream operators.
     */
    private static class FilteringAddInput implements GroupingAggregatorFunction.AddInput {

        private final GroupingAggregatorFunction.AddInput delegate;
        private final boolean[] competitive;
        private final BlockFactory blockFactory;

        FilteringAddInput(GroupingAggregatorFunction.AddInput delegate, boolean[] competitive, BlockFactory blockFactory) {
            this.delegate = delegate;
            this.competitive = competitive;
            this.blockFactory = blockFactory;
        }

        @Override
        public void add(int positionOffset, IntVector groupIds) {
            int positions = groupIds.getPositionCount();
            boolean allCompetitive = true;
            for (int i = 0; i < positions; i++) {
                if (competitive[positionOffset + i] == false) {
                    allCompetitive = false;
                    break;
                }
            }
            if (allCompetitive) {
                delegate.add(positionOffset, groupIds);
                return;
            }
            // Build a filtered block directly without going through asBlock() to avoid the
            // default-method dispatch loop: add(IntVector) → asBlock() → add(IntBlock) → add(IntVector).
            try (IntBlock.Builder builder = blockFactory.newIntBlockBuilder(positions)) {
                for (int j = 0; j < positions; j++) {
                    if (competitive[positionOffset + j]) {
                        builder.appendInt(groupIds.getInt(j));
                    } else {
                        builder.appendNull();
                    }
                }
                try (IntBlock filtered = builder.build()) {
                    delegate.add(positionOffset, filtered);
                }
            }
        }

        @Override
        public void add(int positionOffset, IntArrayBlock groupIds) {
            addFilteredBlock(positionOffset, groupIds);
        }

        @Override
        public void add(int positionOffset, IntBigArrayBlock groupIds) {
            addFilteredBlock(positionOffset, groupIds);
        }

        private void addFilteredBlock(int positionOffset, IntBlock groupIds) {
            int positions = groupIds.getPositionCount();
            // Fast path: all competitive → forward as-is.
            boolean allCompetitive = true;
            for (int i = 0; i < positions; i++) {
                if (competitive[positionOffset + i] == false) {
                    allCompetitive = false;
                    break;
                }
            }
            if (allCompetitive) {
                delegate.add(positionOffset, groupIds);
                return;
            }
            // Build a filtered copy: null out non-competitive positions.
            try (IntBlock.Builder builder = blockFactory.newIntBlockBuilder(positions)) {
                for (int i = 0; i < positions; i++) {
                    if (competitive[positionOffset + i] == false || groupIds.isNull(i)) {
                        builder.appendNull();
                    } else {
                        int start = groupIds.getFirstValueIndex(i);
                        int count = groupIds.getValueCount(i);
                        if (count == 1) {
                            builder.appendInt(groupIds.getInt(start));
                        } else {
                            builder.beginPositionEntry();
                            for (int j = 0; j < count; j++) {
                                builder.appendInt(groupIds.getInt(start + j));
                            }
                            builder.endPositionEntry();
                        }
                    }
                }
                try (IntBlock filtered = builder.build()) {
                    delegate.add(positionOffset, filtered);
                }
            }
        }

        @Override
        public void close() {
            // Do not close the delegate — this wrapper is a pass-through; the caller owns the delegate's lifecycle.
        }
    }
}
