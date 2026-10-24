/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation.table;

import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.IntArray;
import org.elasticsearch.compute.aggregation.blockhash.BlockHash;
import org.elasticsearch.compute.aggregation.blockhash.PartitionedBlockHash;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.Warnings;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.ReleasableIterator;
import org.elasticsearch.core.Releasables;

import java.util.ArrayList;
import java.util.List;

/**
 * Maps each key to all of its build-side row positions for subquery-backed LOOKUP JOIN.
 */
final class RepeatedKeyBlockHashRowInTableLookup extends RowInTableLookup {
    /**
     * Maps probed keys to group ids, or to null for keys the table doesn't have. Like {@link BlockHash#lookup} it
     * must not modify itself, so it can be probed from several threads at once.
     */
    interface GroupLookup extends Releasable {
        ReleasableIterator<IntBlock> lookup(Page page, ByteSizeValue targetBlockSize);
    }

    private final GroupLookup groups;
    /**
     * One more than the largest group id the build keys produced. Group ids depend on the
     * {@link BlockHash} implementation and may not start at {@code 0}, so lookups treat ids
     * at or beyond this as keys without rows.
     */
    private final int groupCount;
    /**
     * The rows of group {@code g} are {@code rowIds[rowOffsets[g]]} up to {@code rowIds[rowOffsets[g + 1]]}, ascending.
     */
    private final IntArray rowOffsets;
    private final IntArray rowIds;
    private final BlockFactory blockFactory;

    /**
     * Takes ownership of {@code groups}, {@code rowOffsets} and {@code rowIds}.
     */
    RepeatedKeyBlockHashRowInTableLookup(
        BlockFactory blockFactory,
        GroupLookup groups,
        int groupCount,
        IntArray rowOffsets,
        IntArray rowIds
    ) {
        this.blockFactory = blockFactory;
        this.groups = groups;
        this.groupCount = groupCount;
        this.rowOffsets = rowOffsets;
        this.rowIds = rowIds;
    }

    RepeatedKeyBlockHashRowInTableLookup(BlockFactory blockFactory, Block[] keys) {
        this.blockFactory = blockFactory;

        int rowCount = keys[0].getPositionCount();
        for (Block key : keys) {
            for (int p = 0; p < rowCount; p++) {
                if (key.getValueCount(p) != 1) {
                    throw new IllegalArgumentException("repeated-key lookup requires non-null, single-valued build keys");
                }
            }
        }

        BlockHash builtHash = PartitionedBlockHash.buildForLookup(groupSpecs(keys), blockFactory);
        IntArray groupsByRow;
        try {
            groupsByRow = blockFactory.bigArrays().newIntArray(rowCount, false);
        } catch (RuntimeException | Error e) {
            Releasables.closeExpectNoException(builtHash);
            throw e;
        }
        IntArray counts = null;
        IntArray offsets = null;
        IntArray rows = null;
        boolean success = false;
        try {
            int[] maxGroupId = new int[] { -1 };
            builtHash.add(new Page(keys), new BlockHashGroupIdsAddInput() {
                @Override
                protected void processPosition(int row, IntBlock groupIds, int firstValueIndex, int valueCount) {
                    if (valueCount != 1) {
                        throw new IllegalArgumentException("repeated-key lookup requires non-null, single-valued build keys");
                    }
                    processGroupId(row, groupIds.getInt(firstValueIndex));
                }

                @Override
                protected void processGroupId(int row, int groupId) {
                    if (groupId < 0) {
                        throw new IllegalStateException("build key produced an invalid group id [" + groupId + "]");
                    }
                    groupsByRow.set(row, groupId);
                    maxGroupId[0] = Math.max(maxGroupId[0], groupId);
                }
            });

            int groupCount = maxGroupId[0] + 1;
            counts = blockFactory.bigArrays().newIntArray(groupCount, true);
            for (int row = 0; row < rowCount; row++) {
                int group = groupsByRow.get(row);
                if (group < 0 || group >= groupCount) {
                    throw new IllegalStateException("build key produced an invalid group id [" + group + "]");
                }
                counts.increment(group, 1);
            }

            offsets = blockFactory.bigArrays().newIntArray((long) groupCount + 1, true);
            for (int group = 0; group < groupCount; group++) {
                offsets.set(group + 1, Math.addExact(offsets.get(group), counts.get(group)));
                counts.set(group, offsets.get(group));
            }

            rows = blockFactory.bigArrays().newIntArray(rowCount, false);
            for (int row = 0; row < rowCount; row++) {
                int group = groupsByRow.get(row);
                int rowOffset = counts.get(group);
                rows.set(rowOffset, row);
                counts.set(group, rowOffset + 1);
            }

            this.groups = new HashGroupLookup(builtHash);
            this.groupCount = groupCount;
            this.rowOffsets = offsets;
            this.rowIds = rows;
            builtHash = null;
            offsets = null;
            rows = null;
            success = true;
        } finally {
            Releasables.closeExpectNoException(groupsByRow, counts, offsets, rows);
            if (success == false) {
                Releasables.closeExpectNoException(builtHash);
            }
        }
    }

    static List<BlockHash.GroupSpec> groupSpecs(Block[] keys) {
        List<BlockHash.GroupSpec> groups = new ArrayList<>(keys.length);
        for (int k = 0; k < keys.length; k++) {
            groups.add(new BlockHash.GroupSpec(k, keys[k].elementType()));
        }
        return groups;
    }

    private record HashGroupLookup(BlockHash hash) implements GroupLookup {
        @Override
        public ReleasableIterator<IntBlock> lookup(Page page, ByteSizeValue targetBlockSize) {
            return hash.lookup(page, targetBlockSize);
        }

        @Override
        public void close() {
            hash.close();
        }

        @Override
        public String toString() {
            return hash.toString();
        }
    }

    @Override
    public ReleasableIterator<IntBlock> lookup(Page page, ByteSizeValue targetBlockSize) {
        return lookup(page, targetBlockSize, Warnings.NOOP_WARNINGS);
    }

    @Override
    public ReleasableIterator<IntBlock> lookup(Page page, ByteSizeValue targetBlockSize, Warnings warnings) {
        if (mayHaveMultivaluedKeys(page) == false) {
            /*
             * Every key position is null or single valued. A null key maps to null or to a group without
             * rows because build keys are never null, so the page can be probed as is. That also keeps
             * ordinal BytesRef blocks, letting the hash look up each dictionary entry once.
             */
            Page probe = page.shallowCopy();
            boolean success = false;
            try {
                ReleasableIterator<IntBlock> groupIds = groups.lookup(probe, targetBlockSize);
                RepeatedLookupResults results = new RepeatedLookupResults(probe, groupIds);
                success = true;
                return results;
            } finally {
                if (success == false) {
                    probe.releaseBlocks();
                }
            }
        }
        Block[] sanitizedBlocks = new Block[page.getBlockCount()];
        Block.Builder[] builders = new Block.Builder[page.getBlockCount()];
        Page sanitizedPage = null;
        boolean success = false;
        boolean encounteredMultiValue = false;
        try {
            for (int k = 0; k < builders.length; k++) {
                builders[k] = page.getBlock(k).elementType().newBlockBuilder(page.getPositionCount(), blockFactory);
            }
            for (int p = 0; p < page.getPositionCount(); p++) {
                boolean matchable = true;
                for (int k = 0; k < page.getBlockCount(); k++) {
                    int valueCount = page.getBlock(k).getValueCount(p);
                    encounteredMultiValue |= valueCount > 1;
                    matchable &= valueCount == 1;
                }
                for (int k = 0; k < page.getBlockCount(); k++) {
                    if (matchable) {
                        builders[k].copyFrom(page.getBlock(k), p, p + 1);
                    } else {
                        builders[k].appendNull();
                    }
                }
            }
            sanitizedBlocks = Block.Builder.buildAll(builders);
            sanitizedPage = new Page(sanitizedBlocks);
            sanitizedBlocks = null;
            if (encounteredMultiValue) {
                warnings.registerException(IllegalArgumentException.class, "LOOKUP JOIN encountered multi-value");
            }

            ReleasableIterator<IntBlock> groupIds = groups.lookup(sanitizedPage, targetBlockSize);
            RepeatedLookupResults results = new RepeatedLookupResults(sanitizedPage, groupIds);
            sanitizedPage = null;
            success = true;
            return results;
        } finally {
            for (Block.Builder builder : builders) {
                if (builder != null) {
                    builder.close();
                }
            }
            if (sanitizedBlocks != null) {
                Releasables.closeExpectNoException(sanitizedBlocks);
            }
            if (success == false && sanitizedPage != null) {
                Releasables.closeExpectNoException(sanitizedPage);
            }
        }
    }

    private static boolean mayHaveMultivaluedKeys(Page page) {
        for (int k = 0; k < page.getBlockCount(); k++) {
            if (page.getBlock(k).mayHaveMultivaluedFields()) {
                return true;
            }
        }
        return false;
    }

    @Override
    public String toString() {
        return "RepeatedKeys[" + groups + "]";
    }

    @Override
    public void close() {
        Releasables.closeExpectNoException(groups, rowOffsets, rowIds);
    }

    private final class RepeatedLookupResults implements ReleasableIterator<IntBlock> {
        private final Page sanitizedPage;
        private final ReleasableIterator<IntBlock> groupIds;

        private RepeatedLookupResults(Page sanitizedPage, ReleasableIterator<IntBlock> groupIds) {
            this.sanitizedPage = sanitizedPage;
            this.groupIds = groupIds;
        }

        @Override
        public boolean hasNext() {
            return groupIds.hasNext();
        }

        @Override
        public IntBlock next() {
            try (
                IntBlock ordinals = groupIds.next();
                IntBlock.Builder result = blockFactory.newIntBlockBuilder(ordinals.getPositionCount())
            ) {
                for (int p = 0; p < ordinals.getPositionCount(); p++) {
                    if (ordinals.getValueCount(p) == 0) {
                        result.appendNull();
                        continue;
                    }
                    if (ordinals.getValueCount(p) != 1) {
                        throw new IllegalStateException("single-valued lookup key produced multiple group ids");
                    }
                    int group = ordinals.getInt(ordinals.getFirstValueIndex(p));
                    if (group >= groupCount) {
                        result.appendNull();
                        continue;
                    }
                    int start = rowOffsets.get(group);
                    int end = rowOffsets.get(group + 1);
                    if (start == end) {
                        result.appendNull();
                    } else if (end - start == 1) {
                        result.appendInt(rowIds.get(start));
                    } else {
                        result.beginPositionEntry();
                        for (int row = start; row < end; row++) {
                            result.appendInt(rowIds.get(row));
                        }
                        result.endPositionEntry();
                    }
                }
                return result.build();
            }
        }

        @Override
        public void close() {
            Releasables.closeExpectNoException(groupIds, sanitizedPage);
        }
    }
}
