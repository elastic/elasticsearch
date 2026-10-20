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
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.compute.operator.Warnings;
import org.elasticsearch.core.ReleasableIterator;
import org.elasticsearch.core.Releasables;

import java.util.ArrayList;
import java.util.List;

/**
 * Maps each key to all of its build-side row positions for subquery-backed LOOKUP JOIN.
 */
final class RepeatedKeyBlockHashRowInTableLookup extends RowInTableLookup {
    private final BlockHash hash;
    private final IntArray rowOffsets;
    private final IntArray rowIds;
    private final BlockFactory blockFactory;
    private final Warnings warnings;

    RepeatedKeyBlockHashRowInTableLookup(BlockFactory blockFactory, Block[] keys, Warnings warnings) {
        this.blockFactory = blockFactory;
        this.warnings = warnings;

        int rowCount = keys[0].getPositionCount();
        for (Block key : keys) {
            for (int p = 0; p < rowCount; p++) {
                if (key.getValueCount(p) != 1) {
                    throw new IllegalArgumentException("repeated-key lookup requires non-null, single-valued build keys");
                }
            }
        }

        List<BlockHash.GroupSpec> groups = new ArrayList<>(keys.length);
        for (int k = 0; k < keys.length; k++) {
            groups.add(new BlockHash.GroupSpec(k, keys[k].elementType()));
        }

        BlockHash builtHash = BlockHash.buildPackedValuesBlockHash(groups, blockFactory, Operator.TARGET_PAGE_SIZE / Long.SIZE);
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
                    groupsByRow.set(row, groupId);
                }
            });

            int groupCount = builtHash.numKeys();
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

            this.hash = builtHash;
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

    @Override
    public ReleasableIterator<IntBlock> lookup(Page page, ByteSizeValue targetBlockSize) {
        return lookup(page, targetBlockSize, blockFactory, warnings);
    }

    @Override
    public ReleasableIterator<IntBlock> lookup(
        Page page,
        ByteSizeValue targetBlockSize,
        BlockFactory resultBlockFactory,
        Warnings warnings
    ) {
        Block[] sanitizedBlocks = new Block[page.getBlockCount()];
        Block.Builder[] builders = new Block.Builder[page.getBlockCount()];
        Page sanitizedPage = null;
        boolean success = false;
        boolean encounteredMultiValue = false;
        try {
            for (int k = 0; k < builders.length; k++) {
                builders[k] = page.getBlock(k).elementType().newBlockBuilder(page.getPositionCount(), resultBlockFactory);
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

            ReleasableIterator<IntBlock> groupIds = hash.concurrentLookup(sanitizedPage, targetBlockSize, resultBlockFactory);
            RepeatedLookupResults results = new RepeatedLookupResults(sanitizedPage, groupIds, resultBlockFactory);
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

    @Override
    public String toString() {
        return "RepeatedKeys[" + hash + "]";
    }

    @Override
    public void close() {
        Releasables.closeExpectNoException(hash, rowOffsets, rowIds);
    }

    private final class RepeatedLookupResults implements ReleasableIterator<IntBlock> {
        private final Page sanitizedPage;
        private final ReleasableIterator<IntBlock> groupIds;
        private final BlockFactory blockFactory;

        private RepeatedLookupResults(Page sanitizedPage, ReleasableIterator<IntBlock> groupIds, BlockFactory blockFactory) {
            this.sanitizedPage = sanitizedPage;
            this.groupIds = groupIds;
            this.blockFactory = blockFactory;
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
