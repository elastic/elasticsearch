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
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.ReleasableIterator;
import org.elasticsearch.core.Releasables;

import java.util.NoSuchElementException;

/**
 * A lookup table whose keys are unique, kept in the hash {@link PartitionedBlockHash#buildForLookup} picks.
 */
final class BlockHashRowInTableLookup extends RowInTableLookup {
    private final BlockFactory blockFactory;
    private final BlockHash hash;
    /**
     * The row of each group id, or {@code -1} if no row has it. {@code null} when every group id is its row, which
     * saves mapping the group ids hashes that number keys from {@code 0} in insertion order return.
     */
    @Nullable
    private final IntArray rowOfGroup;

    BlockHashRowInTableLookup(BlockFactory blockFactory, Block[] keys) {
        this.blockFactory = blockFactory;
        int rowCount = keys[0].getPositionCount();
        BlockHash builtHash = PartitionedBlockHash.buildForLookup(RepeatedKeyBlockHashRowInTableLookup.groupSpecs(keys), blockFactory);
        IntArray rows = null;
        boolean success = false;
        try {
            // Group ids stay below this: some hashes reserve 0 for null and booleans always number false 1 and true 2.
            IntArray groupRows = blockFactory.bigArrays().newIntArray(Math.max(rowCount + 1L, 3), false);
            rows = groupRows;
            groupRows.fill(0, groupRows.size(), -1);
            builtHash.add(new Page(keys), new BlockHashGroupIdsAddInput() {
                @Override
                protected void processPosition(int row, IntBlock groupIds, int firstValueIndex, int valueCount) {
                    if (valueCount != 1) {
                        throw new IllegalStateException("single-valued build key produced [" + valueCount + "] group ids");
                    }
                    processGroupId(row, groupIds.getInt(firstValueIndex));
                }

                @Override
                protected void processGroupId(int row, int groupId) {
                    if (groupId < 0 || groupId >= groupRows.size()) {
                        throw new IllegalStateException("build key produced an invalid group id [" + groupId + "]");
                    }
                    if (groupRows.get(groupId) >= 0) {
                        // TODO double check these errors over REST once we have LOOKUP
                        throw new IllegalArgumentException("found a duplicate row");
                    }
                    groupRows.set(groupId, row);
                }
            });
            if (groupsAreRows(groupRows, rowCount)) {
                groupRows.close();
                rows = null;
            }
            this.hash = builtHash;
            this.rowOfGroup = rows;
            success = true;
        } finally {
            if (success == false) {
                Releasables.closeExpectNoException(builtHash, rows);
            }
        }
    }

    /**
     * Whether every row has the group id of its position. Each row has a distinct group id, so then no other group
     * id has a row.
     */
    private static boolean groupsAreRows(IntArray groupRows, int rowCount) {
        for (int row = 0; row < rowCount; row++) {
            if (groupRows.get(row) != row) {
                return false;
            }
        }
        return true;
    }

    @Override
    public ReleasableIterator<IntBlock> lookup(Page page, ByteSizeValue targetBlockSize) {
        ReleasableIterator<IntBlock> groups = page.getPositionCount() == 0
            ? hash.lookup(page, targetBlockSize)
            : new ChunkedLookup(page, targetBlockSize);
        if (rowOfGroup == null) {
            return groups;
        }
        return new ReleasableIterator<>() {
            @Override
            public boolean hasNext() {
                return groups.hasNext();
            }

            @Override
            public IntBlock next() {
                try (IntBlock groupIds = groups.next()) {
                    return rowsOf(groupIds);
                }
            }

            @Override
            public void close() {
                groups.close();
            }
        };
    }

    /**
     * Looks up slices of the probe page sized so each result stays near the target size, because most hashes
     * return a single block for the whole page. Each probe position yields at most one row per combination of its
     * key values because the keys are unique.
     */
    private final class ChunkedLookup implements ReleasableIterator<IntBlock> {
        private final Page page;
        private final ByteSizeValue targetBlockSize;
        private final long targetBytes;
        private int nextPosition;
        private Page chunk;
        private ReleasableIterator<IntBlock> chunkGroups;

        ChunkedLookup(Page page, ByteSizeValue targetBlockSize) {
            this.page = page.shallowCopy();
            this.targetBlockSize = targetBlockSize;
            this.targetBytes = targetBlockSize.getBytes();
        }

        @Override
        public boolean hasNext() {
            while (chunkGroups == null || chunkGroups.hasNext() == false) {
                closeChunk();
                if (nextPosition >= page.getPositionCount()) {
                    return false;
                }
                int end = chunkEnd(nextPosition);
                chunk = nextPosition == 0 && end == page.getPositionCount() ? page.shallowCopy() : page.slice(nextPosition, end);
                nextPosition = end;
                chunkGroups = hash.lookup(chunk, targetBlockSize);
            }
            return true;
        }

        @Override
        public IntBlock next() {
            if (hasNext() == false) {
                throw new NoSuchElementException();
            }
            return chunkGroups.next();
        }

        private int chunkEnd(int start) {
            int positions = page.getPositionCount();
            boolean multivalued = false;
            for (int b = 0; b < page.getBlockCount(); b++) {
                multivalued |= page.getBlock(b).mayHaveMultivaluedFields();
            }
            if (multivalued == false) {
                return (int) Math.min(positions, start + Math.max(1, targetBytes / Integer.BYTES));
            }
            long bytes = 0;
            int end = start;
            while (end < positions) {
                long values = 1;
                for (int b = 0; b < page.getBlockCount(); b++) {
                    values *= Math.max(1, page.getBlock(b).getValueCount(end));
                }
                // Each row is an int and multivalued results also keep the index of each position's first value.
                bytes += (values + 1) * Integer.BYTES;
                if (bytes > targetBytes && end > start) {
                    break;
                }
                end++;
            }
            return end;
        }

        private void closeChunk() {
            Releasables.closeExpectNoException(chunkGroups, chunk == null ? null : chunk::releaseBlocks);
            chunkGroups = null;
            chunk = null;
        }

        @Override
        public void close() {
            closeChunk();
            page.releaseBlocks();
        }
    }

    private IntBlock rowsOf(IntBlock groupIds) {
        int positions = groupIds.getPositionCount();
        try (IntBlock.Builder builder = blockFactory.newIntBlockBuilder(positions)) {
            for (int p = 0; p < positions; p++) {
                int start = groupIds.getFirstValueIndex(p);
                int end = start + groupIds.getValueCount(p);
                int matches = 0;
                for (int i = start; i < end; i++) {
                    if (rowOf(groupIds.getInt(i)) >= 0) {
                        matches++;
                    }
                }
                switch (matches) {
                    case 0 -> builder.appendNull();
                    case 1 -> {
                        for (int i = start; i < end; i++) {
                            int row = rowOf(groupIds.getInt(i));
                            if (row >= 0) {
                                builder.appendInt(row);
                            }
                        }
                    }
                    default -> {
                        builder.beginPositionEntry();
                        for (int i = start; i < end; i++) {
                            int row = rowOf(groupIds.getInt(i));
                            if (row >= 0) {
                                builder.appendInt(row);
                            }
                        }
                        builder.endPositionEntry();
                    }
                }
            }
            return builder.build();
        }
    }

    /**
     * The row with {@code groupId}, or {@code -1} if there isn't one, like for the null group of a table without
     * null keys.
     */
    private int rowOf(int groupId) {
        return groupId < rowOfGroup.size() ? rowOfGroup.get(groupId) : -1;
    }

    @Override
    public String toString() {
        return hash.toString();
    }

    @Override
    public void close() {
        Releasables.closeExpectNoException(hash, rowOfGroup);
    }
}
