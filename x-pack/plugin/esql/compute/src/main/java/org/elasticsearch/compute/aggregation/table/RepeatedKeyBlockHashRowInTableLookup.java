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
import org.elasticsearch.compute.aggregation.blockhash.PackedValuesBlockHash;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.Warnings;
import org.elasticsearch.core.ReleasableIterator;
import org.elasticsearch.core.Releasables;

import java.util.ArrayList;
import java.util.List;

/**
 * Keeps all build-side row offsets for each hash group so repeated keys can produce one output row per match.
 */
final class RepeatedKeyBlockHashRowInTableLookup extends RowInTableLookup {
    private final PackedValuesBlockHash hash;
    /** The rows for group {@code g} are in {@code rowIds} from {@code rowOffsets[g]} to {@code rowOffsets[g + 1]} (exclusive). */
    private final IntArray rowOffsets;
    private final IntArray rowIds;

    RepeatedKeyBlockHashRowInTableLookup(BlockFactory blockFactory, Block[] keys) {
        int rowCount = keys[0].getPositionCount();
        List<BlockHash.GroupSpec> groups = new ArrayList<>(keys.length);
        for (int k = 0; k < keys.length; k++) {
            Block key = keys[k];
            if (key.mayHaveNulls()) {
                for (int p = 0; p < rowCount; p++) {
                    if (key.isNull(p)) {
                        throw new IllegalArgumentException("repeated-key lookup requires non-null build keys");
                    }
                }
            }
            groups.add(new BlockHash.GroupSpec(k, key.elementType()));
        }

        // The emit batch size counts ordinals, not bytes, and the hash keeps a buffer of that many ints while the table lives.
        PackedValuesBlockHash builtHash = BlockHash.buildPackedValuesBlockHash(groups, blockFactory, 4096);
        IntArray offsets = null;
        IntArray rows = null;
        boolean success = false;
        try {
            try (IntArray groupsByRow = blockFactory.bigArrays().newIntArray(rowCount, false)) {
                builtHash.add(new Page(keys), new BlockHashGroupIdsAddInput() {
                    @Override
                    protected void addGroupId(int row, int groupId) {
                        groupsByRow.set(row, groupId);
                    }
                });
                int groupCount = builtHash.numKeys();
                offsets = blockFactory.bigArrays().newIntArray((long) groupCount + 1, true);
                for (int row = 0; row < rowCount; row++) {
                    offsets.increment(groupsByRow.get(row), 1);
                }
                int end = 0;
                for (int group = 0; group < groupCount; group++) {
                    end += offsets.get(group);
                    offsets.set(group, end);
                }
                offsets.set(groupCount, rowCount);
                rows = blockFactory.bigArrays().newIntArray(rowCount, false);
                // Fill backwards so decrementing the end offsets yields ascending row ids and leaves the start offsets.
                for (int row = rowCount - 1; row >= 0; row--) {
                    rows.set(offsets.increment(groupsByRow.get(row), -1), row);
                }
            }
            this.hash = builtHash;
            this.rowOffsets = offsets;
            this.rowIds = rows;
            success = true;
        } finally {
            if (success == false) {
                Releasables.closeExpectNoException(builtHash, offsets, rows);
            }
        }
    }

    @Override
    public ReleasableIterator<IntBlock> lookup(Page page, BlockFactory blockFactory, ByteSizeValue targetBlockSize) {
        return lookup(page, blockFactory, targetBlockSize, Warnings.NOOP_WARNINGS);
    }

    @Override
    public ReleasableIterator<IntBlock> lookup(Page page, BlockFactory blockFactory, ByteSizeValue targetBlockSize, Warnings warnings) {
        // A multivalued key can't match, so the hash returns a miss for it rather than looking up its values.
        warnOnMultivaluedKeys(page, warnings);
        return new Results(hash.lookup(page, blockFactory, targetBlockSize, true), blockFactory, targetBlockSize.getBytes());
    }

    @Override
    public String toString() {
        return "RepeatedKeys[" + hash + "]";
    }

    @Override
    public void close() {
        Releasables.closeExpectNoException(hash, rowOffsets, rowIds);
    }

    private final class Results implements ReleasableIterator<IntBlock> {
        private final ReleasableIterator<IntBlock> groups;
        private final BlockFactory blockFactory;
        private final long targetBytes;
        private IntBlock groupIds;
        private int position;

        private Results(ReleasableIterator<IntBlock> groups, BlockFactory blockFactory, long targetBytes) {
            this.groups = groups;
            this.blockFactory = blockFactory;
            this.targetBytes = targetBytes;
        }

        @Override
        public boolean hasNext() {
            return groupIds != null || groups.hasNext();
        }

        @Override
        public IntBlock next() {
            if (groupIds == null) {
                groupIds = groups.next();
                position = 0;
            }
            int start = position;
            long valueCount = 0;
            // Size the block for the rows whose matches fit in the target. A single probe row's matches stay together even if
            // they exceed it.
            do {
                valueCount += matchCount(position++);
            } while (position < groupIds.getPositionCount() && valueCount * Integer.BYTES < targetBytes);
            try (IntBlock.Builder result = blockFactory.newIntBlockBuilder(Math.toIntExact(valueCount))) {
                for (int p = start; p < position; p++) {
                    appendMatches(result, p);
                }
                if (position == groupIds.getPositionCount()) {
                    groupIds.close();
                    groupIds = null;
                }
                return result.build();
            }
        }

        /**
         * The number of values {@link #appendMatches} appends for probe row {@code p}, counting a miss as one.
         */
        private int matchCount(int p) {
            if (groupIds.isNull(p)) {
                return 1;
            }
            int group = groupIds.getInt(groupIds.getFirstValueIndex(p));
            return rowOffsets.get(group + 1) - rowOffsets.get(group);
        }

        private void appendMatches(IntBlock.Builder result, int p) {
            if (groupIds.isNull(p)) {
                result.appendNull();
                return;
            }
            assert groupIds.getValueCount(p) == 1;
            int group = groupIds.getInt(groupIds.getFirstValueIndex(p));
            int start = rowOffsets.get(group);
            int end = rowOffsets.get(group + 1);
            if (end - start == 1) {
                result.appendInt(rowIds.get(start));
            } else {
                result.beginPositionEntry();
                for (int i = start; i < end; i++) {
                    result.appendInt(rowIds.get(i));
                }
                result.endPositionEntry();
            }
        }

        @Override
        public void close() {
            Releasables.closeExpectNoException(groupIds, groups);
        }
    }
}
