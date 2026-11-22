/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation.table;

import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.IntArray;
import org.elasticsearch.compute.aggregation.GroupingAggregatorFunction;
import org.elasticsearch.compute.aggregation.blockhash.BlockHash;
import org.elasticsearch.compute.aggregation.blockhash.PackedValuesBlockHash;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.IntArrayBlock;
import org.elasticsearch.compute.data.IntBigArrayBlock;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.core.ReleasableIterator;
import org.elasticsearch.core.Releasables;

import java.util.ArrayList;
import java.util.List;

/**
 * Keeps all build-side row offsets for each hash group so repeated keys can produce one output row per match.
 * <p>
 * A build row with a null or multivalued key matches nothing, as in {@code LOOKUP JOIN}, so it stays out of the hash.
 */
final class RepeatedKeyBlockHashRowInTableLookup extends RowInTableLookup {
    private final PackedValuesBlockHash hash;
    /** The rows for group {@code g} are in {@code rowIds} from {@code rowOffsets[g]} to {@code rowOffsets[g + 1]} (exclusive). */
    private final IntArray rowOffsets;
    private final IntArray rowIds;

    RepeatedKeyBlockHashRowInTableLookup(BlockFactory blockFactory, Block[] keys) {
        List<BlockHash.GroupSpec> groups = new ArrayList<>(keys.length);
        for (int k = 0; k < keys.length; k++) {
            groups.add(new BlockHash.GroupSpec(k, keys[k].elementType()));
        }

        /*
         * The hash would give a null key a group a null probe key finds, and a multivalued key a group for each of its values,
         * so hash a copy of only the rows where every key holds one value. sourceRows maps each copied row to its row in keys.
         */
        IntArray sourceRows = null;
        Block[] hashedKeys = keys;
        PackedValuesBlockHash builtHash = null;
        IntArray offsets = null;
        IntArray rows = null;
        boolean success = false;
        try {
            if (mayHaveUnmatchableRows(keys)) {
                int keyRows = keys[0].getPositionCount();
                sourceRows = blockFactory.bigArrays().newIntArray(keyRows, false);
                int matchable = 0;
                for (int row = 0; row < keyRows; row++) {
                    if (isMatchable(keys, row)) {
                        sourceRows.set(matchable++, row);
                    }
                }
                if (matchable < keyRows) {
                    hashedKeys = copyRows(blockFactory, keys, sourceRows, matchable);
                }
            }
            int rowCount = hashedKeys[0].getPositionCount();
            // The emit batch size counts ordinals, not bytes, and the hash keeps a buffer of that many ints while the table lives.
            builtHash = BlockHash.buildPackedValuesBlockHash(groups, blockFactory, 4096);
            try (IntArray groupsByRow = blockFactory.bigArrays().newIntArray(rowCount, false)) {
                builtHash.add(new Page(hashedKeys), new GroupingAggregatorFunction.AddInput() {
                    @Override
                    public void add(int positionOffset, IntArrayBlock groupIds) {
                        addBlock(positionOffset, groupIds);
                    }

                    @Override
                    public void add(int positionOffset, IntBigArrayBlock groupIds) {
                        addBlock(positionOffset, groupIds);
                    }

                    @Override
                    public void add(int positionOffset, IntVector groupIds) {
                        for (int p = 0; p < groupIds.getPositionCount(); p++) {
                            groupsByRow.set(positionOffset + p, groupIds.getInt(p));
                        }
                    }

                    /**
                     * The hashed keys are single-valued and non-null, so each row has exactly one group.
                     */
                    private void addBlock(int positionOffset, IntBlock groupIds) {
                        for (int p = 0; p < groupIds.getPositionCount(); p++) {
                            assert groupIds.getValueCount(p) == 1 : "expected one group per build row";
                            groupsByRow.set(positionOffset + p, groupIds.getInt(groupIds.getFirstValueIndex(p)));
                        }
                    }

                    @Override
                    public void close() {}
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
                    rows.set(offsets.increment(groupsByRow.get(row), -1), hashedKeys == keys ? row : sourceRows.get(row));
                }
            }
            this.hash = builtHash;
            this.rowOffsets = offsets;
            this.rowIds = rows;
            success = true;
        } finally {
            Releasables.closeExpectNoException(sourceRows);
            if (hashedKeys != keys) {
                Releasables.closeExpectNoException(hashedKeys);
            }
            if (success == false) {
                Releasables.closeExpectNoException(builtHash, offsets, rows);
            }
        }
    }

    private static boolean mayHaveUnmatchableRows(Block[] keys) {
        for (Block key : keys) {
            if (key.mayHaveNulls() || key.mayHaveMultivaluedFields()) {
                return true;
            }
        }
        return false;
    }

    /**
     * Whether every key holds exactly one value at {@code row}: a null holds none.
     */
    private static boolean isMatchable(Block[] keys, int row) {
        for (Block key : keys) {
            if (key.getValueCount(row) != 1) {
                return false;
            }
        }
        return true;
    }

    /**
     * Copies the first {@code count} rows listed in {@code sourceRows} out of each key.
     */
    private static Block[] copyRows(BlockFactory blockFactory, Block[] keys, IntArray sourceRows, int count) {
        Block[] copies = new Block[keys.length];
        boolean success = false;
        try {
            for (int k = 0; k < keys.length; k++) {
                try (Block.Builder builder = keys[k].elementType().newBlockBuilder(count, blockFactory)) {
                    for (int i = 0; i < count; i++) {
                        int row = sourceRows.get(i);
                        builder.copyFrom(keys[k], row, row + 1);
                    }
                    copies[k] = builder.build();
                }
            }
            success = true;
            return copies;
        } finally {
            if (success == false) {
                Releasables.closeExpectNoException(copies);
            }
        }
    }

    @Override
    public ReleasableIterator<IntBlock> lookup(Page page, BlockFactory blockFactory, ByteSizeValue targetBlockSize) {
        // A multivalued key can't match, so the hash returns a miss for it rather than looking up its values.
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
