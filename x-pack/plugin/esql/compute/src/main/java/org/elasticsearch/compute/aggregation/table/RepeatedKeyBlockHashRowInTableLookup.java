/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation.table;

import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.BitArray;
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
import org.elasticsearch.compute.operator.Warnings;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.ReleasableIterator;
import org.elasticsearch.core.Releasables;

import java.util.ArrayList;
import java.util.List;

/**
 * Keeps all build-side row offsets for each hash group so repeated keys can produce one output row per match.
 * <p>
 * A build row with a null or multivalued key matches nothing, as in {@code LOOKUP JOIN}. Its key still goes in the hash, so
 * nothing has to be copied to leave it out, but its row goes in no group's rows. A group whose rows all have such keys is
 * a miss. The groups a multivalued key adds are marked, and a probe key that lands on one gets a warning, as a probe key that
 * matches a multivalued document in a {@code LOOKUP JOIN} lookup index does.
 */
final class RepeatedKeyBlockHashRowInTableLookup extends RowInTableLookup {
    /** Marks a build row that is in no group's rows, because one of its keys is null or multivalued. */
    private static final int NO_GROUP = -1;

    private final PackedValuesBlockHash hash;
    /** The rows for group {@code g} are in {@code rowIds} from {@code rowOffsets[g]} to {@code rowOffsets[g + 1]} (exclusive). */
    private final IntArray rowOffsets;
    private final IntArray rowIds;
    /** The groups that a build row with a multivalued key hashed to, or {@code null} if there is no such row. */
    @Nullable
    private final BitArray multivaluedGroups;

    RepeatedKeyBlockHashRowInTableLookup(BlockFactory blockFactory, Block[] keys) {
        int rowCount = keys[0].getPositionCount();
        List<BlockHash.GroupSpec> groups = new ArrayList<>(keys.length);
        for (int k = 0; k < keys.length; k++) {
            groups.add(new BlockHash.GroupSpec(k, keys[k].elementType()));
        }
        boolean checkEachRow = mayHaveNullOrMultivaluedKeys(keys);

        // The emit batch size counts ordinals, not bytes, and the hash keeps a buffer of that many ints while the table lives.
        PackedValuesBlockHash builtHash = BlockHash.buildPackedValuesBlockHash(groups, blockFactory, 4096);
        IntArray offsets = null;
        IntArray rows = null;
        BitArray multivalued = null;
        boolean success = false;
        try {
            multivalued = new BitArray(1, blockFactory.bigArrays());
            BitArray marked = multivalued;
            try (IntArray groupsByRow = blockFactory.bigArrays().newIntArray(rowCount, false)) {
                builtHash.add(new Page(keys), new GroupingAggregatorFunction.AddInput() {
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
                            addRow(positionOffset + p, groupIds.getInt(p));
                        }
                    }

                    /**
                     * Only a multivalued key gives a row more than one group, one for each value or combination of values.
                     */
                    private void addBlock(int positionOffset, IntBlock groupIds) {
                        for (int p = 0; p < groupIds.getPositionCount(); p++) {
                            int row = positionOffset + p;
                            int first = groupIds.getFirstValueIndex(p);
                            int count = groupIds.getValueCount(p);
                            if (count == 1) {
                                addRow(row, groupIds.getInt(first));
                                continue;
                            }
                            groupsByRow.set(row, NO_GROUP);
                            if (count > 1 && hasNull(keys, row) == false) {
                                for (int i = first; i < first + count; i++) {
                                    markMultivalued(groupIds.getInt(i));
                                }
                            }
                        }
                    }

                    /**
                     * Adds a row the hash gave one group. It may still have a null key, which the hash packs as a value, or a
                     * multivalued key whose values are all the same.
                     */
                    private void addRow(int row, int group) {
                        if (checkEachRow == false) {
                            groupsByRow.set(row, group);
                        } else if (hasNull(keys, row)) {
                            groupsByRow.set(row, NO_GROUP);
                        } else if (isMultivalued(keys, row)) {
                            groupsByRow.set(row, NO_GROUP);
                            markMultivalued(group);
                        } else {
                            groupsByRow.set(row, group);
                        }
                    }

                    private void markMultivalued(int group) {
                        marked.set(group);
                    }

                    @Override
                    public void close() {}
                });
                int groupCount = builtHash.numKeys();
                offsets = blockFactory.bigArrays().newIntArray((long) groupCount + 1, true);
                for (int row = 0; row < rowCount; row++) {
                    int group = groupsByRow.get(row);
                    if (group != NO_GROUP) {
                        offsets.increment(group, 1);
                    }
                }
                int end = 0;
                for (int group = 0; group < groupCount; group++) {
                    end += offsets.get(group);
                    offsets.set(group, end);
                }
                offsets.set(groupCount, end);
                rows = blockFactory.bigArrays().newIntArray(end, false);
                // Fill backwards so decrementing the end offsets yields ascending row ids and leaves the start offsets.
                for (int row = rowCount - 1; row >= 0; row--) {
                    int group = groupsByRow.get(row);
                    if (group != NO_GROUP) {
                        rows.set(offsets.increment(group, -1), row);
                    }
                }
            }
            if (multivalued.cardinality() == 0) {
                multivalued.close();
                multivalued = null;
            }
            this.hash = builtHash;
            this.rowOffsets = offsets;
            this.rowIds = rows;
            this.multivaluedGroups = multivalued;
            success = true;
        } finally {
            if (success == false) {
                Releasables.closeExpectNoException(builtHash, offsets, rows, multivalued);
            }
        }
    }

    private static boolean mayHaveNullOrMultivaluedKeys(Block[] keys) {
        for (Block key : keys) {
            if (key.mayHaveNulls() || key.mayHaveMultivaluedFields()) {
                return true;
            }
        }
        return false;
    }

    private static boolean hasNull(Block[] keys, int row) {
        for (Block key : keys) {
            if (key.isNull(row)) {
                return true;
            }
        }
        return false;
    }

    private static boolean isMultivalued(Block[] keys, int row) {
        for (Block key : keys) {
            if (key.getValueCount(row) > 1) {
                return true;
            }
        }
        return false;
    }

    @Override
    public ReleasableIterator<IntBlock> lookup(Page page, BlockFactory blockFactory, ByteSizeValue targetBlockSize) {
        return lookup(page, blockFactory, targetBlockSize, Warnings.NOOP_WARNINGS);
    }

    @Override
    public ReleasableIterator<IntBlock> lookup(Page page, BlockFactory blockFactory, ByteSizeValue targetBlockSize, Warnings warnings) {
        // A multivalued key can't match, so the hash returns a miss for it rather than looking up its values.
        return new Results(hash.lookup(page, blockFactory, targetBlockSize, true), blockFactory, targetBlockSize.getBytes(), warnings);
    }

    @Override
    public String toString() {
        return "RepeatedKeys[" + hash + "]";
    }

    @Override
    public void close() {
        Releasables.closeExpectNoException(hash, rowOffsets, rowIds, multivaluedGroups);
    }

    private final class Results implements ReleasableIterator<IntBlock> {
        private final ReleasableIterator<IntBlock> groups;
        private final BlockFactory blockFactory;
        private final long targetBytes;
        private final Warnings warnings;
        /** Whether this lookup warned about a probe key that landed on a multivalued build key. It warns once. */
        private boolean warned;
        private IntBlock groupIds;
        private int position;

        private Results(ReleasableIterator<IntBlock> groups, BlockFactory blockFactory, long targetBytes, Warnings warnings) {
            this.groups = groups;
            this.blockFactory = blockFactory;
            this.targetBytes = targetBytes;
            this.warnings = warnings;
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
            // A group whose rows all have a null or multivalued key has no rows, and is a miss.
            return Math.max(1, rowOffsets.get(group + 1) - rowOffsets.get(group));
        }

        private void appendMatches(IntBlock.Builder result, int p) {
            if (groupIds.isNull(p)) {
                result.appendNull();
                return;
            }
            assert groupIds.getValueCount(p) == 1;
            int group = groupIds.getInt(groupIds.getFirstValueIndex(p));
            if (warned == false && multivaluedGroups != null && multivaluedGroups.get(group)) {
                warnings.registerException(IllegalArgumentException.class, MULTIVALUED_KEY_WARNING);
                warned = true;
            }
            int start = rowOffsets.get(group);
            int end = rowOffsets.get(group + 1);
            if (end == start) {
                result.appendNull();
            } else if (end - start == 1) {
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
