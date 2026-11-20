/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation.blockhash;

import org.elasticsearch.common.util.IntArray;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Hashes the keys of each row to one group, keeping a multivalue whole as a list: {@code [1, 2]}, {@code [2, 1]} and
 * {@code [1, 1, 2]} are three keys, none of them the key of {@code 1}. This is what {@code LIMIT ... BY} needs, where
 * {@link BlockHash} gives a row one group per value of a multivalue. Rows whose keys hold at most one value are hashed
 * by the same {@link BlockHash}es as {@code STATS ... BY}.
 * <p>
 *     With one key column of a type that has a specialized hash, those rows go through the hash {@link BlockHash#build}
 *     picks for the column, and the rows with a multivalue through a {@link PackedValuesBlockHash} with whole multivalues,
 *     created on the first one. The ids of both are mapped to one range. Otherwise the packed hash takes every row.
 * </p>
 */
public final class RowKeyHash implements Releasable {
    /**
     * The number of group ids a hash hands back at once, as for {@code STATS ... BY}.
     */
    private static final int EMIT_BATCH_SIZE = Operator.TARGET_PAGE_SIZE / Long.SIZE;

    private final BlockFactory blockFactory;
    private final List<BlockHash.GroupSpec> multivalueSpecs;
    /**
     * Reads the single key column, {@code -1} with several key columns.
     */
    private final int singleKeyChannel;
    @Nullable
    private final BlockHash singleValues;
    @Nullable
    private final GroupMap singleValueGroups;
    @Nullable
    private PackedValuesBlockHash multivalues;
    /**
     * Maps the ids of {@link #multivalues} when {@link #singleValues} is used, {@code null} otherwise.
     */
    @Nullable
    private GroupMap multivalueGroups;
    private int groupCount;

    /**
     * @param elementTypes the element type of every channel of the input, indexed by channel
     */
    public RowKeyHash(int[] keyChannels, List<ElementType> elementTypes, BlockFactory blockFactory) {
        this.blockFactory = blockFactory;
        if (keyChannels.length == 1 && hasSpecializedHash(elementTypes.get(keyChannels[0]))) {
            // Both hashes read a page that holds only the key column.
            List<BlockHash.GroupSpec> specs = List.of(new BlockHash.GroupSpec(0, elementTypes.get(keyChannels[0])));
            this.multivalueSpecs = specs;
            this.singleKeyChannel = keyChannels[0];
            BlockHash singleValues = BlockHash.build(specs, blockFactory, EMIT_BATCH_SIZE, false);
            boolean success = false;
            try {
                this.singleValueGroups = new GroupMap();
                success = true;
            } finally {
                if (success == false) {
                    singleValues.close();
                }
            }
            this.singleValues = singleValues;
        } else {
            List<BlockHash.GroupSpec> specs = new ArrayList<>(keyChannels.length);
            for (int channel : keyChannels) {
                specs.add(new BlockHash.GroupSpec(channel, elementTypes.get(channel)));
            }
            this.multivalueSpecs = specs;
            this.singleKeyChannel = -1;
            this.singleValues = null;
            this.singleValueGroups = null;
            this.multivalues = newMultivalues();
        }
    }

    private static boolean hasSpecializedHash(ElementType type) {
        return switch (type) {
            case BOOLEAN, INT, LONG, BYTES_REF -> true;
            // The specialized hash for doubles maps every NaN to one key. The packed hash keeps the raw bits, so one double
            // key groups the same way as several.
            case DOUBLE, FLOAT, NULL, DOC, COMPOSITE, UNKNOWN, AGGREGATE_METRIC_DOUBLE, EXPONENTIAL_HISTOGRAM, TDIGEST, LONG_RANGE,
                DOUBLE_RANGE -> false;
        };
    }

    private PackedValuesBlockHash newMultivalues() {
        return new PackedValuesBlockHash(multivalueSpecs, blockFactory, EMIT_BATCH_SIZE, true);
    }

    /**
     * Adds the keys of every row of {@code page} and returns the group of each row.
     */
    public int[] add(Page page) {
        int[] groupIds = new int[page.getPositionCount()];
        if (singleValues == null) {
            hash(multivalues, page, null, groupIds);
            groupCount = multivalues.numKeys();
            return groupIds;
        }
        Block keys = page.getBlock(singleKeyChannel);
        int[] multivalued = multivaluedPositions(keys);
        if (multivalued.length == 0) {
            keys.incRef();
            add(singleValues, singleValueGroups, keys, null, groupIds);
            return groupIds;
        }
        if (multivalues == null) {
            multivalues = newMultivalues();
            multivalueGroups = new GroupMap();
        }
        if (multivalued.length == groupIds.length) {
            // Every row holds a multivalue, so there's nothing to split.
            keys.incRef();
            add(multivalues, multivalueGroups, keys, null, groupIds);
            return groupIds;
        }
        int[] singleValued = complement(multivalued, groupIds.length);
        add(singleValues, singleValueGroups, keys.filter(false, singleValued), singleValued, groupIds);
        add(multivalues, multivalueGroups, keys.filter(false, multivalued), multivalued, groupIds);
        return groupIds;
    }

    /**
     * Hashes {@code keys}, releases them, and writes the group of each at its row in {@code groupIds}.
     * @param positions the row of each key, or {@code null} if the keys are every row
     */
    private static void add(BlockHash hash, GroupMap groups, Block keys, @Nullable int[] positions, int[] groupIds) {
        Page page = new Page(keys);
        try {
            hash(hash, page, positions, groupIds);
        } finally {
            page.releaseBlocks();
        }
        int count = positions == null ? groupIds.length : positions.length;
        for (int i = 0; i < count; i++) {
            int p = positions == null ? i : positions[i];
            groupIds[p] = groups.group(groupIds[p]);
        }
    }

    /**
     * Adds {@code page} to {@code hash} and writes the id of each of its rows into {@code groupIds}, failing if the
     * hash doesn't give one to every row.
     */
    private static void hash(BlockHash hash, Page page, @Nullable int[] positions, int[] groupIds) {
        int count = page.getPositionCount();
        for (int i = 0; i < count; i++) {
            groupIds[positions == null ? i : positions[i]] = -1;
        }
        hash.add(page, new PositionGroupIds(groupIds, positions));
        for (int i = 0; i < count; i++) {
            if (groupIds[positions == null ? i : positions[i]] < 0) {
                throw new IllegalStateException("row [" + i + "] has no group in " + hash);
            }
        }
    }

    private static int[] multivaluedPositions(Block keys) {
        if (keys.mayHaveMultivaluedFields() == false) {
            return new int[0];
        }
        int[] positions = new int[keys.getPositionCount()];
        int count = 0;
        for (int p = 0; p < keys.getPositionCount(); p++) {
            if (keys.getValueCount(p) > 1) {
                positions[count++] = p;
            }
        }
        return Arrays.copyOf(positions, count);
    }

    /**
     * The positions below {@code positionCount} that aren't in {@code sorted}.
     */
    private static int[] complement(int[] sorted, int positionCount) {
        int[] result = new int[positionCount - sorted.length];
        int next = 0;
        int count = 0;
        for (int p = 0; p < positionCount; p++) {
            if (next < sorted.length && sorted[next] == p) {
                next++;
            } else {
                result[count++] = p;
            }
        }
        return result;
    }

    /**
     * The number of groups.
     */
    public int numKeys() {
        return groupCount;
    }

    @Override
    public void close() {
        Releasables.close(singleValues, singleValueGroups, multivalues, multivalueGroups);
    }

    @Override
    public String toString() {
        return "RowKeyHash{singleValues=" + singleValues + ", multivalues=" + multivalues + ", groups=" + groupCount + "}";
    }

    /**
     * Gives each id of one hash the next group the first time it's seen.
     */
    private final class GroupMap implements Releasable {
        private IntArray groups = blockFactory.bigArrays().newIntArray(16, false);
        /**
         * Entries up to here are set: a group, or {@code -1} for an id not seen yet.
         */
        private long size;

        int group(int id) {
            if (id >= size) {
                groups = blockFactory.bigArrays().grow(groups, id + 1);
                groups.fill(size, id + 1, -1);
                size = id + 1;
            }
            int group = groups.get(id);
            if (group < 0) {
                group = groupCount++;
                groups.set(id, group);
            }
            return group;
        }

        @Override
        public void close() {
            groups.close();
        }
    }
}
