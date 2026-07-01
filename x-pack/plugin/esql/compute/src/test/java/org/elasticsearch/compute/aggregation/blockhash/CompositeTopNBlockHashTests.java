/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation.blockhash;

import org.elasticsearch.compute.aggregation.GroupingAggregatorFunction;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.IntArrayBlock;
import org.elasticsearch.compute.data.IntBigArrayBlock;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.core.Releasables;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.in;

/**
 * Tests for {@link CompositeTopNBlockHash}: a 2-key (LONG × LONG) grouping where the primary sort key
 * is the first column. We verify that non-competitive groups (primary key outside the top-K range) are
 * pruned from the results returned by {@link BlockHash#nonEmpty}.
 */
public class CompositeTopNBlockHashTests extends BlockHashTestCase {

    /**
     * Feed groups (1, 10), (3, 30), (2, 20), (4, 40), (5, 50) with limit=2 ASC on primary key.
     * Expected result: only groups with primary key 1 and 2 survive in {@code nonEmpty()}.
     */
    public void testPrunesNonCompetitivePrimaryKeyAsc() {
        int limit = 2;
        long[][] rows = { { 1, 10 }, { 3, 30 }, { 2, 20 }, { 4, 40 }, { 5, 50 } };

        try (BlockHash hash = buildCompositeTopNHash(0, true, false, limit, 2)) {
            addLongLongRows(hash, rows);

            Set<Long> primaryKeysInNonEmpty = primaryKeysFromNonEmpty(hash);
            // Only keys 1 and 2 (the 2 smallest ASC) should survive
            assertThat(primaryKeysInNonEmpty, containsInAnyOrder(1L, 2L));
        }
    }

    /**
     * Feed groups (1, 10), (3, 30), (2, 20), (4, 40), (5, 50) with limit=2 DESC on primary key.
     * Expected result: only groups with primary key 5 and 4 survive in {@code nonEmpty()}.
     */
    public void testPrunesNonCompetitivePrimaryKeyDesc() {
        int limit = 2;
        long[][] rows = { { 1, 10 }, { 3, 30 }, { 2, 20 }, { 4, 40 }, { 5, 50 } };

        try (BlockHash hash = buildCompositeTopNHash(0, false, false, limit, 2)) {
            addLongLongRows(hash, rows);

            Set<Long> primaryKeysInNonEmpty = primaryKeysFromNonEmpty(hash);
            assertThat(primaryKeysInNonEmpty, containsInAnyOrder(5L, 4L));
        }
    }

    /**
     * When limit is larger than the number of distinct primary key values, all groups are retained.
     */
    public void testAllGroupsRetainedWhenLimitNotReached() {
        int limit = 100;
        long[][] rows = { { 1, 10 }, { 2, 20 }, { 3, 30 } };

        try (BlockHash hash = buildCompositeTopNHash(0, true, false, limit, 2)) {
            addLongLongRows(hash, rows);

            Set<Long> primaryKeysInNonEmpty = primaryKeysFromNonEmpty(hash);
            assertThat(primaryKeysInNonEmpty, containsInAnyOrder(1L, 2L, 3L));
        }
    }

    /**
     * Groups arrive in two separate pages; pruning must work across pages.
     */
    public void testPruningWorksAcrossMultiplePages() {
        int limit = 2;
        long[][] page1 = { { 1, 10 }, { 3, 30 } };
        long[][] page2 = { { 2, 20 }, { 4, 40 } };

        try (BlockHash hash = buildCompositeTopNHash(0, true, false, limit, 2)) {
            addLongLongRows(hash, page1);
            addLongLongRows(hash, page2);

            Set<Long> primaryKeysInNonEmpty = primaryKeysFromNonEmpty(hash);
            assertThat(primaryKeysInNonEmpty, containsInAnyOrder(1L, 2L));
        }
    }

    /**
     * Every ordinal assigned by {@code add()} and surfaced to the aggregator (non-null) must appear in
     * {@code nonEmpty()}. This mirrors the invariant checked by {@link BlockHashTestCase#hash}.
     */
    public void testOrdsAssignedAreAlwaysInNonEmpty() {
        int limit = 2;
        long[][] rows = { { 5, 50 }, { 1, 10 }, { 4, 40 }, { 2, 20 }, { 3, 30 } };

        try (BlockHash hash = buildCompositeTopNHash(0, true, false, limit, 2)) {
            Set<Integer> seenOrds = new HashSet<>();
            addLongLongRowsCapturingOrds(hash, rows, seenOrds);

            Set<Integer> nonEmptyOrds = new HashSet<>();
            try (IntVector nonEmpty = hash.nonEmpty()) {
                for (int i = 0; i < nonEmpty.getPositionCount(); i++) {
                    nonEmptyOrds.add(nonEmpty.getInt(i));
                }
            }
            // Every ord surfaced to the aggregator must be in nonEmpty.
            assertThat(seenOrds, everyItem(in(nonEmptyOrds)));
        }
    }

    // ---- helpers ----

    private BlockHash buildCompositeTopNHash(int primaryGroupingIndex, boolean asc, boolean nullsFirst, int limit, int numKeys) {
        assert numKeys == 2 : "only 2-key hashes supported in this test";
        BlockHash.TopNDef topNDef = new BlockHash.TopNDef(List.of(new BlockHash.SortKey(primaryGroupingIndex, asc, nullsFirst)), limit);
        List<BlockHash.GroupSpec> groups = List.of(
            new BlockHash.GroupSpec(0, ElementType.LONG, null, topNDef),
            new BlockHash.GroupSpec(1, ElementType.LONG, null, null)
        );
        return new CompositeTopNBlockHash(groups, topNDef, blockFactory, PackedValuesBlockHash.DEFAULT_BATCH_SIZE);
    }

    /** Adds rows as a single page of (long, long) columns and discards the assigned ords. */
    private void addLongLongRows(BlockHash hash, long[][] rows) {
        addLongLongRowsCapturingOrds(hash, rows, new HashSet<>());
    }

    private void addLongLongRowsCapturingOrds(BlockHash hash, long[][] rows, Set<Integer> capturedOrds) {
        long[] col0 = Arrays.stream(rows).mapToLong(r -> r[0]).toArray();
        long[] col1 = Arrays.stream(rows).mapToLong(r -> r[1]).toArray();

        Block block0 = blockFactory.newLongArrayVector(col0, col0.length).asBlock();
        Block block1 = blockFactory.newLongArrayVector(col1, col1.length).asBlock();
        try {
            hash.add(new Page(block0, block1), new GroupingAggregatorFunction.AddInput() {
                private void capture(IntBlock groupIds) {
                    for (int i = 0; i < groupIds.getPositionCount(); i++) {
                        if (groupIds.isNull(i) == false) {
                            capturedOrds.add(groupIds.getInt(groupIds.getFirstValueIndex(i)));
                        }
                    }
                }

                @Override
                public void add(int positionOffset, IntArrayBlock groupIds) {
                    capture(groupIds);
                }

                @Override
                public void add(int positionOffset, IntBigArrayBlock groupIds) {
                    capture(groupIds);
                }

                @Override
                public void add(int positionOffset, IntVector groupIds) {
                    capture(groupIds.asBlock());
                }

                @Override
                public void close() {}
            });
        } finally {
            Releasables.close(block0, block1);
        }
    }

    /**
     * Returns the primary key values (column 0) for all groups returned by {@code nonEmpty()}.
     */
    private Set<Long> primaryKeysFromNonEmpty(BlockHash hash) {
        try (IntVector nonEmpty = hash.nonEmpty()) {
            Block[] keys = hash.getKeys(nonEmpty);
            try {
                LongBlock primaryKeys = (LongBlock) keys[0];
                Set<Long> result = new HashSet<>();
                for (int i = 0; i < primaryKeys.getPositionCount(); i++) {
                    if (primaryKeys.isNull(i) == false) {
                        result.add(primaryKeys.getLong(primaryKeys.getFirstValueIndex(i)));
                    }
                }
                return result;
            } finally {
                Releasables.close(keys);
            }
        }
    }
}
