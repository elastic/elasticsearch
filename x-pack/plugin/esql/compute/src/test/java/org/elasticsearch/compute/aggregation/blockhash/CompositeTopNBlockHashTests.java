/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation.blockhash;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.aggregation.GroupingAggregatorFunction;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BytesRefBlock;
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
import java.util.TreeSet;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.in;

/**
 * Tests for {@link CompositeTopNBlockHash}: 2-key groupings (LONG×LONG and BYTES_REF×LONG) where the
 * primary sort key is the first column. We verify that non-competitive groups are pruned from
 * {@link BlockHash#nonEmpty} and that the hash produces results identical to an un-hinted hash.
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
     * Every ordinal surfaced to the aggregator (non-null) during a <em>single page</em> must appear in
     * {@code nonEmpty()} after that page is processed. This is weaker than the standard BlockHash invariant:
     * across multiple pages the boundary can tighten and retroactively exclude a group that was emitted
     * earlier, so the guarantee only holds per-page. This test uses one page intentionally.
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

    /**
     * BYTES_REF primary key ASC: the two lexicographically smallest strings must survive.
     * Exercises the {@code getWorstValueView}/{@code isBytesRefInTop} branch that has a distinct
     * code path from the LONG branch.
     */
    public void testBytesRefPrimaryKeyAscPruning() {
        int limit = 2;
        String[] primary = { "cherry", "apple", "elderberry", "banana", "date" };
        long[] secondary = { 3, 1, 5, 2, 4 };

        try (BlockHash hash = buildCompositeTopNHashBytesRefPrimary(true, false, limit)) {
            addBytesRefLongRows(hash, primary, secondary);

            Set<String> keysInNonEmpty = bytesRefPrimaryKeysFromNonEmpty(hash);
            // "apple" and "banana" are the 2 lexicographically smallest (ASC)
            assertThat(keysInNonEmpty, containsInAnyOrder("apple", "banana"));
        }
    }

    /**
     * BYTES_REF primary key DESC: the two lexicographically largest strings must survive.
     */
    public void testBytesRefPrimaryKeyDescPruning() {
        int limit = 2;
        String[] primary = { "cherry", "apple", "elderberry", "banana", "date" };
        long[] secondary = { 3, 1, 5, 2, 4 };

        try (BlockHash hash = buildCompositeTopNHashBytesRefPrimary(false, false, limit)) {
            addBytesRefLongRows(hash, primary, secondary);

            Set<String> keysInNonEmpty = bytesRefPrimaryKeysFromNonEmpty(hash);
            // "elderberry" and "date" are the 2 lexicographically largest (DESC)
            assertThat(keysInNonEmpty, containsInAnyOrder("date", "elderberry"));
        }
    }

    /**
     * When multiple distinct groups share a primary key that falls exactly on the TopN boundary,
     * ALL of them must be retained — the boundary comparison is inclusive.
     *
     * <p>With limit=2 ASC and rows (1,100), (1,200), (2,300), (3,400): the 2 smallest distinct
     * primary keys are 1 and 2. Groups (1,100) and (1,200) both have primary key 1 (the boundary),
     * so all three groups with keys 1 and 2 must appear in {@code nonEmpty()}; group (3,400) must not.
     */
    public void testTieAtBoundaryKeepsAllGroupsWithBoundaryPrimaryKey() {
        int limit = 2;
        // Two distinct groups with primary key 1, one with primary key 2, one with primary key 3.
        long[][] rows = { { 1, 100 }, { 1, 200 }, { 2, 300 }, { 3, 400 } };

        try (BlockHash hash = buildCompositeTopNHash(0, true, false, limit, 2)) {
            addLongLongRows(hash, rows);

            Set<Long> primaryKeysInNonEmpty = primaryKeysFromNonEmpty(hash);
            assertThat(primaryKeysInNonEmpty, containsInAnyOrder(1L, 2L));

            // Verify group count: (1,100), (1,200), (2,300) — at least 3 groups, group (3,400) excluded.
            try (IntVector nonEmpty = hash.nonEmpty()) {
                assertTrue("expected at least 3 distinct groups for (1,100),(1,200),(2,300)", nonEmpty.getPositionCount() >= 3);
            }
        }
    }

    /**
     * Parity test: feed identical rows to a {@link CompositeTopNBlockHash} (with TopN hint) and to a
     * plain {@link PackedValuesBlockHash} (without hint). The true top-{@code limit} primary key values
     * (ASC-sorted from the plain hash) must all be present in the TopN hash's {@code nonEmpty()} output.
     * No primary key value beyond the boundary may appear in the TopN hash.
     */
    public void testParityWithPlainHashRandomized() {
        int limit = randomIntBetween(2, 8);
        int numDistinctPrimary = randomIntBetween(limit + 1, limit + 8);
        int rowsPerPrimary = randomIntBetween(1, 3);
        int numRows = numDistinctPrimary * rowsPerPrimary;

        // Build deterministic rows: primary key in [1..numDistinctPrimary], distinct secondary keys.
        long[][] rows = new long[numRows][2];
        for (int i = 0; i < numRows; i++) {
            rows[i][0] = (long) (i % numDistinctPrimary) + 1;
            rows[i][1] = (long) i + 100;
        }
        // Shuffle so groups arrive in non-sorted order.
        for (int i = numRows - 1; i > 0; i--) {
            int j = randomIntBetween(0, i);
            long[] tmp = rows[i];
            rows[i] = rows[j];
            rows[j] = tmp;
        }

        BlockHash.TopNDef topNDef = new BlockHash.TopNDef(List.of(new BlockHash.SortKey(0, true, false)), limit);
        List<BlockHash.GroupSpec> topNGroupSpecs = List.of(
            new BlockHash.GroupSpec(0, ElementType.LONG, null, topNDef),
            new BlockHash.GroupSpec(1, ElementType.LONG, null, null)
        );
        List<BlockHash.GroupSpec> plainGroupSpecs = List.of(
            new BlockHash.GroupSpec(0, ElementType.LONG),
            new BlockHash.GroupSpec(1, ElementType.LONG)
        );

        try (
            BlockHash topNHash = new CompositeTopNBlockHash(
                topNGroupSpecs,
                topNDef,
                blockFactory,
                PackedValuesBlockHash.DEFAULT_BATCH_SIZE
            );
            BlockHash plainHash = new PackedValuesBlockHash(plainGroupSpecs, blockFactory, PackedValuesBlockHash.DEFAULT_BATCH_SIZE)
        ) {
            addLongLongRows(topNHash, rows);
            addLongLongRows(plainHash, rows);

            Set<Long> topNPrimaryKeys = primaryKeysFromNonEmpty(topNHash);
            Set<Long> plainPrimaryKeys = primaryKeysFromNonEmpty(plainHash);

            // Compute the true top-K primary keys (ASC) from the plain hash.
            TreeSet<Long> sortedPlain = new TreeSet<>(plainPrimaryKeys);
            Set<Long> trueTopK = new HashSet<>();
            long boundary = Long.MIN_VALUE;
            int count = 0;
            for (long pk : sortedPlain) {
                if (count >= limit) break;
                trueTopK.add(pk);
                boundary = pk;
                count++;
            }

            // Every primary key in the true top-K must appear in the TopN hash.
            for (long pk : trueTopK) {
                assertTrue("primary key " + pk + " from true top-K must be present in TopN hash", topNPrimaryKeys.contains(pk));
            }

            // No primary key beyond the boundary may appear in the TopN hash.
            for (long pk : topNPrimaryKeys) {
                assertTrue("TopN hash must not contain primary key " + pk + " beyond ASC boundary " + boundary, pk <= boundary);
            }
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

    private BlockHash buildCompositeTopNHashBytesRefPrimary(boolean asc, boolean nullsFirst, int limit) {
        BlockHash.TopNDef topNDef = new BlockHash.TopNDef(List.of(new BlockHash.SortKey(0, asc, nullsFirst)), limit);
        List<BlockHash.GroupSpec> groups = List.of(
            new BlockHash.GroupSpec(0, ElementType.BYTES_REF, null, topNDef),
            new BlockHash.GroupSpec(1, ElementType.LONG, null, null)
        );
        return new CompositeTopNBlockHash(groups, topNDef, blockFactory, PackedValuesBlockHash.DEFAULT_BATCH_SIZE);
    }

    private void addBytesRefLongRows(BlockHash hash, String[] primaryKeys, long[] secondaryKeys) {
        assert primaryKeys.length == secondaryKeys.length;
        try (BytesRefBlock.Builder primaryBuilder = blockFactory.newBytesRefBlockBuilder(primaryKeys.length)) {
            for (String s : primaryKeys) {
                primaryBuilder.appendBytesRef(new BytesRef(s));
            }
            try (
                Block primaryBlock = primaryBuilder.build();
                Block secondaryBlock = blockFactory.newLongArrayVector(secondaryKeys, secondaryKeys.length).asBlock()
            ) {
                hash.add(new Page(primaryBlock, secondaryBlock), new GroupingAggregatorFunction.AddInput() {
                    @Override
                    public void add(int positionOffset, IntArrayBlock groupIds) {}

                    @Override
                    public void add(int positionOffset, IntBigArrayBlock groupIds) {}

                    @Override
                    public void add(int positionOffset, IntVector groupIds) {}

                    @Override
                    public void close() {}
                });
            }
        }
    }

    private Set<String> bytesRefPrimaryKeysFromNonEmpty(BlockHash hash) {
        try (IntVector nonEmpty = hash.nonEmpty()) {
            Block[] keys = hash.getKeys(nonEmpty);
            try {
                BytesRefBlock primaryKeys = (BytesRefBlock) keys[0];
                Set<String> result = new HashSet<>();
                BytesRef scratch = new BytesRef();
                for (int i = 0; i < primaryKeys.getPositionCount(); i++) {
                    if (primaryKeys.isNull(i) == false) {
                        result.add(primaryKeys.getBytesRef(primaryKeys.getFirstValueIndex(i), scratch).utf8ToString());
                    }
                }
                return result;
            } finally {
                Releasables.close(keys);
            }
        }
    }
}
