/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator.lookup;

import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.Warnings;
import org.elasticsearch.compute.test.ComputeTestCase;
import org.elasticsearch.core.ReleasableIterator;

import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

public class HashJoinTests extends ComputeTestCase {

    private Page joinSinglePage(ReleasableIterator<Page> iter) {
        try (iter) {
            assertTrue(iter.hasNext());
            Page page = iter.next();
            assertFalse(iter.hasNext());
            return page;
        }
    }

    /**
     * Simple test: left has keys [1, 2, 3], right has key=1 -> "a", key=2 -> "b".
     * Key 3 should get nulls (LEFT JOIN).
     */
    public void testBasicJoin() {
        BlockFactory factory = blockFactory();

        // Right pages: [positions, key, value]
        // positions column is ignored, key=long, value=long
        Page rightPage = new Page(
            factory.newConstantIntBlockWith(0, 3).asVector().asBlock(),  // positions (all 0)
            factory.newLongArrayVector(new long[] { 10, 20, 30 }, 3).asBlock(),  // key
            factory.newLongArrayVector(new long[] { 100, 200, 300 }, 3).asBlock()  // value
        );

        // Left page: [leftData, key]
        Page leftPage = new Page(
            factory.newLongArrayVector(new long[] { 1, 2, 3 }, 3).asBlock(),  // leftData
            factory.newLongArrayVector(new long[] { 10, 30, 99 }, 3).asBlock()  // key (10 matches, 30 matches, 99 no match)
        );

        // rightOutputColumnCount=2: output both key and value columns (last 2 of 3 right cols)
        try (HashJoin join = new HashJoin(List.of(rightPage), new int[] { 1 }, 2, factory, Warnings.NOOP_WARNINGS)) {
            assertThat(join.keyCount(), equalTo(3));

            Page result = joinSinglePage(join.join(leftPage, new int[] { 1 }));
            try {
                assertThat(result.getPositionCount(), equalTo(3));
                assertThat(result.getBlockCount(), equalTo(4)); // 2 left + 2 right data columns

                // Left columns preserved (row order: key=10 match, key=30 match, key=99 no match)
                LongBlock leftDataOut = result.getBlock(0);
                assertThat(leftDataOut.getLong(0), equalTo(1L));  // left row 0
                assertThat(leftDataOut.getLong(1), equalTo(2L));  // left row 1
                assertThat(leftDataOut.getLong(2), equalTo(3L));  // left row 2

                // Right key column
                LongBlock rightKeyOut = result.getBlock(2);
                assertThat(rightKeyOut.getLong(0), equalTo(10L));  // matched
                assertThat(rightKeyOut.getLong(1), equalTo(30L));  // matched
                assertThat(rightKeyOut.isNull(2), equalTo(true));  // no match -> null

                // Right value column
                LongBlock rightValueOut = result.getBlock(3);
                assertThat(rightValueOut.getLong(0), equalTo(100L));  // matched
                assertThat(rightValueOut.getLong(1), equalTo(300L));  // matched
                assertThat(rightValueOut.isNull(2), equalTo(true));   // no match -> null
            } finally {
                result.releaseBlocks();
            }
        } finally {
            leftPage.releaseBlocks();
        }
    }

    /**
     * Test multiple matches for same key: key=10 appears twice in right side.
     * Left row with key=10 should be duplicated.
     */
    public void testMultipleMatches() {
        BlockFactory factory = blockFactory();

        Page rightPage = new Page(
            factory.newConstantIntBlockWith(0, 3).asVector().asBlock(),
            factory.newLongArrayVector(new long[] { 10, 10, 20 }, 3).asBlock(),  // key (10 appears twice)
            factory.newLongArrayVector(new long[] { 100, 101, 200 }, 3).asBlock()  // value
        );

        Page leftPage = new Page(
            factory.newLongArrayVector(new long[] { 1 }, 1).asBlock(),  // leftData
            factory.newLongArrayVector(new long[] { 10 }, 1).asBlock()  // key matches twice
        );

        try (HashJoin join = new HashJoin(List.of(rightPage), new int[] { 1 }, 2, factory, Warnings.NOOP_WARNINGS)) {
            Page result = joinSinglePage(join.join(leftPage, new int[] { 1 }));
            try {
                assertThat(result.getPositionCount(), equalTo(2));  // duplicated

                // Left data duplicated
                LongBlock leftDataOut = result.getBlock(0);
                assertThat(leftDataOut.getLong(0), equalTo(1L));
                assertThat(leftDataOut.getLong(1), equalTo(1L));

                // Right values
                LongBlock rightValueOut = result.getBlock(3);
                assertThat(rightValueOut.getLong(0), equalTo(100L));
                assertThat(rightValueOut.getLong(1), equalTo(101L));
            } finally {
                result.releaseBlocks();
            }
        } finally {
            leftPage.releaseBlocks();
        }
    }

    /**
     * Test with no matches at all: all left rows get null right columns.
     */
    public void testNoMatches() {
        BlockFactory factory = blockFactory();

        Page rightPage = new Page(
            factory.newConstantIntBlockWith(0, 2).asVector().asBlock(),
            factory.newLongArrayVector(new long[] { 10, 20 }, 2).asBlock(),
            factory.newLongArrayVector(new long[] { 100, 200 }, 2).asBlock()
        );

        Page leftPage = new Page(
            factory.newLongArrayVector(new long[] { 1, 2 }, 2).asBlock(),
            factory.newLongArrayVector(new long[] { 99, 98 }, 2).asBlock()  // no matches
        );

        try (HashJoin join = new HashJoin(List.of(rightPage), new int[] { 1 }, 2, factory, Warnings.NOOP_WARNINGS)) {
            Page result = joinSinglePage(join.join(leftPage, new int[] { 1 }));
            try {
                assertThat(result.getPositionCount(), equalTo(2));
                assertThat(result.getBlockCount(), equalTo(4)); // 2 left + 2 right data columns

                // Right value column is null
                LongBlock rightValueOut = result.getBlock(3);
                assertThat(rightValueOut.isNull(0), equalTo(true));
                assertThat(rightValueOut.isNull(1), equalTo(true));
            } finally {
                result.releaseBlocks();
            }
        } finally {
            leftPage.releaseBlocks();
        }
    }

    /**
     * Test with null join keys on left side: should produce null right columns.
     */
    public void testNullLeftKey() {
        BlockFactory factory = blockFactory();

        Page rightPage = new Page(
            factory.newConstantIntBlockWith(0, 1).asVector().asBlock(),
            factory.newLongArrayVector(new long[] { 10 }, 1).asBlock(),
            factory.newLongArrayVector(new long[] { 100 }, 1).asBlock()
        );

        // Left page with null key at position 0
        LongBlock.Builder leftKeyBuilder = factory.newLongBlockBuilder(2);
        leftKeyBuilder.appendNull();
        leftKeyBuilder.appendLong(10);

        Page leftPage = new Page(factory.newLongArrayVector(new long[] { 1, 2 }, 2).asBlock(), leftKeyBuilder.build());

        try (HashJoin join = new HashJoin(List.of(rightPage), new int[] { 1 }, 2, factory, Warnings.NOOP_WARNINGS)) {
            Page result = joinSinglePage(join.join(leftPage, new int[] { 1 }));
            try {
                assertThat(result.getPositionCount(), equalTo(2));

                // Position 0: null key -> null right columns
                LongBlock rightValueOut = result.getBlock(3);
                assertThat(rightValueOut.isNull(0), equalTo(true));
                // Position 1: key=10 matches
                assertThat(rightValueOut.getLong(1), equalTo(100L));
            } finally {
                result.releaseBlocks();
            }
        } finally {
            leftPage.releaseBlocks();
        }
    }

    /**
     * Test with multiple right pages.
     */
    public void testMultipleRightPages() {
        BlockFactory factory = blockFactory();

        Page rightPage1 = new Page(
            factory.newConstantIntBlockWith(0, 2).asVector().asBlock(),
            factory.newLongArrayVector(new long[] { 10, 20 }, 2).asBlock(),
            factory.newLongArrayVector(new long[] { 100, 200 }, 2).asBlock()
        );
        Page rightPage2 = new Page(
            factory.newConstantIntBlockWith(0, 2).asVector().asBlock(),
            factory.newLongArrayVector(new long[] { 30, 40 }, 2).asBlock(),
            factory.newLongArrayVector(new long[] { 300, 400 }, 2).asBlock()
        );

        Page leftPage = new Page(
            factory.newLongArrayVector(new long[] { 1, 2, 3 }, 3).asBlock(),
            factory.newLongArrayVector(new long[] { 20, 30, 50 }, 3).asBlock()
        );

        try (HashJoin join = new HashJoin(List.of(rightPage1, rightPage2), new int[] { 1 }, 2, factory, Warnings.NOOP_WARNINGS)) {
            assertThat(join.keyCount(), equalTo(4));  // 10, 20, 30, 40

            Page result = joinSinglePage(join.join(leftPage, new int[] { 1 }));
            try {
                assertThat(result.getPositionCount(), equalTo(3));
                assertThat(result.getBlockCount(), equalTo(4)); // 2 left + 2 right data columns

                LongBlock rightValueOut = result.getBlock(3);
                assertThat(rightValueOut.getLong(0), equalTo(200L));  // key=20 from page1
                assertThat(rightValueOut.getLong(1), equalTo(300L));  // key=30 from page2
                assertThat(rightValueOut.isNull(2), equalTo(true));   // key=50 no match
            } finally {
                result.releaseBlocks();
            }
        } finally {
            leftPage.releaseBlocks();
        }
    }

    /**
     * Test with empty right side: all left rows get nulls.
     */
    public void testEmptyRightSide() {
        BlockFactory factory = blockFactory();

        Page leftPage = new Page(
            factory.newLongArrayVector(new long[] { 1, 2 }, 2).asBlock(),
            factory.newLongArrayVector(new long[] { 10, 20 }, 2).asBlock()
        );

        try (HashJoin join = new HashJoin(List.of(), new int[] { 1 }, 2, factory, Warnings.NOOP_WARNINGS)) {
            assertThat(join.keyCount(), equalTo(0));
            assertThat(join.indexedRowCount(), equalTo(0));

            Page result = joinSinglePage(join.join(leftPage, new int[] { 1 }));
            try {
                // With no right pages, right columns are all nulls
                assertThat(result.getPositionCount(), equalTo(2));
                assertThat(result.getBlockCount(), equalTo(4)); // 2 left + 2 right null columns

                // Right columns should be all nulls
                assertThat(result.getBlock(2).isNull(0), equalTo(true));
                assertThat(result.getBlock(2).isNull(1), equalTo(true));
                assertThat(result.getBlock(3).isNull(0), equalTo(true));
                assertThat(result.getBlock(3).isNull(1), equalTo(true));
            } finally {
                result.releaseBlocks();
            }
        } finally {
            leftPage.releaseBlocks();
        }
    }

    /**
     * Test composite key join: matching on two key columns simultaneously (AND semantics).
     * Right: (key1, key2, value) = (1,10,100), (1,20,200), (2,10,300)
     * Left: (data, key1, key2) = (A, 1, 10), (B, 1, 20), (C, 2, 20), (D, 1, 10)
     * Expected matches: A->(1,10,100), B->(1,20,200), C->null, D->(1,10,100)
     */
    public void testCompositeKeyJoin() {
        BlockFactory factory = blockFactory();

        // Right pages: [positions, key1, key2, value]
        Page rightPage = new Page(
            factory.newConstantIntBlockWith(0, 3).asVector().asBlock(),       // positions
            factory.newLongArrayVector(new long[] { 1, 1, 2 }, 3).asBlock(), // key1
            factory.newLongArrayVector(new long[] { 10, 20, 10 }, 3).asBlock(), // key2
            factory.newLongArrayVector(new long[] { 100, 200, 300 }, 3).asBlock() // value
        );

        // Left page: [data, key1, key2]
        Page leftPage = new Page(
            factory.newLongArrayVector(new long[] { 1, 2, 3, 4 }, 4).asBlock(), // data
            factory.newLongArrayVector(new long[] { 1, 1, 2, 1 }, 4).asBlock(), // key1
            factory.newLongArrayVector(new long[] { 10, 20, 20, 10 }, 4).asBlock() // key2
        );

        // joinKeyColumnsInRight = {1, 2} (key1 at col 1, key2 at col 2)
        // rightOutputColumnCount = 3 (output key1, key2, value — last 3 of 4 right cols)
        try (HashJoin join = new HashJoin(List.of(rightPage), new int[] { 1, 2 }, 3, factory, Warnings.NOOP_WARNINGS)) {
            assertThat(join.keyCount(), equalTo(3)); // (1,10), (1,20), (2,10)

            Page result = joinSinglePage(join.join(leftPage, new int[] { 1, 2 }));
            try {
                assertThat(result.getPositionCount(), equalTo(4));
                assertThat(result.getBlockCount(), equalTo(6)); // 3 left + 3 right data columns

                // Left data column preserved
                LongBlock leftDataOut = result.getBlock(0);
                assertThat(leftDataOut.getLong(0), equalTo(1L));
                assertThat(leftDataOut.getLong(1), equalTo(2L));
                assertThat(leftDataOut.getLong(2), equalTo(3L));
                assertThat(leftDataOut.getLong(3), equalTo(4L));

                // Right value column (last right output column, index 5)
                LongBlock rightValueOut = result.getBlock(5);
                assertThat(rightValueOut.getLong(0), equalTo(100L));   // (1,10) -> 100
                assertThat(rightValueOut.getLong(1), equalTo(200L));   // (1,20) -> 200
                assertThat(rightValueOut.isNull(2), equalTo(true));    // (2,20) -> no match
                assertThat(rightValueOut.getLong(3), equalTo(100L));   // (1,10) -> 100
            } finally {
                result.releaseBlocks();
            }
        } finally {
            leftPage.releaseBlocks();
        }
    }

    /**
     * Test that highly expanding joins produce multiple output pages when the page size is limited.
     * 1 left row with 20 right-side matches should produce multiple pages with maxOutputPageSize=5.
     */
    public void testChunkedOutputForExpandingJoin() {
        BlockFactory factory = blockFactory();

        int rightRows = 20;
        long[] rightKeys = new long[rightRows];
        long[] rightValues = new long[rightRows];
        java.util.Arrays.fill(rightKeys, 42L);
        for (int i = 0; i < rightRows; i++) {
            rightValues[i] = 1000 + i;
        }

        Page rightPage = new Page(
            factory.newConstantIntBlockWith(0, rightRows).asVector().asBlock(),
            factory.newLongArrayVector(rightKeys, rightRows).asBlock(),
            factory.newLongArrayVector(rightValues, rightRows).asBlock()
        );

        Page leftPage = new Page(
            factory.newLongArrayVector(new long[] { 1 }, 1).asBlock(),
            factory.newLongArrayVector(new long[] { 42 }, 1).asBlock()
        );

        try (HashJoin join = new HashJoin(List.of(rightPage), new int[] { 1 }, 2, factory, Warnings.NOOP_WARNINGS)) {
            List<Page> pages = new ArrayList<>();
            try (ReleasableIterator<Page> iter = join.join(leftPage, new int[] { 1 }, 5)) {
                while (iter.hasNext()) {
                    pages.add(iter.next());
                }
            }
            try {
                assertThat(pages.size(), equalTo(4)); // 20 rows / 5 per page = 4 pages

                int totalRows = 0;
                for (Page p : pages) {
                    assertThat(p.getPositionCount(), greaterThan(0));
                    totalRows += p.getPositionCount();
                }
                assertThat(totalRows, equalTo(rightRows));

                // Verify first page values
                LongBlock firstValues = pages.get(0).getBlock(3);
                for (int i = 0; i < pages.get(0).getPositionCount(); i++) {
                    assertThat(firstValues.getLong(i), equalTo(1000L + i));
                }
            } finally {
                for (Page p : pages) {
                    p.releaseBlocks();
                }
            }
        } finally {
            leftPage.releaseBlocks();
        }
    }
}
