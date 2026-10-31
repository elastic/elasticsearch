/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator.lookup;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BytesRefVector;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.test.BlockTestUtils;
import org.elasticsearch.compute.test.ComputeTestCase;
import org.elasticsearch.compute.test.RandomBlock;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.test.ListMatcher;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.stream.IntStream;

import static org.elasticsearch.test.ListMatcher.matchesList;
import static org.elasticsearch.test.MapMatcher.assertMap;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.sameInstance;

public class RightChunkedInnerJoinTests extends ComputeTestCase {
    /**
     * The example in the main javadoc of {@link RightChunkedInnerJoin}: unmatched rows are dropped and a row matching twice is
     * repeated.
     */
    public void testExample() {
        testExample(blockFactory());
    }

    public void testExampleCranky() {
        testWithCrankyBlockFactory(this::testExample);
    }

    private void testExample(BlockFactory factory) {
        try (RightChunkedInnerJoin join = new RightChunkedInnerJoin(buildLeftHand(factory, 4), 2)) {
            assertJoined(
                factory,
                join,
                new int[][] {
                    { 1, 2, 3 }, // formatter
                    { 1, 3, 3 }, // formatter
                    { 3, 9, 9 }, // formatter
                },
                new Object[][] {
                    { "l1", 2, 3 }, // formatter
                    { "l1", 3, 3 }, // formatter
                    { "l3", 9, 9 }, // formatter
                }
            );
            assertThat(join.noMoreRightHandPages(), equalTo(Optional.empty()));
        }
    }

    /**
     * A left row whose matches span two right pages shows up in both outputs.
     */
    public void testOverlapAcrossPages() {
        testOverlapAcrossPages(blockFactory());
    }

    public void testOverlapAcrossPagesCranky() {
        testWithCrankyBlockFactory(this::testOverlapAcrossPages);
    }

    private void testOverlapAcrossPages(BlockFactory factory) {
        try (RightChunkedInnerJoin join = new RightChunkedInnerJoin(buildLeftHand(factory, 8), 2)) {
            assertJoined(
                factory,
                join,
                new int[][] {
                    { 0, 1, 1 }, // formatter
                    { 2, 2, 2 }, // formatter
                },
                new Object[][] {
                    { "l0", 1, 1 }, // formatter
                    { "l2", 2, 2 }, // formatter
                }
            );
            assertJoined(
                factory,
                join,
                new int[][] {
                    { 2, 3, 3 }, // formatter
                    { 7, 4, 4 }, // formatter
                },
                new Object[][] {
                    { "l2", 3, 3 }, // formatter
                    { "l7", 4, 4 }, // formatter
                }
            );
            assertThat(join.noMoreRightHandPages(), equalTo(Optional.empty()));
        }
    }

    /**
     * Every left row matches exactly once and in order: the left blocks are reused rather than filtered.
     */
    public void testIdentityReusesLeftBlocks() {
        BlockFactory factory = blockFactory();
        int size = between(1, 100);
        Page left = buildLeftHand(factory, size);
        try (RightChunkedInnerJoin join = new RightChunkedInnerJoin(left, 2)) {
            Page right = buildPage(factory, IntStream.range(0, size).mapToObj(p -> new int[] { p, p, p }).toArray(int[][]::new));
            try {
                Page joined = join.join(right);
                try {
                    assertThat(joined.getPositionCount(), equalTo(size));
                    assertThat(joined.getBlock(0), sameInstance(left.getBlock(0)));
                    assertThat(joined.getBlock(1), sameInstance(right.getBlock(1)));
                } finally {
                    joined.releaseBlocks();
                }
            } finally {
                right.releaseBlocks();
            }
        }
    }

    public void testRejectsDecreasingPositions() {
        BlockFactory factory = blockFactory();
        try (RightChunkedInnerJoin join = new RightChunkedInnerJoin(buildLeftHand(factory, 10), 2)) {
            assertJoined(factory, join, new int[][] { { 5, 1, 1 } }, new Object[][] { { "l5", 1, 1 } });
            Page right = buildPage(factory, new int[][] { { 4, 2, 2 } });
            try {
                IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> join.join(right));
                assertThat(e.getMessage(), containsString("positions must be non-decreasing, got [4] after [5]"));
            } finally {
                right.releaseBlocks();
            }
        }
    }

    public void testRandom() {
        testRandom(blockFactory());
    }

    public void testRandomCranky() {
        testWithCrankyBlockFactory(this::testRandom);
    }

    /**
     * Random left and right columns, with right pages pointing at random sorted left positions, duplicates included. Checked
     * against a naive nested loop.
     */
    private void testRandom(BlockFactory factory) {
        int leftSize = between(1, 1000);
        ElementType[] leftColumns = randomColumns();
        ElementType[] rightColumns = randomColumns();
        RandomPage left = randomPage(factory, leftColumns, leftSize, null);
        try (RightChunkedInnerJoin join = new RightChunkedInnerJoin(left.page, rightColumns.length)) {
            int[] selected = IntStream.range(0, between(1, 2 * leftSize)).map(i -> between(0, leftSize - 1)).sorted().toArray();
            int rightPageCount = between(1, Math.min(5, selected.length));
            int start = 0;
            for (int page = 0; page < rightPageCount; page++) {
                int end = page == rightPageCount - 1 ? selected.length : between(start + 1, selected.length - (rightPageCount - page - 1));
                int[] chunk = Arrays.copyOfRange(selected, start, end);
                RandomPage right = randomPage(factory, rightColumns, chunk.length, factory.newIntArrayVector(chunk, chunk.length));
                try {
                    Page joined = join.join(right.page);
                    try {
                        assertThat(joined.getPositionCount(), equalTo(chunk.length));
                        List<List<Object>> actualColumns = new ArrayList<>();
                        BlockTestUtils.readInto(actualColumns, joined);
                        for (int row = 0; row < chunk.length; row++) {
                            List<Object> actualRow = new ArrayList<>();
                            for (List<Object> actualColumn : actualColumns) {
                                actualRow.add(actualColumn.get(row));
                            }
                            ListMatcher matcher = ListMatcher.matchesList();
                            for (int c = 0; c < leftColumns.length; c++) {
                                matcher = matcher.item(unwrapSingletonLists(left.blocks[c].values().get(chunk[row])));
                            }
                            for (int c = 0; c < rightColumns.length; c++) {
                                matcher = matcher.item(unwrapSingletonLists(right.blocks[c].values().get(row)));
                            }
                            assertMap(actualRow, matcher);
                        }
                    } finally {
                        joined.releaseBlocks();
                    }
                } finally {
                    right.page.releaseBlocks();
                }
                start = end;
            }
            assertThat(join.noMoreRightHandPages(), equalTo(Optional.empty()));
        }
    }

    private ElementType[] randomColumns() {
        return randomArray(
            1,
            10,
            ElementType[]::new,
            () -> RandomBlock.randomElementExcluding(List.of(ElementType.AGGREGATE_METRIC_DOUBLE, ElementType.TDIGEST))
        );
    }

    private Page buildLeftHand(BlockFactory factory, int size) {
        try (BytesRefVector.Builder builder = factory.newBytesRefVectorBuilder(size)) {
            for (int i = 0; i < size; i++) {
                builder.appendBytesRef(new BytesRef(String.format(Locale.ROOT, "l%d", i)));
            }
            return new Page(builder.build().asBlock());
        }
    }

    private Page buildPage(BlockFactory factory, int[][] rows) {
        try (
            IntVector.Builder positions = factory.newIntVectorFixedBuilder(rows.length);
            IntVector.Builder r1 = factory.newIntVectorFixedBuilder(rows.length);
            IntVector.Builder r2 = factory.newIntVectorFixedBuilder(rows.length);
        ) {
            for (int[] row : rows) {
                positions.appendInt(row[0]);
                r1.appendInt(row[1]);
                r2.appendInt(row[2]);
            }
            return new Page(positions.build().asBlock(), r1.build().asBlock(), r2.build().asBlock());
        }
    }

    private void assertJoined(BlockFactory factory, RightChunkedInnerJoin join, int[][] rightRows, Object[][] expectRows) {
        Page rightHand = buildPage(factory, rightRows);
        try {
            Page joined = join.join(rightHand);
            try {
                assertThat(joined.getPositionCount(), equalTo(expectRows.length));
                List<List<Object>> actualColumns = new ArrayList<>();
                BlockTestUtils.readInto(actualColumns, joined);
                for (int r = 0; r < expectRows.length; r++) {
                    List<Object> actualRow = new ArrayList<>();
                    for (List<Object> actualColumn : actualColumns) {
                        Object v = actualColumn.get(r);
                        actualRow.add(v instanceof BytesRef b ? b.utf8ToString() : v);
                    }
                    ListMatcher rowMatcher = matchesList();
                    for (Object v : expectRows[r]) {
                        rowMatcher = rowMatcher.item(v);
                    }
                    assertMap("row " + r, actualRow, rowMatcher);
                }
            } finally {
                joined.releaseBlocks();
            }
        } finally {
            rightHand.releaseBlocks();
        }
    }

    private static Object unwrapSingletonLists(Object o) {
        if (o instanceof List<?> l && l.size() == 1) {
            return l.getFirst();
        }
        return o;
    }

    private record RandomPage(Page page, RandomBlock[] blocks) {}

    private RandomPage randomPage(BlockFactory factory, ElementType[] types, int positions, IntVector prepend) {
        int offset = prepend == null ? 0 : 1;
        RandomBlock[] randomBlocks = new RandomBlock[types.length];
        Block[] blocks = new Block[offset + types.length];
        try {
            if (prepend != null) {
                blocks[0] = prepend.asBlock();
            }
            for (int c = 0; c < types.length; c++) {
                int min = between(0, 3);
                randomBlocks[c] = RandomBlock.randomBlock(factory, types[c], positions, randomBoolean(), min, between(min, min + 3), 0, 0);
                blocks[offset + c] = randomBlocks[c].block();
            }
            Page p = new Page(blocks);
            blocks = null;
            return new RandomPage(p, randomBlocks);
        } finally {
            if (blocks != null) {
                Releasables.close(blocks);
            }
        }
    }
}
