/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.test.BlockTestUtils;
import org.elasticsearch.compute.test.OperatorTestCase;
import org.elasticsearch.compute.test.operator.blocksource.SequenceLongBlockSourceOperator;
import org.hamcrest.Matcher;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.LongStream;

import static org.elasticsearch.test.MapMatcher.assertMap;
import static org.elasticsearch.test.MapMatcher.matchesMap;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;

public class GroupedLimitOperatorTests extends OperatorTestCase {

    @Override
    protected GroupedLimitOperator.Factory simple(SimpleOptions options) {
        return new GroupedLimitOperator.Factory(100, List.of(0), List.of(ElementType.LONG));
    }

    @Override
    protected SourceOperator simpleInput(BlockFactory blockFactory, int size) {
        return new SequenceLongBlockSourceOperator(blockFactory, LongStream.range(0, size));
    }

    @Override
    protected Matcher<String> expectedDescriptionOfSimple() {
        return equalTo("GroupedLimitOperator[limitPerGroup = 100]");
    }

    @Override
    protected Matcher<String> expectedToStringOfSimple() {
        return equalTo("GroupedLimitOperator[limitPerGroup = 100, groupKeys = [0], groups = 0]");
    }

    @Override
    protected void assertSimpleOutput(List<Page> input, List<Page> results) {
        int inputPositionCount = input.stream().mapToInt(Page::getPositionCount).sum();
        int outputPositionCount = results.stream().mapToInt(Page::getPositionCount).sum();
        assertThat(outputPositionCount, equalTo(inputPositionCount));
    }

    public void testStatus() {
        DriverContext ctx = driverContext();
        BlockFactory blockFactory = ctx.blockFactory();
        try (GroupedLimitOperator op = simple(SimpleOptions.DEFAULT).get(ctx)) {
            GroupedLimitOperator.Status status = op.status();
            assertThat(status.pagesProcessed(), equalTo(0));
            assertThat(status.rowsReceived(), equalTo(0L));
            assertThat(status.rowsEmitted(), equalTo(0L));

            Page p = new Page(BlockTestUtils.asBlock(blockFactory, ElementType.LONG, List.of(1L, 2L, 1L)));
            op.addInput(p);
            Page output = op.getOutput();
            try {
                assertThat(output.getPositionCount(), equalTo(3));
            } finally {
                output.releaseBlocks();
            }

            status = op.status();
            assertThat(status.pagesProcessed(), equalTo(1));
            assertThat(status.rowsReceived(), equalTo(3L));
            assertThat(status.rowsEmitted(), equalTo(3L));
        }
    }

    public void testNeedInput() {
        DriverContext ctx = driverContext();
        BlockFactory blockFactory = ctx.blockFactory();
        try (GroupedLimitOperator op = simple(SimpleOptions.DEFAULT).get(ctx)) {
            assertTrue(op.needsInput());
            Page p = new Page(BlockTestUtils.asBlock(blockFactory, ElementType.LONG, List.of(1L, 2L, 3L)));
            op.addInput(p);
            assertFalse(op.needsInput());
            op.getOutput().releaseBlocks();
            assertTrue(op.needsInput());
            op.finish();
            assertFalse(op.needsInput());
        }
    }

    /**
     * With limit=2, group 1 appears 3 times and group 2 appears 2 times.
     * Only the first 2 rows of group 1 should be retained.
     */
    public void testLimitPerGroupSinglePage() {
        DriverContext ctx = driverContext();
        BlockFactory blockFactory = ctx.blockFactory();
        try (
            GroupedLimitOperator op = new GroupedLimitOperator(
                2,
                groupKeyEncoder(blockFactory, new int[] { 0 }, List.of(ElementType.LONG)),
                blockFactory
            )
        ) {
            Page p = new Page(BlockTestUtils.asBlock(blockFactory, ElementType.LONG, List.of(1L, 2L, 1L, 1L, 2L)));
            op.addInput(p);
            Page output = op.getOutput();
            try {
                assertThat(output.getPositionCount(), equalTo(4));
                LongBlock resultBlock = output.getBlock(0);
                assertThat(resultBlock.getLong(0), equalTo(1L));
                assertThat(resultBlock.getLong(1), equalTo(2L));
                assertThat(resultBlock.getLong(2), equalTo(1L));
                assertThat(resultBlock.getLong(3), equalTo(2L));
            } finally {
                output.releaseBlocks();
            }
        }
    }

    /**
     * Group limits are enforced across page boundaries: a group saturated
     * in the first page rejects further rows in subsequent pages.
     */
    public void testLimitPerGroupAcrossPages() {
        DriverContext ctx = driverContext();
        BlockFactory blockFactory = ctx.blockFactory();
        try (
            GroupedLimitOperator op = new GroupedLimitOperator(
                2,
                groupKeyEncoder(blockFactory, new int[] { 0 }, List.of(ElementType.LONG)),
                blockFactory
            )
        ) {
            Page p1 = new Page(BlockTestUtils.asBlock(blockFactory, ElementType.LONG, List.of(1L, 1L, 2L)));
            op.addInput(p1);
            Page out1 = op.getOutput();
            try {
                assertThat(out1.getPositionCount(), equalTo(3));
            } finally {
                out1.releaseBlocks();
            }

            Page p2 = new Page(BlockTestUtils.asBlock(blockFactory, ElementType.LONG, List.of(1L, 2L, 2L)));
            op.addInput(p2);
            Page out2 = op.getOutput();
            try {
                assertThat(out2.getPositionCount(), equalTo(1));
                LongBlock b = out2.getBlock(0);
                assertThat(b.getLong(0), equalTo(2L));
            } finally {
                out2.releaseBlocks();
            }
        }
    }

    /**
     * When every row in a page belongs to an already-saturated group,
     * the entire page is dropped and getOutput returns null.
     */
    public void testEntirePageFiltered() {
        DriverContext ctx = driverContext();
        BlockFactory blockFactory = ctx.blockFactory();
        try (
            GroupedLimitOperator op = new GroupedLimitOperator(
                1,
                groupKeyEncoder(blockFactory, new int[] { 0 }, List.of(ElementType.LONG)),
                blockFactory
            )
        ) {
            Page p1 = new Page(BlockTestUtils.asBlock(blockFactory, ElementType.LONG, List.of(1L)));
            op.addInput(p1);
            Page out1 = op.getOutput();
            try {
                assertThat(out1.getPositionCount(), equalTo(1));
            } finally {
                out1.releaseBlocks();
            }

            Page p2 = new Page(BlockTestUtils.asBlock(blockFactory, ElementType.LONG, List.of(1L, 1L, 1L)));
            op.addInput(p2);
            assertThat(op.getOutput(), nullValue());
        }
    }

    public void testPagePassesThroughWhenAllAccepted() {
        DriverContext ctx = driverContext();
        BlockFactory blockFactory = ctx.blockFactory();
        try (
            GroupedLimitOperator op = new GroupedLimitOperator(
                10,
                groupKeyEncoder(blockFactory, new int[] { 0 }, List.of(ElementType.LONG)),
                blockFactory
            );
            Page p = new Page(BlockTestUtils.asBlock(blockFactory, ElementType.LONG, List.of(1L, 2L, 3L)));
        ) {
            op.addInput(p.shallowCopy());
            try (Page output = op.getOutput()) {
                for (int b = 0; b < output.getBlockCount(); b++) {
                    assertThat(output.getBlock(b), sameInstance(p.getBlock(b)));
                }
            }
        }
    }

    /**
     * With limitPerGroup=0 every row is dropped regardless of the group key.
     * No keys should be tracked and status must reflect zero emitted rows.
     */
    public void testLimitPerGroupZero() {
        DriverContext ctx = driverContext();
        BlockFactory blockFactory = ctx.blockFactory();
        try (
            GroupedLimitOperator op = new GroupedLimitOperator(
                0,
                groupKeyEncoder(blockFactory, new int[] { 0 }, List.of(ElementType.LONG)),
                blockFactory
            )
        ) {
            Page p1 = new Page(BlockTestUtils.asBlock(blockFactory, ElementType.LONG, List.of(1L, 2L, 3L)));
            op.addInput(p1);
            assertThat(op.getOutput(), nullValue());

            Page p2 = new Page(BlockTestUtils.asBlock(blockFactory, ElementType.LONG, List.of(1L, 1L, 2L)));
            op.addInput(p2);
            assertThat(op.getOutput(), nullValue());

            GroupedLimitOperator.Status status = op.status();
            assertThat(status.rowsReceived(), equalTo(6L));
            assertThat(status.rowsEmitted(), equalTo(0L));
            assertThat(status.pagesProcessed(), equalTo(0));
            assertThat(status.groupCount(), equalTo(0));
        }
    }

    /**
     * Groups are determined by composite keys across multiple channels.
     * (1,10), (1,20), and (2,10) are three distinct groups.
     */
    public void testMultipleGroupChannels() {
        DriverContext ctx = driverContext();
        BlockFactory blockFactory = ctx.blockFactory();
        try (
            GroupedLimitOperator op = new GroupedLimitOperator(
                1,
                groupKeyEncoder(blockFactory, new int[] { 0, 1 }, List.of(ElementType.LONG, ElementType.LONG)),
                blockFactory
            )
        ) {
            Page p = new Page(
                BlockTestUtils.asBlock(blockFactory, ElementType.LONG, List.of(1L, 1L, 1L, 2L)),
                BlockTestUtils.asBlock(blockFactory, ElementType.LONG, List.of(10L, 20L, 10L, 10L))
            );
            op.addInput(p);
            Page output = op.getOutput();
            try {
                assertThat(output.getPositionCount(), equalTo(3));
                LongBlock col0 = output.getBlock(0);
                LongBlock col1 = output.getBlock(1);
                assertThat(col0.getLong(0), equalTo(1L));
                assertThat(col1.getLong(0), equalTo(10L));
                assertThat(col0.getLong(1), equalTo(1L));
                assertThat(col1.getLong(1), equalTo(20L));
                assertThat(col0.getLong(2), equalTo(2L));
                assertThat(col1.getLong(2), equalTo(10L));
            } finally {
                output.releaseBlocks();
            }
        }
    }

    /**
     * A multivalue {@code [1,2]} and a single value {@code 1} are different groups.
     */
    public void testMultivalueAndSingleValueAreDifferentGroups() {
        assertDistinctGroups(2, List.of(1L, 2L), 1L);
    }

    /**
     * A null position and a multivalue {@code [1,2]} are different groups.
     */
    public void testNullAndMultivalueAreDifferentGroups() {
        assertDistinctGroups(2, null, List.of(1L, 2L));
    }

    /**
     * Two identical multivalues {@code [1,2]} belong to the same group.
     */
    public void testIdenticalMultivaluesAreSameGroup() {
        assertDistinctGroups(1, List.of(1L, 2L), List.of(1L, 2L));
    }

    /**
     * {@code [1,2]} and {@code [2,1]} are different groups because
     * multivalues use order-sensitive list semantics.
     */
    public void testMultivaluesWithDifferentOrderAreDifferentGroups() {
        assertDistinctGroups(2, List.of(1L, 2L), List.of(2L, 1L));
    }

    /**
     * A single-element multivalue {@code [1]} and a scalar {@code 1} are the
     * same group because the block representation is identical (valueCount=1).
     */
    public void testSingleElementMultivalueAndScalarAreSameGroup() {
        assertDistinctGroups(1, 1L, 1L);
    }

    /**
     * A multivalue with duplicates {@code [1,1]} and a single value {@code 1}
     * are different groups because the value count differs.
     */
    public void testMultivalueWithDuplicatesAndSingleValueAreDifferentGroups() {
        assertDistinctGroups(2, List.of(1L, 1L), 1L);
    }

    /**
     * Two identical multivalues with duplicates {@code [1,1]} are the same group.
     */
    public void testIdenticalMultivaluesWithDuplicatesAreSameGroup() {
        assertDistinctGroups(1, List.of(1L, 1L), List.of(1L, 1L));
    }

    /**
     * Multivalues of different lengths sharing a prefix are different groups:
     * {@code [1,2,3]} vs {@code [1,2]}.
     */
    public void testDifferentLengthMultivaluesAreDifferentGroups() {
        assertDistinctGroups(2, List.of(1L, 2L, 3L), List.of(1L, 2L));
    }

    /**
     * Multivalue semantics apply within composite keys: {@code ([1,2], 10)},
     * {@code ([1,2], 20)}, {@code ([2,1], 10)} and {@code ([2,1], 10)} are three distinct groups.
     */
    public void testMultivalueInCompositeKey() {
        DriverContext ctx = driverContext();
        BlockFactory blockFactory = ctx.blockFactory();
        Page p = new Page(
            BlockTestUtils.asBlock(
                blockFactory,
                ElementType.LONG,
                List.of(List.of(1L, 2L), List.of(1L, 2L), List.of(2L, 1L), List.of(2L, 1L))
            ),
            BlockTestUtils.asBlock(blockFactory, ElementType.LONG, List.of(10L, 20L, 10L, 10L))
        );
        try (
            GroupedLimitOperator op = new GroupedLimitOperator(
                1,
                groupKeyEncoder(blockFactory, new int[] { 0, 1 }, List.of(ElementType.LONG, ElementType.LONG)),
                blockFactory
            )
        ) {
            op.addInput(p);
            Page output = op.getOutput();
            try {
                assertThat(output.getPositionCount(), equalTo(3));
            } finally {
                output.releaseBlocks();
            }
        }
    }

    /**
     * Two null positions belong to the same group.
     */
    public void testTwoNullsAreSameGroup() {
        assertDistinctGroups(1, null, null);
    }

    /**
     * Feeds the given values through a {@code limit=1} single-channel operator
     * and asserts the number of output positions (i.e. distinct groups).
     * Each entry can be: {@code null} for a null position, a {@code Long} scalar,
     * or a {@code List<Long>} for a multivalue.
     */
    private void assertDistinctGroups(int expectedGroups, Object... values) {
        DriverContext ctx = driverContext();
        BlockFactory blockFactory = ctx.blockFactory();
        try (
            GroupedLimitOperator op = new GroupedLimitOperator(
                1,
                groupKeyEncoder(blockFactory, new int[] { 0 }, List.of(ElementType.LONG)),
                blockFactory
            )
        ) {
            op.addInput(new Page(BlockTestUtils.asBlock(blockFactory, ElementType.LONG, Arrays.asList(values))));
            Page output = op.getOutput();
            try {
                assertThat(output.getPositionCount(), equalTo(expectedGroups));
            } finally {
                output.releaseBlocks();
            }
        }
    }

    private static GroupKeyEncoder groupKeyEncoder(BlockFactory blockFactory, int[] groupChannels, List<ElementType> elementTypes) {
        return new GroupKeyEncoder(groupChannels, elementTypes, new BreakingBytesRefBuilder(blockFactory.breaker(), "group-key-encoder"));
    }

    @Override
    protected void assertStatus(Map<String, Object> map, List<Page> input, List<Page> output) {
        var emittedRows = output.stream().mapToInt(Page::getPositionCount).sum();
        var inputRows = input.stream().mapToInt(Page::getPositionCount).sum();

        var mapMatcher = matchesMap().entry("limit_per_group", 100)
            .entry("group_count", greaterThanOrEqualTo(0))
            .entry("pages_processed", output.size())
            .entry("rows_received", allOf(greaterThanOrEqualTo(emittedRows), lessThanOrEqualTo(inputRows)))
            .entry("rows_emitted", emittedRows)
            .entry("ram_bytes_used", greaterThanOrEqualTo(0))
            .entry("ram_used", notNullValue());

        assertMap(map, mapMatcher);
    }
}
