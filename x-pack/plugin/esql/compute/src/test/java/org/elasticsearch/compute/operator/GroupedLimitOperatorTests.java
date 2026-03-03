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
import org.elasticsearch.compute.test.OperatorTestCase;
import org.elasticsearch.compute.test.operator.blocksource.SequenceLongBlockSourceOperator;
import org.hamcrest.Matcher;

import java.util.List;
import java.util.Map;
import java.util.stream.LongStream;

import static org.elasticsearch.test.MapMatcher.assertMap;
import static org.elasticsearch.test.MapMatcher.matchesMap;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
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
        return equalTo("GroupedLimitOperator[limit = 100]");
    }

    @Override
    protected Matcher<String> expectedToStringOfSimple() {
        return equalTo("GroupedLimitOperator[limit = 100, groups = 0]");
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

            Page p = new Page(blockFactory.newLongArrayVector(new long[] { 1, 2, 1 }, 3).asBlock());
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
            Page p = new Page(blockFactory.newLongArrayVector(new long[] { 1, 2, 3 }, 3).asBlock());
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
                new PositionKeyEncoder(new int[] { 0 }, List.of(ElementType.LONG)),
                blockFactory
            )
        ) {
            Page p = new Page(blockFactory.newLongArrayVector(new long[] { 1, 2, 1, 1, 2 }, 5).asBlock());
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
                new PositionKeyEncoder(new int[] { 0 }, List.of(ElementType.LONG)),
                blockFactory
            )
        ) {
            Page p1 = new Page(blockFactory.newLongArrayVector(new long[] { 1, 1, 2 }, 3).asBlock());
            op.addInput(p1);
            Page out1 = op.getOutput();
            try {
                assertThat(out1.getPositionCount(), equalTo(3));
            } finally {
                out1.releaseBlocks();
            }

            Page p2 = new Page(blockFactory.newLongArrayVector(new long[] { 1, 2, 2 }, 3).asBlock());
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
                new PositionKeyEncoder(new int[] { 0 }, List.of(ElementType.LONG)),
                blockFactory
            )
        ) {
            Page p1 = new Page(blockFactory.newLongArrayVector(new long[] { 1 }, 1).asBlock());
            op.addInput(p1);
            Page out1 = op.getOutput();
            try {
                assertThat(out1.getPositionCount(), equalTo(1));
            } finally {
                out1.releaseBlocks();
            }

            Page p2 = new Page(blockFactory.newLongArrayVector(new long[] { 1, 1, 1 }, 3).asBlock());
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
                new PositionKeyEncoder(new int[] { 0 }, List.of(ElementType.LONG)),
                blockFactory
            )
        ) {
            Page p = new Page(blockFactory.newLongArrayVector(new long[] { 1, 2, 3 }, 3).asBlock());
            op.addInput(p);
            Page output = op.getOutput();
            try {
                assertThat(output, sameInstance(p));
            } finally {
                output.releaseBlocks();
            }
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
                new PositionKeyEncoder(new int[] { 0, 1 }, List.of(ElementType.LONG, ElementType.LONG)),
                blockFactory
            )
        ) {
            Page p = new Page(
                blockFactory.newLongArrayVector(new long[] { 1, 1, 1, 2 }, 4).asBlock(),
                blockFactory.newLongArrayVector(new long[] { 10, 20, 10, 10 }, 4).asBlock()
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

    @Override
    protected void assertStatus(Map<String, Object> map, List<Page> input, List<Page> output) {
        var emittedRows = output.stream().mapToInt(Page::getPositionCount).sum();
        var inputRows = input.stream().mapToInt(Page::getPositionCount).sum();

        var mapMatcher = matchesMap().entry("limit_per_group", 100)
            .entry("group_count", greaterThanOrEqualTo(0))
            .entry("pages_processed", output.size())
            .entry("rows_received", allOf(greaterThanOrEqualTo(emittedRows), lessThanOrEqualTo(inputRows)))
            .entry("rows_emitted", emittedRows);

        assertMap(map, mapMatcher);
    }
}
