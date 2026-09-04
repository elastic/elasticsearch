/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.elasticsearch.common.bytes.PagedBytesBuilder;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.test.BlockTestUtils;
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
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class GroupedRatioLimitOperatorTests extends OperatorTestCase {

    @Override
    protected GroupedRatioLimitOperator.Factory simple(SimpleOptions options) {
        return new GroupedRatioLimitOperator.Factory(0.5, List.of(0), List.of(ElementType.LONG));
    }

    @Override
    protected SourceOperator simpleInput(BlockFactory blockFactory, int size) {
        return new SequenceLongBlockSourceOperator(blockFactory, LongStream.range(0, size));
    }

    @Override
    protected Matcher<String> expectedDescriptionOfSimple() {
        return equalTo("GroupedRatioLimitOperator[ratio=0.5]");
    }

    @Override
    protected Matcher<String> expectedToStringOfSimple() {
        return equalTo("GroupedRatioLimitOperator[ratio=0.5, groupKeys=[0], groups=0]");
    }

    @Override
    protected void assertSimpleOutput(List<Page> input, List<Page> results) {
        long inputRows = input.stream().mapToLong(Page::getPositionCount).sum();
        long outputRows = results.stream().mapToLong(Page::getPositionCount).sum();
        // With ratio=0.5 and unique keys (sequence), ceil(0.5 * 1) = 1 per group, so all rows pass.
        assertThat(outputRows, equalTo(inputRows));
    }

    /**
     * With ratio=0.5, group 1 appears 4 times. ceil(0.5 * 4) = 2 rows should be kept.
     * Group 2 appears 2 times. ceil(0.5 * 2) = 1 row kept.
     */
    public void testRatioSamplingPerGroup() {
        DriverContext ctx = driverContext();
        BlockFactory blockFactory = ctx.blockFactory();
        try (GroupedRatioLimitOperator op = op(0.5, blockFactory, new int[] { 0 })) {
            Page p = new Page(BlockTestUtils.asBlock(blockFactory, ElementType.LONG, List.of(1L, 1L, 1L, 1L, 2L, 2L)));
            op.addInput(p);
            Page out = op.getOutput();
            try {
                assertThat(out.getPositionCount(), equalTo(3));
                LongBlock b = out.getBlock(0);
                assertThat(b.getLong(0), equalTo(1L));
                assertThat(b.getLong(1), equalTo(1L));
                assertThat(b.getLong(2), equalTo(2L));
            } finally {
                out.releaseBlocks();
            }
        }
    }

    /**
     * Bresenham counters persist across pages: a group half-sampled on page 1
     * continues the same error accumulation on page 2.
     */
    public void testSamplingAcrossPages() {
        DriverContext ctx = driverContext();
        BlockFactory blockFactory = ctx.blockFactory();
        try (GroupedRatioLimitOperator op = op(0.5, blockFactory, new int[] { 0 })) {
            Page p1 = new Page(BlockTestUtils.asBlock(blockFactory, ElementType.LONG, List.of(1L, 1L)));
            op.addInput(p1);
            Page out1 = op.getOutput();
            try {
                // ceil(0.5*1)=1, ceil(0.5*2)=1 -- first row accepted, second rejected
                assertThat(out1.getPositionCount(), equalTo(1));
            } finally {
                out1.releaseBlocks();
            }

            Page p2 = new Page(BlockTestUtils.asBlock(blockFactory, ElementType.LONG, List.of(1L, 1L)));
            op.addInput(p2);
            Page out2 = op.getOutput();
            try {
                // total=2+2=4, accepted so far=1. ceil(0.5*3)=2>1 -> accept; ceil(0.5*4)=2==2 -> reject
                assertThat(out2.getPositionCount(), equalTo(1));
            } finally {
                out2.releaseBlocks();
            }
        }
    }

    /**
     * ratio=0.0 drops every row; getOutput returns null for every page.
     */
    public void testRatioZeroDropsAll() {
        DriverContext ctx = driverContext();
        BlockFactory blockFactory = ctx.blockFactory();
        try (GroupedRatioLimitOperator op = op(0.0, blockFactory, new int[] { 0 })) {
            Page p = new Page(BlockTestUtils.asBlock(blockFactory, ElementType.LONG, List.of(1L, 2L, 3L)));
            op.addInput(p);
            assertThat(op.getOutput(), nullValue());

            GroupedRatioLimitOperator.Status status = op.status();
            assertThat(status.rowsReceived(), equalTo(3L));
            assertThat(status.rowsEmitted(), equalTo(0L));
        }
    }

    /**
     * ratio=1.0 keeps every row; the page passes through without copying blocks.
     */
    public void testRatioOneKeepsAll() {
        DriverContext ctx = driverContext();
        BlockFactory blockFactory = ctx.blockFactory();
        try (GroupedRatioLimitOperator op = op(1.0, blockFactory, new int[] { 0 })) {
            Page p = new Page(BlockTestUtils.asBlock(blockFactory, ElementType.LONG, List.of(1L, 2L, 1L, 3L)));
            op.addInput(p);
            Page out = op.getOutput();
            try {
                assertThat(out.getPositionCount(), equalTo(4));
            } finally {
                out.releaseBlocks();
            }
        }
    }

    /**
     * With ratio=1/3, a group of 3 rows should keep exactly ceil(1/3 * 3)=1 row.
     */
    public void testExactCeilComputation() {
        DriverContext ctx = driverContext();
        BlockFactory blockFactory = ctx.blockFactory();
        try (GroupedRatioLimitOperator op = op(1.0 / 3.0, blockFactory, new int[] { 0 })) {
            Page p = new Page(BlockTestUtils.asBlock(blockFactory, ElementType.LONG, List.of(1L, 1L, 1L)));
            op.addInput(p);
            Page out = op.getOutput();
            try {
                assertThat(out.getPositionCount(), equalTo(1));
            } finally {
                out.releaseBlocks();
            }
        }
    }

    public void testStatus() {
        DriverContext ctx = driverContext();
        BlockFactory blockFactory = ctx.blockFactory();
        try (GroupedRatioLimitOperator op = simple(SimpleOptions.DEFAULT).get(ctx)) {
            GroupedRatioLimitOperator.Status status = op.status();
            assertThat(status.pagesProcessed(), equalTo(0));
            assertThat(status.rowsReceived(), equalTo(0L));
            assertThat(status.rowsEmitted(), equalTo(0L));

            Page p = new Page(BlockTestUtils.asBlock(blockFactory, ElementType.LONG, List.of(1L, 2L)));
            op.addInput(p);
            Page output = op.getOutput();
            try {
                assertThat(output.getPositionCount(), greaterThanOrEqualTo(1));
            } finally {
                output.releaseBlocks();
            }

            status = op.status();
            assertThat(status.pagesProcessed(), equalTo(1));
            assertThat(status.rowsReceived(), equalTo(2L));
        }
    }

    @Override
    protected void assertStatus(Map<String, Object> map, List<Page> input, List<Page> output) {
        var emittedRows = output.stream().mapToInt(Page::getPositionCount).sum();
        var inputRows = input.stream().mapToInt(Page::getPositionCount).sum();

        assertMap(
            map,
            matchesMap().entry("ratio", 0.5)
                .entry("group_count", greaterThanOrEqualTo(0))
                .entry("pages_processed", output.size())
                .entry("rows_received", allOf(greaterThanOrEqualTo(emittedRows), lessThanOrEqualTo(inputRows)))
                .entry("rows_emitted", emittedRows)
                .entry("ram_bytes_used", greaterThanOrEqualTo(0))
                .entry("ram_used", notNullValue())
        );
    }

    private static GroupedRatioLimitOperator op(double ratio, BlockFactory blockFactory, int[] groupChannels) {
        List<ElementType> types = new java.util.ArrayList<>();
        for (int i = 0; i < groupChannels.length; i++) {
            types.add(ElementType.LONG);
        }
        return new GroupedRatioLimitOperator(
            ratio,
            new GroupKeyEncoder(
                groupChannels,
                types,
                new PagedBytesBuilder(blockFactory.bigArrays().recycler(), blockFactory.breaker(), "group-key-encoder", 64)
            ),
            blockFactory
        );
    }
}
