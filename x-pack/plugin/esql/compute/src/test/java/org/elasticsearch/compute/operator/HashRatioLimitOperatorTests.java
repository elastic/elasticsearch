/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.elasticsearch.common.bytes.PagedBytesBuilder;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.test.BlockTestUtils;
import org.elasticsearch.compute.test.OperatorTestCase;
import org.elasticsearch.compute.test.operator.blocksource.SequenceLongBlockSourceOperator;
import org.hamcrest.Matcher;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.LongStream;

import static org.elasticsearch.test.MapMatcher.assertMap;
import static org.elasticsearch.test.MapMatcher.matchesMap;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.nullValue;

public class HashRatioLimitOperatorTests extends OperatorTestCase {

    @Override
    protected HashRatioLimitOperator.Factory simple(SimpleOptions options) {
        return new HashRatioLimitOperator.Factory(0.5, List.of(0), List.of(ElementType.LONG));
    }

    @Override
    protected SourceOperator simpleInput(BlockFactory blockFactory, int size) {
        return new SequenceLongBlockSourceOperator(blockFactory, LongStream.range(0, size));
    }

    @Override
    protected Matcher<String> expectedDescriptionOfSimple() {
        return equalTo("HashRatioLimitOperator[ratio=0.5, keyChannels=[0]]");
    }

    @Override
    protected Matcher<String> expectedToStringOfSimple() {
        return equalTo("HashRatioLimitOperator[ratio=0.5, keyChannels=[0]]");
    }

    @Override
    protected void assertSimpleOutput(List<Page> input, List<Page> results) {
        Set<Long> inputIds = idsOf(input);
        Set<Long> outputIds = idsOf(results);
        // The operator only filters: every emitted id was an input id.
        assertThat(outputIds.stream().allMatch(inputIds::contains), equalTo(true));
        assertThat(outputIds.size(), lessThanOrEqualTo(inputIds.size()));
    }

    /**
     * ratio=0.0 drops every row; getOutput returns null for every page.
     */
    public void testRatioZeroDropsAll() {
        DriverContext ctx = driverContext();
        BlockFactory blockFactory = ctx.blockFactory();
        try (HashRatioLimitOperator op = op(0.0, blockFactory, new int[] { 0 })) {
            Page p = new Page(BlockTestUtils.asBlock(blockFactory, ElementType.LONG, List.of(1L, 2L, 3L)));
            op.addInput(p);
            assertThat(op.getOutput(), nullValue());

            AbstractPageMappingOperator.Status status = op.status();
            assertThat(status.rowsReceived(), equalTo(3L));
            assertThat(status.rowsEmitted(), equalTo(0L));
        }
    }

    /**
     * ratio=1.0 keeps every row; the page passes through without copying blocks.
     */
    public void testRatioOneKeepsAll() {
        assertKeepsExactly(1.0, List.of(1L, 2L, 3L, 4L));
    }

    /**
     * Ratios above one keep every row.
     */
    public void testRatioGreaterThanOneKeepsAll() {
        assertKeepsExactly(2.0, List.of(1L, 2L, 3L));
    }

    /**
     * ratio=-1.0 keeps everything via the inverted branch.
     */
    public void testNegativeOneKeepsAll() {
        assertKeepsExactly(-1.0, List.of(1L, 2L, 3L));
    }

    /**
     * NaN keeps nothing since both predicate comparisons are false.
     */
    public void testNaNKeepsNothing() {
        DriverContext ctx = driverContext();
        BlockFactory blockFactory = ctx.blockFactory();
        try (HashRatioLimitOperator op = op(Double.NaN, blockFactory, new int[] { 0 })) {
            op.addInput(new Page(BlockTestUtils.asBlock(blockFactory, ElementType.LONG, List.of(1L, 2L, 3L))));
            assertThat(op.getOutput(), nullValue());
        }
    }

    /**
     * A negative ratio keeps exactly the complement of the positive ratio: for r in (0, 1) every
     * row is kept by exactly one of {@code r} and {@code -r}, without coupling the test to
     * specific hash values.
     */
    public void testNegativeRatioKeepsComplement() {
        List<Long> ids = LongStream.range(0, 50).boxed().toList();
        Set<Long> positive = keptIds(0.5, ids);
        Set<Long> negative = keptIds(-0.5, ids);
        Set<Long> all = new HashSet<>(ids);

        Set<Long> union = new HashSet<>(positive);
        union.addAll(negative);
        assertThat(union, equalTo(all));

        Set<Long> intersection = new HashSet<>(positive);
        intersection.retainAll(negative);
        assertThat(intersection.isEmpty(), equalTo(true));

        // And the split is non-trivial: neither side keeps everything.
        assertThat(positive.isEmpty() == false && positive.size() < all.size(), equalTo(true));
    }

    /**
     * The kept subset depends only on the key, so the same values are kept or dropped
     * identically on every page, however pages are partitioned or ordered.
     */
    public void testSameKeyDecidedIdenticallyAcrossPages() {
        DriverContext ctx = driverContext();
        BlockFactory blockFactory = ctx.blockFactory();
        try (HashRatioLimitOperator op = op(0.5, blockFactory, new int[] { 0 })) {
            for (long id : List.of(11L, 22L, 33L, 44L)) {
                op.addInput(new Page(BlockTestUtils.asBlock(blockFactory, ElementType.LONG, List.of(id))));
                Page out1 = op.getOutput();
                boolean kept1 = out1 != null;
                if (out1 != null) {
                    out1.releaseBlocks();
                }
                op.addInput(new Page(BlockTestUtils.asBlock(blockFactory, ElementType.LONG, List.of(id))));
                Page out2 = op.getOutput();
                boolean kept2 = out2 != null;
                if (out2 != null) {
                    out2.releaseBlocks();
                }
                assertThat("id [" + id + "] decided inconsistently", kept1, equalTo(kept2));
            }
        }
    }

    /**
     * Over many distinct keys roughly the requested fraction is kept.
     */
    public void testRoughlyHalfKept() {
        List<Long> ids = LongStream.range(0, 2000).boxed().toList();
        int kept = keptIds(0.5, ids).size();
        assertThat(kept, greaterThanOrEqualTo(800));
        assertThat(kept, lessThanOrEqualTo(1200));
    }

    /**
     * Multiple key channels combine into one identity: the decision is deterministic per key
     * tuple and a proper subset is kept.
     */
    public void testMultipleKeyChannels() {
        DriverContext ctx = driverContext();
        BlockFactory blockFactory = ctx.blockFactory();
        List<Long> first = LongStream.range(0, 100).boxed().toList();
        List<Long> second = LongStream.range(0, 100).map(i -> i % 7).boxed().toList();
        Set<List<Long>> firstRun = keptTuples(0.5, blockFactory, first, second);
        Set<List<Long>> secondRun = keptTuples(0.5, blockFactory, first, second);
        assertThat(secondRun, equalTo(firstRun));
        assertThat(firstRun.isEmpty(), equalTo(false));
        assertThat(firstRun.size(), lessThanOrEqualTo(100));
    }

    /**
     * Predicate edge cases: out-of-range ratios keep everything, NaN keeps nothing. The operator
     * itself is total; callers needing stricter validation reject such ratios before planning.
     */
    public void testKeepPredicateEdges() {
        int hash = 0x12345678;
        assertThat(HashRatioLimitOperator.keep(Double.NaN, hash), equalTo(false));
        assertThat(HashRatioLimitOperator.keep(Double.POSITIVE_INFINITY, hash), equalTo(true));
        assertThat(HashRatioLimitOperator.keep(Double.NEGATIVE_INFINITY, hash), equalTo(true));
        assertThat(HashRatioLimitOperator.keep(-0.0, hash), equalTo(false));
        assertThat(HashRatioLimitOperator.keep(2.0, hash), equalTo(true));
        assertThat(HashRatioLimitOperator.keep(-2.0, hash), equalTo(true));
        assertThat(HashRatioLimitOperator.keep(0.0, hash), equalTo(false));
        assertThat(HashRatioLimitOperator.keep(1.0, hash), equalTo(true));
    }

    public void testStatus() {
        DriverContext ctx = driverContext();
        BlockFactory blockFactory = ctx.blockFactory();
        try (HashRatioLimitOperator op = simple(SimpleOptions.DEFAULT).get(ctx)) {
            AbstractPageMappingOperator.Status status = op.status();
            assertThat(status.pagesProcessed(), equalTo(0));
            assertThat(status.rowsReceived(), equalTo(0L));
            assertThat(status.rowsEmitted(), equalTo(0L));

            Page p = new Page(BlockTestUtils.asBlock(blockFactory, ElementType.LONG, List.of(1L, 2L)));
            op.addInput(p);
            Page output = op.getOutput();
            int emitted;
            try {
                emitted = output == null ? 0 : output.getPositionCount();
            } finally {
                if (output != null) {
                    output.releaseBlocks();
                }
            }

            status = op.status();
            assertThat(status.pagesProcessed(), equalTo(1));
            assertThat(status.rowsReceived(), equalTo(2L));
            assertThat(status.rowsEmitted(), equalTo((long) emitted));
        }
    }

    /**
     * Pages with an extra non-key channel pass through with the channel preserved.
     */
    public void testNonKeyChannelsPreserved() {
        DriverContext ctx = driverContext();
        BlockFactory blockFactory = ctx.blockFactory();
        try (HashRatioLimitOperator op = op(0.5, blockFactory, new int[] { 0 })) {
            Page p = new Page(
                BlockTestUtils.asBlock(blockFactory, ElementType.LONG, List.of(1L, 2L, 3L, 4L)),
                BlockTestUtils.asBlock(blockFactory, ElementType.LONG, List.of(10L, 20L, 30L, 40L))
            );
            op.addInput(p);
            Page out = op.getOutput();
            try {
                if (out != null) {
                    assertThat(out.getBlockCount(), equalTo(2));
                    assertThat(idsOf(List.of(out)).size(), lessThanOrEqualTo(4));
                }
            } finally {
                if (out != null) {
                    out.releaseBlocks();
                }
            }
        }
    }

    @Override
    protected void assertStatus(Map<String, Object> map, List<Page> input, List<Page> output) {
        var emittedRows = output.stream().mapToInt(Page::getPositionCount).sum();
        var inputRows = input.stream().mapToInt(Page::getPositionCount).sum();

        assertMap(
            map,
            matchesMap().entry("process_nanos", greaterThanOrEqualTo(0))
                .entry("pages_processed", output.size())
                .entry("rows_received", inputRows)
                .entry("rows_emitted", emittedRows)
        );
    }

    private void assertKeepsExactly(double ratio, List<Long> ids) {
        assertThat(keptIds(ratio, ids), equalTo(new HashSet<>(ids)));
    }

    private Set<Long> keptIds(double ratio, List<Long> ids) {
        DriverContext ctx = driverContext();
        BlockFactory blockFactory = ctx.blockFactory();
        try (HashRatioLimitOperator op = op(ratio, blockFactory, new int[] { 0 })) {
            op.addInput(new Page(BlockTestUtils.asBlock(blockFactory, ElementType.LONG, ids.stream().map(o -> (Object) o).toList())));
            Page out = op.getOutput();
            try {
                if (out == null) {
                    return Set.of();
                }
                return idsOf(List.of(out));
            } finally {
                if (out != null) {
                    out.releaseBlocks();
                }
            }
        }
    }

    private Set<List<Long>> keptTuples(double ratio, BlockFactory blockFactory, List<Long> first, List<Long> second) {
        try (HashRatioLimitOperator op = op(ratio, blockFactory, new int[] { 0, 1 })) {
            op.addInput(
                new Page(
                    BlockTestUtils.asBlock(blockFactory, ElementType.LONG, first.stream().map(o -> (Object) o).toList()),
                    BlockTestUtils.asBlock(blockFactory, ElementType.LONG, second.stream().map(o -> (Object) o).toList())
                )
            );
            Page out = op.getOutput();
            try {
                Set<List<Long>> kept = new HashSet<>();
                if (out != null) {
                    List<Object> col0 = new ArrayList<>();
                    List<Object> col1 = new ArrayList<>();
                    BlockTestUtils.readInto(col0, out.getBlock(0));
                    BlockTestUtils.readInto(col1, out.getBlock(1));
                    for (int i = 0; i < col0.size(); i++) {
                        kept.add(List.of((Long) col0.get(i), (Long) col1.get(i)));
                    }
                }
                return kept;
            } finally {
                if (out != null) {
                    out.releaseBlocks();
                }
            }
        }
    }

    private static Set<Long> idsOf(List<Page> pages) {
        Set<Long> ids = new HashSet<>();
        for (Page page : pages) {
            Block block = page.getBlock(0);
            List<Object> values = new ArrayList<>();
            BlockTestUtils.readInto(values, block);
            for (Object value : values) {
                ids.add((Long) value);
            }
        }
        return ids;
    }

    private static HashRatioLimitOperator op(double ratio, BlockFactory blockFactory, int[] keyChannels) {
        List<ElementType> types = new java.util.ArrayList<>();
        for (int i = 0; i < keyChannels.length; i++) {
            types.add(ElementType.LONG);
        }
        return new HashRatioLimitOperator(
            ratio,
            new GroupKeyEncoder(
                keyChannels,
                types,
                new PagedBytesBuilder(blockFactory.bigArrays().recycler(), blockFactory.breaker(), "group-key-encoder", 64)
            )
        );
    }
}
