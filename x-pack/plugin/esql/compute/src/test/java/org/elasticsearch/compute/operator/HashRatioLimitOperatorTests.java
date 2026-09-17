/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.test.BlockTestUtils;
import org.elasticsearch.compute.test.CannedSourceOperator;
import org.elasticsearch.compute.test.OperatorTestCase;
import org.hamcrest.Matcher;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.test.MapMatcher.assertMap;
import static org.elasticsearch.test.MapMatcher.matchesMap;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.nullValue;

public class HashRatioLimitOperatorTests extends OperatorTestCase {

    @Override
    protected HashRatioLimitOperator.Factory simple(SimpleOptions options) {
        return new HashRatioLimitOperator.Factory(0.5, 0);
    }

    @Override
    protected SourceOperator simpleInput(BlockFactory blockFactory, int size) {
        List<Page> pages = new ArrayList<>();
        int remaining = size;
        while (remaining > 0) {
            int count = Math.min(remaining, 100);
            List<Object> ids = new ArrayList<>(count);
            for (int i = 0; i < count; i++) {
                ids.add(new BytesRef(randomAlphaOfLength(8)));
            }
            pages.add(new Page(BlockTestUtils.asBlock(blockFactory, ElementType.BYTES_REF, ids)));
            remaining -= count;
        }
        return new CannedSourceOperator(pages.iterator());
    }

    @Override
    protected Matcher<String> expectedDescriptionOfSimple() {
        return equalTo("HashRatioLimitOperator[ratio=0.5, seriesChannel=0]");
    }

    @Override
    protected Matcher<String> expectedToStringOfSimple() {
        return equalTo("HashRatioLimitOperator[ratio=0.5, seriesChannel=0]");
    }

    @Override
    protected void assertSimpleOutput(List<Page> input, List<Page> results) {
        Set<BytesRef> inputIds = idsOf(input);
        Set<BytesRef> outputIds = idsOf(results);
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
        try (HashRatioLimitOperator op = new HashRatioLimitOperator(0.0, 0)) {
            Page p = page(blockFactory, "a", "b", "c");
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
        assertKeepsExactly(1.0, ids("a", "b", "c", "d"));
    }

    /**
     * Ratios above one keep every row, matching Prometheus clamping.
     */
    public void testRatioGreaterThanOneKeepsAll() {
        assertKeepsExactly(2.0, ids("a", "b", "c"));
    }

    /**
     * ratio=-1.0 keeps everything via the complement branch, matching Prometheus clamping.
     */
    public void testNegativeOneKeepsAll() {
        assertKeepsExactly(-1.0, ids("a", "b", "c"));
    }

    /**
     * NaN keeps nothing since both predicate comparisons are false, matching Prometheus.
     */
    public void testNaNKeepsNothing() {
        DriverContext ctx = driverContext();
        BlockFactory blockFactory = ctx.blockFactory();
        try (HashRatioLimitOperator op = new HashRatioLimitOperator(Double.NaN, 0)) {
            op.addInput(page(blockFactory, "a", "b", "c"));
            assertThat(op.getOutput(), nullValue());
        }
    }

    /**
     * A negative ratio keeps exactly the complement of the positive ratio: for r in (0, 1) every
     * series is kept by exactly one of {@code r} and {@code -r}. This mirrors the Prometheus
     * negative-complement rule without coupling the test to specific hash values.
     */
    public void testNegativeRatioKeepsComplement() {
        List<String> names = new ArrayList<>();
        for (int i = 0; i < 50; i++) {
            names.add("series-" + i);
        }
        Set<BytesRef> positive = keptIds(0.5, names);
        Set<BytesRef> negative = keptIds(-0.5, names);
        Set<BytesRef> all = new HashSet<>();
        names.forEach(n -> all.add(new BytesRef(n)));

        Set<BytesRef> union = new HashSet<>(positive);
        union.addAll(negative);
        assertThat(union, equalTo(all));

        Set<BytesRef> intersection = new HashSet<>(positive);
        intersection.retainAll(negative);
        assertThat(intersection.isEmpty(), equalTo(true));

        // And the split is non-trivial: neither side keeps everything.
        assertThat(positive.isEmpty() == false && positive.size() < all.size(), equalTo(true));
    }

    /**
     * The kept subset depends only on the series id, so the same id is kept or dropped
     * identically on every page. This is the stability property PromQL compliance requires:
     * a series kept at one step is kept at every step.
     */
    public void testSameIdDecidedIdenticallyAcrossPages() {
        DriverContext ctx = driverContext();
        BlockFactory blockFactory = ctx.blockFactory();
        try (HashRatioLimitOperator op = new HashRatioLimitOperator(0.5, 0)) {
            for (String id : List.of("alpha", "beta", "gamma", "delta")) {
                op.addInput(page(blockFactory, id));
                Page out1 = op.getOutput();
                boolean kept1 = out1 != null;
                if (out1 != null) {
                    out1.releaseBlocks();
                }
                op.addInput(page(blockFactory, id));
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
     * Over many distinct ids roughly the requested fraction is kept.
     */
    public void testRoughlyHalfKept() {
        List<String> names = new ArrayList<>();
        for (int i = 0; i < 2000; i++) {
            names.add("series-" + i);
        }
        int kept = keptIds(0.5, names).size();
        assertThat(kept, greaterThanOrEqualTo(800));
        assertThat(kept, lessThanOrEqualTo(1200));
    }

    /**
     * Predicate edge cases mirror Prometheus {@code AddRatioSampleWithOffset} exactly. The operator
     * itself is total (NaN keeps nothing since both comparisons are false); NaN literals are
     * rejected upstream at analysis time, like Prometheus.
     */
    public void testKeepPredicateEdges() {
        BytesRef id = new BytesRef("series-1");
        assertThat(HashRatioLimitOperator.keep(Double.NaN, id), equalTo(false));
        assertThat(HashRatioLimitOperator.keep(Double.POSITIVE_INFINITY, id), equalTo(true));
        assertThat(HashRatioLimitOperator.keep(Double.NEGATIVE_INFINITY, id), equalTo(true));
        assertThat(HashRatioLimitOperator.keep(-0.0, id), equalTo(false));
        assertThat(HashRatioLimitOperator.keep(2.0, id), equalTo(true));
        assertThat(HashRatioLimitOperator.keep(-2.0, id), equalTo(true));
        assertThat(HashRatioLimitOperator.keep(0.0, id), equalTo(false));
        assertThat(HashRatioLimitOperator.keep(1.0, id), equalTo(true));
    }

    public void testStatus() {
        DriverContext ctx = driverContext();
        BlockFactory blockFactory = ctx.blockFactory();
        try (HashRatioLimitOperator op = simple(SimpleOptions.DEFAULT).get(ctx)) {
            AbstractPageMappingOperator.Status status = op.status();
            assertThat(status.pagesProcessed(), equalTo(0));
            assertThat(status.rowsReceived(), equalTo(0L));
            assertThat(status.rowsEmitted(), equalTo(0L));

            Page p = page(blockFactory, "a", "b");
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

    private void assertKeepsExactly(double ratio, List<String> names) {
        DriverContext ctx = driverContext();
        BlockFactory blockFactory = ctx.blockFactory();
        try (HashRatioLimitOperator op = new HashRatioLimitOperator(ratio, 0)) {
            op.addInput(page(blockFactory, names.toArray(new String[0])));
            Page out = op.getOutput();
            try {
                assertThat(out.getPositionCount(), equalTo(names.size()));
            } finally {
                out.releaseBlocks();
            }
        }
    }

    private Set<BytesRef> keptIds(double ratio, List<String> names) {
        DriverContext ctx = driverContext();
        BlockFactory blockFactory = ctx.blockFactory();
        try (HashRatioLimitOperator op = new HashRatioLimitOperator(ratio, 0)) {
            op.addInput(page(blockFactory, names.toArray(new String[0])));
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

    private static Set<BytesRef> idsOf(List<Page> pages) {
        Set<BytesRef> ids = new HashSet<>();
        for (Page page : pages) {
            Block block = page.getBlock(0);
            List<Object> values = new ArrayList<>();
            BlockTestUtils.readInto(values, block);
            for (Object value : values) {
                ids.add((BytesRef) value);
            }
        }
        return ids;
    }

    private static Page page(BlockFactory blockFactory, String... ids) {
        List<Object> values = new ArrayList<>(ids.length);
        for (String id : ids) {
            values.add(new BytesRef(id));
        }
        return new Page(BlockTestUtils.asBlock(blockFactory, ElementType.BYTES_REF, values));
    }

    private static List<String> ids(String... ids) {
        return List.of(ids);
    }
}
