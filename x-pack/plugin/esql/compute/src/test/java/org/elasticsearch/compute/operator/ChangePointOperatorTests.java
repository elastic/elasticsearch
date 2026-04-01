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
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.test.OperatorTestCase;
import org.elasticsearch.compute.test.TestWarningsSource;
import org.elasticsearch.compute.test.operator.blocksource.SequenceLongBlockSourceOperator;
import org.hamcrest.Matcher;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static java.util.Collections.nCopies;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.nullValue;

public class ChangePointOperatorTests extends OperatorTestCase {

    @Override
    protected SourceOperator simpleInput(BlockFactory blockFactory, int size) {
        // size must be in [25, 1000] for ChangePoint to function correctly
        // and detect the step change.
        size = Math.clamp(size, 25, 1000);
        List<Long> data = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            if (i <= size / 2) {
                data.add(0L);
            } else {
                data.add(1L);
            }
        }
        return new SequenceLongBlockSourceOperator(blockFactory, data);
    }

    @Override
    protected void assertSimpleOutput(List<Page> input, List<Page> results) {
        boolean seenOne = false;
        assertThat(results, hasSize(input.size()));
        for (int i = 0; i < results.size(); i++) {
            Page inputPage = input.get(i);
            Page resultPage = results.get(i);
            assertThat(resultPage.getPositionCount(), equalTo(inputPage.getPositionCount()));
            assertThat(resultPage.getBlockCount(), equalTo(3));
            for (int j = 0; j < resultPage.getPositionCount(); j++) {
                long inputValue = ((LongBlock) resultPage.getBlock(0)).getLong(j);
                long resultValue = ((LongBlock) resultPage.getBlock(0)).getLong(j);
                assertThat(resultValue, equalTo(inputValue));
                if (seenOne == false && resultValue == 1L) {
                    BytesRef type = ((BytesRefBlock) resultPage.getBlock(1)).getBytesRef(j, new BytesRef());
                    double pvalue = ((DoubleBlock) resultPage.getBlock(2)).getDouble(j);
                    assertThat(type.utf8ToString(), equalTo("step_change"));
                    assertThat(pvalue, equalTo(0.0));
                    seenOne = true;
                } else {
                    assertThat(resultPage.getBlock(1).isNull(j), equalTo(true));
                    assertThat(resultPage.getBlock(2).isNull(j), equalTo(true));
                }
            }
        }
        assertThat(seenOne, equalTo(true));
    }

    @Override
    protected Operator.OperatorFactory simple(SimpleOptions options) {
        return new ChangePointOperator.Factory(0, null, new TestWarningsSource(null));
    }

    @Override
    protected Matcher<String> expectedDescriptionOfSimple() {
        return equalTo("ChangePointOperator[channel=0]");
    }

    @Override
    protected Matcher<String> expectedToStringOfSimple() {
        return equalTo("ChangePointOperator[channel=0]");
    }

    @Override
    protected void assertStatus(Map<String, Object> map, List<Page> input, List<Page> output) {
        assertThat(map, nullValue());
    }

    @Override
    public void testCanProduceMoreDataWithoutExtraInput() {
        // Change point cannot work with empty input, so skip this test
    }

    public void testGroupedChangepointPerGroupPerPage() {
        DriverContext ctx = driverContext();
        BlockFactory blockFactory = ctx.blockFactory();

        // Group A: [0×15, 1×15] -> step at row 15 of page0
        // Group B: [1×15, 0×15] -> step at row 15 of page1
        // Group C: [0×15, 1×15] -> step at row 15 of page2
        List<Long> valuesColumn = Stream.of(
            nCopies(15, 0L),
            nCopies(15, 1L),
            nCopies(15, 1L),
            nCopies(15, 0L),
            nCopies(15, 0L),
            nCopies(15, 1L)
        ).flatMap(List::stream).toList();
        List<String> groupsColumn = Stream.of(nCopies(30, "A"), nCopies(30, "B"), nCopies(30, "C")).flatMap(List::stream).toList();
        List<Page> pages = buildPages(blockFactory, List.of(30, 60), valuesColumn, groupsColumn);

        List<Page> outputPages = invokeChangePoint(ctx, pages);
        try {
            assertChangePointAt(outputPages.get(0), 15);
            assertChangePointAt(outputPages.get(1), 15);
            assertChangePointAt(outputPages.get(2), 15);
        } finally {
            outputPages.forEach(Page::releaseBlocks);
        }
    }

    public void testGroupedChangepointInGroupSplitOnFirstPage() {
        DriverContext ctx = driverContext();
        BlockFactory blockFactory = ctx.blockFactory();

        // Group A: [0×15, 1×20] -> step at row 15 of page0
        // Group B: [1×85]
        List<String> groupsColumn = Stream.concat(nCopies(35, "A").stream(), nCopies(85, "B").stream()).toList();
        List<Long> valuesColumn = Stream.concat(nCopies(15, 0L).stream(), nCopies(105, 1L).stream()).toList();
        List<Page> pages = buildPages(blockFactory, List.of(40, 80), valuesColumn, groupsColumn);

        List<Page> outputPages = invokeChangePoint(ctx, pages);
        try {
            assertChangePointAt(outputPages.get(0), 15);
            assertNoChangePoints(outputPages.get(1));
            assertNoChangePoints(outputPages.get(2));
        } finally {
            outputPages.forEach(Page::releaseBlocks);
        }
    }

    public void testChangePointInGroupSplitByPageOnMiddlePage() {
        DriverContext ctx = driverContext();
        BlockFactory blockFactory = ctx.blockFactory();

        // Group A: [0×40, 1×25] -> step at row 20 of page1
        // Group B: [0×35]
        List<String> groupsColumn = Stream.concat(nCopies(65, "A").stream(), nCopies(35, "B").stream()).toList();
        List<Long> valuesColumn = Stream.of(nCopies(40, 0L), nCopies(25, 1L), nCopies(35, 0L)).flatMap(List::stream).toList();
        List<Page> pages = buildPages(blockFactory, List.of(20, 60), valuesColumn, groupsColumn);

        List<Page> outputPages = invokeChangePoint(ctx, pages);
        try {
            assertNoChangePoints(outputPages.get(0));
            assertChangePointAt(outputPages.get(1), 20);
            assertNoChangePoints(outputPages.get(2));
        } finally {
            outputPages.forEach(Page::releaseBlocks);
        }
    }

    public void testGroupedChangePointSplitOnLastPage() {
        DriverContext ctx = driverContext();
        BlockFactory blockFactory = ctx.blockFactory();

        // Group A: [1×85]
        // Group B: [0×20, 1×15] -> step at row 25 of page2
        List<String> groupsColumn = Stream.concat(nCopies(85, "A").stream(), nCopies(35, "B").stream()).toList();
        List<Long> valuesColumn = Stream.of(nCopies(85, 1L), nCopies(20, 0L), nCopies(15, 1L)).flatMap(List::stream).toList();
        List<Page> pages = buildPages(blockFactory, List.of(40, 80), valuesColumn, groupsColumn);

        List<Page> outputPages = invokeChangePoint(ctx, pages);
        try {
            assertNoChangePoints(outputPages.get(0));
            assertNoChangePoints(outputPages.get(1));
            assertChangePointAt(outputPages.get(2), 25);
        } finally {
            outputPages.forEach(Page::releaseBlocks);
        }
    }

    public void testAllNullInputProducesWarning() {
        DriverContext ctx = driverContext();
        BlockFactory blockFactory = ctx.blockFactory();

        List<Page> inputPages = buildPages(blockFactory, List.of(), nCopies(30, null));
        List<Page> outputPages = invokeChangePoint(ctx, inputPages, 0, null);

        try {
            assertThat(outputPages, hasSize(1));
            assertWarnings(
                "Line 1:1: evaluation of [null] failed, treating result as null. Only first 20 failures recorded.",
                "Line 1:1: java.lang.IllegalArgumentException: not enough buckets to calculate change_point. Requires at least [22]; "
                    + "found [0]",
                "Line 1:1: java.lang.IllegalArgumentException: values contain nulls; skipping them"
            );
            Block typeBlock = outputPages.get(0).getBlock(1);
            Block pvalueBlock = outputPages.get(0).getBlock(2);
            for (int j = 0; j < 30; j++) {
                assertThat("unexpected change type at " + j, typeBlock.isNull(j), equalTo(true));
                assertThat("unexpected pvalue at " + j, pvalueBlock.isNull(j), equalTo(true));
            }
        } finally {
            outputPages.forEach(Page::releaseBlocks);
        }
    }

    public void testNoInputPagesProducesWarning() {
        DriverContext ctx = driverContext();

        List<Page> outputPages = invokeChangePoint(ctx, List.of(), 0, null);

        assertThat(outputPages, hasSize(0));
        assertWarnings(
            "Line 1:1: evaluation of [null] failed, treating result as null. Only first 20 failures recorded.",
            "Line 1:1: java.lang.IllegalArgumentException: not enough buckets to calculate change_point. Requires at least [22]; found [0]"
        );
    }

    public void testGroupedSingleRowGroupProducesWarnings() {
        DriverContext ctx = driverContext();
        BlockFactory blockFactory = ctx.blockFactory();

        // Group A: [0×1]
        // Group B: [0×15, 1×15] -> step at row 15 of page1
        List<String> groupsColumn = Stream.concat(nCopies(1, "A").stream(), nCopies(30, "B").stream()).toList();
        List<Long> valuesColumn = Stream.concat(nCopies(1, 0L).stream(), Stream.concat(nCopies(15, 0L).stream(), nCopies(15, 1L).stream()))
            .toList();
        List<Page> pages = buildPages(blockFactory, List.of(1), valuesColumn, groupsColumn);

        List<Page> outputPages = invokeChangePoint(ctx, pages);
        try {
            assertNoChangePoints(outputPages.get(0));
            assertChangePointAt(outputPages.get(1), 15);
            assertWarnings(
                "Line 1:1: evaluation of [null] failed, treating result as null. Only first 20 failures recorded.",
                "Line 1:1: java.lang.IllegalArgumentException: not enough buckets to calculate change_point. Requires at least [22]; "
                    + "found [1]"
            );
        } finally {
            outputPages.forEach(Page::releaseBlocks);
        }
    }

    public void testGroupedAllNullGroupAtEndProducesWarnings() {
        DriverContext ctx = driverContext();
        BlockFactory blockFactory = ctx.blockFactory();

        // Group A: [0×15, 1×15] -> step at row 15 of page0
        // Group B: [null×5] -> warnings produced
        List<String> groupsColumn = Stream.concat(nCopies(30, "A").stream(), nCopies(5, "B").stream()).toList();
        List<Long> valuesColumn = Stream.of(nCopies(15, 0L), nCopies(15, 1L), nCopies(5, (Long) null)).flatMap(List::stream).toList();
        List<Page> pages = buildPages(blockFactory, List.of(30), valuesColumn, groupsColumn);

        List<Page> outputPages = invokeChangePoint(ctx, pages);
        try {
            assertChangePointAt(outputPages.get(0), 15);
            assertNoChangePoints(outputPages.get(1));
            assertWarnings(
                "Line 1:1: evaluation of [null] failed, treating result as null. Only first 20 failures recorded.",
                "Line 1:1: java.lang.IllegalArgumentException: not enough buckets to calculate change_point. Requires at least [22]; "
                    + "found [0]",
                "Line 1:1: java.lang.IllegalArgumentException: values contain nulls; skipping them"
            );
        } finally {
            outputPages.forEach(Page::releaseBlocks);
        }
    }

    public void testGroupedAllNullGroupAtStartProducesWarnings() {
        DriverContext ctx = driverContext();
        BlockFactory blockFactory = ctx.blockFactory();

        // Group A: [null×5] -> should produce indeterminable warning
        // Group B: [0×15, 1×15] -> step change at row 15 of page1
        List<String> groupsColumn = Stream.concat(nCopies(5, "A").stream(), nCopies(30, "B").stream()).toList();
        List<Long> valuesColumn = Stream.of(nCopies(5, (Long) null), nCopies(15, 0L), nCopies(15, 1L)).flatMap(List::stream).toList();
        List<Page> pages = buildPages(blockFactory, List.of(5), valuesColumn, groupsColumn);

        List<Page> outputPages = invokeChangePoint(ctx, pages);
        try {
            assertNoChangePoints(outputPages.get(0));
            assertChangePointAt(outputPages.get(1), 15);
            assertWarnings(
                "Line 1:1: evaluation of [null] failed, treating result as null. Only first 20 failures recorded.",
                "Line 1:1: java.lang.IllegalArgumentException: not enough buckets to calculate change_point. Requires at least [22]; "
                    + "found [0]",
                "Line 1:1: java.lang.IllegalArgumentException: values contain nulls; skipping them"
            );
        } finally {
            outputPages.forEach(Page::releaseBlocks);
        }
    }

    public void testGroupedTooManyValuesPerGroupWarning() {
        DriverContext ctx = driverContext();
        BlockFactory blockFactory = ctx.blockFactory();

        // Group A: [0x500, 1x501] -> should detect step change and produce warning
        // Group B: [0x500, 1x501] -> should detect step change
        List<String> groupsColumn = Stream.concat(nCopies(1001, "A").stream(), nCopies(1001, "B").stream()).toList();
        List<Long> valuesColumn = Stream.concat(
            Stream.of(nCopies(500, 0L), nCopies(501, 1L)).flatMap(List::stream),
            Stream.of(nCopies(500, 0L), nCopies(501, 1L)).flatMap(List::stream)
        ).toList();
        List<Page> pages = buildPages(blockFactory, List.of(1001), valuesColumn, groupsColumn);

        List<Page> outputPages = invokeChangePoint(ctx, pages);
        try {
            assertChangePointAt(outputPages.get(0), 500);
            assertChangePointAt(outputPages.get(1), 500);
            assertWarnings(
                "Line 1:1: warnings during evaluation of [null]. Only first 20 failures recorded.",
                "Line 1:1: java.lang.IllegalArgumentException: too many values; keeping only first 1000 values"
            );
        } finally {
            outputPages.forEach(Page::releaseBlocks);
        }
    }

    public void testGroupedNoInputPages() {
        DriverContext ctx = driverContext();
        List<Page> outputPages = invokeChangePoint(ctx, List.of(), 1, 0);
        assertThat(outputPages, hasSize(0));
    }

    public void testGroupedTwoChangepointsOnSinglePage() {
        DriverContext ctx = driverContext();
        BlockFactory blockFactory = ctx.blockFactory();

        // Group A: [0×15, 1×15] -> step at row 15 of page0
        // Group B: [0×15, 1×15] -> step at row 45 of page0
        List<String> groupsColumn = Stream.concat(nCopies(30, "A").stream(), nCopies(30, "B").stream()).toList();
        List<Long> valuesColumn = Stream.of(nCopies(15, 0L), nCopies(15, 1L), nCopies(15, 0L), nCopies(15, 1L))
            .flatMap(List::stream)
            .toList();
        List<Page> pages = buildPages(blockFactory, List.of(), valuesColumn, groupsColumn);

        List<Page> outputPages = invokeChangePoint(ctx, pages);
        try {
            assertChangePointAt(outputPages.get(0), 15);
            assertChangePointAt(outputPages.get(0), 45);
        } finally {
            outputPages.forEach(Page::releaseBlocks);
        }
    }

    public void testGroupedChangepointExactlyOnLastRowOfPage() {
        DriverContext ctx = driverContext();
        BlockFactory blockFactory = ctx.blockFactory();

        // Group A: [0×29, 1×31] -> step at row 29 (last row of page0).
        List<String> groupsColumn = nCopies(60, "A");
        List<Long> valuesColumn = Stream.concat(nCopies(29, 0L).stream(), nCopies(31, 1L).stream()).toList();
        List<Page> pages = buildPages(blockFactory, List.of(30), valuesColumn, groupsColumn);

        List<Page> outputPages = invokeChangePoint(ctx, pages);
        try {
            assertChangePointAt(outputPages.get(0), 29);
            assertNoChangePoints(outputPages.get(1));
        } finally {
            outputPages.forEach(Page::releaseBlocks);
        }
    }

    public void testGroupedChangePointExactlyOnFirstRowOfPage() {
        DriverContext ctx = driverContext();
        BlockFactory blockFactory = ctx.blockFactory();

        // Group A: [0×30, 1×30] -> step at row 30 (first row of page1).
        List<String> groupsColumn = nCopies(60, "A");
        List<Long> valuesColumn = Stream.concat(nCopies(30, 0L).stream(), nCopies(30, 1L).stream()).toList();
        List<Page> pages = buildPages(blockFactory, List.of(30), valuesColumn, groupsColumn);

        List<Page> outputPages = invokeChangePoint(ctx, pages);
        try {
            assertNoChangePoints(outputPages.get(0));
            assertChangePointAt(outputPages.get(1), 0);
        } finally {
            outputPages.forEach(Page::releaseBlocks);
        }
    }

    private static List<Page> buildPages(
        BlockFactory blockFactory,
        List<Integer> splits,
        List<Long> valuesColumn,
        List<String> groupsColumn
    ) {
        assert groupsColumn.size() == valuesColumn.size();
        List<Page> pages = new ArrayList<>();
        int start = 0;
        for (int split : splits) {
            pages.add(buildPage(blockFactory, valuesColumn.subList(start, split), groupsColumn.subList(start, split)));
            start = split;
        }
        pages.add(
            buildPage(blockFactory, valuesColumn.subList(start, valuesColumn.size()), groupsColumn.subList(start, groupsColumn.size()))
        );
        return pages;
    }

    private static List<Page> buildPages(BlockFactory blockFactory, List<Integer> splits, List<Long> valuesColumn) {
        List<Page> pages = new ArrayList<>();
        int start = 0;
        for (int split : splits) {
            pages.add(buildPage(blockFactory, valuesColumn.subList(start, split)));
            start = split;
        }
        pages.add(buildPage(blockFactory, valuesColumn.subList(start, valuesColumn.size())));
        return pages;
    }

    private static Page buildPage(BlockFactory blockFactory, List<Long> valuesColumn, List<String> groupsColumn) {
        try (
            BytesRefBlock.Builder g = blockFactory.newBytesRefBlockBuilder(groupsColumn.size());
            LongBlock.Builder v = blockFactory.newLongBlockBuilder(valuesColumn.size())
        ) {
            for (int i = 0; i < groupsColumn.size(); i++) {
                g.appendBytesRef(new BytesRef(groupsColumn.get(i)));
                Long val = valuesColumn.get(i);
                if (val == null) v.appendNull();
                else v.appendLong(val);
            }
            return new Page(g.build(), v.build());
        }
    }

    private static Page buildPage(BlockFactory blockFactory, List<Long> valuesColumn) {
        try (LongBlock.Builder v = blockFactory.newLongBlockBuilder(valuesColumn.size())) {
            for (Long val : valuesColumn) {
                if (val == null) v.appendNull();
                else v.appendLong(val);
            }
            return new Page(v.build());
        }
    }

    private List<Page> invokeChangePoint(DriverContext ctx, List<Page> inputPages) {
        try (ChangePointOperator op = new ChangePointOperator(ctx, 1, 0, new TestWarningsSource(null))) {
            for (Page page : inputPages) {
                op.addInput(page);
            }
            op.finish();
            Page out;
            List<Page> outputPages = new ArrayList<>();
            while ((out = op.getOutput()) != null) {
                outputPages.add(out);
            }
            return outputPages;
        }
    }

    private List<Page> invokeChangePoint(DriverContext ctx, List<Page> inputPages, int keyChannel, Integer groupingChannel) {
        try (ChangePointOperator op = new ChangePointOperator(ctx, keyChannel, groupingChannel, new TestWarningsSource(null))) {
            for (Page page : inputPages) {
                op.addInput(page);
            }
            op.finish();
            Page out;
            List<Page> outputPages = new ArrayList<>();
            while ((out = op.getOutput()) != null) {
                outputPages.add(out);
            }
            return outputPages;
        }
    }

    private void assertChangePointAt(Page page, int position) {
        BytesRefBlock typeBlock = page.getBlock(2);
        DoubleBlock pvalueBlock = page.getBlock(3);

        assertThat("expected change type at " + position, typeBlock.isNull(position), equalTo(false));
        assertThat("expected pvalue at " + position, pvalueBlock.isNull(position), equalTo(false));
        assertThat(typeBlock.getBytesRef(position, new BytesRef()).utf8ToString(), equalTo("step_change"));
    }

    private void assertNoChangePoints(Page page) {
        Block typeBlock = page.getBlock(2);
        Block pvalueBlock = page.getBlock(3);
        for (int j = 0; j < page.getPositionCount(); j++) {
            assertThat("unexpected change type at " + j, typeBlock.isNull(j), equalTo(true));
            assertThat("unexpected pvalue at " + j, pvalueBlock.isNull(j), equalTo(true));
        }
    }
}
