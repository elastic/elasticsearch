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
        return new ChangePointOperator.Factory(0, List.of(), new TestWarningsSource(null));
    }

    @Override
    protected Matcher<String> expectedDescriptionOfSimple() {
        return equalTo("ChangePointOperator[channel=0, groupingChannels=[]]");
    }

    @Override
    protected Matcher<String> expectedToStringOfSimple() {
        return equalTo("ChangePointOperator[channel=0, groupingChannels=[]]");
    }

    @Override
    protected void assertStatus(Map<String, Object> map, List<Page> input, List<Page> output) {
        assertThat(map, nullValue());
    }

    @Override
    public void testCanProduceMoreDataWithoutExtraInput() {
        // Change point cannot work with empty input, so skip this test
    }

    public void testGroupedFactoryDescribe() {
        // The OperatorTestCase-driven simple() above exercises the ungrouped form (groupingChannels=[]).
        // This asserts that grouping channels round-trip into the factory's describe() output.
        var factory = new ChangePointOperator.Factory(0, List.of(1, 2), new TestWarningsSource(null));
        assertThat(factory.describe(), equalTo("ChangePointOperator[channel=0, groupingChannels=[1, 2]]"));
    }

    public void testGroupedOperatorToString() {
        DriverContext ctx = driverContext();
        try (ChangePointOperator op = new ChangePointOperator(ctx, 0, new int[] { 1, 2 }, new TestWarningsSource(null))) {
            assertThat(op.toString(), equalTo("ChangePointOperator[channel=0, groupingChannels=[1, 2]]"));
        }
    }

    public void testNonGroupedFlushOnFinish() {
        DriverContext ctx = driverContext();
        BlockFactory blockFactory = ctx.blockFactory();

        // Non-grouped mode: a single implicit group spans all input pages, so finish() must
        // drain the buffer through the detector and emit one annotated page per input page.
        // page0 [rows 0-29]: values=[0×30]
        // page1 [rows 30-59]: values=[0×15, 1×15] -> step at global row 45 (page1 pos 15)
        // page2 [rows 60-89]: values=[1×30]
        List<Long> valuesColumn = Stream.of(nCopies(45, 0L), nCopies(45, 1L)).flatMap(List::stream).toList();
        List<Page> inputPages = buildPages(blockFactory, List.of(30, 60), valuesColumn);

        List<Page> outputPages = invokeChangePoint(ctx, inputPages);
        try {
            assertThat(outputPages, hasSize(3));
            assertNoChangePoints(outputPages.get(0));
            assertChangePointAt(outputPages.get(1), 15);
            assertNoChangePoints(outputPages.get(2));
        } finally {
            outputPages.forEach(Page::releaseBlocks);
        }
    }

    public void testAllNullInputProducesWarning() {
        DriverContext ctx = driverContext();
        BlockFactory blockFactory = ctx.blockFactory();

        List<Page> inputPages = buildPages(blockFactory, List.of(), nCopies(30, null));
        List<Page> outputPages = invokeChangePoint(ctx, inputPages);

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

        List<Page> outputPages = invokeChangePoint(ctx, List.of());

        assertThat(outputPages, hasSize(0));
        assertWarnings(
            "Line 1:1: evaluation of [null] failed, treating result as null. Only first 20 failures recorded.",
            "Line 1:1: java.lang.IllegalArgumentException: not enough buckets to calculate change_point. Requires at least [22]; found [0]"
        );
    }

    public void testEmptyPagesAreDropped() {
        DriverContext ctx = driverContext();
        BlockFactory blockFactory = ctx.blockFactory();

        List<Long> valuesColumn = Stream.of(nCopies(15, 0L), nCopies(15, 1L)).flatMap(List::stream).toList();
        List<String> groupsColumn = nCopies(30, "A");
        Page leadingEmpty = buildPage(blockFactory, List.of(), List.of());
        Page normal = buildPage(blockFactory, valuesColumn, groupsColumn);
        Page trailingEmpty = buildPage(blockFactory, List.of(), List.of());

        List<Page> outputPages = invokeChangePoint(ctx, List.of(leadingEmpty, normal, trailingEmpty), 1);
        try {
            assertThat(outputPages, hasSize(1));
            assertChangePointAt(outputPages.get(0), 15);
        } finally {
            outputPages.forEach(Page::releaseBlocks);
        }
    }

    public void testGroupedChangepointPerGroupPerPage() {
        DriverContext ctx = driverContext();
        BlockFactory blockFactory = ctx.blockFactory();

        // Pages split at rows 30 and 60 — one group per page:
        // page0 [rows 0-29]: grp=A, values=[0×15, 1×15] -> step at row 15
        // page1 [rows 30-59]: grp=B, values=[1×15, 0×15] -> step at row 45
        // page2 [rows 60-89]: grp=C, values=[0×15, 1×15] -> step at row 75
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

        List<Page> outputPages = invokeChangePoint(ctx, pages, 1);
        try {
            assertThat(outputPages, hasSize(3));
            assertChangePointAt(outputPages.get(0), 15);
            assertChangePointAt(outputPages.get(1), 15);
            assertChangePointAt(outputPages.get(2), 15);
        } finally {
            outputPages.forEach(Page::releaseBlocks);
        }
    }

    public void testChangePointInGroupSplitOnMiddlePage() {
        DriverContext ctx = driverContext();
        BlockFactory blockFactory = ctx.blockFactory();

        // Pages split at rows 10 and 45 — group switch and step both fall on page1:
        // page0 [rows 0-9]: grp=A values=[0×10]
        // page1 [rows 10-44]: grp=A values=[0×15, 1×15] -> step at row 25
        // grp=B values=[0×5]
        // page2 [rows 45-69]: grp=B values=[0×25]
        List<String> groupsColumn = Stream.concat(nCopies(40, "A").stream(), nCopies(30, "B").stream()).toList();
        List<Long> valuesColumn = Stream.of(nCopies(25, 0L), nCopies(15, 1L), nCopies(30, 0L)).flatMap(List::stream).toList();
        List<Page> pages = buildPages(blockFactory, List.of(10, 45), valuesColumn, groupsColumn);

        List<Page> outputPages = invokeChangePoint(ctx, pages, 1);
        try {
            assertThat(outputPages, hasSize(4));
            assertNoChangePoints(outputPages.get(0));
            assertChangePointAt(outputPages.get(1), 15);
            assertNoChangePoints(outputPages.get(2));
            assertNoChangePoints(outputPages.get(3));
        } finally {
            outputPages.forEach(Page::releaseBlocks);
        }
    }

    public void testGroupedChangePointSplitOnLastPage() {
        DriverContext ctx = driverContext();
        BlockFactory blockFactory = ctx.blockFactory();

        // Pages split at rows 40 and 80 — group A spans the first two pages, B starts mid-last-page:
        // page0 [rows 0-39]: grp=A, values=[1×40]
        // page1 [rows 40-79]: grp=A, values=[1×40]
        // page2 [rows 80-119]: grp=A, values=[1×5]
        // grp=B, values=[0×20, 1×15] -> step at row 105
        List<String> groupsColumn = Stream.concat(nCopies(85, "A").stream(), nCopies(35, "B").stream()).toList();
        List<Long> valuesColumn = Stream.of(nCopies(85, 1L), nCopies(20, 0L), nCopies(15, 1L)).flatMap(List::stream).toList();
        List<Page> pages = buildPages(blockFactory, List.of(40, 80), valuesColumn, groupsColumn);

        List<Page> outputPages = invokeChangePoint(ctx, pages, 1);
        try {
            assertThat(outputPages, hasSize(4));
            assertNoChangePoints(outputPages.get(0));
            assertNoChangePoints(outputPages.get(1));
            assertNoChangePoints(outputPages.get(2));
            assertChangePointAt(outputPages.get(3), 20);
        } finally {
            outputPages.forEach(Page::releaseBlocks);
        }
    }

    public void testGroupedThreeGroupsOnSinglePage() {
        DriverContext ctx = driverContext();
        BlockFactory blockFactory = ctx.blockFactory();

        // Single page, three groups — exercises pageChangePoints accumulating across all three groups:
        // page0 [rows 0-29]: grp=A, values=[0×15, 1×15] -> step at row 15
        // page0 [rows 30-59]: grp=B, values=[0×15, 1×15] -> step at row 45
        // page0 [rows 60-89]: grp=C, values=[0×15, 1×15] -> step at row 75
        List<String> groupsColumn = Stream.of(nCopies(30, "A"), nCopies(30, "B"), nCopies(30, "C")).flatMap(List::stream).toList();
        List<Long> valuesColumn = Stream.of(
            nCopies(15, 0L),
            nCopies(15, 1L),
            nCopies(15, 0L),
            nCopies(15, 1L),
            nCopies(15, 0L),
            nCopies(15, 1L)
        ).flatMap(List::stream).toList();
        List<Page> pages = buildPages(blockFactory, List.of(), valuesColumn, groupsColumn);

        List<Page> outputPages = invokeChangePoint(ctx, pages, 1);
        try {
            assertThat(outputPages, hasSize(3));
            assertChangePointAt(outputPages.get(0), 15);
            assertChangePointAt(outputPages.get(1), 15);
            assertChangePointAt(outputPages.get(2), 15);
        } finally {
            outputPages.forEach(Page::releaseBlocks);
        }
    }

    public void testGroupedBoundaryAtLastPositionOfPage() {
        DriverContext ctx = driverContext();
        BlockFactory blockFactory = ctx.blockFactory();

        // Page split at row 30 — group boundary falls on the LAST position of page0, so page0's
        // trailing slice is a 1-row group-B prefix. Exercises the i == positionCount - 1 slice path.
        // page0 [rows 0-28]: grp=A (29 rows), values=[0×15, 1×14] -> step at A-pos 15
        // page0 [row 29]: grp=B (1 row), values=[0]
        // page1 [rows 30-59]: grp=B (30 rows), values=[0×14, 1×16]
        // -> group B is 31 rows total, [0×15, 1×16], step at B-pos 15 (= page1 pos 14)
        List<String> groupsColumn = Stream.concat(nCopies(29, "A").stream(), nCopies(31, "B").stream()).toList();
        List<Long> valuesColumn = Stream.of(nCopies(15, 0L), nCopies(14, 1L), nCopies(15, 0L), nCopies(16, 1L))
            .flatMap(List::stream)
            .toList();
        List<Page> pages = buildPages(blockFactory, List.of(30), valuesColumn, groupsColumn);

        List<Page> outputPages = invokeChangePoint(ctx, pages, 1);
        try {
            assertThat(outputPages, hasSize(3));
            assertThat(outputPages.get(0).getPositionCount(), equalTo(29));
            assertThat(outputPages.get(1).getPositionCount(), equalTo(1));
            assertThat(outputPages.get(2).getPositionCount(), equalTo(30));
            assertChangePointAt(outputPages.get(0), 15);
            assertNoChangePoints(outputPages.get(1));
            assertChangePointAt(outputPages.get(2), 14);
        } finally {
            outputPages.forEach(Page::releaseBlocks);
        }
    }

    public void testGroupedChangepointExactlyOnLastRowOfPage() {
        DriverContext ctx = driverContext();
        BlockFactory blockFactory = ctx.blockFactory();

        // Page split at row 30 — step falls on the last row of page0:
        // page0 [rows 0-29]: grp=A, values=[0×29, 1×1] -> step at row 29
        // page1 [rows 30-59]: grp=A, values=[1×30]
        List<String> groupsColumn = nCopies(60, "A");
        List<Long> valuesColumn = Stream.concat(nCopies(29, 0L).stream(), nCopies(31, 1L).stream()).toList();
        List<Page> pages = buildPages(blockFactory, List.of(30), valuesColumn, groupsColumn);

        List<Page> outputPages = invokeChangePoint(ctx, pages, 1);
        try {
            assertThat(outputPages, hasSize(2));
            assertChangePointAt(outputPages.get(0), 29);
            assertNoChangePoints(outputPages.get(1));
        } finally {
            outputPages.forEach(Page::releaseBlocks);
        }
    }

    public void testGroupedChangePointExactlyOnFirstRowOfPage() {
        DriverContext ctx = driverContext();
        BlockFactory blockFactory = ctx.blockFactory();

        // Page split at row 30 — step falls on the first row of page1:
        // page0 [rows 0-29]: grp=A, values=[0×30]
        // page1 [rows 30-59]: grp=A, values=[1×30] -> step at row 30
        List<String> groupsColumn = nCopies(60, "A");
        List<Long> valuesColumn = Stream.concat(nCopies(30, 0L).stream(), nCopies(30, 1L).stream()).toList();
        List<Page> pages = buildPages(blockFactory, List.of(30), valuesColumn, groupsColumn);

        List<Page> outputPages = invokeChangePoint(ctx, pages, 1);
        try {
            assertThat(outputPages, hasSize(2));
            assertNoChangePoints(outputPages.get(0));
            assertChangePointAt(outputPages.get(1), 0);
        } finally {
            outputPages.forEach(Page::releaseBlocks);
        }
    }

    public void testGroupedSingleRowGroupProducesWarnings() {
        DriverContext ctx = driverContext();
        BlockFactory blockFactory = ctx.blockFactory();

        // Page split at row 1 — group A is a single-row page, group B follows:
        // page0 [rows 0-0]: grp=A, values=[0×1] -> (too few values, indeterminable)
        // page1 [rows 1-30]: grp=B, values=[0×15, 1×15] -> step at row 16
        List<String> groupsColumn = Stream.concat(nCopies(1, "A").stream(), nCopies(30, "B").stream()).toList();
        List<Long> valuesColumn = Stream.concat(nCopies(1, 0L).stream(), Stream.concat(nCopies(15, 0L).stream(), nCopies(15, 1L).stream()))
            .toList();
        List<Page> pages = buildPages(blockFactory, List.of(1), valuesColumn, groupsColumn);

        List<Page> outputPages = invokeChangePoint(ctx, pages, 1);
        try {
            assertThat(outputPages, hasSize(2));
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

    public void testGroupedAllNullGroupProducesWarnings() {
        DriverContext ctx = driverContext();
        BlockFactory blockFactory = ctx.blockFactory();

        // Page split at row 30 — group A fills page0, group B is a trailing all-null page:
        // page0 [rows 0-29]: grp=A, values=[0×15, 1×15] -> step at row 15
        // page1 [rows 30-34]: grp=B, values=[null×5] -> (no change point, null warning produced)
        List<String> groupsColumn = Stream.concat(nCopies(30, "A").stream(), nCopies(5, "B").stream()).toList();
        List<Long> valuesColumn = Stream.of(nCopies(15, 0L), nCopies(15, 1L), nCopies(5, (Long) null)).flatMap(List::stream).toList();
        List<Page> pages = buildPages(blockFactory, List.of(30), valuesColumn, groupsColumn);

        List<Page> outputPages = invokeChangePoint(ctx, pages, 1);
        try {
            assertThat(outputPages, hasSize(2));
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

    public void testGroupedTooManyValuesPerGroupWarning() {
        DriverContext ctx = driverContext();
        BlockFactory blockFactory = ctx.blockFactory();

        // Page split at row 1001 — one group per page, each exceeds INPUT_VALUE_COUNT_LIMIT (1000):
        // page0 [rows 0-1000]: grp=A, values=[0×500, 1×501] -> step at row 500 (warning: too many values)
        // page1 [rows 1001-2001]: grp=B, values=[0×500, 1×501] -> step at row 1501 (warning: too many values)
        List<String> groupsColumn = Stream.concat(nCopies(1001, "A").stream(), nCopies(1001, "B").stream()).toList();
        List<Long> valuesColumn = Stream.concat(
            Stream.of(nCopies(500, 0L), nCopies(501, 1L)).flatMap(List::stream),
            Stream.of(nCopies(500, 0L), nCopies(501, 1L)).flatMap(List::stream)
        ).toList();
        List<Page> pages = buildPages(blockFactory, List.of(1001), valuesColumn, groupsColumn);

        List<Page> outputPages = invokeChangePoint(ctx, pages, 1);
        try {
            assertThat(outputPages, hasSize(2));
            assertChangePointAt(outputPages.get(0), 500);
            assertChangePointAt(outputPages.get(1), 500);
            assertWarnings(
                "Line 1:1: warnings during evaluation of [null]. Only first 20 failures recorded.",
                "Line 1:1: java.lang.IllegalArgumentException: too many values in a group; keeping only first 1000 values per group"
            );
        } finally {
            outputPages.forEach(Page::releaseBlocks);
        }
    }

    public void testCompositeGroupingKeys() {
        DriverContext ctx = driverContext();
        BlockFactory blockFactory = ctx.blockFactory();

        // Single page, 4 composite-key groups (region, service), 25 rows each:
        // page0 [rows 0-24]: grp=(us,web), values=[0×13, 1×12] -> step at row 13
        // page0 [rows 25-49]: grp=(us,db), values=[0×13, 1×12] -> step at row 38
        // page0 [rows 50-74]: grp=(eu,web), values=[0×13, 1×12] -> step at row 63
        // page0 [rows 75-99]: grp=(eu,db), values=[0×13, 1×12] -> step at row 88
        int groupSize = 25;
        List<String> regionsColumn = Stream.of(
            nCopies(groupSize, "us"),
            nCopies(groupSize, "us"),
            nCopies(groupSize, "eu"),
            nCopies(groupSize, "eu")
        ).flatMap(List::stream).toList();
        List<String> servicesColumn = Stream.of(
            nCopies(groupSize, "web"),
            nCopies(groupSize, "db"),
            nCopies(groupSize, "web"),
            nCopies(groupSize, "db")
        ).flatMap(List::stream).toList();
        List<Long> valuesColumn = Stream.of(
            nCopies(13, 0L),
            nCopies(12, 1L),
            nCopies(13, 0L),
            nCopies(12, 1L),
            nCopies(13, 0L),
            nCopies(12, 1L),
            nCopies(13, 0L),
            nCopies(12, 1L)
        ).flatMap(List::stream).toList();

        Page inputPage = buildPage(blockFactory, valuesColumn, regionsColumn, servicesColumn);
        List<Page> outputPages = invokeChangePoint(ctx, List.of(inputPage), 1, 2);
        try {
            assertThat(outputPages, hasSize(4));
            int stepAt = 13;
            for (int g = 0; g < 4; g++) {
                BytesRefBlock typeBlock = outputPages.get(g).getBlock(3);
                DoubleBlock pvalueBlock = outputPages.get(g).getBlock(4);
                assertThat("expected change type at " + stepAt + " (group " + g + ")", typeBlock.isNull(stepAt), equalTo(false));
                assertThat(typeBlock.getBytesRef(stepAt, new BytesRef()).utf8ToString(), equalTo("step_change"));
                assertThat("expected pvalue at " + stepAt + " (group " + g + ")", pvalueBlock.isNull(stepAt), equalTo(false));
            }
        } finally {
            outputPages.forEach(Page::releaseBlocks);
        }
    }

    public void testGroupedNoInputPagesProducesWarning() {
        DriverContext ctx = driverContext();
        List<Page> outputPages = invokeChangePoint(ctx, List.of(), 1);
        assertThat(outputPages, hasSize(0));
        assertWarnings(
            "Line 1:1: evaluation of [null] failed, treating result as null. Only first 20 failures recorded.",
            "Line 1:1: java.lang.IllegalArgumentException: not enough buckets to calculate change_point. Requires at least [22]; found [0]"
        );
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

    @SafeVarargs
    private static Page buildPage(BlockFactory blockFactory, List<Long> valuesColumn, List<String>... groupColumns) {
        // Layout: value at block 0, grouping columns at blocks 1..N.
        int size = valuesColumn.size();
        Block[] blocks = new Block[groupColumns.length + 1];
        try (LongBlock.Builder v = blockFactory.newLongBlockBuilder(size)) {
            for (Long val : valuesColumn) {
                if (val == null) v.appendNull();
                else v.appendLong(val);
            }
            blocks[0] = v.build();
        }
        for (int g = 0; g < groupColumns.length; g++) {
            assert groupColumns[g].size() == size;
            try (BytesRefBlock.Builder builder = blockFactory.newBytesRefBlockBuilder(size)) {
                for (String val : groupColumns[g]) {
                    builder.appendBytesRef(new BytesRef(val));
                }
                blocks[g + 1] = builder.build();
            }
        }
        return new Page(blocks);
    }

    private List<Page> invokeChangePoint(DriverContext ctx, List<Page> inputPages, int... groupingChannels) {
        try (ChangePointOperator op = new ChangePointOperator(ctx, 0, groupingChannels, new TestWarningsSource(null))) {
            List<Page> outputPages = new ArrayList<>();
            for (Page page : inputPages) {
                while (op.needsInput() == false) {
                    Page out = op.getOutput();
                    if (out != null) {
                        outputPages.add(out);
                    }
                }
                op.addInput(page);
            }
            op.finish();
            Page out;
            while ((out = op.getOutput()) != null) {
                outputPages.add(out);
            }
            return outputPages;
        }
    }

    private void assertChangePointAt(Page page, int position) {
        // change_type and change_pvalue are the last two blocks regardless of input shape.
        BytesRefBlock typeBlock = page.getBlock(page.getBlockCount() - 2);
        DoubleBlock pvalueBlock = page.getBlock(page.getBlockCount() - 1);

        assertThat("expected change type at " + position, typeBlock.isNull(position), equalTo(false));
        assertThat("expected pvalue at " + position, pvalueBlock.isNull(position), equalTo(false));
        assertThat(typeBlock.getBytesRef(position, new BytesRef()).utf8ToString(), equalTo("step_change"));
    }

    private void assertNoChangePoints(Page page) {
        Block typeBlock = page.getBlock(page.getBlockCount() - 2);
        Block pvalueBlock = page.getBlock(page.getBlockCount() - 1);
        for (int j = 0; j < page.getPositionCount(); j++) {
            assertThat("unexpected change type at " + j, typeBlock.isNull(j), equalTo(true));
            assertThat("unexpected pvalue at " + j, pvalueBlock.isNull(j), equalTo(true));
        }
    }
}
