/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.expression.function.scalar.nested;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.DocBlock;
import org.elasticsearch.compute.data.DocVector;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.AbstractPageMappingToIteratorOperator;
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.compute.test.TestDriverRunner;
import org.elasticsearch.xpack.esql.EsqlTestUtils;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.UnionTypeEsField;
import org.elasticsearch.xpack.esql.planner.PlannerSettings;
import org.hamcrest.Matcher;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

public class NestedExpandOperatorTests extends NestedIndexOperatorTestCase {
    private int pageSize;

    @Before
    public void chooseExpandPageSize() {
        pageSize = randomBoolean() ? between(1, 5) : between(1, 2_000);
    }

    @Override
    protected Operator.OperatorFactory simple(SimpleOptions options) {
        return factory(pageSize);
    }

    private NestedExpandOperator.Factory factory(int pageSize) {
        return factory(pageSize, PlannerSettings.DEFAULTS);
    }

    private NestedExpandOperator.Factory factory(int pageSize, PlannerSettings settings) {
        NestedChildren.Factory children = NestedChildren.factory(
            shards(),
            "items",
            List.of("price", "note"),
            List.of(DataType.LONG, DataType.TEXT),
            Collections.nCopies(2, (UnionTypeEsField) null),
            settings,
            EsqlTestUtils.TEST_CFG
        );
        return new NestedExpandOperator.Factory(0, children, "items", pageSize);
    }

    @Override
    protected void assertSimpleOutput(List<Page> input, List<Page> results) {
        List<Integer> parents = new ArrayList<>();
        for (Page page : input) {
            for (int p = 0; p < page.getPositionCount(); p++) {
                parents.add(parent(page, p));
            }
        }
        assertThat(rows(results), equalTo(expectedRows(parents)));
        for (Page page : results) {
            assertThat(page.getPositionCount(), lessThanOrEqualTo(pageSize));
        }
    }

    /** One row per child of each parent, in order, and one null row for a parent without children. */
    private static List<List<Object>> expectedRows(List<Integer> parents) {
        List<List<Object>> expected = new ArrayList<>();
        for (int parent : parents) {
            if (parent % 4 == 0) {
                expected.add(Arrays.asList(parent, null, null));
            }
            for (int j = 0; j < parent % 4; j++) {
                expected.add(List.of(parent, 10L * parent + j, "n" + j));
            }
        }
        return expected;
    }

    private List<List<Object>> rows(List<Page> results) {
        List<List<Object>> actual = new ArrayList<>();
        for (Page page : results) {
            LongBlock prices = page.getBlock(1);
            BytesRefBlock notes = page.getBlock(2);
            for (int p = 0; p < page.getPositionCount(); p++) {
                actual.add(
                    Arrays.asList(
                        parent(page, p),
                        prices.isNull(p) ? null : prices.getLong(prices.getFirstValueIndex(p)),
                        notes.isNull(p) ? null : notes.getBytesRef(notes.getFirstValueIndex(p), new BytesRef()).utf8ToString()
                    )
                );
            }
        }
        return actual;
    }

    @Override
    protected Matcher<String> expectedDescriptionOfSimple() {
        return equalTo("NestedExpandOperator[path=items]");
    }

    @Override
    protected Matcher<String> expectedToStringOfSimple() {
        return equalTo("NestedExpandOperator[path=items]");
    }

    /** Children are counted even when the query kept none of their columns. */
    public void testEnumeratesChildrenWithoutColumns() {
        var context = driverContext();
        NestedChildren.Factory children = NestedChildren.factory(
            shards(),
            "items",
            List.of(),
            List.of(),
            List.of(),
            PlannerSettings.DEFAULTS,
            EsqlTestUtils.TEST_CFG
        );
        int parents = 20;
        Page input = parents(context.blockFactory(), 0, parents);
        List<Page> results = new TestDriverRunner().builder(context)
            .input(List.of(input))
            .run(new NestedExpandOperator.Factory(0, children, "items", pageSize).get(context));
        int rows = 0;
        for (Page page : results) {
            assertThat(page.getBlockCount(), equalTo(1));
            rows += page.getPositionCount();
            page.releaseBlocks();
        }
        int expected = 0;
        for (int i = 0; i < parents; i++) {
            expected += Math.max(1, i % 4);
        }
        assertThat(rows, equalTo(expected));
        assertThat(context.breaker().getUsed(), equalTo(0L));
    }

    public void testPagesStayWithinPageSize() {
        for (int size : new int[] { 1, 2, 3, 7, 64 }) {
            var context = driverContext();
            List<Page> results = new TestDriverRunner().builder(context)
                .input(List.of(parents(context.blockFactory(), 0, PARENTS)))
                .run(factory(size).get(context));
            for (Page page : results) {
                assertThat(page.getPositionCount(), lessThanOrEqualTo(size));
            }
            assertThat(rows(results), equalTo(expectedRows(range(0, PARENTS))));
            releaseAll(results);
            assertThat(context.breaker().getUsed(), equalTo(0L));
        }
    }

    public void testParentWithMoreChildrenThanPageSizeSpansPages() {
        var context = driverContext();
        List<Page> results = new TestDriverRunner().builder(context)
            .input(List.of(parentsAt(context.blockFactory(), 3)))
            .run(factory(2).get(context));
        assertThat(results.stream().map(Page::getPositionCount).toList(), equalTo(List.of(2, 1)));
        assertThat(rows(results), equalTo(expectedRows(List.of(3))));
        releaseAll(results);
        assertThat(context.breaker().getUsed(), equalTo(0L));
    }

    public void testParentsWithoutChildrenAppearOnceAtPageBoundaries() {
        List<List<Integer>> inputs = List.of(List.of(0, 4, 8), List.of(0, 1, 4, 2, 8), List.of(1, 0, 4, 3, 8, 12, 5));
        for (int size : new int[] { 1, 2, 3 }) {
            for (List<Integer> parents : inputs) {
                var context = driverContext();
                List<Page> results = new TestDriverRunner().builder(context)
                    .input(List.of(parentsAt(context.blockFactory(), parents.stream().mapToInt(Integer::intValue).toArray())))
                    .run(factory(size).get(context));
                for (Page page : results) {
                    assertThat(page.getPositionCount(), lessThanOrEqualTo(size));
                }
                assertThat(rows(results), equalTo(expectedRows(parents)));
                releaseAll(results);
                assertThat(context.breaker().getUsed(), equalTo(0L));
            }
        }
    }

    public void testReaderGivingFewChildrenAtATimeStillGivesEveryRowOnce() {
        PlannerSettings oneChildPerReadPage = PlannerSettings.DEFAULTS.valuesLoadingJumboSize(ByteSizeValue.ofBytes(1));
        for (int size : new int[] { 1, 2, 5, 64 }) {
            var context = driverContext();
            List<Page> results = new TestDriverRunner().builder(context)
                .input(List.of(parents(context.blockFactory(), 0, 200)))
                .run(factory(size, oneChildPerReadPage).get(context));
            for (Page page : results) {
                assertThat(page.getPositionCount(), lessThanOrEqualTo(size));
            }
            assertThat(rows(results), equalTo(expectedRows(range(0, 200))));
            releaseAll(results);
            assertThat(context.breaker().getUsed(), equalTo(0L));
        }
    }

    public void testRepeatedParentRepeatsItsChildren() {
        int[] parents = { 3, 2, 3, 3, 4, 7, 1, 2 };
        for (int size : new int[] { 1, 2, 4, 64 }) {
            var context = driverContext();
            List<Page> results = new TestDriverRunner().builder(context)
                .input(List.of(parentsAt(context.blockFactory(), parents)))
                .run(factory(size).get(context));
            for (Page page : results) {
                assertThat(page.getPositionCount(), lessThanOrEqualTo(size));
            }
            assertThat(rows(results), equalTo(expectedRows(Arrays.stream(parents).boxed().toList())));
            releaseAll(results);
            assertThat(context.breaker().getUsed(), equalTo(0L));
        }
    }

    public void testBuffersAreReservedOncePerOperator() {
        long small = breakerUsedWhileOperatorIdle(10);
        long large = breakerUsedWhileOperatorIdle(50);
        assertThat(large - small, equalTo(5L * Integer.BYTES * 40));
    }

    /** The bytes the breaker holds once the operator has given back every row of three input pages, which it still holds. */
    private long breakerUsedWhileOperatorIdle(int size) {
        var context = driverContext();
        long[] used = new long[3];
        try (Operator operator = factory(size).get(context)) {
            int[][] ranges = { { 0, 6 }, { 6, 10 }, { 10, 11 } };
            for (int r = 0; r < ranges.length; r++) {
                operator.addInput(parents(context.blockFactory(), ranges[r][0], ranges[r][1] - ranges[r][0]));
                List<Page> results = new ArrayList<>();
                while (operator.canProduceMoreDataWithoutExtraInput()) {
                    results.add(operator.getOutput());
                }
                releaseAll(results);
                used[r] = context.breaker().getUsed();
            }
        }
        assertThat(context.breaker().getUsed(), equalTo(0L));
        assertThat(used[1], equalTo(used[0]));
        assertThat(used[2], equalTo(used[0]));
        return used[0];
    }

    public void testExpansionsOpenOneAtATimeAndBuffersGrowForLargerPages() {
        var context = driverContext();
        Page page = parents(context.blockFactory(), 0, 4);
        DocVector docs = page.<DocBlock>getBlock(0).asVector();
        try (NestedChildren children = factory(2).children().get(context)) {
            long before = context.breaker().getUsed();
            try (NestedChildren.Expansion first = children.expand(docs, 2)) {
                expectThrows(IllegalStateException.class, () -> children.expand(docs, 2));
                assertThat(first.hasNext(), is(true));
                assertThat(context.breaker().getUsed() - before, equalTo(5L * Integer.BYTES * 2));
            }
            try (NestedChildren.Expansion second = children.expand(docs, 2)) {
                assertThat(second.hasNext(), is(true));
                assertThat(context.breaker().getUsed() - before, equalTo(5L * Integer.BYTES * 2));
            }
            try (NestedChildren.Expansion third = children.expand(docs, 8)) {
                assertThat(third.hasNext(), is(true));
                assertThat(context.breaker().getUsed() - before, equalTo(5L * Integer.BYTES * 8));
            }
        }
        page.releaseBlocks();
        assertThat(context.breaker().getUsed(), equalTo(0L));
    }

    public void testEmptyInputPage() {
        var context = driverContext();
        List<Page> results = new TestDriverRunner().builder(context)
            .input(List.of(parentsAt(context.blockFactory())))
            .run(factory(2).get(context));
        assertThat(results, hasSize(0));
        assertThat(context.breaker().getUsed(), equalTo(0L));
    }

    public void testHoldsInputUntilItIsFullyExpanded() {
        var context = driverContext();
        try (Operator operator = factory(2).get(context)) {
            assertThat(operator.needsInput(), is(true));
            assertThat(operator.canProduceMoreDataWithoutExtraInput(), is(false));
            operator.addInput(parents(context.blockFactory(), 0, 6));
            List<Page> results = new ArrayList<>();
            while (operator.canProduceMoreDataWithoutExtraInput()) {
                assertThat(operator.needsInput(), is(false));
                assertThat(operator.isFinished(), is(false));
                results.add(operator.getOutput());
            }
            assertThat(results.stream().map(Page::getPositionCount).toList(), equalTo(List.of(2, 2, 2, 2, 1)));
            assertThat(rows(results), equalTo(expectedRows(range(0, 6))));
            assertThat(operator.needsInput(), is(true));
            AbstractPageMappingToIteratorOperator.Status status = (AbstractPageMappingToIteratorOperator.Status) operator.status();
            assertThat(status.pagesReceived(), equalTo(1));
            assertThat(status.pagesEmitted(), equalTo(5));
            assertThat(status.rowsEmitted(), equalTo(9L));
            operator.addInput(parents(context.blockFactory(), 6, 2));
            operator.finish();
            assertThat(operator.isFinished(), is(false));
            while (operator.isFinished() == false) {
                results.add(operator.getOutput());
            }
            assertThat(rows(results), equalTo(expectedRows(range(0, 8))));
            releaseAll(results);
        }
        assertThat(context.breaker().getUsed(), equalTo(0L));
    }

    public void testCloseWithPartiallyConsumedPageReleasesEverything() {
        var context = driverContext();
        Operator operator = factory(2).get(context);
        operator.addInput(parents(context.blockFactory(), 0, 100));
        Page first = operator.getOutput();
        assertThat(first.getPositionCount(), equalTo(2));
        assertThat(operator.canProduceMoreDataWithoutExtraInput(), is(true));
        first.releaseBlocks();
        operator.close();
        assertThat(context.breaker().getUsed(), equalTo(0L));
    }

    /**
     * The reader gives one child per page, so after the first output page it still holds the rest of the children it
     * was asked for. Closing the operator then must release them too.
     */
    public void testCloseWhileReaderHoldsChildrenReleasesEverything() {
        assumeTrue("the reader only splits children of one segment of one shard", parentsPerSegment() >= 100 && shardCount() == 1);
        PlannerSettings oneChildPerReadPage = PlannerSettings.DEFAULTS.valuesLoadingJumboSize(ByteSizeValue.ofBytes(1));
        var context = driverContext();
        Operator operator = factory(64, oneChildPerReadPage).get(context);
        operator.addInput(parents(context.blockFactory(), 0, 100));
        Page first = operator.getOutput();
        assertThat(first.getPositionCount(), lessThan(64));
        assertThat(operator.canProduceMoreDataWithoutExtraInput(), is(true));
        first.releaseBlocks();
        operator.close();
        assertThat(context.breaker().getUsed(), equalTo(0L));
    }

    private static List<Integer> range(int from, int to) {
        List<Integer> parents = new ArrayList<>();
        for (int i = from; i < to; i++) {
            parents.add(i);
        }
        return parents;
    }

    private static void releaseAll(List<Page> pages) {
        for (Page page : pages) {
            page.releaseBlocks();
        }
    }
}
