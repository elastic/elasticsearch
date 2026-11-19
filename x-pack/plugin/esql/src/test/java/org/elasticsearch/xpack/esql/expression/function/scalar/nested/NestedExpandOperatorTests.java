/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.expression.function.scalar.nested;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.DocBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.lucene.IndexedByShardIdFromSingleton;
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
            new IndexedByShardIdFromSingleton<>(shard),
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
            new IndexedByShardIdFromSingleton<>(shard),
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

    /** One page of the parents at {@code indexes}, in that order. */
    private Page parentsAt(BlockFactory blockFactory, int... indexes) {
        try (DocBlock.Builder builder = DocBlock.newBlockBuilder(blockFactory, indexes.length)) {
            for (int i : indexes) {
                builder.appendShard(0).appendSegment(0).appendDoc(parentDoc(i));
            }
            return new Page(builder.build());
        }
    }
}
