/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.expression.function.scalar.nested;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.lucene.IndexedByShardIdFromSingleton;
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.compute.test.TestDriverRunner;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.UnionTypeEsField;
import org.elasticsearch.xpack.esql.planner.PlannerSettings;
import org.hamcrest.Matcher;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

public class NestedExpandOperatorTests extends NestedIndexOperatorTestCase {
    @Override
    protected Operator.OperatorFactory simple(SimpleOptions options) {
        NestedChildren.Factory children = NestedChildren.factory(
            new IndexedByShardIdFromSingleton<>(shard),
            "items",
            List.of("price", "note"),
            List.of(DataType.LONG, DataType.TEXT),
            Collections.nCopies(2, (UnionTypeEsField) null),
            PlannerSettings.DEFAULTS
        );
        return new NestedExpandOperator.Factory(0, children, "items", List.of(DataType.LONG, DataType.TEXT));
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
    }

    /**
     * One page of every parent has more children than {@link NestedChildren#PAGE_SIZE}, so the operator gets them in several
     * pages, and the children of one parent start on one page and end on the next.
     */
    public void testChildrenSpanReadPages() {
        int children = 0;
        boolean straddles = false;
        for (int i = 0; i < PARENTS; i++) {
            straddles |= children < NestedChildren.PAGE_SIZE && NestedChildren.PAGE_SIZE < children + i % 4;
            children += i % 4;
        }
        assertThat(children, greaterThan(NestedChildren.PAGE_SIZE));
        assertThat("a parent's children span two pages", straddles, equalTo(true));

        var context = driverContext();
        List<Page> results = new TestDriverRunner().builder(context)
            .input(List.of(parents(context.blockFactory(), 0, PARENTS)))
            .run(simple().get(context));
        List<Integer> parents = new ArrayList<>();
        for (int i = 0; i < PARENTS; i++) {
            parents.add(i);
        }
        assertThat(rows(results), equalTo(expectedRows(parents)));
        for (Page page : results) {
            page.releaseBlocks();
        }
        assertThat(context.breaker().getUsed(), equalTo(0L));
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
            PlannerSettings.DEFAULTS
        );
        int parents = 20;
        Page input = parents(context.blockFactory(), 0, parents);
        List<Page> results = new TestDriverRunner().builder(context)
            .input(List.of(input))
            .run(new NestedExpandOperator.Factory(0, children, "items", List.of()).get(context));
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
}
