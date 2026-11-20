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
import org.elasticsearch.xpack.esql.EsqlTestUtils;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.UnionTypeEsField;
import org.elasticsearch.xpack.esql.planner.PlannerSettings;
import org.hamcrest.Matcher;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

/** One row per parent. Parent {@code i} packs its {@code i % 4} children, or nulls when it has none. */
public class NestedFlattenOperatorTests extends NestedIndexOperatorTestCase {
    @Override
    protected Operator.OperatorFactory simple(SimpleOptions options) {
        return factory(List.of("price", "note"), List.of(DataType.LONG, DataType.TEXT));
    }

    private Operator.OperatorFactory factory(List<String> paths, List<DataType> types) {
        NestedChildren.Factory children = NestedChildren.factory(
            new IndexedByShardIdFromSingleton<>(shard),
            "items",
            paths,
            types,
            Collections.nCopies(paths.size(), (UnionTypeEsField) null),
            PlannerSettings.DEFAULTS,
            EsqlTestUtils.TEST_CFG
        );
        return new NestedFlattenOperator.Factory(0, children, "items", types);
    }

    @Override
    protected void assertSimpleOutput(List<Page> input, List<Page> results) {
        List<Integer> parents = new ArrayList<>();
        for (Page page : input) {
            for (int p = 0; p < page.getPositionCount(); p++) {
                parents.add(parent(page, p));
            }
        }
        assertThat(rows(results), equalTo(expected(parents)));
    }

    /** A parent with more children than one read page still packs every value into its one position. */
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
        List<Integer> parents = new ArrayList<>();
        for (int i = 0; i < PARENTS; i++) {
            parents.add(i);
        }
        List<Page> results = new TestDriverRunner().builder(context)
            .input(List.of(parents(context.blockFactory(), 0, PARENTS)))
            .run(simple().get(context));
        assertThat(rows(results), equalTo(expected(parents)));
        for (Page page : results) {
            page.releaseBlocks();
        }
        assertThat(context.breaker().getUsed(), equalTo(0L));
    }

    @Override
    protected Matcher<String> expectedDescriptionOfSimple() {
        return equalTo("NestedFlattenOperator[path=items]");
    }

    @Override
    protected Matcher<String> expectedToStringOfSimple() {
        return equalTo("NestedFlattenOperator[path=items]");
    }

    private List<List<Object>> rows(List<Page> results) {
        List<List<Object>> actual = new ArrayList<>();
        for (Page page : results) {
            LongBlock prices = page.getBlock(1);
            BytesRefBlock notes = page.getBlock(2);
            for (int p = 0; p < page.getPositionCount(); p++) {
                actual.add(java.util.Arrays.asList(parent(page, p), values(prices, p), values(notes, p)));
            }
        }
        return actual;
    }

    private static Object values(LongBlock block, int position) {
        if (block.isNull(position)) {
            return null;
        }
        List<Long> values = new ArrayList<>();
        int first = block.getFirstValueIndex(position);
        for (int v = first; v < first + block.getValueCount(position); v++) {
            values.add(block.getLong(v));
        }
        return values;
    }

    private static Object values(BytesRefBlock block, int position) {
        if (block.isNull(position)) {
            return null;
        }
        List<String> values = new ArrayList<>();
        int first = block.getFirstValueIndex(position);
        BytesRef scratch = new BytesRef();
        for (int v = first; v < first + block.getValueCount(position); v++) {
            values.add(block.getBytesRef(v, scratch).utf8ToString());
        }
        return values;
    }

    private static List<List<Object>> expected(List<Integer> parents) {
        List<List<Object>> expected = new ArrayList<>();
        for (int parent : parents) {
            if (parent % 4 == 0) {
                expected.add(java.util.Arrays.asList(parent, null, null));
            } else {
                List<Long> prices = new ArrayList<>();
                List<String> notes = new ArrayList<>();
                for (int j = 0; j < parent % 4; j++) {
                    prices.add(10L * parent + j);
                    notes.add("n" + j);
                }
                expected.add(List.of(parent, prices, notes));
            }
        }
        return expected;
    }
}
