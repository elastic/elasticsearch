/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.nested;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.expression.ExpressionEvaluator;
import org.elasticsearch.compute.lucene.IndexedByShardIdFromSingleton;
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.compute.test.TestDriverRunner;
import org.elasticsearch.xpack.esql.EsqlTestUtils;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.UnionTypeEsField;
import org.elasticsearch.xpack.esql.planner.PlannerSettings;
import org.hamcrest.Matcher;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

/**
 * Packs the children of each parent of a real index into a container column, and expands the column again. The expansion must
 * give the rows that {@link NestedExpandOperator} gives over the index.
 */
public class NestedContainerOperatorTests extends NestedIndexOperatorTestCase {
    private static final List<DataType> TYPES = List.of(DataType.LONG, DataType.TEXT);

    @Override
    protected Operator.OperatorFactory simple(SimpleOptions options) {
        return new NestedContainerPackOperator.Factory(0, children(), TYPES);
    }

    private NestedChildren.Factory children() {
        return NestedChildren.factory(
            new IndexedByShardIdFromSingleton<>(shard),
            "items",
            List.of("price", "note"),
            TYPES,
            Collections.nCopies(2, (UnionTypeEsField) null),
            PlannerSettings.DEFAULTS,
            EsqlTestUtils.TEST_CFG
        );
    }

    @Override
    protected void assertSimpleOutput(List<Page> input, List<Page> results) {
        List<Integer> parents = new ArrayList<>();
        for (Page page : input) {
            for (int p = 0; p < page.getPositionCount(); p++) {
                parents.add(parent(page, p));
            }
        }
        List<Boolean> hasChildren = new ArrayList<>();
        for (Page page : results) {
            BytesRefBlock container = page.getBlock(1);
            for (int p = 0; p < page.getPositionCount(); p++) {
                hasChildren.add(container.isNull(p) == false);
            }
        }
        List<Boolean> expected = parents.stream().map(parent -> parent % 4 != 0).toList();
        assertThat(hasChildren, equalTo(expected));
    }

    public void testExpandingTheContainerGivesTheRowsOfExpandingTheIndex() {
        var context = driverContext();
        List<Page> packed = new TestDriverRunner().builder(context)
            .input(List.of(parents(context.blockFactory(), 0, PARENTS)))
            .run(simple().get(context));
        List<Page> expanded = new TestDriverRunner().builder(context)
            .input(packed)
            .run(new NestedExpandFromContainerOperator.Factory(1, TYPES).get(context));
        List<List<Object>> actual = new ArrayList<>();
        for (Page page : expanded) {
            LongBlock prices = page.getBlock(2);
            BytesRefBlock notes = page.getBlock(3);
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
        List<List<Object>> expected = new ArrayList<>();
        for (int parent = 0; parent < PARENTS; parent++) {
            if (parent % 4 == 0) {
                expected.add(Arrays.asList(parent, null, null));
            }
            for (int j = 0; j < parent % 4; j++) {
                expected.add(List.of(parent, 10L * parent + j, "n" + j));
            }
        }
        assertThat(actual, equalTo(expected));
        for (Page page : expanded) {
            page.releaseBlocks();
        }
        assertThat(context.breaker().getUsed(), equalTo(0L));
    }

    public void testExpandingAConstantNullContainerKeepsEveryParentWithNullChildren() {
        var context = driverContext();
        Page input = new Page(context.blockFactory().newConstantNullBlock(3));
        List<Page> expanded = new TestDriverRunner().builder(context)
            .input(List.of(input))
            .run(new NestedExpandFromContainerOperator.Factory(0, TYPES).get(context));
        assertThat(expanded, hasSize(1));
        Page page = expanded.get(0);
        assertThat(page.getPositionCount(), equalTo(3));
        for (int p = 0; p < 3; p++) {
            assertThat(page.getBlock(1).isNull(p), equalTo(true));
            assertThat(page.getBlock(2).isNull(p), equalTo(true));
        }
        page.releaseBlocks();
        assertThat(context.breaker().getUsed(), equalTo(0L));
    }

    public void testPredicateOverAConstantNullContainerIsFalse() {
        var context = driverContext();
        ExpressionEvaluator container = new ExpressionEvaluator() {
            @Override
            public Block eval(Page page) {
                return context.blockFactory().newConstantNullBlock(page.getPositionCount());
            }

            @Override
            public long baseRamBytesUsed() {
                return 0;
            }

            @Override
            public void close() {}
        };
        ExpressionEvaluator predicate = new ExpressionEvaluator() {
            @Override
            public Block eval(Page page) {
                return context.blockFactory().newConstantBooleanBlockWith(true, page.getPositionCount());
            }

            @Override
            public long baseRamBytesUsed() {
                return 0;
            }

            @Override
            public void close() {}
        };
        Page input = new Page(context.blockFactory().newConstantNullBlock(3));
        try (
            NestedContainerPredicateEvaluator evaluator = new NestedContainerPredicateEvaluator(context, container, predicate, 1);
            Block matches = evaluator.eval(input)
        ) {
            assertThat(matches.getPositionCount(), equalTo(3));
            for (int p = 0; p < 3; p++) {
                assertThat(((BooleanBlock) matches).getBoolean(p), equalTo(false));
            }
        }
        input.releaseBlocks();
        assertThat(context.breaker().getUsed(), equalTo(0L));
    }

    @Override
    protected Matcher<String> expectedDescriptionOfSimple() {
        return equalTo("NestedContainerPackOperator[columns=2]");
    }

    @Override
    protected Matcher<String> expectedToStringOfSimple() {
        return equalTo("NestedContainerPackOperator[columns=2]");
    }
}
