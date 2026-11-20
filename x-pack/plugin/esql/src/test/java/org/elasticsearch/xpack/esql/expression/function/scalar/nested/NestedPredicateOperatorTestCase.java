/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.nested;

import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.expression.ExpressionEvaluator;
import org.elasticsearch.compute.lucene.IndexedByShardIdFromSingleton;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.xpack.esql.EsqlTestUtils;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.evaluator.EvalMapper;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.GreaterThan;
import org.elasticsearch.xpack.esql.planner.Layout;
import org.elasticsearch.xpack.esql.planner.PlannerSettings;
import org.hamcrest.Matcher;

import java.util.List;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.startsWith;

/**
 * Runs a {@link NestedPredicate} over the parents of a real nested index as an {@code EVAL}: a parent matches when one of its
 * {@code i % 4} children, priced {@code 10 * i + j}, costs more than {@link #THRESHOLD}. The operator tests the cranky breaker
 * through the inherited tests, so a predicate that leaks a block on a failure is caught.
 */
abstract class NestedPredicateOperatorTestCase extends NestedIndexOperatorTestCase {
    static final long THRESHOLD = 5_000L;
    static final ReferenceAttribute PRICE = new ReferenceAttribute(Source.EMPTY, "items.price", DataType.LONG);

    /** The predicate under test, built on the parent page whose block 0 is the parents' documents. */
    abstract NestedPredicate predicate();

    /** Whether parent {@code i} is expected to match. */
    abstract boolean matches(int parent);

    static Expression priceAbove(Expression bound) {
        return new GreaterThan(Source.EMPTY, PRICE, bound);
    }

    static Expression priceAboveThreshold() {
        return priceAbove(new Literal(Source.EMPTY, THRESHOLD, DataType.LONG));
    }

    static NestedPredicate nested(Expression body, QueryBuilder query) {
        return new NestedPredicate(Source.EMPTY, "items", body, List.of(PRICE), List.of("price"), query, EsqlTestUtils.TEST_CFG);
    }

    @Override
    protected Operator.OperatorFactory simple(SimpleOptions options) {
        Layout layout = new Layout.Builder().append(new ReferenceAttribute(Source.EMPTY, "_doc", DataType.DOC_DATA_TYPE)).build();
        ExpressionEvaluator.Factory evaluator = EvalMapper.toEvaluator(
            FoldContext.small(),
            predicate(),
            layout,
            new IndexedByShardIdFromSingleton<>(shard),
            null,
            PlannerSettings.DEFAULTS
        );
        return new EvalOperator.EvalOperatorFactory(evaluator);
    }

    @Override
    protected void assertSimpleOutput(List<Page> input, List<Page> results) {
        int position = 0;
        for (Page result : results) {
            BooleanBlock flags = result.getBlock(result.getBlockCount() - 1);
            for (int p = 0; p < result.getPositionCount(); p++) {
                assertThat(
                    "parent " + parent(result, p),
                    flags.getBoolean(flags.getFirstValueIndex(p)),
                    equalTo(matches(parent(result, p)))
                );
                position++;
            }
        }
        assertThat(position, equalTo(input.stream().mapToInt(Page::getPositionCount).sum()));
    }

    @Override
    protected Matcher<String> expectedDescriptionOfSimple() {
        return startsWith("EvalOperator");
    }

    @Override
    protected Matcher<String> expectedToStringOfSimple() {
        return startsWith("EvalOperator");
    }

    /** The highest price of parent {@code i}, or -1 where it has no children. */
    static long highestPrice(int parent) {
        return parent % 4 == 0 ? -1L : 10L * parent + parent % 4 - 1;
    }
}
