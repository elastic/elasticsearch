/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical.promql;

import org.elasticsearch.index.IndexMode;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.EsqlTestUtils;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.UnresolvedTimestamp;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.expression.Order;
import org.elasticsearch.xpack.esql.expression.promql.function.PromqlBuiltinFunctionDefinitions;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.OrderBy;
import org.elasticsearch.xpack.esql.plan.logical.TimeSeriesCollapse;
import org.elasticsearch.xpack.esql.plan.logical.promql.PromqlCommand;
import org.elasticsearch.xpack.esql.plan.logical.promql.SortFunction;

import java.time.Duration;
import java.time.Instant;
import java.util.List;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.analyzer;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.as;

public class AddPromqlResultOrderTests extends ESTestCase {

    public void testWrapsTimeSeriesCollapseForInstantSort() {
        EsRelation relation = EsqlTestUtils.relation(IndexMode.TIME_SERIES);
        PromqlCommand cmd = command(sort(relation), true);
        TimeSeriesCollapse collapse = collapse(cmd);

        LogicalPlan out = new AddPromqlResultOrder().apply(collapse, analyzer().buildContext());
        OrderBy orderBy = as(out, OrderBy.class);
        assertSame(collapse, orderBy.child());
        assertSortByValueAsc(orderBy, cmd);
    }

    public void testWrapsBarePromqlCommandForInstantSort() {
        EsRelation relation = EsqlTestUtils.relation(IndexMode.TIME_SERIES);
        PromqlCommand cmd = command(sort(relation), true);

        LogicalPlan out = new AddPromqlResultOrder().apply(cmd, analyzer().buildContext());
        OrderBy orderBy = as(out, OrderBy.class);
        assertSame(cmd, orderBy.child());
        assertSortByValueAsc(orderBy, cmd);
    }

    public void testRangeCollapseIsUnchanged() {
        EsRelation relation = EsqlTestUtils.relation(IndexMode.TIME_SERIES);
        PromqlCommand cmd = command(sort(relation), false);
        TimeSeriesCollapse collapse = collapse(cmd);

        assertSame(collapse, new AddPromqlResultOrder().apply(collapse, analyzer().buildContext()));
        assertWarnings("sort: ordering is discarded for range queries");
    }

    public void testRangeBareCommandIsUnchanged() {
        EsRelation relation = EsqlTestUtils.relation(IndexMode.TIME_SERIES);
        PromqlCommand cmd = command(sort(relation), false);

        assertSame(cmd, new AddPromqlResultOrder().apply(cmd, analyzer().buildContext()));
        assertWarnings("sort: ordering is discarded for range queries");
    }

    public void testNonOrderingCommandIsUnchanged() {
        EsRelation relation = EsqlTestUtils.relation(IndexMode.TIME_SERIES);
        PromqlCommand cmd = command(relation, true);
        TimeSeriesCollapse collapse = collapse(cmd);

        assertSame(collapse, new AddPromqlResultOrder().apply(collapse, analyzer().buildContext()));
        assertSame(cmd, new AddPromqlResultOrder().apply(cmd, analyzer().buildContext()));
    }

    private static void assertSortByValueAsc(OrderBy orderBy, PromqlCommand cmd) {
        assertFalse(orderBy.child() instanceof Eval);
        assertEquals(1, orderBy.order().size());
        Order order = orderBy.order().getFirst();
        assertEquals(Order.OrderDirection.ASC, order.direction());
        assertEquals(Order.NullsPosition.LAST, order.nullsPosition());
        assertTrue(order.child().semanticEquals(cmd.output().getFirst()));
    }

    public void testWrapsTimeSeriesCollapseForInstantSortDesc() {
        EsRelation relation = EsqlTestUtils.relation(IndexMode.TIME_SERIES);
        SortFunction sortDesc = new SortFunction(Source.EMPTY, relation, PromqlBuiltinFunctionDefinitions.SORT_DESC, List.of());
        PromqlCommand cmd = command(sortDesc, true);
        TimeSeriesCollapse collapse = collapse(cmd);

        LogicalPlan out = new AddPromqlResultOrder().apply(collapse, analyzer().buildContext());
        OrderBy orderBy = as(out, OrderBy.class);
        Eval eval = as(orderBy.child(), Eval.class);
        assertSame(collapse, eval.child());
        assertEquals(1, eval.fields().size());
        var nanKey = eval.fields().getFirst();
        assertTrue(nanKey.synthetic());
        assertEquals(Attribute.rawTemporaryName("promql_sort", "nan"), nanKey.name());
        assertEquals(2, orderBy.order().size());
        Order nanOrder = orderBy.order().getFirst();
        assertEquals(Order.OrderDirection.ASC, nanOrder.direction());
        assertEquals(Order.NullsPosition.LAST, nanOrder.nullsPosition());
        assertTrue(nanOrder.child().semanticEquals(nanKey.toAttribute()));
        Order valueOrder = orderBy.order().get(1);
        assertEquals(Order.OrderDirection.DESC, valueOrder.direction());
        assertEquals(Order.NullsPosition.LAST, valueOrder.nullsPosition());
        assertTrue(valueOrder.child().semanticEquals(cmd.output().getFirst()));
    }

    private static SortFunction sort(LogicalPlan child) {
        return new SortFunction(Source.EMPTY, child, PromqlBuiltinFunctionDefinitions.SORT, List.of());
    }

    private static TimeSeriesCollapse collapse(PromqlCommand cmd) {
        return new TimeSeriesCollapse(Source.EMPTY, cmd, cmd.valueAttribute(), cmd.stepAttribute(), List.of());
    }

    private static PromqlCommand command(LogicalPlan promqlPlan, boolean instant) {
        EsRelation relation = EsqlTestUtils.relation(IndexMode.TIME_SERIES);
        Literal time = Literal.dateTime(Source.EMPTY, Instant.parse("2024-05-10T00:03:00Z"));
        Literal step = instant ? Literal.NULL : Literal.timeDuration(Source.EMPTY, Duration.ofHours(1));
        return new PromqlCommand(
            Source.EMPTY,
            relation,
            promqlPlan,
            time,
            time,
            step,
            Literal.NULL,
            Literal.timeDuration(Source.EMPTY, Duration.ofSeconds(15)),
            "value",
            new UnresolvedTimestamp(Source.EMPTY)
        );
    }
}
