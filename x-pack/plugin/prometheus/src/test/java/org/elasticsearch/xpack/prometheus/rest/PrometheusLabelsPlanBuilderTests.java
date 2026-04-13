/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.prometheus.rest;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.UnresolvedAttribute;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.GreaterThanOrEqual;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.LessThanOrEqual;
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.Limit;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.MvExpand;
import org.elasticsearch.xpack.esql.plan.logical.OrderBy;
import org.elasticsearch.xpack.esql.plan.logical.TsInfo;
import org.elasticsearch.xpack.esql.plan.logical.UnresolvedRelation;

import java.time.Instant;
import java.util.List;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

public class PrometheusLabelsPlanBuilderTests extends ESTestCase {

    private static final Instant START = Instant.ofEpochSecond(1_700_000_000L);
    private static final Instant END = Instant.ofEpochSecond(1_700_003_600L);

    public void testBuildPlanZeroLimitTopIsLimitWithMaxValue() {
        // limit=0 means "unlimited": an explicit Limit(Integer.MAX_VALUE) is emitted so that ESQL's
        // AddImplicitLimit rule sees an existing node and does not inject its own default.
        LogicalPlan plan = PrometheusLabelsPlanBuilder.buildPlan("*", List.of("up"), START, END, 0);
        assertThat(plan, instanceOf(Limit.class));
        assertThat(((Limit) plan).child(), instanceOf(OrderBy.class));
        assertThat(((Limit) plan).limit().fold(null), is(Integer.MAX_VALUE));
    }

    public void testBuildPlanWithLimitTopIsLimitPlusOneSentinel() {
        // limit>0 uses limit+1 as sentinel so the response listener can detect truncation.
        LogicalPlan plan = PrometheusLabelsPlanBuilder.buildPlan("*", List.of("up"), START, END, 100);
        assertThat(plan, instanceOf(Limit.class));
        assertThat(((Limit) plan).child(), instanceOf(OrderBy.class));
        assertThat(((Limit) plan).limit().fold(null), is(101));
    }

    public void testBuildPlanContainsAggregateUnderOrderBy() {
        LogicalPlan plan = PrometheusLabelsPlanBuilder.buildPlan("*", List.of("up"), START, END, 0);
        OrderBy orderBy = (OrderBy) ((Limit) plan).child();
        assertThat(orderBy.child(), instanceOf(Aggregate.class));
    }

    public void testBuildPlanContainsMvExpandUnderAggregate() {
        LogicalPlan plan = PrometheusLabelsPlanBuilder.buildPlan("*", List.of("up"), START, END, 0);
        Aggregate agg = (Aggregate) ((OrderBy) ((Limit) plan).child()).child();
        assertThat(agg.child(), instanceOf(MvExpand.class));
    }

    public void testBuildPlanContainsTsInfoUnderMvExpand() {
        LogicalPlan plan = PrometheusLabelsPlanBuilder.buildPlan("*", List.of("up"), START, END, 0);
        Aggregate agg = (Aggregate) ((OrderBy) ((Limit) plan).child()).child();
        MvExpand mvExpand = (MvExpand) agg.child();
        assertThat(mvExpand.child(), instanceOf(TsInfo.class));
    }

    public void testBuildPlanContainsFilterUnderTsInfo() {
        LogicalPlan plan = PrometheusLabelsPlanBuilder.buildPlan("*", List.of("up"), START, END, 0);
        assertThat(findFilter(plan), notNullValue());
    }

    public void testBuildPlanFilterUnderTsInfoAboveUnresolvedRelation() {
        LogicalPlan plan = PrometheusLabelsPlanBuilder.buildPlan("*", List.of("up"), START, END, 0);
        Filter filter = findFilter(plan);
        assertThat(filter.child(), instanceOf(UnresolvedRelation.class));
    }

    public void testBuildPlanEmptyMatchSelectorsIsAllowed() {
        LogicalPlan plan = PrometheusLabelsPlanBuilder.buildPlan("*", List.of(), START, END, 0);
        assertThat(plan, notNullValue());
    }

    public void testBuildPlanEmptyMatchSelectorsHasOnlyTimeCondition() {
        LogicalPlan plan = PrometheusLabelsPlanBuilder.buildPlan("*", List.of(), START, END, 0);
        Filter filter = findFilter(plan);
        assertThat(containsExpressionOfType(filter.condition(), GreaterThanOrEqual.class), is(true));
        assertThat(containsExpressionOfType(filter.condition(), LessThanOrEqual.class), is(true));
    }

    public void testBuildPlanFilterContainsTimeRangeCondition() {
        LogicalPlan plan = PrometheusLabelsPlanBuilder.buildPlan("*", List.of("up"), START, END, 0);
        Filter filter = findFilter(plan);
        assertThat(containsExpressionOfType(filter.condition(), GreaterThanOrEqual.class), is(true));
        assertThat(containsExpressionOfType(filter.condition(), LessThanOrEqual.class), is(true));
    }

    public void testBuildPlanRejectsRangeSelector() {
        IllegalArgumentException ex = expectThrows(
            IllegalArgumentException.class,
            () -> PrometheusLabelsPlanBuilder.buildPlan("*", List.of("up[5m]"), START, END, 0)
        );
        assertThat(ex.getMessage(), containsString("instant vector selector"));
    }

    public void testBuildPlanRejectsAggregationExpression() {
        IllegalArgumentException ex = expectThrows(
            IllegalArgumentException.class,
            () -> PrometheusLabelsPlanBuilder.buildPlan("*", List.of("sum(up)"), START, END, 0)
        );
        assertThat(ex.getMessage(), containsString("sum(up)"));
    }

    public void testBuildPlanRejectsInvalidSelectorSyntax() {
        IllegalArgumentException ex = expectThrows(
            IllegalArgumentException.class,
            () -> PrometheusLabelsPlanBuilder.buildPlan("*", List.of("{not valid!!!}"), START, END, 0)
        );
        assertThat(ex.getMessage(), containsString("Invalid match[] selector"));
    }

    public void testBuildPlanSingleSelectorFiltersOnMatcherField() {
        LogicalPlan plan = PrometheusLabelsPlanBuilder.buildPlan("*", List.of("{job=\"prometheus\"}"), START, END, 0);
        Filter filter = findFilter(plan);
        assertThat(containsAttribute(filter.condition(), "job"), is(true));
    }

    public void testBuildPlanMultipleSelectorsAreCombinedWithOr() {
        LogicalPlan plan = PrometheusLabelsPlanBuilder.buildPlan("*", List.of("up", "node_cpu_seconds_total"), START, END, 0);
        Filter filter = findFilter(plan);
        assertThat(containsAttribute(filter.condition(), "up"), is(true));
        assertThat(containsAttribute(filter.condition(), "node_cpu_seconds_total"), is(true));
    }

    /** Walks UnaryPlan nodes to find the innermost Filter. */
    private static Filter findFilter(LogicalPlan plan) {
        LogicalPlan current = plan;
        while (current instanceof org.elasticsearch.xpack.esql.plan.logical.UnaryPlan unary) {
            if (current instanceof Filter filter) {
                return filter;
            }
            current = unary.child();
        }
        throw new AssertionError("No Filter node found in plan: " + plan);
    }

    private static boolean containsAttribute(Expression expr, String name) {
        if (expr instanceof UnresolvedAttribute attr && name.equals(attr.name())) {
            return true;
        }
        for (Expression child : expr.children()) {
            if (containsAttribute(child, name)) {
                return true;
            }
        }
        return false;
    }

    private static boolean containsExpressionOfType(Expression expr, Class<? extends Expression> type) {
        if (type.isInstance(expr)) {
            return true;
        }
        for (Expression child : expr.children()) {
            if (containsExpressionOfType(child, type)) {
                return true;
            }
        }
        return false;
    }
}
