/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.prometheus.rest;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.expression.predicate.nulls.IsNotNull;
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.Limit;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.MetricsInfo;
import org.elasticsearch.xpack.esql.plan.logical.OrderBy;
import org.elasticsearch.xpack.esql.plan.logical.UnresolvedRelation;

import java.time.Instant;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;

public class PrometheusLabelValuesPlanBuilderTests extends ESTestCase {

    private static final Instant START = Instant.ofEpochSecond(1_700_000_000L);
    private static final Instant END = Instant.ofEpochSecond(1_700_003_600L);

    public void testNameLabelPlanTopIsOrderByWhenNoLimit() {
        LogicalPlan plan = PrometheusLabelValuesPlanBuilder.buildPlan("__name__", "*", List.of(), START, END, 0);
        assertThat(plan, instanceOf(OrderBy.class));
    }

    public void testNameLabelPlanTopIsLimitWhenLimitSet() {
        LogicalPlan plan = PrometheusLabelValuesPlanBuilder.buildPlan("__name__", "*", List.of(), START, END, 10);
        assertThat(plan, instanceOf(Limit.class));
        assertThat(((Limit) plan).child(), instanceOf(OrderBy.class));
    }

    public void testNameLabelPlanZeroLimitOmitsLimitNode() {
        LogicalPlan plan = PrometheusLabelValuesPlanBuilder.buildPlan("__name__", "*", List.of(), START, END, 0);
        assertThat(plan, instanceOf(OrderBy.class));
    }

    public void testNameLabelPlanLimitSentinelIsLimitPlusOne() {
        LogicalPlan plan = PrometheusLabelValuesPlanBuilder.buildPlan("__name__", "*", List.of(), START, END, 5);
        Limit limit = (Limit) plan;
        // limit node value should be limit + 1 = 6
        assertThat(limit.limit().toString(), containsString("6"));
    }

    public void testNameLabelPlanContainsAggregateUnderOrderBy() {
        LogicalPlan plan = PrometheusLabelValuesPlanBuilder.buildPlan("__name__", "*", List.of(), START, END, 0);
        assertThat(((OrderBy) plan).child(), instanceOf(Aggregate.class));
    }

    public void testNameLabelPlanContainsMetricsInfoUnderAggregate() {
        LogicalPlan plan = PrometheusLabelValuesPlanBuilder.buildPlan("__name__", "*", List.of(), START, END, 0);
        Aggregate agg = (Aggregate) ((OrderBy) plan).child();
        assertThat(agg.child(), instanceOf(MetricsInfo.class));
    }

    public void testNameLabelPlanContainsFilterUnderMetricsInfo() {
        LogicalPlan plan = PrometheusLabelValuesPlanBuilder.buildPlan("__name__", "*", List.of(), START, END, 0);
        Aggregate agg = (Aggregate) ((OrderBy) plan).child();
        MetricsInfo metricsInfo = (MetricsInfo) agg.child();
        assertThat(metricsInfo.child(), instanceOf(Filter.class));
    }

    public void testNameLabelPlanSourceIsUnresolvedRelation() {
        LogicalPlan plan = PrometheusLabelValuesPlanBuilder.buildPlan("__name__", "*", List.of(), START, END, 0);
        Aggregate agg = (Aggregate) ((OrderBy) plan).child();
        MetricsInfo metricsInfo = (MetricsInfo) agg.child();
        Filter filter = (Filter) metricsInfo.child();
        assertThat(filter.child(), instanceOf(UnresolvedRelation.class));
    }

    public void testNameLabelPlanFilterConditionIsNotNull() {
        LogicalPlan plan = PrometheusLabelValuesPlanBuilder.buildPlan("__name__", "*", List.of(), START, END, 0);
        Aggregate agg = (Aggregate) ((OrderBy) plan).child();
        MetricsInfo metricsInfo = (MetricsInfo) agg.child();
        Filter filter = (Filter) metricsInfo.child();
        // Just verify the filter condition is present (non-null) — structural checks above cover the plan shape
        assertThat(filter.condition(), instanceOf(Expression.class));
    }

    public void testRegularLabelPlanTopIsOrderByWhenNoLimit() {
        LogicalPlan plan = PrometheusLabelValuesPlanBuilder.buildPlan("job", "*", List.of(), START, END, 0);
        assertThat(plan, instanceOf(OrderBy.class));
    }

    public void testRegularLabelPlanTopIsLimitWhenLimitSet() {
        LogicalPlan plan = PrometheusLabelValuesPlanBuilder.buildPlan("job", "*", List.of(), START, END, 10);
        assertThat(plan, instanceOf(Limit.class));
        assertThat(((Limit) plan).child(), instanceOf(OrderBy.class));
    }

    public void testRegularLabelPlanZeroLimitOmitsLimitNode() {
        LogicalPlan plan = PrometheusLabelValuesPlanBuilder.buildPlan("job", "*", List.of(), START, END, 0);
        assertThat(plan, instanceOf(OrderBy.class));
    }

    public void testRegularLabelPlanLimitSentinelIsLimitPlusOne() {
        LogicalPlan plan = PrometheusLabelValuesPlanBuilder.buildPlan("job", "*", List.of(), START, END, 7);
        Limit limit = (Limit) plan;
        assertThat(limit.limit().toString(), containsString("8"));
    }

    public void testRegularLabelPlanContainsAggregateUnderOrderBy() {
        LogicalPlan plan = PrometheusLabelValuesPlanBuilder.buildPlan("job", "*", List.of(), START, END, 0);
        assertThat(((OrderBy) plan).child(), instanceOf(Aggregate.class));
    }

    public void testRegularLabelPlanHasNoMetricsInfo() {
        LogicalPlan plan = PrometheusLabelValuesPlanBuilder.buildPlan("job", "*", List.of(), START, END, 0);
        Aggregate agg = (Aggregate) ((OrderBy) plan).child();
        // The child of Aggregate must be Filter (not MetricsInfo)
        assertThat(agg.child(), instanceOf(Filter.class));
    }

    public void testRegularLabelPlanFilterContainsIsNotNull() {
        LogicalPlan plan = PrometheusLabelValuesPlanBuilder.buildPlan("job", "*", List.of(), START, END, 0);
        Aggregate agg = (Aggregate) ((OrderBy) plan).child();
        Filter filter = (Filter) agg.child();
        assertThat("Filter condition must contain an IsNotNull node", containsIsNotNull(filter.condition()), is(true));
    }

    /** Recursively checks whether an expression tree contains an {@link IsNotNull} node. */
    private static boolean containsIsNotNull(Expression expr) {
        if (expr == null) {
            return false;
        }
        if (expr instanceof IsNotNull) {
            return true;
        }
        Deque<Expression> stack = new ArrayDeque<>(expr.children());
        while (stack.isEmpty() == false) {
            Expression current = stack.pop();
            if (current instanceof IsNotNull) {
                return true;
            }
            stack.addAll(current.children());
        }
        return false;
    }

    public void testRegularLabelPlanSourceIsUnresolvedRelation() {
        LogicalPlan plan = PrometheusLabelValuesPlanBuilder.buildPlan("job", "*", List.of(), START, END, 0);
        Aggregate agg = (Aggregate) ((OrderBy) plan).child();
        Filter filter = (Filter) agg.child();
        assertThat(filter.child(), instanceOf(UnresolvedRelation.class));
    }

    public void testEmptyMatchSelectorsAllowed() {
        // Should not throw
        PrometheusLabelValuesPlanBuilder.buildPlan("job", "*", List.of(), START, END, 100);
    }

    public void testInvalidSelectorThrowsIllegalArgument() {
        Exception ex = expectThrows(
            IllegalArgumentException.class,
            () -> PrometheusLabelValuesPlanBuilder.buildPlan("job", "*", List.of("not_a_selector{{{"), START, END, 100)
        );
        assertThat(ex.getMessage(), containsString("Invalid match[] selector"));
    }

    public void testRangeSelectorRejected() {
        Exception ex = expectThrows(
            IllegalArgumentException.class,
            () -> PrometheusLabelValuesPlanBuilder.buildPlan("job", "*", List.of("up[5m]"), START, END, 100)
        );
        assertThat(ex.getMessage(), containsString("instant vector selector"));
    }
}
