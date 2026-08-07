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
import org.elasticsearch.xpack.esql.expression.predicate.nulls.IsNotNull;
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.Limit;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.MetricsInfo;
import org.elasticsearch.xpack.esql.plan.logical.UnresolvedRelation;

import java.time.Instant;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.as;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;

public class PrometheusMetadataPlanBuilderTests extends ESTestCase {

    private static final Instant START = Instant.ofEpochSecond(1_700_000_000L);
    private static final Instant END = Instant.ofEpochSecond(1_700_003_600L);

    // --- Plan shape without metric filter ---

    public void testNoMetricFilterTopIsLimit() {
        LogicalPlan plan = buildPlan(null, 0, 0);
        as(limit(plan).child(), Aggregate.class);
    }

    public void testNoMetricFilterAggregateChildIsEval() {
        LogicalPlan plan = buildPlan(null, 0, 0);
        as(aggregate(plan).child(), Eval.class);
    }

    public void testNoMetricFilterEvalChildIsMetricsInfo() {
        LogicalPlan plan = buildPlan(null, 0, 0);
        as(eval(plan).child(), MetricsInfo.class);
    }

    public void testNoMetricFilterMetricsInfoChildIsFilter() {
        LogicalPlan plan = buildPlan(null, 0, 0);
        as(metricsInfo(plan).child(), Filter.class);
    }

    public void testNoMetricFilterSourceIsUnresolvedRelation() {
        LogicalPlan plan = buildPlan(null, 0, 0);
        as(sourceFilter(plan).child(), UnresolvedRelation.class);
    }

    // --- Plan shape with metric filter ---

    public void testWithMetricFilterAggregateChildIsEval() {
        LogicalPlan plan = buildPlan("up", 0, 0);
        as(aggregate(plan).child(), Eval.class);
    }

    public void testWithMetricFilterEvalChildIsMetricsInfo() {
        LogicalPlan plan = buildPlan("up", 0, 0);
        as(eval(plan).child(), MetricsInfo.class);
    }

    public void testWithMetricFilterMetricsInfoChildIsFilter() {
        LogicalPlan plan = buildPlan("up", 0, 0);
        as(metricsInfo(plan).child(), Filter.class);
    }

    public void testWithMetricFilterFilterChildIsUnresolvedRelation() {
        LogicalPlan plan = buildPlan("up", 0, 0);
        as(sourceFilter(plan).child(), UnresolvedRelation.class);
    }

    public void testWithMetricFilterUsesIsNotNullPredicate() {
        LogicalPlan plan = buildPlan("up", 0, 0);
        assertThat(containsIsNotNullOnAttribute(sourceFilter(plan).condition(), "up"), is(true));
    }

    public void testWithMetricFilterUsesRequestedMetricField() {
        LogicalPlan plan = buildPlan("metrics.up", 0, 0);
        assertThat(containsIsNotNullOnAttribute(sourceFilter(plan).condition(), "metrics.up"), is(true));
    }

    // --- Limit calculations ---

    public void testBothLimitsZeroUsesIntMaxValue() {
        Limit limit = limit(buildPlan(null, 0, 0));
        assertThat(limit.limit().toString(), containsString(String.valueOf(Integer.MAX_VALUE)));
    }

    public void testOnlyLimitSetUsesIntMaxValue() {
        // limitPerMetric=0 means entries per metric are unbounded, so we can't compute a finite
        // total row count from limit alone
        Limit limit = limit(buildPlan(null, 5, 0));
        assertThat(limit.limit().toString(), containsString(String.valueOf(Integer.MAX_VALUE)));
    }

    public void testOnlyLimitPerMetricSetUsesIntMaxValue() {
        Limit limit = limit(buildPlan(null, 0, 10));
        assertThat(limit.limit().toString(), containsString(String.valueOf(Integer.MAX_VALUE)));
    }

    public void testBothLimitsSetUsesProduct() {
        Limit limit = limit(buildPlan(null, 5, 3));
        // (5 + 1) * 3 = 18
        assertThat(limit.limit().toString(), containsString("18"));
    }

    // --- Aggregate groups by correct columns ---

    public void testAggregateGroupsOnThreeColumns() {
        LogicalPlan plan = buildPlan(null, 0, 0);
        assertThat(aggregate(plan).groupings().size(), is(3));
    }

    public void testAggregateGroupingsIncludeMetricName() {
        LogicalPlan plan = buildPlan(null, 0, 0);
        assertThat(containsAttributeNamed(aggregate(plan).groupings(), PrometheusMetadataPlanBuilder.METRIC_NAME_FIELD), is(true));
    }

    public void testAggregateGroupingsIncludeMetricType() {
        LogicalPlan plan = buildPlan(null, 0, 0);
        assertThat(containsAttributeNamed(aggregate(plan).groupings(), PrometheusMetadataPlanBuilder.METRIC_TYPE_FIELD), is(true));
    }

    public void testAggregateGroupingsIncludeUnit() {
        LogicalPlan plan = buildPlan(null, 0, 0);
        assertThat(containsAttributeNamed(aggregate(plan).groupings(), PrometheusMetadataPlanBuilder.UNIT_FIELD), is(true));
    }

    // --- Time filter is present ---

    public void testTimeFilterIsPresent() {
        LogicalPlan plan = buildPlan(null, 0, 0);
        assertThat(sourceFilter(plan).condition(), instanceOf(Expression.class));
    }

    // --- Custom index ---

    public void testCustomIndexIsUsed() {
        LogicalPlan plan = PrometheusMetadataPlanBuilder.buildPlan("my-index", null, 0, 0, START, END);
        UnresolvedRelation relation = as(sourceFilter(plan).child(), UnresolvedRelation.class);
        assertThat(relation.toString(), containsString("my-index"));
    }

    // --- Helpers ---

    private static LogicalPlan buildPlan(String metric, int limit, int limitPerMetric) {
        return PrometheusMetadataPlanBuilder.buildPlan("*", metric, limit, limitPerMetric, START, END);
    }

    private static Limit limit(LogicalPlan plan) {
        return as(plan, Limit.class);
    }

    private static Aggregate aggregate(LogicalPlan plan) {
        return as(limit(plan).child(), Aggregate.class);
    }

    private static Eval eval(LogicalPlan plan) {
        return as(aggregate(plan).child(), Eval.class);
    }

    private static MetricsInfo metricsInfo(LogicalPlan plan) {
        return as(eval(plan).child(), MetricsInfo.class);
    }

    private static Filter sourceFilter(LogicalPlan plan) {
        return as(metricsInfo(plan).child(), Filter.class);
    }

    private static boolean containsAttributeNamed(List<? extends Expression> expressions, String name) {
        for (Expression expr : expressions) {
            Deque<Expression> stack = new ArrayDeque<>();
            stack.push(expr);
            while (stack.isEmpty() == false) {
                Expression current = stack.pop();
                if (current.toString().contains(name)) {
                    return true;
                }
                stack.addAll(current.children());
            }
        }
        return false;
    }

    private static boolean containsIsNotNullOnAttribute(Expression expression, String name) {
        Deque<Expression> stack = new ArrayDeque<>();
        stack.push(expression);
        while (stack.isEmpty() == false) {
            Expression current = stack.pop();
            if (current instanceof IsNotNull isNotNull
                && isNotNull.field() instanceof UnresolvedAttribute attribute
                && name.equals(attribute.name())) {
                return true;
            }
            stack.addAll(current.children());
        }
        return false;
    }
}
