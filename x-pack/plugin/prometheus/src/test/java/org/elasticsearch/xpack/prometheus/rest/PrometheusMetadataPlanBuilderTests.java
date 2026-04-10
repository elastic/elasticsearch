/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.prometheus.rest;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.Equals;
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
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;

public class PrometheusMetadataPlanBuilderTests extends ESTestCase {

    private static final Instant START = Instant.ofEpochSecond(1_700_000_000L);
    private static final Instant END = Instant.ofEpochSecond(1_700_003_600L);

    // --- Plan shape without metric filter ---

    public void testNoMetricFilterTopIsLimit() {
        LogicalPlan plan = buildPlan(null, 0, 0);
        Limit limit = as(plan, Limit.class);
        as(limit.child(), Aggregate.class);
    }

    public void testNoMetricFilterAggregateChildIsEval() {
        LogicalPlan plan = buildPlan(null, 0, 0);
        Aggregate agg = as(as(plan, Limit.class).child(), Aggregate.class);
        as(agg.child(), Eval.class);
    }

    public void testNoMetricFilterEvalChildIsMetricsInfo() {
        LogicalPlan plan = buildPlan(null, 0, 0);
        Aggregate agg = as(as(plan, Limit.class).child(), Aggregate.class);
        Eval eval = as(agg.child(), Eval.class);
        as(eval.child(), MetricsInfo.class);
    }

    public void testNoMetricFilterMetricsInfoChildIsFilter() {
        LogicalPlan plan = buildPlan(null, 0, 0);
        Aggregate agg = as(as(plan, Limit.class).child(), Aggregate.class);
        Eval eval = as(agg.child(), Eval.class);
        MetricsInfo metricsInfo = as(eval.child(), MetricsInfo.class);
        as(metricsInfo.child(), Filter.class);
    }

    public void testNoMetricFilterSourceIsUnresolvedRelation() {
        LogicalPlan plan = buildPlan(null, 0, 0);
        Aggregate agg = as(as(plan, Limit.class).child(), Aggregate.class);
        Eval eval = as(agg.child(), Eval.class);
        MetricsInfo metricsInfo = as(eval.child(), MetricsInfo.class);
        Filter filter = as(metricsInfo.child(), Filter.class);
        as(filter.child(), UnresolvedRelation.class);
    }

    // --- Plan shape with metric filter ---

    public void testWithMetricFilterAggregateChildIsEval() {
        LogicalPlan plan = buildPlan("up", 0, 0);
        Aggregate agg = as(as(plan, Limit.class).child(), Aggregate.class);
        as(agg.child(), Eval.class);
    }

    public void testWithMetricFilterEvalChildIsFilter() {
        LogicalPlan plan = buildPlan("up", 0, 0);
        Aggregate agg = as(as(plan, Limit.class).child(), Aggregate.class);
        Eval eval = as(agg.child(), Eval.class);
        as(eval.child(), Filter.class);
    }

    public void testWithMetricFilterFilterChildIsMetricsInfo() {
        LogicalPlan plan = buildPlan("up", 0, 0);
        Aggregate agg = as(as(plan, Limit.class).child(), Aggregate.class);
        Eval eval = as(agg.child(), Eval.class);
        Filter metricFilter = as(eval.child(), Filter.class);
        as(metricFilter.child(), MetricsInfo.class);
    }

    public void testWithMetricFilterMetricsInfoChildIsTimeFilter() {
        LogicalPlan plan = buildPlan("up", 0, 0);
        Aggregate agg = as(as(plan, Limit.class).child(), Aggregate.class);
        Eval eval = as(agg.child(), Eval.class);
        Filter metricFilter = as(eval.child(), Filter.class);
        MetricsInfo metricsInfo = as(metricFilter.child(), MetricsInfo.class);
        as(metricsInfo.child(), Filter.class);
    }

    public void testWithMetricFilterUsesEqualsPredicate() {
        LogicalPlan plan = buildPlan("up", 0, 0);
        Aggregate agg = as(as(plan, Limit.class).child(), Aggregate.class);
        Eval eval = as(agg.child(), Eval.class);
        Filter metricFilter = as(eval.child(), Filter.class);
        as(metricFilter.condition(), Equals.class);
    }

    public void testWithMetricFilterUsesBareMetricNameNoPrefixAdded() {
        LogicalPlan plan = buildPlan("up", 0, 0);
        Aggregate agg = as(as(plan, Limit.class).child(), Aggregate.class);
        Eval eval = as(agg.child(), Eval.class);
        Filter metricFilter = as(eval.child(), Filter.class);
        // The right-hand side of the Equals should be the bare metric name, not "metrics.up"
        Equals equals = as(metricFilter.condition(), Equals.class);
        Literal rightLiteral = as(equals.right(), Literal.class);
        String metricValue = ((BytesRef) rightLiteral.value()).utf8ToString();
        assertThat(metricValue, equalTo("up"));
    }

    // --- Limit calculations ---

    public void testBothLimitsZeroUsesIntMaxValue() {
        Limit limit = as(buildPlan(null, 0, 0), Limit.class);
        assertThat(limit.limit().toString(), containsString(String.valueOf(Integer.MAX_VALUE)));
    }

    public void testOnlyLimitSetUsesIntMaxValue() {
        // limitPerMetric=0 means entries per metric are unbounded, so we can't compute a finite
        // total row count from limit alone
        Limit limit = as(buildPlan(null, 5, 0), Limit.class);
        assertThat(limit.limit().toString(), containsString(String.valueOf(Integer.MAX_VALUE)));
    }

    public void testOnlyLimitPerMetricSetUsesIntMaxValue() {
        Limit limit = as(buildPlan(null, 0, 10), Limit.class);
        assertThat(limit.limit().toString(), containsString(String.valueOf(Integer.MAX_VALUE)));
    }

    public void testBothLimitsSetUsesProduct() {
        Limit limit = as(buildPlan(null, 5, 3), Limit.class);
        // (5 + 1) * 3 = 18
        assertThat(limit.limit().toString(), containsString("18"));
    }

    // --- Aggregate groups by correct columns ---

    public void testAggregateGroupsOnThreeColumns() {
        LogicalPlan plan = buildPlan(null, 0, 0);
        Aggregate agg = as(as(plan, Limit.class).child(), Aggregate.class);
        assertThat(agg.groupings().size(), is(3));
    }

    public void testAggregateGroupingsIncludeMetricName() {
        LogicalPlan plan = buildPlan(null, 0, 0);
        Aggregate agg = as(as(plan, Limit.class).child(), Aggregate.class);
        assertThat(containsAttributeNamed(agg.groupings(), PrometheusMetadataPlanBuilder.METRIC_NAME_FIELD), is(true));
    }

    public void testAggregateGroupingsIncludeMetricType() {
        LogicalPlan plan = buildPlan(null, 0, 0);
        Aggregate agg = as(as(plan, Limit.class).child(), Aggregate.class);
        assertThat(containsAttributeNamed(agg.groupings(), PrometheusMetadataPlanBuilder.METRIC_TYPE_FIELD), is(true));
    }

    public void testAggregateGroupingsIncludeUnit() {
        LogicalPlan plan = buildPlan(null, 0, 0);
        Aggregate agg = as(as(plan, Limit.class).child(), Aggregate.class);
        assertThat(containsAttributeNamed(agg.groupings(), PrometheusMetadataPlanBuilder.UNIT_FIELD), is(true));
    }

    // --- Time filter is present ---

    public void testTimeFilterIsPresent() {
        LogicalPlan plan = buildPlan(null, 0, 0);
        Aggregate agg = as(as(plan, Limit.class).child(), Aggregate.class);
        Eval eval = as(agg.child(), Eval.class);
        MetricsInfo metricsInfo = as(eval.child(), MetricsInfo.class);
        Filter filter = as(metricsInfo.child(), Filter.class);
        assertThat(filter.condition(), instanceOf(Expression.class));
    }

    // --- Custom index ---

    public void testCustomIndexIsUsed() {
        LogicalPlan plan = PrometheusMetadataPlanBuilder.buildPlan("my-index", null, 0, 0, START, END);
        Aggregate agg = as(as(plan, Limit.class).child(), Aggregate.class);
        Eval eval = as(agg.child(), Eval.class);
        MetricsInfo metricsInfo = as(eval.child(), MetricsInfo.class);
        Filter filter = as(metricsInfo.child(), Filter.class);
        UnresolvedRelation relation = as(filter.child(), UnresolvedRelation.class);
        assertThat(relation.toString(), containsString("my-index"));
    }

    // --- Helpers ---

    private static LogicalPlan buildPlan(String metric, int limit, int limitPerMetric) {
        return PrometheusMetadataPlanBuilder.buildPlan("*", metric, limit, limitPerMetric, START, END);
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
}
