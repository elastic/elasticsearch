/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.promql;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.predicate.regex.RegexMatch;
import org.elasticsearch.xpack.esql.expression.function.aggregate.LastOverTime;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Sum;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.StartsWith;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.regex.RLike;
import org.elasticsearch.xpack.esql.expression.predicate.logical.Not;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.Equals;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.In;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.NotEquals;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.TimeSeriesAggregate;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.as;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

public class PromqlPlanSelectorTests extends AbstractPromqlPlanOptimizerTests {

    public void testRangeSelector() {
        var plan = planPromql("PROMQL index=k8s step=1h ( max by (pod) (last_over_time(network.bytes_in[1h])) )");
        var lot = collectInnerLastOverTimes(plan).getFirst();
        assertThat(lot.window().fold(FoldContext.small()), equalTo(Duration.ofHours(1)));
    }

    public void testRangeSelectorWithDifferentStep() {
        var plan = planPromql("PROMQL index=k8s step=5m sum by (pod) (avg_over_time(events_received[10m]))");
        var tsAggregate = plan.collect(TimeSeriesAggregate.class).getFirst();
        assertThat(tsAggregate.timeBucket().buckets().fold(FoldContext.small()), equalTo(Duration.ofMinutes(5)));
        var sum = tsAggregate.aggregates().getFirst().collect(Sum.class).getFirst();
        assertThat(sum.window().fold(FoldContext.small()), equalTo(Duration.ofMinutes(10)));
    }

    public void testLabelSelector() {
        var plan = planPromql("""
            PROMQL index=k8s step=1m (
                max by (pod) (avg_over_time(network.bytes_in{pod=~"host-0|host-1|host-2"}[5m]))
              )
            """);
        var in = collectInnermostSelectorFilter(plan, In.class);
        assertThat(in.value().sourceText(), equalTo("pod"));
        assertThat(in.list().stream().map(Expression::toString).toList(), containsInAnyOrder("host-0", "host-1", "host-2"));
    }

    public void testLabelSelectorPrefix() {
        var plan = planPromql("""
            PROMQL index=k8s step=1m avg by (pod) (avg_over_time(network.bytes_in{pod=~"host-.*"}[5m]))
            """);
        assertTrue(anyFilterContains(plan, StartsWith.class));
        assertFalse(anyFilterContains(plan, NotEquals.class));
    }

    public void testLabelSelectorProperPrefix() {
        var plan = planPromql("""
            PROMQL index=k8s step=1m avg(avg_over_time(network.bytes_in{pod=~"host-.+"}[1h]))
            """);
        assertTrue(anyFilterContains(plan, StartsWith.class));
        assertTrue(anyFilterContains(plan, NotEquals.class));
    }

    public void testLabelSelectorRegex() {
        var plan = planPromql("PROMQL index=k8s step=1m avg(avg_over_time(network.bytes_in{pod=~\"[a-z]+\"}[1h]))");
        assertTrue(anyFilterContains(plan, RegexMatch.class));
    }

    public void testLabelSelectorNotEquals() {
        var plan = planPromql("PROMQL index=k8s step=1m avg(network.bytes_in{pod!=\"foo\"})");
        var not = collectInnermostSelectorFilter(plan, Not.class);
        var in = as(not.field(), In.class);
        assertThat(as(in.value(), FieldAttribute.class).name(), equalTo("pod"));
        assertThat(in.list(), hasSize(1));
        assertThat(as(as(in.list().getFirst(), Literal.class).value(), BytesRef.class).utf8ToString(), equalTo("foo"));
    }

    public void testLabelSelectorRegexNegation() {
        var plan = planPromql("PROMQL index=k8s step=1m avg(network.bytes_in{pod!~\"f.o\"})");
        var not = collectInnermostSelectorFilter(plan, Not.class);
        var rLike = as(not.field(), RLike.class);
        assertThat(as(rLike.field(), FieldAttribute.class).name(), equalTo("pod"));
        assertThat(rLike.pattern().pattern(), equalTo("f.o"));
    }

    public void testLabelSelectors() {
        var plan = planPromql("PROMQL index=k8s step=1m avg(network.bytes_in{pod!=\"foo\",cluster=~\"bar|baz\",region!~\"us-.*\"})");
        assertTrue(anyFilterContains(plan, Not.class));
        assertTrue(anyFilterContains(plan, In.class));
        assertTrue(anyFilterContains(plan, StartsWith.class));
    }

    public void testGroupByAllInstantSelector() {
        assertThat(
            outputColumns(planPromql("PROMQL index=k8s step=1m network.bytes_in")),
            equalTo(List.of("network.bytes_in", "step", "_timeseries"))
        );
    }

    public void testGroupByAllInstantSelectorRate() {
        assertThat(
            outputColumns(planPromql("PROMQL index=k8s step=1m rate=(rate(network.total_bytes_in[1m]))")),
            equalTo(List.of("rate", "step", "_timeseries"))
        );
    }

    public void testGroupByAllWithinSeriesAggregate() {
        assertThat(
            outputColumns(planPromql("PROMQL index=k8s step=1m count=(count_over_time(network.bytes_in[1m]))")),
            equalTo(List.of("count", "step", "_timeseries"))
        );
    }

    public void testSelectorFilterPushedToSource() {
        assertTrue(anyFilterContains(planPromql("PROMQL index=k8s step=1m result=(sum(network.bytes_in{pod=\"p1\"}))"), Equals.class));
    }

    public void testSelectorFilterPushedToSourceThroughUnaryOp() {
        assertTrue(anyFilterContains(planPromql("PROMQL index=k8s step=1m result=(-sum(network.bytes_in{pod=\"p1\"}))"), Equals.class));
    }

    public void testSelectorFilterPushedToSourceThroughScalar() {
        var plan = planPromql("PROMQL index=k8s step=1m result=(scalar(sum by (cluster) (network.bytes_in{cluster=\"prod\"})))");
        assertTrue(anyFilterContains(plan, Equals.class));
    }

    public void testBinaryOpConflictingSelectors() {
        var plan = planPromql("PROMQL index=k8s step=1m ratio=(sum(network.bytes_in{pod=\"p1\"}) / sum(network.bytes_in{pod=\"p2\"}))");
        var lots = collectInnerLastOverTimes(plan);
        assertThat(lots, hasSize(2));
        assertTrue(lots.stream().allMatch(LastOverTime::hasFilter));
        assertFalse(anyFilterContains(plan, Equals.class));
    }

    public void testBinaryOpDifferentLabelKeys() {
        var plan = planPromql("PROMQL index=k8s step=1m ratio=(sum(network.bytes_in{cluster=\"c1\"}) / sum(network.bytes_in{pod=\"p1\"}))");
        var lots = collectInnerLastOverTimes(plan);
        assertThat(lots, hasSize(2));
        assertTrue(lots.stream().allMatch(LastOverTime::hasFilter));
        assertFalse(anyFilterContains(plan, Equals.class));
    }

    public void testBinaryOpSelectorOnOneSideOnly() {
        var plan = planPromql("PROMQL index=k8s step=1m ratio=(sum(network.bytes_in{pod=\"p1\"}) / sum(network.bytes_in))");
        var lots = collectInnerLastOverTimes(plan);
        assertThat(lots, hasSize(2));
        assertThat(lots.stream().filter(LastOverTime::hasFilter).count(), equalTo(1L));
        assertFalse(anyFilterContains(plan, Equals.class));
    }

    public void testBinaryOpConflictingSelectorsInFunction() {
        var plan = planPromql("PROMQL index=k8s step=1m r=(ceil(sum(network.bytes_in{pod=\"p1\"}) / sum(network.bytes_in{pod=\"p2\"})))");
        var lots = collectInnerLastOverTimes(plan);
        assertThat(lots, hasSize(2));
        assertTrue(lots.stream().allMatch(LastOverTime::hasFilter));
        assertFalse(anyFilterContains(plan, Equals.class));
    }

    private static List<String> outputColumns(LogicalPlan plan) {
        return plan.output().stream().map(a -> a.name()).toList();
    }

    private static List<LastOverTime> collectInnerLastOverTimes(LogicalPlan plan) {
        return plan.collect(TimeSeriesAggregate.class)
            .stream()
            .flatMap(tsa -> tsa.aggregates().stream())
            .flatMap(ne -> ne.collect(LastOverTime.class).stream())
            .toList();
    }

    private static List<Filter> collectSelectorFilters(LogicalPlan plan) {
        List<Filter> result = new ArrayList<>();
        plan.forEachDown(Filter.class, f -> {
            if (f.child() instanceof EsRelation) {
                result.add(f);
            }
        });
        return result;
    }

    private static boolean anyFilterContains(LogicalPlan plan, Class<? extends Expression> type) {
        return collectSelectorFilters(plan).stream().anyMatch(f -> f.condition().anyMatch(type::isInstance));
    }

    private static <T extends Expression> T collectInnermostSelectorFilter(LogicalPlan plan, Class<T> type) {
        return collectSelectorFilters(plan).stream()
            .flatMap(f -> f.condition().collect(type).stream())
            .findFirst()
            .orElseThrow(() -> new AssertionError("No " + type.getSimpleName() + " in source filters"));
    }
}
