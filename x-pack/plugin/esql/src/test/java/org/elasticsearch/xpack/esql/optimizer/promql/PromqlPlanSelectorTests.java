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
import org.elasticsearch.xpack.esql.expression.predicate.logical.And;
import org.elasticsearch.xpack.esql.expression.predicate.logical.Not;
import org.elasticsearch.xpack.esql.expression.predicate.nulls.IsNotNull;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.In;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.NotEquals;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.TimeSeriesAggregate;

import java.time.Duration;
import java.util.List;
import java.util.Optional;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.as;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

public class PromqlPlanSelectorTests extends AbstractPromqlPlanOptimizerTests {

    public void testRangeSelector() {
        var plan = planPromql("""
            PROMQL index=k8s step=1h ( max by (pod) (last_over_time(network.bytes_in[1h])) )
            """);
        TimeSeriesAggregate tsAggregate = plan.collect(TimeSeriesAggregate.class).getFirst();
        LastOverTime lastOverTime = tsAggregate.aggregates().getFirst().collect(LastOverTime.class).getFirst();
        assertThat(lastOverTime.window().fold(FoldContext.small()), equalTo(Duration.ofHours(1)));
    }

    /**
     * Expect the logical plan structure:
     * Project
     * \_Eval
     *   \_Limit
     *     \_Aggregate
     *       \_Eval
     *         \_TimeSeriesAggregate[[...],[SUM(...,PT10M,...), COUNT(...,PT10M,...), ...], BUCKET(@timestamp,PT5M)]
     */
    public void testRangeSelectorWithDifferentStep() {
        var plan = planPromql("""
            PROMQL index=k8s step=5m sum by (pod) (avg_over_time(events_received[10m]))
            """);

        var tsAggregate = plan.collect(TimeSeriesAggregate.class).getFirst();

        // Verify bucket is 5 minutes
        assertThat(tsAggregate.timeBucket().buckets().fold(FoldContext.small()), equalTo(Duration.ofMinutes(5)));

        // Verify window is 10 minutes
        var sum = tsAggregate.aggregates().getFirst().collect(Sum.class).getFirst();
        assertThat(sum.window().fold(FoldContext.small()), equalTo(Duration.ofMinutes(10)));
    }

    public void testLabelSelector() {
        var plan = planPromql("""
            PROMQL index=k8s step=1m (
                max by (pod) (avg_over_time(network.bytes_in{pod=~"host-0|host-1|host-2"}[5m]))
              )
            """);
        var filters = plan.collect(Filter.class);
        Optional<In> in = filters.stream().map(Filter::condition).filter(In.class::isInstance).map(In.class::cast).findAny();
        assertThat(in.isPresent(), equalTo(true));
        assertThat(in.get().value().sourceText(), equalTo("pod"));
        assertThat(in.get().list().stream().map(Expression::toString).toList(), containsInAnyOrder("host-0", "host-1", "host-2"));
    }

    public void testLabelSelectorPrefix() {
        String testQuery = """
            PROMQL index=k8s step=1m (
                avg by (pod) (avg_over_time(network.bytes_in{pod=~"host-.*"}[5m]))
                )
            """;

        var plan = planPromql(testQuery);
        var filters = plan.collect(Filter.class);
        assertThat(filters.stream().map(Filter::condition).anyMatch(StartsWith.class::isInstance), equalTo(true));
        assertThat(filters.stream().map(Filter::condition).anyMatch(NotEquals.class::isInstance), equalTo(false));
    }

    public void testLabelSelectorProperPrefix() {
        var plan = planPromql("""
            PROMQL index=k8s step=1m (
                avg(avg_over_time(network.bytes_in{pod=~"host-.+"}[1h]))
              )
            """);

        var filters = plan.collect(Filter.class);
        assertThat(filters.stream().anyMatch(f -> f.condition().anyMatch(StartsWith.class::isInstance)), equalTo(true));
        assertThat(filters.stream().anyMatch(f -> f.condition().anyMatch(NotEquals.class::isInstance)), equalTo(true));
    }

    public void testLabelSelectorRegex() {
        var plan = planPromql("""
            PROMQL index=k8s step=1m (
                avg(avg_over_time(network.bytes_in{pod=~"[a-z]+"}[1h]))
              )
            """);

        var filters = plan.collect(Filter.class);
        assertThat(filters.stream().map(Filter::condition).anyMatch(RegexMatch.class::isInstance), equalTo(true));
    }

    public void testLabelSelectorNotEquals() {
        var plan = planPromql("PROMQL index=k8s step=1m avg(network.bytes_in{pod!=\"foo\"})");

        var not = plan.collect(Filter.class)
            .stream()
            .map(Filter::condition)
            .filter(Not.class::isInstance)
            .map(Not.class::cast)
            .findFirst()
            .get();
        var in = as(not.field(), In.class);
        assertThat(as(in.value(), FieldAttribute.class).name(), equalTo("pod"));
        assertThat(in.list(), hasSize(1));
        assertThat(as(as(in.list().getFirst(), Literal.class).value(), BytesRef.class).utf8ToString(), equalTo("foo"));
    }

    public void testLabelSelectorRegexNegation() {
        var plan = planPromql("PROMQL index=k8s step=1m avg(network.bytes_in{pod!~\"f.o\"})");

        var filters = plan.collect(Filter.class);
        var not = filters.stream().map(Filter::condition).filter(Not.class::isInstance).map(Not.class::cast).findFirst().get();
        var rLike = as(not.field(), RLike.class);
        assertThat(as(rLike.field(), FieldAttribute.class).name(), equalTo("pod"));
        assertThat(rLike.pattern().pattern(), equalTo("f.o"));
    }

    public void testLabelSelectors() {
        var plan = planPromql("PROMQL index=k8s step=1m avg(network.bytes_in{pod!=\"foo\",cluster=~\"bar|baz\",region!~\"us-.*\"})");

        var filters = plan.collect(Filter.class);
        var and = filters.stream().map(Filter::condition).filter(And.class::isInstance).map(And.class::cast).findFirst().get();
        if (and.left() instanceof IsNotNull) {
            and = as(and.right(), And.class);
        }
        var left = as(and.left(), And.class);
        var podNotFoo = as(as(left.left(), Not.class).field(), In.class);
        assertThat(podNotFoo.list(), hasSize(1));
        assertThat(as(podNotFoo.list().getFirst(), Literal.class).value(), equalTo(new BytesRef("foo")));

        var clusterInBarBaz = as(left.right(), In.class);
        assertThat(clusterInBarBaz.list(), hasSize(2));
        assertThat(as(clusterInBarBaz.list().get(0), Literal.class).value(), equalTo(new BytesRef("bar")));
        assertThat(as(clusterInBarBaz.list().get(1), Literal.class).value(), equalTo(new BytesRef("baz")));

        var regionNotUs = as(as(and.right(), Not.class).field(), StartsWith.class);
        assertThat(as(regionNotUs.prefix(), Literal.class).value(), equalTo(new BytesRef("us-")));
    }

    public void testGroupByAllInstantSelector() {
        var plan = planPromql("PROMQL index=k8s step=1m network.bytes_in");
        assertThat(plan.output().stream().map(a -> a.name()).toList(), equalTo(List.of("network.bytes_in", "step", "_timeseries")));
    }

    public void testGroupByAllInstantSelectorRate() {
        var plan = planPromql("PROMQL index=k8s step=1m rate=(rate(network.total_bytes_in[1m]))");
        assertThat(plan.output().stream().map(a -> a.name()).toList(), equalTo(List.of("rate", "step", "_timeseries")));
    }

    public void testGroupByAllWithinSeriesAggregate() {
        var plan = planPromql("PROMQL index=k8s step=1m count=(count_over_time(network.bytes_in[1m]))");
        assertThat(plan.output().stream().map(a -> a.name()).toList(), equalTo(List.of("count", "step", "_timeseries")));
    }
}
