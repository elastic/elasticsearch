/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.promql;

import org.elasticsearch.common.Rounding;
import org.elasticsearch.xpack.esql.VerificationException;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expressions;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Rate;
import org.elasticsearch.xpack.esql.expression.function.grouping.Bucket;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.GreaterThanOrEqual;
import org.elasticsearch.xpack.esql.parser.ParsingException;
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.Project;
import org.elasticsearch.xpack.esql.plan.logical.TimeSeriesAggregate;
import org.elasticsearch.xpack.esql.plan.logical.TopN;
import org.elasticsearch.xpack.esql.plan.logical.local.EmptyLocalSupplier;
import org.elasticsearch.xpack.esql.plan.logical.local.LocalRelation;

import java.time.Duration;
import java.time.Instant;
import java.util.List;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.as;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.sameInstance;

public class PromqlEsqlCommandTests extends AbstractPromqlPlanOptimizerTests {

    public void testPromqlTrailingSpaces() {
        planPromql("PROMQL index=k8s step=1h (max(network.bytes_in)) ");
        planPromql("PROMQL index=k8s step=1h (max(network.bytes_in)) | SORT step");
    }

    public void testPromqlMaxOfLongField() {
        var plan = planPromql("PROMQL index=k8s step=1h max(network.bytes_in)");
        // In PromQL, the output is always double
        assertThat(plan.output().getFirst().dataType(), equalTo(DataType.DOUBLE));
        assertThat(plan.output().getFirst().name(), equalTo("max(network.bytes_in)"));
    }

    public void testPromqlExplicitOutputName() {
        var plan = planPromql("PROMQL index=k8s step=1h max_bytes=(max(network.bytes_in))");
        assertThat(plan.output().getFirst().name(), equalTo("max_bytes"));
    }

    public void testSort() {
        var plan = planPromql("""
            PROMQL index=k8s step=1h (
                avg(network.bytes_in) by (pod)
              )
            | SORT step, pod, `avg(network.bytes_in) by (pod)`
            """);
        List<String> order = plan.collect(TopN.class)
            .getFirst()
            .order()
            .stream()
            .map(o -> as(o.child(), NamedExpression.class).name())
            .toList();
        assertThat(order, hasSize(3));
        assertThat(order, equalTo(List.of("step", "pod", "avg(network.bytes_in) by (pod)")));
    }

    public void testNonExistentFieldsOptimizesToEmptyPlan() {
        List.of("non_existent_metric", "network.eth0.rx{non_existent_label=\"value\"}", "avg(non_existent_metric)"
        // TODO because we wrap group-by-all aggregates into Values, this does not optimize away yet
        // "rate(non_existent_metric[5m])"
        ).forEach(query -> {
            var plan = planPromql("PROMQL index=k8s step=1m " + query);
            assertThat(as(plan, LocalRelation.class).supplier(), equalTo(EmptyLocalSupplier.EMPTY));
        });
    }

    public void testGroupByStepCollision() {
        // "step" as a BY label collides with the built-in step output column.
        // If this proves too restrictive, we could add an option to rename the built-in step column.
        for (String query : List.of(
            "PROMQL index=k8s step=1m result=(sum by (step) (network.eth0.rx))",
            "PROMQL index=k8s step=1m result=(sum by (step, pod) (network.eth0.rx))"
        )) {
            var e = expectThrows(VerificationException.class, () -> planPromql(query));
            assertThat(e.getMessage(), containsString("label [step] collides with the built-in [step] output column"));
        }
    }

    public void testGroupByNonExistentLabel() {
        var plan = planPromql("PROMQL index=k8s step=1m result=(sum by (non_existent_label) (network.eth0.rx))");
        // equivalent to avg(network.eth0.rx) since the label does not exist
        assertThat(plan.output().stream().map(Attribute::name).toList(), equalTo(List.of("result", "step")));
        // the non-existent label should not appear in the groupings
        plan.collect(Aggregate.class)
            .forEach(
                agg -> assertThat(
                    agg.groupings().stream().map(Attribute.class::cast).map(Attribute::name).toList(),
                    not(hasItem("non_existent_label"))
                )
            );
    }

    public void testAvgAvgOverTimeOutput() {
        var plan = planPromql("""
            PROMQL index=k8s step=1h ( avg by (pod) (avg_over_time(network.bytes_in{pod=~"host-0|host-1|host-2"}[1h])) )
            | LIMIT 1000
            """);

        var project = as(plan, Project.class);
        assertThat(project.projections(), hasSize(3));

        var aggregate = plan.collect(Aggregate.class).getFirst();
        assertThat(aggregate.groupings(), hasSize(2));

        var evalMiddle = as(aggregate.child(), Eval.class);

        var tsAggregate = as(evalMiddle.child(), TimeSeriesAggregate.class);
        assertThat(tsAggregate.groupings(), hasSize(2));

        // verify TBUCKET duration plus reuse
        var evalBucket = as(tsAggregate.child(), Eval.class);
        assertThat(evalBucket.fields(), hasSize(1));
        var bucketAlias = as(evalBucket.fields().get(0), Alias.class);
        var bucket = as(bucketAlias.child(), Bucket.class);

        var bucketSpan = bucket.buckets();
        assertThat(bucketSpan.fold(FoldContext.small()), equalTo(Duration.ofHours(1)));

        var tbucketId = bucketAlias.toAttribute().id();
        assertThat(Expressions.attribute(tsAggregate.groupings().get(1)).id(), equalTo(tbucketId));
        assertThat(Expressions.attribute(aggregate.groupings().get(0)).id(), equalTo(tbucketId));
        assertThat(Expressions.attribute(project.projections().get(1)).id(), equalTo(tbucketId));

        // Filter should contain: IN(host-0, host-1, host-2, pod)
        var filter = as(evalBucket.child(), Filter.class);
        var in = as(filter.condition(), org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.In.class);
        assertThat(in.list(), hasSize(3));

        as(filter.child(), EsRelation.class);
    }

    public void testImplicitRangeSelectorUsesStepWindow() {
        var plan = planPromql("""
            PROMQL index=k8s step=5m rate=(rate(network.total_bytes_in))
            """);

        TimeSeriesAggregate tsAggregate = plan.collect(TimeSeriesAggregate.class).getFirst();
        Rate rate = tsAggregate.aggregates().getFirst().collect(Rate.class).getFirst();
        assertThat(rate.window().fold(FoldContext.small()), equalTo(Duration.ofMinutes(5)));
    }

    public void testImplicitRangeSelectorUsesScrapeIntervalWhenStepIsSmaller() {
        var plan = planPromql("""
            PROMQL index=k8s step=15s rate=(rate(network.total_bytes_in))
            """);

        TimeSeriesAggregate tsAggregate = plan.collect(TimeSeriesAggregate.class).getFirst();
        Rate rate = tsAggregate.aggregates().getFirst().collect(Rate.class).getFirst();
        assertThat(rate.window().fold(FoldContext.small()), equalTo(Duration.ofMinutes(1)));
    }

    public void testImplicitRangeSelectorRoundsWindowToStepMultiple() {
        var plan = planPromql("""
            PROMQL index=k8s step=20s scrape_interval=1m rate=(rate(network.total_bytes_in))
            """);

        TimeSeriesAggregate tsAggregate = plan.collect(TimeSeriesAggregate.class).getFirst();
        Rate rate = tsAggregate.aggregates().getFirst().collect(Rate.class).getFirst();
        assertThat(rate.window().fold(FoldContext.small()), equalTo(Duration.ofMinutes(1)));
    }

    public void testImplicitRangeSelectorUsesInferredStepFromDefaultBuckets() {
        var plan = planPromql("""
            PROMQL index=k8s start="2024-05-10T00:00:00.000Z" end="2024-05-10T01:00:00.000Z" rate=(rate(network.total_bytes_in))
            """);

        TimeSeriesAggregate tsAggregate = plan.collect(TimeSeriesAggregate.class).getFirst();
        assertThat(tsAggregate.timeBucket().buckets().fold(FoldContext.small()), equalTo(Duration.ofMinutes(1)));

        Rate rate = tsAggregate.aggregates().getFirst().collect(Rate.class).getFirst();
        assertThat(rate.window().fold(FoldContext.small()), equalTo(Duration.ofMinutes(1)));
    }

    public void testImplicitRangeSelectorUsesInferredStepFromBuckets() {
        var plan = planPromql("""
            PROMQL index=k8s start="2024-05-10T00:00:00.000Z" end="2024-05-10T01:00:00.000Z" buckets=6 rate=(rate(network.total_bytes_in))
            """);

        TimeSeriesAggregate tsAggregate = plan.collect(TimeSeriesAggregate.class).getFirst();
        assertThat(tsAggregate.timeBucket().buckets().fold(FoldContext.small()), equalTo(Duration.ofMinutes(10)));

        Rate rate = tsAggregate.aggregates().getFirst().collect(Rate.class).getFirst();
        assertThat(rate.window().fold(FoldContext.small()), equalTo(Duration.ofMinutes(10)));
    }

    public void testStartEndStep() {
        String testQuery = """
            PROMQL index=k8s start=$now-1h end=$now step=5m (
                avg(avg_over_time(network.bytes_in[5m]))
                )
            """;

        var plan = planPromql(testQuery);
        var filters = plan.collect(Filter.class);
        assertThat(
            filters.stream()
                .map(Filter::condition)
                .flatMap(c -> c.collect(FieldAttribute.class).stream())
                .map(FieldAttribute::name)
                .filter("@timestamp"::equals)
                .count(),
            equalTo(2L)
        );
    }

    public void testInferredStepUsesDefaultBuckets() {
        var plan = planPromql("""
            PROMQL index=k8s start="2024-05-10T00:00:00.000Z" end="2024-05-10T01:00:00.000Z" (
                avg(avg_over_time(network.bytes_in[6m]))
              )
            """);
        TimeSeriesAggregate tsAggregate = plan.collect(TimeSeriesAggregate.class).getFirst();
        assertThat(tsAggregate.timeBucket().buckets().fold(FoldContext.small()), equalTo(Duration.ofMinutes(1)));
    }

    public void testInferredStepMinStepIsUnknownParameter() {
        ParsingException e = assertThrows(ParsingException.class, () -> planPromql("""
            PROMQL index=k8s start="2024-05-10T00:00:00.000Z" end="2024-05-10T01:00:00.000Z" min_step=1s (
                avg(avg_over_time(network.bytes_in[6m]))
              )
            """));
        assertThat(e.getMessage(), containsString("Unknown parameter [min_step]"));
    }

    public void testInferredStepUsesBuckets() {
        var plan = planPromql("""
            PROMQL index=k8s start="2024-05-10T00:00:00.000Z" end="2024-05-10T01:00:00.000Z" buckets=6 (
                avg(avg_over_time(network.bytes_in[1h]))
              )
            """);
        TimeSeriesAggregate tsAggregate = plan.collect(TimeSeriesAggregate.class).getFirst();
        assertThat(tsAggregate.timeBucket().buckets().fold(FoldContext.small()), equalTo(Duration.ofMinutes(10)));
    }

    public void testUsesTStepBucketWhenHasTimeRange() {
        var plan = planPromql("""
            PROMQL index=k8s start="2024-05-10T00:20:00.000Z" end="2024-05-10T00:25:00.000Z" step=5m (
                avg_over_time(network.bytes_in[5m])
            )
            """);
        TimeSeriesAggregate tsAgg = plan.collect(TimeSeriesAggregate.class).getFirst();
        // timeBucket() comes from TStep.timeBucketSpecRef() — duration must equal the step
        assertThat(tsAgg.timeBucket().buckets().fold(FoldContext.small()), equalTo(Duration.ofMinutes(5)));
        // start=00:20 is a multiple of 5m so offset must be zero
        assertThat(tsAgg.timeBucket().offset(), equalTo(0L));
    }

    public void testTimestampFilterExtendsStartByWindow() {
        Instant start = Instant.parse("2024-05-10T00:20:00.000Z");
        Instant end = Instant.parse("2024-05-10T00:25:00.000Z");
        Duration window = Duration.ofMinutes(5);
        var plan = planPromql(
            "PROMQL index=k8s start=\"" + start + "\" end=\"" + end + "\" step=5m sum(avg_over_time(network.bytes_in[5m]))"
        );
        long extendedStartMs = start.toEpochMilli() - window.toMillis();
        boolean found = plan.collect(Filter.class)
            .stream()
            .anyMatch(
                f -> f.condition()
                    .collect(GreaterThanOrEqual.class)
                    .stream()
                    .anyMatch(gte -> gte.right() instanceof Literal lit && lit.value() instanceof Long ms && ms == extendedStartMs)
            );
        assertTrue("expected a filter lower bound of start - window = " + Instant.ofEpochMilli(extendedStartMs), found);
    }

    public void testRangeQueryStepBucketUsesUpperRoundingConfiguration() {
        var plan = planPromql("""
            PROMQL index=k8s step=2m start="2024-05-10T00:15:00.000Z" end="2024-05-10T00:25:00.000Z"
                rate_bytes_in=(avg by (cluster) (rate(network.total_bytes_in[2m])))
            """);
        TimeSeriesAggregate tsAggregate = plan.collect(TimeSeriesAggregate.class).getFirst();
        assertThat(tsAggregate.timeBucket().roundingConfiguration(), equalTo(Rounding.RoundingConvention.UP));
        assertThat(tsAggregate.outputTimeBucket().roundingConfiguration(), equalTo(Rounding.RoundingConvention.UP));

        Rounding timeBucketUnprepared = tsAggregate.timeBucket().getDateRoundingOrNull(FoldContext.small()).getUnprepared();
        Rounding outputTimeBucketUnprepared = tsAggregate.outputTimeBucket().getDateRoundingOrNull(FoldContext.small()).getUnprepared();
        assertThat(timeBucketUnprepared, instanceOf(Rounding.ToUpperRounding.class));
        assertThat(outputTimeBucketUnprepared, instanceOf(Rounding.ToUpperRounding.class));
        assertThat(Rounding.ToUpperRounding.createRounding(timeBucketUnprepared), sameInstance(timeBucketUnprepared));
        assertThat(Rounding.ToUpperRounding.createRounding(outputTimeBucketUnprepared), sameInstance(outputTimeBucketUnprepared));
    }
}
