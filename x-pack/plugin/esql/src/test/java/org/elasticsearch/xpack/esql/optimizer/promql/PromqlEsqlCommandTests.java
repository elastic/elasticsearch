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
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.Add;
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

        // verify bucket duration plus reuse
        var evalBucket = as(tsAggregate.child(), Eval.class);
        // bucket alias + ToDouble(network.bytes_in) extracted from the avg surrogate's Sum
        assertThat(evalBucket.fields(), hasSize(2));
        var bucketAlias = as(evalBucket.fields().get(0), Alias.class);

        var bucketSpan = tsAggregate.timeBucket().buckets();
        assertThat(bucketSpan.fold(FoldContext.small()), equalTo(Duration.ofHours(1)));

        var tbucketId = bucketAlias.toAttribute().id();
        assertThat(Expressions.attribute(tsAggregate.groupings().get(1)).id(), equalTo(tbucketId));
        assertThat(Expressions.attribute(aggregate.groupings().get(0)).id(), equalTo(tbucketId));
        assertThat(Expressions.attribute(project.projections().get(1)).id(), equalTo(tbucketId));

        // Filter should contain: IN(host-0, host-1, host-2, pod) AND the unbounded timestamp range
        var filter = as(evalBucket.child(), Filter.class);
        var in = filter.condition()
            .collect(org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.In.class)
            .stream()
            .findFirst()
            .orElseThrow();
        assertThat(in.list(), hasSize(3));

        as(filter.child(), EsRelation.class);
    }

    /**
     * Regression test for the {@code date_nanos} variant of the "Output has changed" failure surfaced by the PromQL
     * generative tests (e.g. {@code PROMQL index=datenanos-k8s step=1h ...(avg_over_time(...[1h]))}); see
     * <a href="https://github.com/elastic/elasticsearch/issues/146923">#146923</a>.
     * <p>
     * The built-in {@code step} column is a {@code TStep} time bucket over {@code @timestamp}, so on a
     * {@code date_nanos} {@code @timestamp} index the bucket is naturally {@code date_nanos}. The {@code step} column,
     * however, is always declared as {@code datetime} (epoch-millis). When the produced type does not match the
     * declared type, {@link #planPromql(String)} (which runs the {@code LogicalPlanOptimizer} and hence its
     * post-optimization output verifier) throws a {@code datetime -> date_nanos} {@code VerificationException}. The
     * {@code step} column must be exposed as {@code datetime} regardless of the index timestamp resolution.
     */
    public void testDateNanosIndexStepColumnIsDatetime() {
        var plan = planPromql("PROMQL index=datenanos-k8s step=1h avg=(avg_over_time(network.eth0.tx{cluster!=\"qa\"}[1h]))");

        var step = plan.output().stream().filter(a -> a.name().equals("step")).findFirst().orElseThrow();
        assertThat(step.dataType(), equalTo(DataType.DATETIME));
    }

    /** The same query over a plain {@code date} {@code @timestamp} index keeps the {@code step} column as {@code datetime}. */
    public void testDatetimeIndexStepColumnIsDatetime() {
        var plan = planPromql("PROMQL index=k8s step=1h avg=(avg_over_time(network.eth0.tx{cluster!=\"qa\"}[1h]))");

        var step = plan.output().stream().filter(a -> a.name().equals("step")).findFirst().orElseThrow();
        assertThat(step.dataType(), equalTo(DataType.DATETIME));
    }

    /**
     * Same guarantee as {@link #testDateNanosIndexStepColumnIsDatetime()} but through the top-level {@code or} (union)
     * path, whose {@code step} column is built separately from the single-branch path. This path is not reachable by
     * the generative tests (which never emit {@code or}), so it is covered explicitly here.
     */
    public void testDateNanosIndexUnionStepColumnIsDatetime() {
        var plan = planPromql(
            "PROMQL index=datenanos-k8s step=1h "
                + "u=(max by (cluster) (network.total_bytes_in{cluster=\"prod\"}) or max by (cluster) (network.total_bytes_in))"
        );

        var step = plan.output().stream().filter(a -> a.name().equals("step")).findFirst().orElseThrow();
        assertThat(step.dataType(), equalTo(DataType.DATETIME));
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
        // timeBucket() comes from TStep.surrogate() — duration must equal the step
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
        assertHasTimestampLowerBound(plan, extendedStartMs, "window");
    }

    private void assertHasTimestampLowerBound(
        org.elasticsearch.xpack.esql.plan.logical.LogicalPlan plan,
        long expectedLowerBoundMs,
        String windowName
    ) {
        boolean found = plan.collect(Filter.class)
            .stream()
            .anyMatch(
                f -> f.condition()
                    .collect(GreaterThanOrEqual.class)
                    .stream()
                    .anyMatch(gte -> gte.right() instanceof Literal lit && lit.value() instanceof Long ms && ms == expectedLowerBoundMs)
            );
        assertTrue("expected a filter lower bound of start - " + windowName + " = " + Instant.ofEpochMilli(expectedLowerBoundMs), found);
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

    public void testOffsetShiftsTimestampForward() {
        Instant start = Instant.parse("2024-05-10T00:20:00.000Z");
        Instant end = Instant.parse("2024-05-10T00:25:00.000Z");
        Duration window = Duration.ofMinutes(5);
        Duration offset = Duration.ofMinutes(5);
        var plan = planPromql(
            "PROMQL index=k8s start=\"" + start + "\" end=\"" + end + "\" step=5m sum(avg_over_time(network.bytes_in[5m] offset 5m))"
        );
        // `offset 5m` evaluates a sample at real time `s` as if it occurred at `s + 5m`: a materialized @timestamp + 5m.
        assertThat(findTimestampShiftDuration(plan), equalTo(offset));
        // The source window extends further back by the offset: start - window - offset.
        long lowerBoundMs = start.toEpochMilli() - window.toMillis() - offset.toMillis();
        assertHasTimestampLowerBound(plan, lowerBoundMs, "window+offset");
    }

    public void testNegativeOffsetShiftsTimestampBackward() {
        Instant start = Instant.parse("2024-05-10T00:20:00.000Z");
        Instant end = Instant.parse("2024-05-10T00:25:00.000Z");
        Duration window = Duration.ofMinutes(5);
        Duration signedOffset = Duration.ofMinutes(-5);
        var plan = planPromql(
            "PROMQL index=k8s start=\"" + start + "\" end=\"" + end + "\" step=5m sum(avg_over_time(network.bytes_in[5m] offset -5m))"
        );
        // `offset -5m` (look ahead) shifts @timestamp by a negative duration.
        assertThat(findTimestampShiftDuration(plan), equalTo(signedOffset));
        // start - (window + (-5m)) = start - 0 = start
        long lowerBoundMs = start.toEpochMilli() - window.toMillis() - signedOffset.toMillis();
        assertHasTimestampLowerBound(plan, lowerBoundMs, "window+offset");
    }

    /** Finds the duration of the materialized {@code @timestamp + offset} shift produced for an offset selector. */
    private Duration findTimestampShiftDuration(org.elasticsearch.xpack.esql.plan.logical.LogicalPlan plan) {
        return plan.collect(Eval.class)
            .stream()
            .flatMap(e -> e.fields().stream())
            .map(Alias::child)
            .filter(Add.class::isInstance)
            .map(Add.class::cast)
            .filter(add -> add.left() instanceof FieldAttribute fa && fa.name().equals("@timestamp"))
            .map(add -> (Duration) ((Literal) add.right()).value())
            .findFirst()
            .orElseThrow(() -> new AssertionError("no materialized @timestamp offset shift found in plan:\n" + plan));
    }
}
