/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.promql;

import org.elasticsearch.index.IndexMode;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.MetadataAttribute;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.aggregate.PrometheusHistogramQuantile;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Sum;
import org.elasticsearch.xpack.esql.expression.predicate.logical.And;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.GreaterThanOrEqual;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.LessThanOrEqual;
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.TimeSeriesAggregate;

import java.time.Instant;
import java.util.List;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.as;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.not;

public class PromqlHistogramQuantileTests extends AbstractPromqlPlanOptimizerTests {

    public void testHistogramQuantileWithoutLeAfterAggregationResolves() {
        LogicalPlan plan = planHistogramPromql(
            "PROMQL index=prom_hist step=1m result=(histogram_quantile(0.9, sum by (job) (request_duration_seconds_bucket)))"
        );
        assertTrue(plan.resolved());
        assertWarnings("histogram_quantile: input vector has no le label; no buckets to evaluate");
    }

    public void testHistogramQuantileDropsLeFromExplicitGroupingOutput() {
        assertThat(
            outputColumns(
                planHistogramPromql(
                    "PROMQL index=prom_hist step=1m result=(histogram_quantile(0.9, sum by (job, le) (request_duration_seconds_bucket)))"
                )
            ),
            equalTo(List.of("result", "step", "job"))
        );
    }

    public void testHistogramQuantileLowersToPrometheusHistogramQuantileAggregate() {
        LogicalPlan translated = planHistogramPromql(
            "PROMQL index=prom_hist step=1m result=(histogram_quantile(0.9, sum by (job, le) (request_duration_seconds_bucket)))",
            false
        );
        List<PrometheusHistogramQuantile> quantiles = collectQuantiles(translated);

        assertThat(quantiles, hasSize(1));
        assertThat(quantiles.getFirst().upperBound().dataType(), equalTo(DataType.KEYWORD));
        // The upper bound is a synthetic internal column carrying the raw `le` bucket label.
        assertThat(quantiles.getFirst().upperBound().collect(Attribute.class).getFirst().name(), containsString("le"));
        assertThat(as(quantiles.getFirst().quantile().fold(FoldContext.small()), Double.class), equalTo(0.9));
        // The bucket count is always coerced to double, since the aggregator only consumes double counts.
        assertThat(quantiles.getFirst().field().dataType(), equalTo(DataType.DOUBLE));
    }

    public void testHistogramQuantileCastsLongBucketCountsToDouble() {
        // request_count_bucket is a long-typed counter; the lowered aggregate must still receive double counts.
        LogicalPlan translated = planHistogramPromql(
            "PROMQL index=prom_hist step=1m result=(histogram_quantile(0.9, request_count_bucket))",
            false
        );
        List<PrometheusHistogramQuantile> quantiles = collectQuantiles(translated);

        assertThat(quantiles, hasSize(1));
        assertThat(quantiles.getFirst().field().dataType(), equalTo(DataType.DOUBLE));
    }

    public void testHistogramQuantileWithRateWidensTimestampFilterForRangeLookback() {
        LogicalPlan translated = planHistogramPromql(
            "PROMQL index=prom_hist step=5m start=\"2024-05-10T00:10:00Z\" end=\"2024-05-10T00:10:00Z\" "
                + "result=(histogram_quantile(0.9, sum by (job, le) (rate(request_duration_seconds_bucket[5m]))))",
            false
        );
        Filter sourceFilter = translated.collect(Filter.class)
            .stream()
            .filter(filter -> filter.child() instanceof EsRelation)
            .findFirst()
            .orElseThrow();
        And condition = as(sourceFilter.condition(), And.class);

        Object lowerBound = comparisonValue(condition, GreaterThanOrEqual.class);
        Object upperBound = comparisonValue(condition, LessThanOrEqual.class);

        assertThat(lowerBound, equalTo(Instant.parse("2024-05-10T00:05:00Z").toEpochMilli()));
        assertThat(upperBound, equalTo(Instant.parse("2024-05-10T00:10:00Z").toEpochMilli()));
    }

    public void testSumHistogramQuantileUsesOuterAggregateOverPrometheusHistogramQuantile() {
        LogicalPlan translated = planHistogramPromql(
            "PROMQL index=prom_hist step=5m start=\"2024-05-10T00:10:00Z\" end=\"2024-05-10T00:10:00Z\" "
                + "result=(sum(histogram_quantile(0.5, rate(request_duration_seconds_bucket[5m]))) by (job))",
            false
        );

        var outerAggs = translated.collect(Aggregate.class)
            .stream()
            .filter(aggregate -> aggregate instanceof TimeSeriesAggregate == false)
            .toList();
        // histogram_quantile over rate and sum(...) by (job) each add an outer Aggregate.
        assertThat(outerAggs, hasSize(2));
        assertThat(
            outerAggs.stream().anyMatch(aggregate -> aggregate.aggregates().stream().anyMatch(agg -> agg.anyMatch(Sum.class::isInstance))),
            equalTo(true)
        );

        assertThat(collectQuantiles(translated), hasSize(1));
    }

    public void testHistogramQuantileRateWithoutLePreservesTimeseriesOutput() {
        LogicalPlan plan = planHistogramPromqlNoLe(
            "PROMQL index=prom_hist_no_le step=5m start=\"2024-05-10T00:10:00Z\" end=\"2024-05-10T00:10:00Z\" "
                + "result=(histogram_quantile(0.9, rate(request_duration_seconds_bucket[5m])))"
        );
        assertTrue(plan.resolved());
        assertWarnings("histogram_quantile: input vector has no le label; no buckets to evaluate");
        assertThat(plan.output().stream().map(Attribute::name).toList(), hasItem(MetadataAttribute.TIMESERIES));
    }

    public void testHistogramQuantileRateDropsPrometheusLeLabelFromOutput() {
        List<String> columns = outputColumns(
            planHistogramPromqlPrometheusLabels(
                "PROMQL index=prom_hist_prom step=1m result=(histogram_quantile(0.5, rate(request_duration_seconds_bucket[1m])))"
            )
        );
        assertThat(columns, not(hasItem("le")));
        assertThat(columns, not(hasItem("labels.le")));
    }

    public void testHistogramQuantilePrometheusPlanOutputDropsLe() {
        List<String> labels = outputColumns(
            planHistogramPromqlPrometheusLabels(
                "PROMQL index=prom_hist_prom step=1m result=(histogram_quantile(0.5, rate(request_duration_seconds_bucket[1m])))",
                false
            )
        );
        assertThat(labels, not(hasItem("le")));
        assertThat(labels, not(hasItem("labels.le")));
    }

    public void testSumHistogramQuantileByIntegrationResolves() {
        LogicalPlan plan = planHistogramPromqlPrometheusLabels(
            "PROMQL index=prom_hist_prom step=5m start=\"2024-05-10T00:10:00Z\" end=\"2024-05-10T00:10:00Z\" "
                + "value=(sum(histogram_quantile(0.9, rate(request_duration_seconds_bucket[5m]))) by (integration))"
        );
        assertTrue(plan.resolved());
    }

    public void testHistogramQuantileWithoutLeUsesEvalAliasForUpperBound() {
        LogicalPlan translated = planHistogramPromqlNoLe(
            "PROMQL index=prom_hist_no_le step=5m result=(histogram_quantile(0.9, rate(request_duration_seconds_bucket[5m])))",
            false
        );
        assertWarnings("histogram_quantile: input vector has no le label; no buckets to evaluate");
        // rate(...) is already aggregated, so histogram_quantile lowers into a regular Aggregate.
        PrometheusHistogramQuantile quantile = collectQuantiles(translated).getFirst();

        assertThat(quantile.upperBound().collect(Attribute.class), not(empty()));
        assertThat(quantile.upperBound().collect(Literal.class), empty());
    }

    private LogicalPlan planHistogramPromql(String query) {
        return planHistogramPromql(query, true);
    }

    private LogicalPlan planHistogramPromql(String query, boolean optimize) {
        LogicalPlan analyzed = analyzerWithEnrichPolicies().addIndex(
            "prom_hist",
            "mapping-promql-classic-histogram.json",
            IndexMode.TIME_SERIES
        ).query(query);
        return optimize ? logicalOptimizer.optimize(analyzed) : analyzed;
    }

    private LogicalPlan planHistogramPromqlNoLe(String query) {
        return planHistogramPromqlNoLe(query, true);
    }

    private LogicalPlan planHistogramPromqlNoLe(String query, boolean optimize) {
        LogicalPlan analyzed = analyzerWithEnrichPolicies().addIndex(
            "prom_hist_no_le",
            "mapping-promql-histogram-no-le.json",
            IndexMode.TIME_SERIES
        ).query(query);
        return optimize ? logicalOptimizer.optimize(analyzed) : analyzed;
    }

    private LogicalPlan planHistogramPromqlPrometheusLabels(String query) {
        return planHistogramPromqlPrometheusLabels(query, true);
    }

    private LogicalPlan planHistogramPromqlPrometheusLabels(String query, boolean optimize) {
        LogicalPlan analyzed = analyzerWithEnrichPolicies().addIndex(
            "prom_hist_prom",
            "mapping-promql-classic-histogram-prometheus-labels.json",
            IndexMode.TIME_SERIES
        ).query(query);
        return optimize ? logicalOptimizer.optimize(analyzed) : analyzed;
    }

    private static List<PrometheusHistogramQuantile> collectQuantiles(LogicalPlan plan) {
        return plan.collect(Aggregate.class)
            .stream()
            .flatMap(aggregate -> aggregate.aggregates().stream())
            .flatMap(namedExpression -> namedExpression.collect(PrometheusHistogramQuantile.class).stream())
            .toList();
    }

    private static List<String> outputColumns(LogicalPlan plan) {
        return plan.output().stream().map(Attribute::name).toList();
    }

    private static Object comparisonValue(And condition, Class<?> comparisonType) {
        return comparisonRight(condition, comparisonType).fold(FoldContext.small());
    }

    private static Expression comparisonRight(And condition, Class<?> comparisonType) {
        for (Expression expression : List.of(condition.left(), condition.right())) {
            if (comparisonType == GreaterThanOrEqual.class && expression instanceof GreaterThanOrEqual greaterThanOrEqual) {
                return greaterThanOrEqual.right();
            }
            if (comparisonType == LessThanOrEqual.class && expression instanceof LessThanOrEqual lessThanOrEqual) {
                return lessThanOrEqual.right();
            }
        }
        throw new AssertionError("Missing comparison " + comparisonType.getSimpleName());
    }
}
