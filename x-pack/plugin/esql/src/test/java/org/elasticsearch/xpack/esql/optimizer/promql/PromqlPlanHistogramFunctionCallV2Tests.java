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
import org.elasticsearch.xpack.esql.expression.function.aggregate.PromqlHistogramQuantile;
import org.elasticsearch.xpack.esql.expression.function.grouping.TimeSeriesWithout;
import org.elasticsearch.xpack.esql.expression.function.scalar.histogram.HistogramPercentile;
import org.elasticsearch.xpack.esql.expression.function.scalar.timeseries.TimeSeriesMetadataFilter;
import org.elasticsearch.xpack.esql.expression.function.scalar.timeseries.TimeSeriesMetadataPath;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;

import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.as;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.not;

/** PromQL histogram functions translated over a label container. */
public class PromqlPlanHistogramFunctionCallV2Tests extends AbstractPromqlPlanOptimizerTests {

    public PromqlPlanHistogramFunctionCallV2Tests(VersionMode versionMode) {
        super(versionMode);
    }

    /** A classic histogram reads each bucket's bound off {@code le} and groups the buckets by their other labels. */
    public void testClassicHistogramReadsTheBoundOffLe() {
        LogicalPlan plan = planClassic("histogram_quantile(0.9, sum by (job, le) (request_duration_seconds_bucket))", false);

        PromqlHistogramQuantile quantile = expressions(plan, PromqlHistogramQuantile.class).getFirst();
        assertThat(as(quantile.upperBound(), Attribute.class).name(), equalTo("le"));
        assertThat(expressions(plan, TimeSeriesMetadataPath.class), empty());
        assertThat(names(plan.output()), hasItem("job"));
        assertThat(names(plan.output()), not(hasItem("le")));
    }

    /** Over raw buckets the result keeps the series' labels but {@code le} and the metric name. */
    public void testClassicHistogramOverSeriesLeavesLeAndTheNameOut() {
        LogicalPlan plan = planClassic("histogram_quantile(0.9, rate(request_duration_seconds_bucket[5m]))", false);

        PromqlHistogramQuantile quantile = expressions(plan, PromqlHistogramQuantile.class).getFirst();
        assertThat(as(quantile.upperBound(), Attribute.class).name(), equalTo("le"));
        // the index has no __name__ field: the source's view only leaves le out
        TimeSeriesWithout source = expressions(plan, TimeSeriesWithout.class).getFirst();
        assertThat(names(source.children().stream().map(Attribute.class::cast).toList()), equalTo(List.of("le")));
        assertThat(expressions(plan, TimeSeriesMetadataFilter.class), empty());
        assertThat(names(plan.output()), hasItem("_timeseries"));
    }

    /** An input without {@code le} evaluates to nothing, with a warning like Prometheus. */
    public void testClassicHistogramWithoutLeWarns() {
        LogicalPlan plan = planClassic("histogram_quantile(0.9, sum by (job) (request_duration_seconds_bucket))", false);
        assertTrue(plan.resolved());
        assertWarnings("histogram_quantile: input vector has no le label; no buckets to evaluate");
    }

    /** A native histogram is a value transformation over each series. */
    public void testNativeHistogramIsAValueTransformation() {
        LogicalPlan plan = withPromqlTranslationV2(
            analyzerWithEnrichPolicies().addIndex("exp_histo", "exp_histo_sample-mappings.json", IndexMode.TIME_SERIES)
        ).query("PROMQL index=exp_histo step=1m histogram_quantile(0.9, responseTime)");

        assertThat(expressions(plan, HistogramPercentile.class), hasSize(1));
        assertThat(expressions(plan, PromqlHistogramQuantile.class), empty());
        assertThat(names(plan.output()), hasItem("_timeseries"));
    }

    /** The translated histogram functions plan through the optimizer. */
    public void testHistogramsOptimize() {
        for (String query : List.of(
            "histogram_quantile(0.9, sum by (job, le) (request_duration_seconds_bucket))",
            "histogram_quantile(0.9, rate(request_duration_seconds_bucket[5m]))",
            "histogram_fraction(0, 1, sum by (job, le) (rate(request_duration_seconds_bucket[5m])))"
        )) {
            LogicalPlan plan = planClassic(query, true);
            assertTrue(query, plan.resolved());
        }
    }

    private LogicalPlan planClassic(String query, boolean optimize) {
        LogicalPlan analyzed = withPromqlTranslationV2(
            analyzerWithEnrichPolicies().addIndex("prom_hist", "mapping-promql-classic-histogram.json", IndexMode.TIME_SERIES)
        ).query("PROMQL index=prom_hist step=1m " + query);
        return optimize ? logicalOptimizer.optimize(analyzed) : analyzed;
    }

    private static List<String> names(List<Attribute> attributes) {
        return attributes.stream().map(Attribute::name).toList();
    }

    private static <E extends Expression> List<E> expressions(LogicalPlan plan, Class<E> type) {
        var found = new ArrayList<E>();
        plan.forEachExpressionDown(type, found::add);
        return found;
    }
}
