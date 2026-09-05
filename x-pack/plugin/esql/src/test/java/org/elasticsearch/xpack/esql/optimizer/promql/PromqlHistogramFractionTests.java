/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.promql;

import org.elasticsearch.index.IndexMode;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.MetadataAttribute;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.aggregate.PromqlHistogramFraction;
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;

import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

public class PromqlHistogramFractionTests extends AbstractPromqlPlanOptimizerTests {

    public void testClassicHistogramFractionLowersToAggregateAndDropsLe() {
        LogicalPlan translated = planClassic(
            "PROMQL index=prom_hist step=1m result=(histogram_fraction(0.5, 1.5, " + "sum by (job, le) (request_duration_seconds_bucket)))",
            false
        );
        List<PromqlHistogramFraction> fractions = collectFractions(translated);

        assertThat(fractions, hasSize(1));
        assertThat(fractions.getFirst().field().dataType(), equalTo(DataType.DOUBLE));
        assertThat(fractions.getFirst().upperBound().dataType(), equalTo(DataType.KEYWORD));
        assertThat(fractions.getFirst().lower().fold(FoldContext.small()), equalTo(0.5));
        assertThat(fractions.getFirst().upper().fold(FoldContext.small()), equalTo(1.5));
        assertThat(outputColumns(translated), equalTo(List.of("result", "step", "job")));
    }

    public void testClassicHistogramFractionWithoutLeReturnsWarning() {
        LogicalPlan plan = planClassic(
            "PROMQL index=prom_hist step=1m result=(histogram_fraction(0, 1, sum by (job) (request_duration_seconds_bucket)))",
            true
        );

        assertTrue(plan.resolved());
        assertWarnings("histogram_fraction: input vector has no le label; no buckets to evaluate");
    }

    public void testNativeHistogramFractionKeepsScalarImplementation() {
        LogicalPlan translated = planNative("PROMQL index=exp_histo step=1m result=(histogram_fraction(0, 1, responseTime))");
        List<org.elasticsearch.xpack.esql.expression.function.scalar.histogram.HistogramFraction> fractions = new ArrayList<>();
        translated.forEachExpressionDown(
            org.elasticsearch.xpack.esql.expression.function.scalar.histogram.HistogramFraction.class,
            fractions::add
        );

        assertThat(collectFractions(translated), hasSize(0));
        assertThat(fractions, hasSize(1));
        assertThat(outputColumns(translated), equalTo(List.of("result", "step", MetadataAttribute.TIMESERIES)));
    }

    private LogicalPlan planClassic(String query, boolean optimize) {
        LogicalPlan analyzed = analyzerWithEnrichPolicies().addIndex(
            "prom_hist",
            "mapping-promql-classic-histogram.json",
            IndexMode.TIME_SERIES
        ).query(query);
        return optimize ? logicalOptimizer.optimize(analyzed) : analyzed;
    }

    private LogicalPlan planNative(String query) {
        LogicalPlan analyzed = analyzerWithEnrichPolicies().addIndex("exp_histo", "exp_histo_sample-mappings.json", IndexMode.TIME_SERIES)
            .query(query);
        return logicalOptimizer.optimize(analyzed);
    }

    private static List<PromqlHistogramFraction> collectFractions(LogicalPlan plan) {
        return plan.collect(Aggregate.class)
            .stream()
            .flatMap(aggregate -> aggregate.aggregates().stream())
            .flatMap(namedExpression -> namedExpression.collect(PromqlHistogramFraction.class).stream())
            .toList();
    }

    private static List<String> outputColumns(LogicalPlan plan) {
        return plan.output().stream().map(Attribute::name).toList();
    }
}
