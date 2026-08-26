/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.prometheus.rest;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.EsqlTestUtils;
import org.elasticsearch.xpack.esql.TestAnalyzer;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expressions;
import org.elasticsearch.xpack.esql.optimizer.LogicalPlanOptimizer;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.MetricsInfo;
import org.elasticsearch.xpack.esql.plan.physical.EsQueryExec;

import java.time.Instant;
import java.util.List;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.unboundLogicalOptimizerContext;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.withDefaultLimitWarning;

/**
 * Regression tests for Prometheus info-command plans retaining {@code _doc} metadata after
 * analysis and logical optimization. Programmatic Prometheus plans bypass {@code LogicalPlanBuilder},
 * so they must call {@link org.elasticsearch.xpack.esql.plan.logical.InfoCommandPlanUtils#injectDocAttribute}
 * just like parser-built {@code METRICS_INFO}/{@code TS_INFO} plans (see #145766).
 */
public class PrometheusInfoCommandPlanOptimizerTests extends ESTestCase {

    private static final Instant START = Instant.ofEpochSecond(1_700_000_000L);
    private static final Instant END = Instant.ofEpochSecond(1_700_003_600L);
    private static final String INDEX = "k8s";

    public void testMetadataPlanWithoutDocInjectionLosesDocFromEsRelation() {
        LogicalPlan optimized = optimize(buildMetadataPlanWithoutDocInjection());
        assertFalse(hasDocInEsRelation(optimized));
    }

    public void testMetadataPlanPreservesDoc() {
        assertDocPreserved(PrometheusMetadataPlanBuilder.buildPlan(INDEX, null, 0, 0, START, END));
    }

    public void testLabelsPlanPreservesDoc() {
        assertDocPreserved(PrometheusLabelsPlanBuilder.buildPlan(INDEX, List.of(), START, END, 0));
    }

    public void testSeriesPlanPreservesDoc() {
        assertDocPreserved(PrometheusSeriesPlanBuilder.buildPlan(INDEX, List.of("events_received"), START, END, 0));
    }

    public void testNameLabelValuesPlanPreservesDoc() {
        assertDocPreserved(PrometheusLabelValuesPlanBuilder.buildPlan("__name__", INDEX, List.of(), START, END, 0));
    }

    private static void assertDocPreserved(LogicalPlan plan) {
        LogicalPlan optimized = optimize(plan);
        assertTrue(hasDocInEsRelation(optimized));
    }

    private static boolean hasDocInEsRelation(LogicalPlan plan) {
        return plan.anyMatch(
            p -> p instanceof EsRelation er && Expressions.anyMatch(er.output(), a -> EsQueryExec.isDocAttribute((Attribute) a))
        );
    }

    private static LogicalPlan optimize(LogicalPlan plan) {
        LogicalPlan analyzed = timeSeriesAnalyzer().buildAnalyzer().analyze(plan);
        return new LogicalPlanOptimizer(unboundLogicalOptimizerContext()).optimize(analyzed);
    }

    private static LogicalPlan buildMetadataPlanWithoutDocInjection() {
        LogicalPlan plan = PrometheusPlanBuilderUtils.tsSource(INDEX);
        plan = new Filter(
            org.elasticsearch.xpack.esql.core.tree.Source.EMPTY,
            plan,
            PrometheusPlanBuilderUtils.buildTimeCondition(START, END)
        );
        return new MetricsInfo(org.elasticsearch.xpack.esql.core.tree.Source.EMPTY, plan);
    }

    private static TestAnalyzer timeSeriesAnalyzer() {
        return EsqlTestUtils.analyzer().addK8s();
    }

    @Override
    protected List<String> filteredWarnings() {
        return withDefaultLimitWarning(super.filteredWarnings());
    }
}
