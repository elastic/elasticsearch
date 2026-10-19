/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical.promql;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.TransportVersionUtils;
import org.elasticsearch.xpack.esql.TestAnalyzer;
import org.elasticsearch.xpack.esql.VerificationException;
import org.elasticsearch.xpack.esql.common.Failures;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.aggregate.First;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToDouble;
import org.elasticsearch.xpack.esql.expression.function.scalar.histogram.ExtractHistogramComponent;
import org.elasticsearch.xpack.esql.optimizer.rules.PlanConsistencyChecker;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.UnionAll;
import org.elasticsearch.xpack.esql.plugin.EsqlFlags;

import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.analyzer;
import static org.hamcrest.Matchers.containsString;

/** Label operations preserve native histogram samples until a histogram function consumes them. */
public class FunctionalHistogramTranslationTests extends ESTestCase {
    public void testRelabelPreservesHistogramSamples() {
        for (String expression : List.of(
            "histogram_count(label_replace(responseTime, \"copy\", \"$1\", \"instance\", \"(.*)\"))",
            "histogram_sum(label_join(responseTime, \"copy\", \"\", \"instance\"))",
            "histogram_count(label_replace(increase(responseTime[5m]), \"copy\", \"$1\", \"instance\", \"(.*)\"))",
            "histogram_count(sum by(copy)(label_join(responseTime, \"copy\", \"\", \"instance\")))"
        )) {
            LogicalPlan plan = analyze(expression);
            var components = new ArrayList<ExtractHistogramComponent>();
            plan.forEachExpressionDown(ExtractHistogramComponent.class, components::add);
            assertFalse(expression, components.isEmpty());
            assertTrue(
                expression,
                components.stream().allMatch(component -> component.field().dataType() == DataType.EXPONENTIAL_HISTOGRAM)
            );
        }
    }

    public void testSetOperationsPreserveHistogramSamples() {
        for (String expression : List.of(
            "histogram_count(responseTime or mostly_empty)",
            "histogram_count(responseTime and mostly_empty)",
            "histogram_count(responseTime unless mostly_empty)"
        )) {
            LogicalPlan plan = analyze(expression);
            if (expression.contains(" or ")) {
                List<UnionAll> unions = plan.collect(UnionAll.class);
                assertEquals(expression, 1, unions.size());
                assertEquals(DataType.EXPONENTIAL_HISTOGRAM, unions.getFirst().output().getFirst().dataType());
            }
        }
    }

    /** The ordinary collapse aggregate retains histogram values without coercing them into a numeric or multivalued block. */
    public void testHistogramCollapsePreservesSampleType() {
        LogicalPlan plan = analyze("histogram_count(label_join(responseTime, \"copy\", \"\", \"instance\"))");
        var representatives = new ArrayList<First>();
        plan.forEachExpressionDown(First.class, representatives::add);
        assertFalse(representatives.isEmpty());
        assertTrue(representatives.stream().allMatch(first -> first.dataType() == DataType.EXPONENTIAL_HISTOGRAM));
    }

    public void testSupportedNativeHistogramCompositionsRemainValid() {
        for (String expression : List.of(
            "histogram_count(increase(responseTime[5m]))",
            "histogram_quantile(0.5, sum by(instance)(increase(responseTime[5m])))",
            "histogram_avg(responseTime)"
        )) {
            analyze(expression);
            for (TestAnalyzer legacy : List.of(
                prototype().flags(EsqlFlags.DEFAULTS),
                prototype().minimumTransportVersion(TransportVersionUtils.getPreviousVersion(featureVersion()))
            )) {
                assertTrue(legacy.query(query(expression)).resolved());
            }
        }
    }

    public void testCommandStillRequiresNumericOutput() {
        VerificationException error = expectThrows(VerificationException.class, () -> prototype().query(query("responseTime")));
        assertThat(error.getMessage(), containsString("exponential_histogram"));
    }

    private static LogicalPlan analyze(String expression) {
        LogicalPlan plan;
        try {
            plan = prototype().query(query(expression));
        } catch (RuntimeException e) {
            throw new AssertionError(expression, e);
        }
        Failures failures = new Failures();
        plan.forEachUp(node -> PlanConsistencyChecker.checkPlan(node, failures));
        assertFalse(expression + ": " + failures, failures.hasFailures());
        assertEquals(DataType.DOUBLE, plan.output().getFirst().dataType());
        plan.forEachExpressionDown(ToDouble.class, cast -> assertFalse(expression, cast.field().dataType().isHistogram()));
        return plan;
    }

    private static TestAnalyzer prototype() {
        return analyzer().addIndex("histograms", "exp_histo_sample-mappings.json", IndexMode.TIME_SERIES)
            .minimumTransportVersion(featureVersion())
            .flags(EsqlFlags.DEFAULTS.withPromqlFunctionalTimeSeries(true));
    }

    private static TransportVersion featureVersion() {
        return TransportVersion.fromName("promql_functional_timeseries");
    }

    private static String query(String expression) {
        return "PROMQL index=histograms start=\"2025-01-01T00:00:00Z\" end=\"2025-01-01T00:01:00Z\" step=1m result=("
            + expression
            + ") | LIMIT 100";
    }
}
