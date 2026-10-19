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
import org.elasticsearch.xpack.esql.common.Failures;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.TimeSeriesMetadataAttribute;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.expression.function.scalar.timeseries.TimeSeriesUnset;
import org.elasticsearch.xpack.esql.index.EsIndex;
import org.elasticsearch.xpack.esql.index.IndexProperties;
import org.elasticsearch.xpack.esql.optimizer.rules.PlanConsistencyChecker;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.promql.selector.LabelMatcher;
import org.elasticsearch.xpack.esql.plugin.EsqlFlags;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.analyzer;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.loadMapping;

/** Functions evaluate complete source identities before removing the metric-name label. */
public class FunctionalMetricNameTranslationTests extends ESTestCase {
    public void testValueAndRangeFunctionsDropNamesAfterEvaluation() {
        for (String expression : List.of(
            "abs(network.bytes_in)",
            "ceil(network.bytes_in)",
            "rate(network.bytes_in[5m])",
            "delta(network.bytes_in[5m])",
            "sum(abs(network.bytes_in))"
        )) {
            LogicalPlan plan = analyze(expression);
            var removals = new ArrayList<TimeSeriesUnset>();
            plan.forEachExpressionDown(TimeSeriesUnset.class, removals::add);
            assertTrue(
                expression,
                removals.stream().anyMatch(unset -> unset.children().contains(Literal.keyword(unset.source(), LabelMatcher.NAME)))
            );
            for (EsRelation relation : plan.collect(EsRelation.class)) {
                for (Attribute attribute : relation.output()) {
                    if (attribute instanceof TimeSeriesMetadataAttribute metadata) {
                        assertFalse(expression, metadata.excludedFields().contains(LabelMatcher.NAME));
                    }
                }
            }
        }
    }

    public void testPromotedAndDerivedNamesBecomeAbsent() {
        for (String expression : List.of(
            "sum by(__name__)(abs(network.bytes_in))",
            "abs(sum by(__name__)(network.bytes_in))",
            "abs(label_replace(sum by(pod)(network.bytes_in), \"__name__\", \"derived\", \"pod\", \".*\"))"
        )) {
            LogicalPlan plan = analyze(expression);
            var aliases = new ArrayList<Alias>();
            plan.forEachExpressionDown(Alias.class, aliases::add);
            assertTrue(expression, aliases.stream().anyMatch(alias -> alias.child() instanceof Literal literal && literal.value() == null));
        }
    }

    public void testSelectorsAndFirstLastOverTimePreserveNames() {
        for (String expression : List.of(
            "network.bytes_in",
            "last_over_time(network.bytes_in[5m])",
            "first_over_time(network.bytes_in[5m])"
        )) {
            LogicalPlan plan = analyze(expression);
            plan.forEachExpressionDown(
                TimeSeriesUnset.class,
                unset -> assertFalse(expression, unset.children().contains(Literal.keyword(unset.source(), LabelMatcher.NAME)))
            );
        }
    }

    public void testLegacyFunctionPlansRemainUnchanged() {
        for (TestAnalyzer analyzer : List.of(
            prototype().flags(EsqlFlags.DEFAULTS),
            prototype().minimumTransportVersion(TransportVersionUtils.getPreviousVersion(featureVersion()))
        )) {
            LogicalPlan plan = analyzer.query(query("abs(network.bytes_in)"));
            plan.forEachExpressionDown(TimeSeriesUnset.class, unset -> fail("Legacy function acquired typed label removal"));
        }
    }

    public void testFlattenedDimensionsDoNotProveMetricNameAbsent() {
        var mapping = new HashMap<>(loadMapping("k8s-mappings.json"));
        mapping.put("labels", new EsField("labels", DataType.FLATTENED, Map.of(), true, EsField.TimeSeriesFieldType.DIMENSION));
        LogicalPlan plan = prototype(mapping).query(query("sum by(cluster, pod)(rate(network.bytes_in[5m]))"));
        assertTrue(
            plan.collect(EsRelation.class)
                .stream()
                .anyMatch(relation -> relation.output().stream().anyMatch(TimeSeriesMetadataAttribute.class::isInstance))
        );
    }

    private static LogicalPlan analyze(String expression) {
        LogicalPlan plan = prototype().query(query(expression));
        Failures failures = new Failures();
        plan.forEachUp(node -> PlanConsistencyChecker.checkPlan(node, failures));
        assertFalse(expression + ": " + failures, failures.hasFailures());
        return plan;
    }

    private static TestAnalyzer prototype() {
        var mapping = new HashMap<>(loadMapping("k8s-mappings.json"));
        mapping.put(
            LabelMatcher.NAME,
            new EsField(LabelMatcher.NAME, DataType.KEYWORD, Map.of(), true, EsField.TimeSeriesFieldType.DIMENSION)
        );
        return prototype(mapping);
    }

    private static TestAnalyzer prototype(Map<String, EsField> mapping) {
        var index = new EsIndex(
            "k8s",
            mapping,
            Map.of("k8s", new IndexProperties(IndexMode.TIME_SERIES, 0)),
            Map.of("", List.of("k8s")),
            Map.of("", List.of("k8s"))
        );
        return analyzer().addIndex(index)
            .minimumTransportVersion(featureVersion())
            .flags(EsqlFlags.DEFAULTS.withPromqlFunctionalTimeSeries(true));
    }

    private static TransportVersion featureVersion() {
        return TransportVersion.fromName("promql_functional_timeseries");
    }

    private static String query(String expression) {
        return "PROMQL index=k8s start=\"2025-01-01T00:00:00Z\" end=\"2025-01-01T00:01:00Z\" step=1m result=("
            + expression
            + ") | LIMIT 100";
    }
}
