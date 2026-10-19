/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.TransportVersionUtils;
import org.elasticsearch.xpack.esql.TestAnalyzer;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.expression.function.EsqlFunctionRegistry;
import org.elasticsearch.xpack.esql.expression.function.scalar.timeseries.LabelEval;
import org.elasticsearch.xpack.esql.expression.function.scalar.timeseries.TimeSeriesFunction;
import org.elasticsearch.xpack.esql.index.EsIndex;
import org.elasticsearch.xpack.esql.index.IndexProperties;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.PackDims;
import org.elasticsearch.xpack.esql.plan.logical.promql.PromqlCommand;
import org.elasticsearch.xpack.esql.plan.logical.promql.selector.LabelMatcher;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.analyzer;

/** Capabilities must describe the same feature availability that query verification enforces. */
public class FunctionalTimeSeriesCapabilityTests extends ESTestCase {
    /** An unavailable capability must not leak its type or functions into otherwise supported PromQL queries. */
    public void testDisabledFeaturePreservesExistingTranslation() {
        assumeFalse(
            "requires functional time-series metadata to be disabled",
            EsqlCapabilities.Cap.PROMQL_FUNCTIONAL_TIMESERIES.isEnabled()
        );
        assertExistingTranslation(analyzer().addK8s().minimumTransportVersion(TransportVersion.current()));
    }

    /** Enabling the feature locally cannot require older participating nodes to understand typed metadata. */
    public void testOlderParticipatingVersionPreservesExistingTranslation() {
        assumeTrue("requires functional time-series metadata", EsqlCapabilities.Cap.PROMQL_FUNCTIONAL_TIMESERIES.isEnabled());
        TransportVersion previous = TransportVersionUtils.getPreviousVersion(TransportVersion.fromName("promql_functional_timeseries"));
        assertExistingTranslation(analyzer().addK8s().minimumTransportVersion(previous));
    }

    private static void assertExistingTranslation(TestAnalyzer analyzer) {
        for (String expression : List.of(
            "network.cost",
            "sum without(pod)(network.cost)",
            "sum by(cluster,pod)(network.cost)",
            "max without(pod)(sum by(cluster,pod)(network.cost))",
            "sum(network.cost) + sum(network.cost)",
            "abs(network.cost)"
        )) {
            var plan = analyzer.query("PROMQL index=k8s step=1h result=(" + expression + ") | LIMIT 10");
            assertTrue(expression, plan.resolved());
            plan.forEachExpressionDown(Expression.class, e -> {
                assertNotEquals(expression, DataType.TIMESERIES_DATA_TYPE, e.dataType());
                assertFalse(expression, e instanceof TimeSeriesFunction);
                assertFalse(expression, e instanceof LabelEval);
            });
        }
    }

    /** WITHOUT drops the metric name regardless of the feature flag or participating node versions. */
    public void testWithoutMetricNameAcrossVersions() {
        var mapping = new LinkedHashMap<String, EsField>();
        mapping.put("@timestamp", new EsField("@timestamp", DataType.DATETIME, Map.of(), true, EsField.TimeSeriesFieldType.NONE));
        mapping.put("metric", new EsField("metric", DataType.DOUBLE, Map.of(), true, EsField.TimeSeriesFieldType.METRIC));
        for (String label : List.of(LabelMatcher.NAME, "cluster", "pod")) {
            mapping.put(label, new EsField(label, DataType.KEYWORD, Map.of(), true, EsField.TimeSeriesFieldType.DIMENSION));
        }
        var index = new EsIndex("metrics", mapping, Map.of("metrics", new IndexProperties(IndexMode.TIME_SERIES, 0)), Map.of(), Map.of());
        TransportVersion previous = TransportVersionUtils.getPreviousVersion(TransportVersion.fromName("promql_functional_timeseries"));
        for (TransportVersion version : List.of(TransportVersion.current(), previous)) {
            var analyzer = analyzer().addIndex(index).minimumTransportVersion(version);
            var byName = analyzer.query("PROMQL index=metrics step=1h result=(sum by(" + LabelMatcher.NAME + ")(metric)) | LIMIT 10");
            assertTrue(byName.resolved());
            assertEquals(List.of("result", "step", LabelMatcher.NAME), byName.output().stream().map(Attribute::name).toList());

            String without = "sum without(pod)(sum by(" + LabelMatcher.NAME + ",cluster,pod)(metric))";
            var plan = analyzer.query("PROMQL index=metrics step=1h result=(" + without + ") | LIMIT 10");
            assertTrue(plan.resolved());
            assertFalse(plan.output().stream().map(Attribute::name).anyMatch(LabelMatcher.NAME::equals));
            assertEquals(1, plan.collect(PackDims.class).getFirst().dims().size());

            var parent = analyzer.query(
                "PROMQL index=metrics step=1h result=(sum by(" + LabelMatcher.NAME + ")(" + without + ")) | LIMIT 10"
            );
            assertTrue(parent.resolved());
            assertEquals(List.of("result", "step", LabelMatcher.NAME), parent.output().stream().map(Attribute::name).toList());
            assertTrue(
                parent.collect(Eval.class)
                    .stream()
                    .flatMap(eval -> eval.fields().stream())
                    .anyMatch(alias -> alias.child() instanceof Literal literal && literal.value() == null)
            );

            assertTrue(analyzer.query("PROMQL index=metrics step=1h result=(sum without(pod)(metric)) | LIMIT 10").resolved());
        }
    }

    public void testCapabilityFollowsFeatureFlag() {
        var functions = new EsqlFunctionRegistry();
        var capability = EsqlCapabilities.Cap.PROMQL_FUNCTIONAL_TIMESERIES;
        assertEquals(PromqlCommand.FUNCTIONAL_TIMESERIES_FEATURE_FLAG.isEnabled(), capability.isEnabled());
        assertEquals(
            capability.isEnabled(),
            EsqlCapabilities.capabilities(functions, false).capabilities().contains(capability.capabilityName())
        );
        assertTrue(EsqlCapabilities.capabilities(functions, true).capabilities().contains(capability.capabilityName()));
        for (String name : List.of("fn_ts_get", "fn_ts_set", "fn_ts_unset", "fn_ts_hash")) {
            assertEquals(name, capability.isEnabled(), EsqlCapabilities.capabilities(functions, false).capabilities().contains(name));
            assertTrue(name, EsqlCapabilities.capabilities(functions, true).capabilities().contains(name));
        }
    }
}
