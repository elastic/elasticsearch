/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.Build;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.TransportVersionUtils;
import org.elasticsearch.xpack.esql.TestAnalyzer;
import org.elasticsearch.xpack.esql.VerificationException;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.expression.function.EsqlFunctionRegistry;
import org.elasticsearch.xpack.esql.expression.function.scalar.timeseries.TimeSeriesFunction;
import org.elasticsearch.xpack.esql.index.EsIndex;
import org.elasticsearch.xpack.esql.index.IndexProperties;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.PackDims;
import org.elasticsearch.xpack.esql.plan.logical.promql.selector.LabelMatcher;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.analyzer;
import static org.hamcrest.Matchers.containsString;

/** Capabilities must describe the same feature availability that query verification enforces. */
public class FunctionalTimeSeriesCapabilityTests extends ESTestCase {
    /** An unavailable capability must not leak its type or functions into otherwise supported PromQL queries. */
    public void testDisabledFeaturePreservesExistingTranslation() {
        assumeFalse(
            "requires functional time-series metadata to be disabled",
            EsqlCapabilities.Cap.PROMQL_ENABLE_TIMESERIES_TYPE.isEnabled()
        );
        assertExistingTranslation(analyzer().addK8s().minimumTransportVersion(TransportVersion.current()));
    }

    /**
     * Older participating nodes cannot leave labels out of keyword {@code _timeseries}. Shapes that only need the full
     * blob or concrete BY labels still resolve; unset transforms require typed metadata.
     */
    public void testOlderParticipatingVersionReadsKeywordMetadata() {
        assumeTrue("requires functional time-series metadata", EsqlCapabilities.Cap.PROMQL_ENABLE_TIMESERIES_TYPE.isEnabled());
        TransportVersion previous = TransportVersionUtils.getPreviousVersion(TransportVersion.fromName("promql_functional_timeseries"));
        var analyzer = analyzer().addK8s().minimumTransportVersion(previous);
        for (String expression : List.of("network.cost", "sum by(cluster,pod)(network.cost)")) {
            var plan = analyzer.query("PROMQL index=k8s step=1h result=(" + expression + ") | LIMIT 10");
            assertTrue(expression, plan.resolved());
            plan.forEachExpressionDown(Expression.class, e -> assertFalse(expression, e instanceof TimeSeriesFunction));
            plan.forEachDown(
                EsRelation.class,
                relation -> relation.output().forEach(a -> assertNotEquals(expression, DataType.TIMESERIES_DATA_TYPE, a.dataType()))
            );
        }
        // WITHOUT over opaque metadata / value transforms that drop __name__ need typed ts_unset.
        for (String expression : List.of("sum without(pod)(network.cost)", "abs(network.cost)", "network.cost * 8")) {
            VerificationException e = expectThrows(
                VerificationException.class,
                () -> analyzer.query("PROMQL index=k8s step=1h result=(" + expression + ") | LIMIT 10")
            );
            assertThat(expression, e.getMessage(), containsString("leaving labels out of _timeseries requires typed time-series metadata"));
        }
        // WITHOUT over concrete BY labels does not unset metadata — still resolves on keyword.
        assertTrue(
            analyzer.query("PROMQL index=k8s step=1h result=(max without(pod)(sum by(cluster,pod)(network.cost))) | LIMIT 10").resolved()
        );
    }

    private static void assertExistingTranslation(TestAnalyzer analyzer) {
        for (String expression : List.of("network.cost", "sum by(cluster,pod)(network.cost)")) {
            var plan = analyzer.query("PROMQL index=k8s step=1h result=(" + expression + ") | LIMIT 10");
            assertTrue(expression, plan.resolved());
            plan.forEachExpressionDown(Expression.class, e -> {
                assertNotEquals(expression, DataType.TIMESERIES_DATA_TYPE, e.dataType());
                assertFalse(expression, e instanceof TimeSeriesFunction);
            });
        }
        for (String expression : List.of("sum without(pod)(network.cost)", "abs(network.cost)", "network.cost * 8")) {
            VerificationException e = expectThrows(
                VerificationException.class,
                () -> analyzer.query("PROMQL index=k8s step=1h result=(" + expression + ") | LIMIT 10")
            );
            assertThat(expression, e.getMessage(), containsString("leaving labels out of _timeseries requires typed time-series metadata"));
        }
    }

    /** WITHOUT over concrete BY labels works on any version; opaque WITHOUT needs typed metadata. */
    public void testWithoutMetricNameAcrossVersions() {
        assumeTrue("requires functional time-series metadata", EsqlCapabilities.Cap.PROMQL_ENABLE_TIMESERIES_TYPE.isEnabled());
        var mapping = new LinkedHashMap<String, EsField>();
        mapping.put("@timestamp", new EsField("@timestamp", DataType.DATETIME, Map.of(), true, EsField.TimeSeriesFieldType.NONE));
        mapping.put("metric", new EsField("metric", DataType.DOUBLE, Map.of(), true, EsField.TimeSeriesFieldType.METRIC));
        for (String label : List.of(LabelMatcher.NAME, "cluster", "pod")) {
            mapping.put(label, new EsField(label, DataType.KEYWORD, Map.of(), true, EsField.TimeSeriesFieldType.DIMENSION));
        }
        var index = new EsIndex("metrics", mapping, Map.of("metrics", new IndexProperties(IndexMode.TIME_SERIES, 0)), Map.of(), Map.of());
        var analyzer = analyzer().addIndex(index).minimumTransportVersion(TransportVersion.current());
        var byName = analyzer.query("PROMQL index=metrics step=1h result=(sum by(" + LabelMatcher.NAME + ")(metric)) | LIMIT 10");
        assertTrue(byName.resolved());
        assertEquals(List.of("result", "step", LabelMatcher.NAME), byName.output().stream().map(Attribute::name).toList());

        String without = "sum without(pod)(sum by(" + LabelMatcher.NAME + ",cluster,pod)(metric))";
        var plan = analyzer.query("PROMQL index=metrics step=1h result=(" + without + ") | LIMIT 10");
        assertTrue(plan.resolved());
        assertFalse(plan.output().stream().map(Attribute::name).anyMatch(LabelMatcher.NAME::equals));
        assertEquals(1, plan.collect(PackDims.class).getFirst().dims().size());

        assertTrue(analyzer.query("PROMQL index=metrics step=1h result=(sum without(pod)(metric)) | LIMIT 10").resolved());

        TransportVersion previous = TransportVersionUtils.getPreviousVersion(TransportVersion.fromName("promql_functional_timeseries"));
        var keywordAnalyzer = analyzer().addIndex(index).minimumTransportVersion(previous);
        assertTrue(
            keywordAnalyzer.query("PROMQL index=metrics step=1h result=(sum by(" + LabelMatcher.NAME + ")(metric)) | LIMIT 10").resolved()
        );
        VerificationException e = expectThrows(
            VerificationException.class,
            () -> keywordAnalyzer.query("PROMQL index=metrics step=1h result=(sum without(pod)(metric)) | LIMIT 10")
        );
        assertThat(e.getMessage(), containsString("leaving labels out of _timeseries requires typed time-series metadata"));
    }

    public void testCapabilityFollowsSnapshotBuild() {
        var functions = new EsqlFunctionRegistry();
        var capability = EsqlCapabilities.Cap.PROMQL_ENABLE_TIMESERIES_TYPE;
        assertEquals(Build.current().isSnapshot(), capability.isEnabled());
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
