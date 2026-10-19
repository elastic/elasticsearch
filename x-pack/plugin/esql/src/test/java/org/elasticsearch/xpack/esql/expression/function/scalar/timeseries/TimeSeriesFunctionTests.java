/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.timeseries;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.TransportVersionUtils;
import org.elasticsearch.xpack.esql.TestAnalyzer;
import org.elasticsearch.xpack.esql.VerificationException;
import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.EsqlFunctionRegistry;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToString;
import org.elasticsearch.xpack.esql.parser.ParsingException;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.TEST_CFG;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.analyzer;
import static org.hamcrest.Matchers.containsString;

/** Tests the public ES|QL syntax and type contract independently of PromQL translation. */
public class TimeSeriesFunctionTests extends ESTestCase {
    public void testRegisteredFunctions() {
        assumeTrue("requires functional time-series metadata", EsqlCapabilities.Cap.PROMQL_FUNCTIONAL_TIMESERIES.isEnabled());
        var registry = new EsqlFunctionRegistry();
        Map<String, Class<? extends TimeSeriesFunction>> functions = Map.of(
            "ts_get",
            TimeSeriesGet.class,
            "ts_set",
            TimeSeriesSet.class,
            "ts_unset",
            TimeSeriesUnset.class,
            "ts_hash",
            TimeSeriesHash.class
        );
        functions.forEach((name, type) -> assertEquals(type, registry.resolveFunction(name).clazz()));
        assertEquals(ToString.class, registry.resolveFunction("to_string").clazz());
        assertFalse(registry.functionExists("ts_to_string"));
    }

    public void testStandaloneExpressions() {
        assumeTrue("requires functional time-series metadata", EsqlCapabilities.Cap.PROMQL_FUNCTIONAL_TIMESERIES.isEnabled());
        var plan = prototype().query("""
            ROW labels = TS_SET(null, "pod", "a")
            | EVAL pod = TS_GET(labels, "pod"), rest = TS_UNSET(labels, "pod"),
                   key = TS_HASH(labels, "cluster", "prod"), rendered = TO_STRING(TS_SET(labels, "cluster", "prod"))
            | KEEP pod, key, rendered
            | LIMIT 10
            """);
        var functions = new HashSet<Class<?>>();
        plan.forEachExpressionDown(TimeSeriesFunction.class, function -> functions.add(function.getClass()));
        assertEquals(Set.of(TimeSeriesGet.class, TimeSeriesSet.class, TimeSeriesUnset.class, TimeSeriesHash.class), functions);
        plan.forEachExpressionDown(ToString.class, function -> {
            assertEquals(DataType.TIMESERIES_DATA_TYPE, function.field().dataType());
            assertTrue(function.typeResolved().toString(), function.typeResolved().resolved());
        });
        plan.forEachExpressionDown(
            TimeSeriesFunction.class,
            function -> assertTrue(function.typeResolved().toString(), function.typeResolved().resolved())
        );
    }

    public void testMetadataTypes() {
        assumeTrue("requires functional time-series metadata", EsqlCapabilities.Cap.PROMQL_FUNCTIONAL_TIMESERIES.isEnabled());
        var metadata = Literal.NULL;
        var label = Literal.keyword(Source.EMPTY, "pod");
        var set = new TimeSeriesSet(Source.EMPTY, metadata, label, Literal.keyword(Source.EMPTY, "a"));
        var unset = new TimeSeriesUnset(Source.EMPTY, set, List.of(label));
        assertEquals(DataType.TIMESERIES_DATA_TYPE, set.dataType());
        assertEquals(DataType.TIMESERIES_DATA_TYPE, unset.dataType());
        assertTrue(set.typeResolved().resolved());
        assertTrue(unset.typeResolved().resolved());
        assertEquals(DataType.KEYWORD, new TimeSeriesGet(Source.EMPTY, unset, label).dataType());
        assertEquals(DataType.TSID_DATA_TYPE, new TimeSeriesHash(Source.EMPTY, unset, List.of()).dataType());
        assertEquals(DataType.KEYWORD, new ToString(Source.EMPTY, unset, TEST_CFG).dataType());
    }

    public void testFunctionsAreRejectedWhenDisabled() {
        assumeFalse(
            "requires functional time-series metadata to be disabled",
            EsqlCapabilities.Cap.PROMQL_FUNCTIONAL_TIMESERIES.isEnabled()
        );
        for (String expression : List.of(
            "TS_GET(null, \"pod\")",
            "TS_SET(null, \"pod\", \"a\")",
            "TS_UNSET(null, \"pod\")",
            "TS_HASH(null)"
        )) {
            var error = expectThrows(VerificationException.class, () -> analyzer().query("ROW result = " + expression + " | LIMIT 10"));
            assertThat(error.getMessage(), containsString("experimental and disabled"));
        }
    }

    public void testOrdinaryToStringRemainsAvailable() {
        for (TestAnalyzer analyzer : List.of(
            analyzer(),
            analyzer().minimumTransportVersion(
                TransportVersionUtils.getPreviousVersion(TransportVersion.fromName("promql_functional_timeseries"))
            )
        )) {
            analyzer.query("ROW result = TO_STRING(null), text = TO_STRING(\"{}\"), number = TO_STRING(42) | LIMIT 10");
        }
    }

    public void testOlderNodesRejectExplicitFunctions() {
        assumeTrue("requires functional time-series metadata", EsqlCapabilities.Cap.PROMQL_FUNCTIONAL_TIMESERIES.isEnabled());
        for (String expression : List.of(
            "TS_GET(null, \"pod\")",
            "TO_STRING(TS_SET(null, \"pod\", \"a\"))",
            "TO_STRING(TS_UNSET(null, \"pod\"))",
            "TS_HASH(null)"
        )) {
            var error = expectThrows(
                VerificationException.class,
                () -> prototype().minimumTransportVersion(
                    TransportVersionUtils.getPreviousVersion(TransportVersion.fromName("promql_functional_timeseries"))
                ).query("ROW result = " + expression + " | LIMIT 10")
            );
            assertThat(error.getMessage(), containsString("require support on every participating node"));
        }
    }

    public void testRejectsUntypedMetadata() {
        assumeTrue("requires functional time-series metadata", EsqlCapabilities.Cap.PROMQL_FUNCTIONAL_TIMESERIES.isEnabled());
        for (String expression : List.of(
            "TS_GET(\"{}\", \"pod\")",
            "TS_SET(\"{}\", \"pod\", \"a\")",
            "TS_UNSET(\"{}\", \"pod\")",
            "TS_HASH(\"{}\")"
        )) {
            var error = expectThrows(VerificationException.class, () -> prototype().query("ROW result = " + expression + " | LIMIT 10"));
            assertThat(error.getMessage(), containsString("_timeseries"));
        }
    }

    public void testSupportedLabelValues() {
        assumeTrue("requires functional time-series metadata", EsqlCapabilities.Cap.PROMQL_FUNCTIONAL_TIMESERIES.isEnabled());
        for (String function : List.of("TS_SET", "TS_HASH")) {
            for (String value : List.of("\"value\"", "true", "1", "1::long", "1::unsigned_long", "1.5", "\"127.0.0.1\"::ip", "null")) {
                String expression = function + "(null, \"label\", " + value + ")";
                if (function.equals("TS_SET")) {
                    expression = "TO_STRING(" + expression + ")";
                }
                prototype().query("ROW result = " + expression + " | LIMIT 10");
            }
        }
    }

    /** Typed metadata can flow between expressions but must be rendered at the result boundary. */
    public void testMetadataMustBeRenderedForOutput() {
        assumeTrue("requires functional time-series metadata", EsqlCapabilities.Cap.PROMQL_FUNCTIONAL_TIMESERIES.isEnabled());
        for (String expression : List.of("TS_SET(null, \"pod\", \"a\")", "TS_UNSET(null, \"pod\")")) {
            var error = expectThrows(VerificationException.class, () -> prototype().query("ROW labels = " + expression + " | LIMIT 10"));
            assertThat(error.getMessage(), containsString("use TO_STRING(labels) to include it in the results"));
            prototype().query("ROW labels = " + expression + " | EVAL labels = TO_STRING(labels) | LIMIT 10");
        }
    }

    public void testRejectsUnsupportedLabelValues() {
        assumeTrue("requires functional time-series metadata", EsqlCapabilities.Cap.PROMQL_FUNCTIONAL_TIMESERIES.isEnabled());
        for (String function : List.of("TS_SET", "TS_HASH")) {
            for (String value : List.of(
                "TO_GEOPOINT(\"POINT (1 2)\")",
                "TO_DENSE_VECTOR([1.0, 2.0])",
                "TS_SET(null, \"nested\", \"value\")"
            )) {
                var error = expectThrows(
                    VerificationException.class,
                    () -> prototype().query("ROW result = " + function + "(null, \"label\", " + value + ") | LIMIT 10")
                );
                assertThat(error.getMessage(), containsString("must be [string, boolean, integer, long, unsigned_long, double or ip]"));
            }
        }
    }

    public void testRejectsInvalidLabelArguments() {
        assumeTrue("requires functional time-series metadata", EsqlCapabilities.Cap.PROMQL_FUNCTIONAL_TIMESERIES.isEnabled());
        for (String expression : List.of(
            "TS_GET(null, 42)",
            "TS_GET(null, \"\")",
            "TS_SET(null, null, \"a\")",
            "TS_UNSET(null, [\"a\", \"b\"])",
            "TS_HASH(null, \"pod\")",
            "TS_HASH(null, \"pod\", \"a\", \"pod\", \"b\")"
        )) {
            expectThrows(VerificationException.class, () -> prototype().query("ROW result = " + expression + " | LIMIT 10"));
        }
        var error = expectThrows(
            VerificationException.class,
            () -> prototype().addK8s().query("FROM k8s | EVAL result = TS_GET(null, cluster) | LIMIT 10")
        );
        assertThat(error.getMessage(), containsString("must be a constant"));
    }

    public void testArity() {
        assumeTrue("requires functional time-series metadata", EsqlCapabilities.Cap.PROMQL_FUNCTIONAL_TIMESERIES.isEnabled());
        for (String expression : List.of("TS_GET(null)", "TS_SET(null, \"pod\")", "TS_UNSET(null)", "TS_HASH()", "TO_STRING()")) {
            expectThrows(ParsingException.class, () -> prototype().query("ROW result = " + expression + " | LIMIT 10"));
        }
    }

    private static TestAnalyzer prototype() {
        return analyzer().minimumTransportVersion(TransportVersion.fromName("promql_functional_timeseries"));
    }
}
