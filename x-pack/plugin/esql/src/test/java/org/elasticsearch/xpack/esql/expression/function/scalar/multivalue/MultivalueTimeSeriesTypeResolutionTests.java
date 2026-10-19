/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.expression.function.scalar.multivalue;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.core.Strings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.TestAnalyzer;
import org.elasticsearch.xpack.esql.VerificationException;
import org.elasticsearch.xpack.esql.action.EsqlCapabilities;

import java.util.List;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.analyzer;
import static org.hamcrest.Matchers.containsString;

/** Metadata can pass through multivalue functions, but its opaque encoding does not define an order. */
public class MultivalueTimeSeriesTypeResolutionTests extends ESTestCase {
    public void testRejectsMetadataOrdering() {
        assumeTrue("requires functional time-series metadata", EsqlCapabilities.Cap.PROMQL_FUNCTIONAL_TIMESERIES.isEnabled());
        for (String function : List.of("MV_MIN", "MV_MAX", "MV_SORT")) {
            var error = expectThrows(VerificationException.class, () -> prototype().query(Strings.format("""
                ROW a = TS_SET(null, "x", "a"), b = TS_SET(null, "x", "b")
                | EVAL labels = %s(MV_APPEND(a, b))
                | EVAL result = TO_STRING(MV_FIRST(labels))
                | KEEP result
                | LIMIT 10
                """, function)));
            assertThat(error.getMessage(), containsString("[" + function + "] does not support ordering [_timeseries] values"));
        }
    }

    public void testMetadataCanPassThroughMultivalueFunctions() {
        assumeTrue("requires functional time-series metadata", EsqlCapabilities.Cap.PROMQL_FUNCTIONAL_TIMESERIES.isEnabled());
        prototype().query("""
            ROW a = TS_SET(null, "x", "a"), b = TS_SET(null, "x", "b")
            | EVAL labels = MV_APPEND(a, b)
            | EVAL first_value = TS_GET(MV_FIRST(labels), "x"), last_value = TS_GET(MV_LAST(labels), "x"), size = MV_COUNT(labels)
            | KEEP first_value, last_value, size
            | LIMIT 10
            """);
    }

    private static TestAnalyzer prototype() {
        return analyzer().minimumTransportVersion(TransportVersion.fromName("promql_functional_timeseries"));
    }
}
