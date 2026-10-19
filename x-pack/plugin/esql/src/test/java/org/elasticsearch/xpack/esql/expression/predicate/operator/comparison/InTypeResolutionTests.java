/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.expression.predicate.operator.comparison;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.core.Strings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.TestAnalyzer;
import org.elasticsearch.xpack.esql.VerificationException;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.UnresolvedAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.plugin.EsqlFlags;

import java.util.List;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.analyzer;
import static org.hamcrest.Matchers.containsString;

/** Metadata stays opaque to generic comparisons, including null and negated IN expressions. */
public class InTypeResolutionTests extends ESTestCase {
    public void testUnresolvedOperands() {
        var unresolved = new UnresolvedAttribute(Source.EMPTY, "missing");
        assertTrue(new In(Source.EMPTY, unresolved, List.of(Literal.NULL)).typeResolved().unresolved());
        assertTrue(new In(Source.EMPTY, Literal.NULL, List.of(unresolved)).typeResolved().unresolved());
    }

    public void testRejectsTimeSeriesMetadata() {
        for (String expression : List.of("a IN (b, c)", "a NOT IN (b, c)", "null IN (a, null)", "a IN (null)")) {
            var error = expectThrows(VerificationException.class, () -> prototype().query(Strings.format("""
                ROW a = TS_SET(null, "x", "a"), b = TS_SET(null, "x", "b"), c = TS_SET(null, "x", "c")
                | EVAL matched = %s
                | KEEP matched
                | LIMIT 10
                """, expression)));
            assertThat(expression, error.getMessage(), containsString("[_timeseries]"));
        }
    }

    public void testMetadataKeysCanBeCompared() {
        prototype().query("""
            ROW a = TS_SET(null, "x", "a"), b = TS_SET(null, "x", "b"), c = TS_SET(null, "x", "c")
            | EVAL key_a = TS_HASH(a), key_b = TS_HASH(b), key_c = TS_HASH(c)
            | EVAL matched = key_a IN (key_b, key_c)
            | KEEP matched
            | LIMIT 10
            """);
    }

    public void testRejectsTimeSeriesMetadataSubquery() {
        for (String operator : List.of("IN", "NOT IN")) {
            var error = expectThrows(VerificationException.class, () -> prototype().query(Strings.format("""
                ROW labels = TS_SET(null, "x", "a")
                | WHERE labels %s (ROW other = TS_SET(null, "x", "b"))
                | EVAL result = TO_STRING(labels)
                | KEEP result
                | LIMIT 10
                """, operator)));
            assertThat(error.getMessage(), containsString("IN subquery with right field"));
            assertThat(error.getMessage(), containsString("of type [TIMESERIES_DATA_TYPE] is not supported"));
        }
    }

    private static TestAnalyzer prototype() {
        return analyzer().minimumTransportVersion(TransportVersion.fromName("promql_functional_timeseries"))
            .flags(EsqlFlags.DEFAULTS.withPromqlFunctionalTimeSeries(true));
    }
}
