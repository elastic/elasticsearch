/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.analysis;

import org.elasticsearch.xpack.esql.VersionMode;
import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.elasticsearch.xpack.esql.core.querydsl.QueryDslTimestampBoundsExtractor;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ConvertFunction;

import java.time.Instant;
import java.util.List;

import static org.hamcrest.Matchers.containsString;

public class TStepResolutionTests extends AnalyzerTestCase {

    public TStepResolutionTests(VersionMode versionMode) {
        super(versionMode);
    }

    public void testTstepUsesRequestTimestampBounds() {
        assumeTStepEnabled();
        var bounds = new QueryDslTimestampBoundsExtractor.TimestampBounds(
            Instant.parse("2023-10-23T12:15:00Z"),
            Instant.parse("2023-10-23T13:55:01.543Z")
        );
        var plan = analyzer().addSampleData().timestampBounds(bounds).query("""
            FROM sample_data
            | STATS c = COUNT(*) BY b = TSTEP(1 hour)
            | LIMIT 10
            """);
        assertNotNull(plan);
    }

    public void testTstepFailsWithoutRequestTimestampBounds() {
        assumeTStepEnabled();
        analyzer().addSampleData().error("""
            FROM sample_data
            | STATS c = COUNT(*) BY b = TSTEP(1 hour)
            | LIMIT 10
            """, containsString("requires either a `@timestamp` range in the request query filter"));
    }

    public void testTstepFailsWithTrangeInQuery() {
        assumeTStepEnabled();
        var bounds = new QueryDslTimestampBoundsExtractor.TimestampBounds(
            Instant.parse("2023-10-23T12:15:00Z"),
            Instant.parse("2023-10-23T13:55:01.543Z")
        );
        analyzer().addSampleData().timestampBounds(bounds).error("""
            FROM sample_data
            | WHERE TRANGE("2023-10-23T12:15:00.000Z", "2023-10-23T13:55:01.543Z")
            | STATS c = COUNT(*) BY b = TSTEP(1 hour)
            | LIMIT 10
            """, containsString("cannot be used together with TRANGE"));
    }

    public void testTstepExplicitBoundsSucceedsWithoutRequestFilter() {
        assumeTStepEnabled();
        assumeTrue("TSTEP explicit bounds requires corresponding capability", EsqlCapabilities.Cap.TSTEP_EXPLICIT_BOUNDS.isEnabled());
        var plan = analyzer().addSampleData().query("""
            FROM sample_data
            | STATS c = COUNT(*) BY b = TSTEP(1 hour, "2023-10-23T12:15:00.000Z", "2023-10-23T13:55:01.543Z")
            | LIMIT 10
            """);
        assertNotNull(plan);
    }

    public void testTstepExplicitBoundsPartialArgumentFails() {
        assumeTStepEnabled();
        assumeTrue("TSTEP explicit bounds requires corresponding capability", EsqlCapabilities.Cap.TSTEP_EXPLICIT_BOUNDS.isEnabled());
        analyzer().addSampleData().error("""
            FROM sample_data
            | STATS c = COUNT(*) BY b = TSTEP(1 hour, "2023-10-23T12:15:00.000Z")
            | LIMIT 10
            """, containsString("requires both 'from' and 'to' arguments"));
    }

    public void testTstepPartialBoundsNotInjectedWithRequestFilter() {
        assumeTStepEnabled();
        assumeTrue("TSTEP explicit bounds requires corresponding capability", EsqlCapabilities.Cap.TSTEP_EXPLICIT_BOUNDS.isEnabled());
        var bounds = new QueryDslTimestampBoundsExtractor.TimestampBounds(
            Instant.parse("2023-10-23T12:15:00Z"),
            Instant.parse("2023-10-23T13:55:01.543Z")
        );
        // A request filter must not inject the missing bound - partial args should always fail.
        analyzer().addSampleData().timestampBounds(bounds).error("""
            FROM sample_data
            | STATS c = COUNT(*) BY b = TSTEP(1 hour, "2023-10-23T12:15:00.000Z")
            | LIMIT 10
            """, containsString("requires both 'from' and 'to' arguments"));
    }

    public void testTstepBucketCountWithBoundsSucceeds() {
        assumeTStepEnabled();
        assumeTrue("TSTEP bucket count requires corresponding capability", EsqlCapabilities.Cap.TSTEP_BUCKET_COUNT.isEnabled());
        var plan = analyzer().addSampleData().query("""
            FROM sample_data
            | STATS c = COUNT(*) BY b = TSTEP(10, "2023-10-23T12:15:00.000Z", "2023-10-23T13:55:01.543Z")
            | LIMIT 10
            """);
        assertNotNull(plan);
    }

    public void testTstepBucketCountWithoutBoundsFails() {
        assumeTStepEnabled();
        assumeTrue("TSTEP bucket count requires corresponding capability", EsqlCapabilities.Cap.TSTEP_BUCKET_COUNT.isEnabled());
        analyzer().addSampleData().error("""
            FROM sample_data
            | STATS c = COUNT(*) BY b = TSTEP(10)
            | LIMIT 10
            """, containsString("requires either a `@timestamp` range in the request query filter"));
    }

    public void testTstepBucketCountUsesRequestTimestampBounds() {
        assumeTStepEnabled();
        assumeTrue("TSTEP bucket count requires corresponding capability", EsqlCapabilities.Cap.TSTEP_BUCKET_COUNT.isEnabled());
        var bounds = new QueryDslTimestampBoundsExtractor.TimestampBounds(
            Instant.parse("2023-10-23T12:15:00Z"),
            Instant.parse("2023-10-23T13:55:01.543Z")
        );
        var plan = analyzer().addSampleData().timestampBounds(bounds).query("""
            FROM sample_data
            | STATS c = COUNT(*) BY b = TSTEP(10)
            | LIMIT 10
            """);
        assertNotNull(plan);
    }

    /**
     * A constant that folds to {@code null}, like {@code TO_DATETIME(null)}, has the type of the cast but no value. It can't be a
     * bound or a step, and it fails the verification instead of throwing a {@link NullPointerException}.
     */
    public void testTstepConstantsThatAreNullFail() {
        assumeTStepEnabled();
        assumeTrue("TSTEP explicit bounds requires corresponding capability", EsqlCapabilities.Cap.TSTEP_EXPLICIT_BOUNDS.isEnabled());
        assumeTrue("TSTEP bucket count requires corresponding capability", EsqlCapabilities.Cap.TSTEP_BUCKET_COUNT.isEnabled());
        assumeTrue("the cast is lenient", minimumVersion.supports(ConvertFunction.LENIENT_CAST));
        String from = "\"2023-10-23T12:15:00.000Z\"";
        String to = "\"2023-10-23T13:55:01.543Z\"";
        for (String nullConstant : List.of("TO_DATETIME(null)")) {
            for (String args : List.of(
                "1 hour, " + nullConstant + ", " + to,
                "1 hour, " + from + ", " + nullConstant,
                "10, " + nullConstant + ", " + to,
                "10, " + from + ", " + nullConstant
            )) {
                analyzer().addSampleData().error("""
                    FROM sample_data
                    | STATS c = COUNT(*) BY b = TSTEP(%s)
                    | LIMIT 10
                    """.formatted(args), containsString("`from` and `to` must not be null"));
            }
        }
        for (String nullConstant : List.of("CASE(false, 1)")) {
            analyzer().addSampleData().error("""
                FROM sample_data
                | STATS c = COUNT(*) BY b = TSTEP(%s, %s, %s)
                | LIMIT 10
                """.formatted(nullConstant, from, to), containsString("target bucket count must not be null"));
        }
    }

    private static void assumeTStepEnabled() {
        assumeTrue("TSTEP requires corresponding capability", EsqlCapabilities.Cap.TSTEP.isEnabled());
    }
}
