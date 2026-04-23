/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.analysis;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.EsqlTestUtils;
import org.elasticsearch.xpack.esql.core.querydsl.QueryDslTimestampBoundsExtractor;

import java.time.Instant;

import static org.hamcrest.Matchers.containsString;

public class TStepResolutionTests extends ESTestCase {

    public void testTstepUsesRequestTimestampBounds() {
        var bounds = new QueryDslTimestampBoundsExtractor.TimestampBounds(
            Instant.parse("2023-10-23T12:15:00Z"),
            Instant.parse("2023-10-23T13:55:01.543Z")
        );
        var plan = EsqlTestUtils.analyzer().addSampleData().timestampBounds(bounds).query("""
            FROM sample_data
            | STATS c = COUNT(*) BY b = TSTEP(1 hour)
            | LIMIT 10
            """);
        assertNotNull(plan);
    }

    public void testTstepFailsWithoutRequestTimestampBounds() {
        EsqlTestUtils.analyzer().addSampleData().error("""
            FROM sample_data
            | STATS c = COUNT(*) BY b = TSTEP(1 hour)
            | LIMIT 10
            """, containsString("requires a `@timestamp` range in the request query filter"));
    }

    public void testTstepFailsWithTrangeInQuery() {
        var bounds = new QueryDslTimestampBoundsExtractor.TimestampBounds(
            Instant.parse("2023-10-23T12:15:00Z"),
            Instant.parse("2023-10-23T13:55:01.543Z")
        );
        EsqlTestUtils.analyzer().addSampleData().timestampBounds(bounds).error("""
            FROM sample_data
            | WHERE TRANGE("2023-10-23T12:15:00.000Z", "2023-10-23T13:55:01.543Z")
            | STATS c = COUNT(*) BY b = TSTEP(1 hour)
            | LIMIT 10
            """, containsString("cannot be used together with TRANGE"));
    }
}
