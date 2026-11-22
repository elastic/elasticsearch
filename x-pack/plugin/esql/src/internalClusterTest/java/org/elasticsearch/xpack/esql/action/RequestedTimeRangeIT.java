/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.xpack.esql.VerificationException;
import org.elasticsearch.xpack.esql.core.querydsl.QueryDslTimestampBoundsExtractor.TimestampBounds;
import org.junit.Before;

import java.time.Instant;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.syncEsqlQueryRequest;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

/**
 * The {@code @timestamp} range of the request filter is kept on the request however the query ends. Requests are executed directly
 * rather than via {@code run()}, which may wrap them in a copy.
 */
public class RequestedTimeRangeIT extends AbstractEsqlIntegTestCase {

    private static final Instant FROM = Instant.parse("2026-01-01T00:00:00Z");
    private static final Instant TO = Instant.parse("2026-01-01T01:00:00Z");

    @Before
    public void createIndex() {
        assertAcked(prepareCreate("logs").setMapping("@timestamp", "type=date"));
        client().prepareIndex("logs").setSource("@timestamp", "2026-01-01T00:30:00Z").get();
        refresh("logs");
        ensureGreen("logs");
    }

    public void testSuccess() {
        // Kibana's nested filter shape.
        var request = syncEsqlQueryRequest("FROM logs | LIMIT 1").filter(
            new BoolQueryBuilder().filter(new RangeQueryBuilder("@timestamp").gte(FROM.toString()).lte(TO.toString()))
        );
        client().execute(EsqlQueryAction.INSTANCE, request).actionGet().close();
        assertThat(request.requestedTimeRange(), equalTo(new TimestampBounds(FROM, TO)));
    }

    public void testFailure() {
        var request = syncEsqlQueryRequest("FROM logs | EVAL value = COUNT(*)").filter(
            new RangeQueryBuilder("@timestamp").gte(FROM.toString()).lte(TO.toString())
        );
        expectThrows(VerificationException.class, () -> client().execute(EsqlQueryAction.INSTANCE, request).actionGet());
        assertThat(request.requestedTimeRange(), equalTo(new TimestampBounds(FROM, TO)));
    }

    public void testNoRequestedTimeRange() {
        // A range in the query itself is not interpreted, and neither is a filter range with only one bound.
        var withoutFilter = syncEsqlQueryRequest("FROM logs | WHERE @timestamp >= NOW() - 1 hour | LIMIT 1");
        var withLowerBoundOnly = syncEsqlQueryRequest("FROM logs | LIMIT 1").filter(new RangeQueryBuilder("@timestamp").gte("now-1h"));
        for (EsqlQueryRequest request : new EsqlQueryRequest[] { withoutFilter, withLowerBoundOnly }) {
            client().execute(EsqlQueryAction.INSTANCE, request).actionGet().close();
            assertThat(request.requestedTimeRange(), nullValue());
        }
    }
}
