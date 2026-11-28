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
import org.elasticsearch.xpack.esql.parser.ParsingException;
import org.junit.Before;

import java.time.Duration;
import java.time.Instant;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.syncEsqlQueryRequest;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

/**
 * The {@code @timestamp} range the query asks for, through its request filter and the filters in the query itself, is kept on the
 * request however the query ends. Requests are executed directly rather than via {@code run()}, which may wrap them in a copy.
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
        RequestedTimeRange range = request.requestedTimeRange();
        assertThat(range, equalTo(new RequestedTimeRange(FROM, TO, range.now())));
    }

    public void testFailure() {
        var request = syncEsqlQueryRequest("FROM logs | EVAL value = COUNT(*)").filter(
            new RangeQueryBuilder("@timestamp").gte(FROM.toString()).lte(TO.toString())
        );
        expectThrows(VerificationException.class, () -> client().execute(EsqlQueryAction.INSTANCE, request).actionGet());
        RequestedTimeRange range = request.requestedTimeRange();
        assertThat(range, equalTo(new RequestedTimeRange(FROM, TO, range.now())));
    }

    public void testRelativeRange() {
        var request = syncEsqlQueryRequest("FROM logs | LIMIT 1").filter(new RangeQueryBuilder("@timestamp").gte("now-1h").lte("now"));
        client().execute(EsqlQueryAction.INSTANCE, request).actionGet().close();
        // Date math is resolved against the now that is kept with the bounds, so the age of the range is exact.
        RequestedTimeRange range = request.requestedTimeRange();
        assertThat(range, equalTo(new RequestedTimeRange(range.now().minus(Duration.ofHours(1)), range.now(), range.now())));
    }

    public void testRangeInQuery() {
        // NOW() is resolved against the same now that is kept with the bounds, and a range in the query may have a single bound.
        var request = syncEsqlQueryRequest("FROM logs | WHERE @timestamp >= NOW() - 15 minutes | LIMIT 1");
        client().execute(EsqlQueryAction.INSTANCE, request).actionGet().close();
        RequestedTimeRange range = request.requestedTimeRange();
        assertThat(range, equalTo(new RequestedTimeRange(range.now().minus(Duration.ofMinutes(15)), null, range.now())));

        // The request filter and the query narrow each other.
        Instant mid = FROM.plus(Duration.ofMinutes(15));
        var narrowed = syncEsqlQueryRequest("FROM logs | WHERE @timestamp >= \"" + mid + "\" | LIMIT 1").filter(
            new RangeQueryBuilder("@timestamp").gte(FROM.toString()).lte(TO.toString())
        );
        client().execute(EsqlQueryAction.INSTANCE, narrowed).actionGet().close();
        range = narrowed.requestedTimeRange();
        assertThat(range, equalTo(new RequestedTimeRange(mid, TO, range.now())));
    }

    public void testFailureBeforeAnalysis() {
        // The range is only extracted once the query's now is fixed, after parsing, so that it does not need a now of its own.
        var request = syncEsqlQueryRequest("FROM logs | NOT_A_COMMAND").filter(
            new RangeQueryBuilder("@timestamp").gte(FROM.toString()).lte(TO.toString())
        );
        expectThrows(ParsingException.class, () -> client().execute(EsqlQueryAction.INSTANCE, request).actionGet());
        assertThat(request.requestedTimeRange(), nullValue());
    }

    public void testNoRequestedTimeRange() {
        // A request filter range with only one bound is not interpreted.
        var withoutRange = syncEsqlQueryRequest("FROM logs | LIMIT 1");
        var withLowerBoundOnly = syncEsqlQueryRequest("FROM logs | LIMIT 1").filter(new RangeQueryBuilder("@timestamp").gte("now-1h"));
        for (EsqlQueryRequest request : new EsqlQueryRequest[] { withoutRange, withLowerBoundOnly }) {
            client().execute(EsqlQueryAction.INSTANCE, request).actionGet().close();
            assertThat(request.requestedTimeRange(), nullValue());
        }
    }
}
