/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.ml;

import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.index.query.MatchNoneQueryBuilder;
import org.elasticsearch.xpack.esql.action.AbstractEsqlIntegTestCase;
import org.elasticsearch.xpack.esql.action.EsqlQueryResponse;
import org.elasticsearch.xpack.ml.datafeed.esql.spi.ParsedEsqlQuery;
import org.elasticsearch.xpack.ml.datafeed.esql.spi.TimeBucket;
import org.junit.Before;

import java.util.Optional;

import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.syncEsqlQueryRequest;
import static org.hamcrest.Matchers.equalTo;

/**
 * Checks on a real cluster that the bucket span ES|QL reports in {@code column_metadata} reaches the datafeed through
 * {@link ParsedEsqlQuery#timeBucket}, which is what lets datafeeds avoid a second interpretation of the plan.
 */
public class EsqlDatafeedQueryParserIT extends AbstractEsqlIntegTestCase {

    private final EsqlDatafeedQueryParser parser = new EsqlDatafeedQueryParser();

    @Before
    public void createLogsIndex() {
        client().admin().indices().prepareCreate("logs").setMapping("@timestamp", "type=date", "v", "type=long").get();
        client().prepareIndex("logs")
            .setSource("@timestamp", "2025-01-01T10:30:00Z", "v", 1)
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            .get();
    }

    private Optional<TimeBucket> timeBucket(String query, String timeColumn) {
        ParsedEsqlQuery parsed = parser.parse(query, internalCluster().getCurrentMasterNodeInstance(ClusterService.class));
        // A filter that matches nothing reads no rows and, unlike LIMIT 0, keeps the column metadata.
        var request = syncEsqlQueryRequest(parsed.columnMetadataProbeQuery().query()).filter(new MatchNoneQueryBuilder());
        try (EsqlQueryResponse response = run(request)) {
            return parsed.timeBucket(response.columns(), timeColumn);
        }
    }

    public void testBucketShouldReportItsSpan() {
        assertThat(
            timeBucket("FROM logs | STATS c = COUNT(*) BY b = BUCKET(@timestamp, 1 hour)", "b"),
            equalTo(Optional.of(new TimeBucket(1, "hour")))
        );
    }

    public void testTBucketShouldReportItsSpan() {
        assertThat(
            timeBucket("FROM logs | STATS c = COUNT(*) BY b = TBUCKET(15 minutes)", "b"),
            equalTo(Optional.of(new TimeBucket(15, "minute")))
        );
        assertThat(timeBucket("FROM logs | STATS c = COUNT(*) BY b = TBUCKET(1 day)", "b"), equalTo(Optional.of(new TimeBucket(1, "day"))));
    }

    public void testSpanShouldSurviveCommandsAfterTheStats() {
        assertThat(
            timeBucket("FROM logs | STATS c = COUNT(*) BY b = TBUCKET(1 hour) | EVAL d = c + 1 | SORT b ASC | KEEP b, d | LIMIT 10", "b"),
            equalTo(Optional.of(new TimeBucket(1, "hour")))
        );
    }

    public void testDateTruncShouldReportNoSpan() {
        assertThat(timeBucket("FROM logs | STATS c = COUNT(*) BY b = DATE_TRUNC(1 hour, @timestamp)", "b"), equalTo(Optional.empty()));
    }

    public void testRawQueryShouldReportNoSpan() {
        assertThat(timeBucket("FROM logs | KEEP @timestamp, v", "@timestamp"), equalTo(Optional.empty()));
    }
}
