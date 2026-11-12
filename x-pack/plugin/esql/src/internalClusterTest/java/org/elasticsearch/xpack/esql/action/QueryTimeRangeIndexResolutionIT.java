/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.junit.Before;

import java.time.Instant;
import java.util.List;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.syncEsqlQueryRequest;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;

/**
 * The time range that a query puts on {@code @timestamp} filters the indices that field caps resolves, see
 * <a href="https://github.com/elastic/elasticsearch/issues/154596">#154596</a>. Here that changes the mapping that the
 * query is analyzed with: {@code code} is a keyword in the index with the old data and a long in the index with the new
 * data, so it is a long only for a query that leaves the old index out, and a conflict for any other.
 */
public class QueryTimeRangeIndexResolutionIT extends AbstractEsqlIntegTestCase {

    private static final String OLD_INDEX = "events-old";
    private static final String NEW_INDEX = "events-new";
    private static final int DOCS = 10;
    private static final String NEW_DATA = "@timestamp >= \"2024-05-01T00:00:00Z\"";
    private static final String NO_DATA = "@timestamp >= \"2030-01-01T00:00:00Z\"";

    @Before
    public void setupIndices() {
        Settings.Builder settings = Settings.builder().put("index.number_of_shards", 1).put("index.number_of_replicas", 0);
        assertAcked(
            client().admin()
                .indices()
                .prepareCreate(OLD_INDEX)
                .setSettings(settings)
                .setMapping("@timestamp", "type=date", "code", "type=keyword", "old_only", "type=keyword")
        );
        assertAcked(
            client().admin()
                .indices()
                .prepareCreate(NEW_INDEX)
                .setSettings(settings)
                .setMapping("@timestamp", "type=date", "code", "type=long")
        );
        long old = Instant.parse("2020-01-01T00:00:00Z").toEpochMilli();
        long recent = Instant.parse("2024-05-01T00:00:00Z").toEpochMilli();
        BulkRequestBuilder bulk = client().prepareBulk().setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        for (int d = 0; d < DOCS; d++) {
            bulk.add(client().prepareIndex(OLD_INDEX).setSource("@timestamp", old + d, "code", "c" + d, "old_only", "o" + d));
            bulk.add(client().prepareIndex(NEW_INDEX).setSource("@timestamp", recent + d, "code", d));
        }
        BulkResponse response = bulk.get();
        assertFalse(response.buildFailureMessage(), response.hasFailures());
    }

    /** The sum is over a long once the old index is out of the resolution. Without a time range the two types conflict. */
    public void testTimeRangeChangesResolvedMapping() {
        Exception conflict = expectThrows(Exception.class, () -> run(syncEsqlQueryRequest("FROM events-* | STATS s = SUM(code)")).close());
        assertThat(conflict.getMessage(), containsString("code"));

        try (EsqlQueryResponse response = run(syncEsqlQueryRequest("FROM events-* | WHERE " + NEW_DATA + " | STATS s = SUM(code)"))) {
            assertThat(response.columns().get(0).outputType(), equalTo("long"));
            assertThat(getValuesList(response), contains(List.of((long) DOCS * (DOCS - 1) / 2)));
        }
    }

    /**
     * No index holds data in the time range. The indices are resolved again without it, which gives the query the columns of
     * every index of its pattern in place of a failure for a pattern that matches no index.
     */
    public void testTimeRangeWithoutData() {
        try (EsqlQueryResponse response = run(syncEsqlQueryRequest("FROM events-* | WHERE " + NO_DATA + " | KEEP @timestamp, old_only"))) {
            assertThat(response.columns().stream().map(ColumnInfoImpl::name).toList(), contains("@timestamp", "old_only"));
            assertThat(getValuesList(response), empty());
        }
    }

    /** The column that only the old index maps fails the analysis without that index, which is then tried again with it. */
    public void testColumnOfPrunedIndex() {
        String query = "FROM events-* | WHERE " + NEW_DATA + " | STATS c = COUNT(*), o = COUNT(old_only)";
        try (EsqlQueryResponse response = run(syncEsqlQueryRequest(query))) {
            assertThat(getValuesList(response), contains(List.of((long) DOCS, 0L)));
        }
    }

    /**
     * The request filter matches the new index and the time range of the query matches none. The indices are resolved again
     * with the request filter alone, which keeps the old index and with it the conflict out of the mapping.
     */
    public void testTimeRangeWithoutDataKeepsRequestFilter() {
        QueryBuilder requestFilter = QueryBuilders.rangeQuery("@timestamp").gte("2024-01-01T00:00:00Z");
        String query = "FROM events-* | WHERE " + NO_DATA + " | STATS s = SUM(code)";
        try (EsqlQueryResponse response = run(syncEsqlQueryRequest(query).filter(requestFilter))) {
            assertThat(response.columns().get(0).outputType(), equalTo("long"));
            assertThat(getValuesList(response).get(0).get(0), equalTo(null));
        }
    }

    /** The two filters are combined: the request filter keeps both indices, the time range of the query only the new one. */
    public void testTimeRangeNarrowsRequestFilterOnTime() {
        QueryBuilder requestFilter = QueryBuilders.rangeQuery("@timestamp").gte("2019-01-01T00:00:00Z");
        String query = "FROM events-* | WHERE " + NEW_DATA + " | STATS s = SUM(code)";
        try (EsqlQueryResponse response = run(syncEsqlQueryRequest(query).filter(requestFilter))) {
            assertThat(getValuesList(response), contains(List.of((long) DOCS * (DOCS - 1) / 2)));
        }
    }
}
