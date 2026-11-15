/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.hamcrest.Matchers.equalTo;

/**
 * Counts by {@code DATE_TRUNC} over a date field that is multivalued in some documents. The counts are pushed down to Lucene as
 * one query per bucket, and must be the same as when the buckets are computed from the values of the field: a multivalued date
 * has no bucket, and does not pass a comparison either.
 */
public class CountByDateTruncMultiValueIT extends AbstractEsqlIntegTestCase {

    private static final String INDEX = "mv_dates";

    @Before
    public void populate() {
        client().admin()
            .indices()
            .prepareCreate(INDEX)
            .setSettings(Settings.builder().put("index.number_of_shards", 1))
            .setMapping("d", "type=date")
            .get();
        BulkRequestBuilder bulk = client().prepareBulk().setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        bulk.add(client().prepareIndex(INDEX).setSource("d", "2015-01-05T00:00:00Z"));
        bulk.add(client().prepareIndex(INDEX).setSource("d", List.of("2015-01-05T00:00:00Z", "2015-01-06T00:00:00Z")));
        bulk.add(client().prepareIndex(INDEX).setSource("d", List.of("2015-01-07T00:00:00Z", "2015-02-16T00:00:00Z")));
        bulk.add(client().prepareIndex(INDEX).setSource("d", "2015-02-15T00:00:00Z"));
        bulk.add(client().prepareIndex(INDEX).setSource("other", 1));
        assertFalse(bulk.get().hasFailures());
    }

    public void testFilterOnTheDate() {
        String query = "FROM " + INDEX + """
             | WHERE d >= "2015-01-01T00:00:00Z"
             | STATS c = COUNT(*) BY bucket = DATE_TRUNC(1 month, d)
             | SORT bucket
            """;
        // The multivalued dates do not pass the filter
        List<List<Object>> expected = List.of(List.of(1L, "2015-01-01T00:00:00.000Z"), List.of(1L, "2015-02-01T00:00:00.000Z"));
        assertThat(run(query, false), equalTo(expected));
        assertThat(run(query, true), equalTo(expected));
    }

    public void testRangeOnTheDate() {
        String query = "FROM " + INDEX + """
             | WHERE d >= "2015-01-01T00:00:00Z" AND d < "2015-02-16T00:00:00Z"
             | STATS c = COUNT(*) BY bucket = DATE_TRUNC(1 month, d)
             | SORT bucket
            """;
        List<List<Object>> expected = List.of(List.of(1L, "2015-01-01T00:00:00.000Z"), List.of(1L, "2015-02-01T00:00:00.000Z"));
        assertThat(run(query, false), equalTo(expected));
        assertThat(run(query, true), equalTo(expected));
    }

    public void testNoFilter() {
        String query = "FROM " + INDEX + """
             | STATS c = COUNT(*) BY bucket = DATE_TRUNC(1 month, d)
             | SORT bucket
            """;
        List<List<Object>> expected = new ArrayList<>();
        expected.add(List.of(1L, "2015-01-01T00:00:00.000Z"));
        expected.add(List.of(1L, "2015-02-01T00:00:00.000Z"));
        // The two multivalued dates and the missing one
        expected.add(Arrays.asList(3L, null));
        assertThat(run(query, false), equalTo(expected));
        assertThat(run(query, true), equalTo(expected));
    }

    /**
     * A single bucket for all the dates, which needs no range: the documents are told apart by whether the field exists.
     */
    public void testSingleBucket() {
        String query = "FROM " + INDEX + """
             | STATS c = COUNT(*) BY bucket = DATE_TRUNC(1 year, d)
             | SORT bucket
            """;
        List<List<Object>> expected = new ArrayList<>();
        expected.add(List.of(2L, "2015-01-01T00:00:00.000Z"));
        expected.add(Arrays.asList(3L, null));
        assertThat(run(query, null, false), equalTo(expected));
        assertThat(run(query, null, true), equalTo(expected));
    }

    /**
     * A filter of the query DSL matches a multivalued date if any of its values match, unlike a comparison of the query.
     * The matching multivalued dates still have no bucket.
     */
    public void testRequestFilter() {
        String query = "FROM " + INDEX + """
             | STATS c = COUNT(*) BY bucket = DATE_TRUNC(1 month, d)
             | SORT bucket
            """;
        QueryBuilder filter = QueryBuilders.rangeQuery("d").gte("2015-01-01T00:00:00Z").lt("2015-02-01T00:00:00Z");
        List<List<Object>> expected = new ArrayList<>();
        expected.add(List.of(1L, "2015-01-01T00:00:00.000Z"));
        expected.add(Arrays.asList(2L, null));
        assertThat(run(query, filter, false), equalTo(expected));
        assertThat(run(query, filter, true), equalTo(expected));
    }

    private List<List<Object>> run(String query, boolean pushDown) {
        return run(query, null, pushDown);
    }

    /**
     * Runs the query with the buckets either pushed down to Lucene as queries, or computed from the values of the field.
     */
    private List<List<Object>> run(String query, @Nullable QueryBuilder filter, boolean pushDown) {
        EsqlQueryRequest request = new EsqlQueryRequest();
        request.query(query);
        if (filter != null) {
            request.filter(filter);
        }
        if (pushDown == false) {
            request.pragmas(new QueryPragmas(Settings.builder().put(QueryPragmas.ROUNDTO_PUSHDOWN_THRESHOLD.getKey(), 0).build()));
            request.acceptedPragmaRisks(true);
        }
        try (EsqlQueryResponse response = run(request)) {
            return getValuesList(response);
        }
    }
}
