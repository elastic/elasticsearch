/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.compute.lucene.query.LuceneSourceOperator;
import org.elasticsearch.compute.operator.DriverProfile;
import org.elasticsearch.compute.operator.OperatorStatus;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThan;

/**
 * {@code field IN (subquery)} over an indexed field stays an IN list, which is pushed to Lucene as a {@code terms} query that reads only
 * the matching rows, however many values the subquery returns, up to the most terms the index accepts. Past that, or when the limit is
 * pinned, it becomes a hash join that reads every row. Both shapes return the same rows.
 * <p>
 * The rows the Lucene sources emit tell the two shapes apart: the subquery adds the same rows to both, so the main query reads
 * fewer than {@link #DOCS} in total only when the IN list was pushed.
 */
public class InSubqueryFilterPushdownIT extends AbstractEsqlIntegTestCase {

    private static final int DOCS = 1_000;

    /**
     * More values than the default limit of an IN list evaluated per row, which is 100, and fewer than {@link #DOCS}.
     */
    private static final int MANY_VALUES = 300;

    public void testManyValuesOnIndexedFieldArePushedToLucene() {
        createEvents("events", null);

        QueryResult result = query("events", MANY_VALUES, getPragmas());

        assertThat(result.matches(), equalTo((long) MANY_VALUES));
        assertThat("the IN list was pushed to Lucene, so only the matching rows were read", result.rowsRead(), lessThan((long) DOCS));
    }

    public void testPinnedThresholdUsesHashJoin() {
        assumeTrue("requires query pragmas", canUseQueryPragmas());
        createEvents("events", null);

        QueryResult result = query("events", MANY_VALUES, pinnedThreshold(0));

        assertThat(result.matches(), equalTo((long) MANY_VALUES));
        assertThat("a hash join reads every row", result.rowsRead(), greaterThanOrEqualTo((long) DOCS));
    }

    public void testPinnedThresholdAboveValuesKeepsInList() {
        assumeTrue("requires query pragmas", canUseQueryPragmas());
        createEvents("events", null);

        QueryResult result = query("events", MANY_VALUES, pinnedThreshold(MANY_VALUES));

        assertThat(result.matches(), equalTo((long) MANY_VALUES));
        assertThat(result.rowsRead(), lessThan((long) DOCS));
    }

    /**
     * A shard rejects a {@code terms} query longer than its {@code index.max_terms_count}, so a list longer than that must be a hash
     * join rather than a failed query.
     */
    public void testMoreValuesThanMaxTermsCountUseHashJoin() {
        int maxTermsCount = MANY_VALUES / 2;
        createEvents("events", maxTermsCount);

        QueryResult result = query("events", MANY_VALUES, getPragmas());

        assertThat(result.matches(), equalTo((long) MANY_VALUES));
        assertThat(result.rowsRead(), greaterThanOrEqualTo((long) DOCS));
    }

    public void testValuesWithinMaxTermsCountArePushedToLucene() {
        createEvents("events", MANY_VALUES);

        QueryResult result = query("events", MANY_VALUES, getPragmas());

        assertThat(result.matches(), equalTo((long) MANY_VALUES));
        assertThat(result.rowsRead(), lessThan((long) DOCS));
    }

    /**
     * The smallest limit of the indices the query reads is the one that counts, as every shard of every index runs the query.
     */
    public void testSmallestMaxTermsCountOfTheIndicesDecides() {
        createEvents("events-wide", null);
        createEvents("events-narrow", MANY_VALUES / 2);

        QueryResult result = query("events-*", MANY_VALUES, getPragmas());

        // Both indices hold the same sessions, so each value matches one document in each of them.
        assertThat(result.matches(), equalTo(2L * MANY_VALUES));
        assertThat(result.rowsRead(), greaterThanOrEqualTo(2L * DOCS));
    }

    private void createEvents(String index, Integer maxTermsCount) {
        Settings.Builder settings = Settings.builder().put("index.number_of_shards", 1).put("index.number_of_replicas", 0);
        if (maxTermsCount != null) {
            settings.put(IndexSettings.MAX_TERMS_COUNT_SETTING.getKey(), maxTermsCount);
        }
        assertAcked(
            client().admin()
                .indices()
                .prepareCreate(index)
                .setSettings(settings)
                .setMapping("session", "type=keyword", "marker", "type=long")
        );
        BulkRequestBuilder bulk = client().prepareBulk().setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        for (int i = 0; i < DOCS; i++) {
            bulk.add(new IndexRequest(index).source("session", "s-" + i, "marker", i));
        }
        BulkResponse response = bulk.get();
        assertFalse(response.buildFailureMessage(), response.hasFailures());
        ensureYellow(index);
    }

    /**
     * Counts the events of the sessions of the first {@code values} events, and how many rows the Lucene sources emitted to do so. The
     * aggregation reads a field, so Lucene does not answer it from its own statistics.
     */
    private QueryResult query(String pattern, int values, QueryPragmas pragmas) {
        String query = "FROM "
            + pattern
            + " | WHERE session IN (FROM "
            + pattern
            + " | WHERE marker < "
            + values
            + " | STATS c = COUNT(*) BY session | KEEP session)"
            + " | STATS matches = COUNT(*), total = SUM(marker)";
        EsqlQueryRequest request = EsqlQueryRequest.syncEsqlQueryRequest(query);
        request.profile(true);
        request.pragmas(pragmas);
        try (EsqlQueryResponse response = run(request)) {
            long matches = (long) response.values().next().next();
            long rowsRead = 0;
            for (DriverProfile driver : response.profile().drivers()) {
                for (OperatorStatus operator : driver.operators()) {
                    if (operator.status() instanceof LuceneSourceOperator.Status status) {
                        rowsRead += status.rowsEmitted();
                    }
                }
            }
            return new QueryResult(matches, rowsRead);
        }
    }

    private static QueryPragmas pinnedThreshold(int threshold) {
        return new QueryPragmas(Settings.builder().put("in_subquery_hash_join_threshold", threshold).build());
    }

    private record QueryResult(long matches, long rowsRead) {}
}
