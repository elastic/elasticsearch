/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.compute.lucene.query.LuceneOperator;
import org.elasticsearch.compute.operator.DriverProfile;
import org.elasticsearch.compute.operator.OperatorStatus;
import org.elasticsearch.xpack.esql.action.AbstractEsqlIntegTestCase;
import org.elasticsearch.xpack.esql.action.EsqlQueryRequest;
import org.elasticsearch.xpack.esql.action.EsqlQueryResponse;
import org.elasticsearch.xpack.esql.planner.PlannerSettings;
import org.junit.After;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.syncEsqlQueryRequest;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThan;

/**
 * {@code SORT _score DESC | LIMIT N} above a filter that can't be pushed to Lucene feeds the TopN's
 * bound back into Lucene with {@link PlannerSettings#MIN_COMPETITIVE_SCORE_OPTIMIZATION_ENABLED}.
 * The results must be the same as without it, modulo the order of tied rows.
 */
public class MinCompetitiveScoreIT extends AbstractEsqlIntegTestCase {
    private static final String[] TERMS = { "a", "b", "c", "d", "e" };

    @After
    public void resetSetting() {
        updateClusterSettings(Settings.builder().putNull(PlannerSettings.MIN_COMPETITIVE_SCORE_OPTIMIZATION_ENABLED.getKey()));
    }

    public void testSameResultsAsWithoutOptimization() {
        createIndex(between(1, 3));
        int numDocs = between(100, 3000);
        BulkRequestBuilder bulk = client().prepareBulk().setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        for (int i = 0; i < numDocs; i++) {
            StringBuilder content = new StringBuilder();
            int length = between(1, 30);
            for (int t = 0; t < length; t++) {
                // Skewed so some terms are much more frequent than others
                content.append(TERMS[Math.min(TERMS.length - 1, (int) Math.floor(-Math.log(randomDouble() + 1e-9)))]).append(' ');
            }
            bulk.add(prepareIndex("test").setId(Integer.toString(i)).setSource("id", i, "content", content.toString()));
        }
        assertFalse(bulk.get().hasFailures());

        for (int q = 0; q < 5; q++) {
            String match = randomFrom("match(content, \"a\")", "match(content, \"a c\")", "match(content, \"b d e\")", "content:\"c\"");
            int modulo = between(2, 5);
            int limit = between(1, 100);
            // id % modulo can't be pushed to Lucene, so the TopN can't be either
            String query = String.format(Locale.ROOT, """
                FROM test METADATA _score
                | WHERE %s AND id %% %d != 0
                | SORT _score DESC
                | LIMIT %d
                | KEEP id, _score
                """, match, modulo, limit);
            List<List<Object>> off = runWithOptimization(query, false);
            List<List<Object>> on = runWithOptimization(query, true);
            assertSameTopN(query, off, on);
        }
    }

    /**
     * A few documents score far higher than all the others. With the optimization Lucene should
     * skip most of the others.
     */
    public void testSkipsDocuments() {
        assumeTrue("needs the page_size pragma, which release builds reject", canUseQueryPragmas());
        createIndex(1);
        int numDocs = 20_000;
        Set<Integer> hot = new HashSet<>();
        while (hot.size() < 100) {
            hot.add(between(0, numDocs - 1));
        }
        BulkRequestBuilder bulk = client().prepareBulk();
        for (int i = 0; i < numDocs; i++) {
            String content = hot.contains(i) ? "a a a a a a a a b" : "a " + "b ".repeat(between(10, 40));
            bulk.add(prepareIndex("test").setId(Integer.toString(i)).setSource("id", i, "content", content));
        }
        assertFalse(bulk.get().hasFailures());
        client().admin().indices().prepareForceMerge("test").setMaxNumSegments(1).get();
        client().admin().indices().prepareRefresh("test").get();

        String query = """
            FROM test METADATA _score
            | WHERE match(content, "a") AND id % 3 != 0
            | SORT _score DESC
            | LIMIT 10
            | KEEP id, _score
            """;
        updateClusterSettings(Settings.builder().put(PlannerSettings.MIN_COMPETITIVE_SCORE_OPTIMIZATION_ENABLED.getKey(), false));
        long offRows;
        List<List<Object>> off;
        try (EsqlQueryResponse resp = run(smallPages(profiled(query)))) {
            off = getValuesList(resp);
            offRows = luceneSourceRowsEmitted(resp);
        }
        updateClusterSettings(Settings.builder().put(PlannerSettings.MIN_COMPETITIVE_SCORE_OPTIMIZATION_ENABLED.getKey(), true));
        long onRows;
        List<List<Object>> on;
        try (EsqlQueryResponse resp = run(smallPages(profiled(query)))) {
            on = getValuesList(resp);
            onRows = luceneSourceRowsEmitted(resp);
        }
        logger.info("rows emitted by LuceneSourceOperator: off={} on={}", offRows, onRows);
        assertSameTopN(query, off, on);
        assertThat(offRows, equalTo((long) numDocs));
        assertThat(onRows, greaterThan(0L));
        assertThat(onRows, lessThan(offRows / 2));
    }

    /**
     * A {@code MATCH} that runs in the compute engine adds its score to {@code _score}, so the
     * optimization must not kick in: the TopN's bound would be on the sum, not on Lucene's score.
     */
    public void testFilterAddingToScore() {
        createIndex(between(1, 3));
        int numDocs = between(100, 2000);
        BulkRequestBuilder bulk = client().prepareBulk().setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        for (int i = 0; i < numDocs; i++) {
            String content = randomFrom(TERMS) + " " + randomFrom(TERMS) + " " + randomFrom(TERMS);
            bulk.add(prepareIndex("test").setId(Integer.toString(i)).setSource("id", i, "content", content));
        }
        assertFalse(bulk.get().hasFailures());
        String filter = randomFrom(
            // The OR can't be pushed to Lucene so the source matches everything and MATCH is scored in the compute engine
            "WHERE match(content, \"a\") OR id % 3 == 0",
            // The first MATCH is pushed to Lucene and scored there, the second is scored in the compute engine
            "WHERE match(content, \"a\") AND (match(content, \"b\") OR id % 3 == 0)"
        );
        String query = String.format(Locale.ROOT, """
            FROM test METADATA _score
            | %s
            | SORT _score DESC
            | LIMIT %d
            | KEEP id, _score
            """, filter, between(1, 50));
        updateClusterSettings(Settings.builder().put(PlannerSettings.MIN_COMPETITIVE_SCORE_OPTIMIZATION_ENABLED.getKey(), false));
        long offRows;
        List<List<Object>> off;
        try (EsqlQueryResponse resp = run(profiled(query))) {
            off = getValuesList(resp);
            offRows = luceneSourceRowsEmitted(resp);
        }
        updateClusterSettings(Settings.builder().put(PlannerSettings.MIN_COMPETITIVE_SCORE_OPTIMIZATION_ENABLED.getKey(), true));
        long onRows;
        List<List<Object>> on;
        try (EsqlQueryResponse resp = run(profiled(query))) {
            on = getValuesList(resp);
            onRows = luceneSourceRowsEmitted(resp);
        }
        assertSameTopN(query, off, on);
        // The optimization didn't kick in so the source didn't skip anything
        assertThat(query, onRows, equalTo(offRows));
    }

    private void createIndex(int shards) {
        assertAcked(
            prepareCreate("test").setSettings(Settings.builder().put("index.number_of_shards", shards).put("index.number_of_replicas", 0))
                .setMapping("id", "type=integer", "content", "type=text")
        );
    }

    private List<List<Object>> runWithOptimization(String query, boolean enabled) {
        updateClusterSettings(Settings.builder().put(PlannerSettings.MIN_COMPETITIVE_SCORE_OPTIMIZATION_ENABLED.getKey(), enabled));
        try (EsqlQueryResponse resp = run(query)) {
            return getValuesList(resp);
        }
    }

    /**
     * Profile the query so we can count the rows the source emitted.
     */
    private static EsqlQueryRequest profiled(String query) {
        EsqlQueryRequest request = syncEsqlQueryRequest(query);
        request.profile(true);
        return request;
    }

    /**
     * Use small pages because the TopN can only publish a bound once it received a page, so a
     * single page containing the whole index could never skip anything. Pragmas are rejected on
     * release builds, callers must check {@link #canUseQueryPragmas()} first.
     */
    private static EsqlQueryRequest smallPages(EsqlQueryRequest request) {
        request.pragmas(new QueryPragmas(Settings.builder().put(QueryPragmas.PAGE_SIZE.getKey(), 500).build()));
        return request;
    }

    private static long luceneSourceRowsEmitted(EsqlQueryResponse resp) {
        long rows = 0;
        for (DriverProfile driver : resp.profile().drivers()) {
            for (OperatorStatus op : driver.operators()) {
                if (op.operator().startsWith("LuceneSourceOperator") && op.status() instanceof LuceneOperator.Status status) {
                    rows += status.rowsEmitted();
                }
            }
        }
        return rows;
    }

    /**
     * The scores must match exactly. Rows strictly better than the last score must match too. Rows
     * tied with the last score may be any of the tied rows.
     */
    private static void assertSameTopN(String query, List<List<Object>> expected, List<List<Object>> actual) {
        assertThat(query, scores(actual), equalTo(scores(expected)));
        if (expected.isEmpty()) {
            return;
        }
        double last = (Double) expected.getLast().get(1);
        assertThat(query, idsAbove(actual, last), equalTo(idsAbove(expected, last)));
    }

    private static List<Double> scores(List<List<Object>> rows) {
        List<Double> scores = new ArrayList<>(rows.size());
        for (List<Object> row : rows) {
            scores.add((Double) row.get(1));
        }
        return scores;
    }

    private static Set<Object> idsAbove(List<List<Object>> rows, double score) {
        Set<Object> ids = new HashSet<>();
        for (List<Object> row : rows) {
            if ((Double) row.get(1) > score) {
                ids.add(row.get(0));
            }
        }
        return ids;
    }
}
