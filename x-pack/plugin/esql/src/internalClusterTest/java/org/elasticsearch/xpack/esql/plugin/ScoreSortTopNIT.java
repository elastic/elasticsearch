/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.xpack.esql.action.AbstractEsqlIntegTestCase;
import org.elasticsearch.xpack.esql.action.EsqlQueryRequest;

import java.util.List;
import java.util.Locale;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasSize;

/**
 * Runs top N queries sorted by descending {@code _score} over several shards. The drivers that read the shards skip the
 * documents that score below a top N another driver has already filled. The top N must stay correct: the same rows when
 * the sort orders them completely, and the same scores when rows tie on every sort key.
 */
public class ScoreSortTopNIT extends AbstractEsqlIntegTestCase {
    /**
     * More than the documents in the index, so no top N fills and no document is skipped.
     */
    private static final int NO_LIMIT = 10_000;

    public void testScoreThenNumericKey() {
        assertTopNIsPrefixOfAllRows("_score DESC, id " + randomFrom("ASC", "DESC"), true);
    }

    /**
     * A keyword key shares a collector manager on main, and shares the bound like the other keys with this change.
     */
    public void testScoreThenKeywordKey() {
        assertTopNIsPrefixOfAllRows("_score DESC, kw " + randomFrom("ASC", "DESC") + ", id ASC", true);
    }

    public void testScoreThenDateKey() {
        assertTopNIsPrefixOfAllRows("_score DESC, ts " + randomFrom("ASC", "DESC") + ", id ASC", true);
    }

    /**
     * Which of the rows that tie on the last score are returned is not defined, so only the scores are compared.
     */
    public void testScoreAlone() {
        assertTopNIsPrefixOfAllRows("_score DESC", false);
    }

    /**
     * Every shard holds the same number of identical documents, so every row has the same score and the second sort key
     * alone orders them. A driver that has filled its top N must not make the others skip the rows that tie with it: the
     * rows with the lowest ids are spread over all shards.
     */
    public void testTiesOnScoreAcrossShards() {
        int indices = between(2, 4);
        int docsPerIndex = between(50, 200);
        for (int index = 0; index < indices; index++) {
            String name = "ties-" + index;
            assertAcked(
                client().admin()
                    .indices()
                    .prepareCreate(name)
                    .setSettings(Settings.builder().put("index.number_of_shards", 1))
                    .setMapping("id", "type=integer", "content", "type=text")
            );
            BulkRequestBuilder bulk = client().prepareBulk();
            // Highest ids first, so a collector keeps finding better rows after its top N is full
            for (int d = docsPerIndex - 1; d >= 0; d--) {
                int id = index + d * indices;
                bulk.add(new IndexRequest(name).id(Integer.toString(id)).source("id", id, "content", "fox"));
            }
            bulk.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE).get();
            ensureYellow(name);
        }
        for (int i = 0; i < 5; i++) {
            int limit = between(1, docsPerIndex);
            String query = String.format(Locale.ROOT, """
                FROM ties-* METADATA _score
                | WHERE match(content, "fox")
                | SORT _score DESC, id ASC
                | LIMIT %d
                | KEEP id, _score
                """, limit);
            try (var resp = run(query)) {
                List<List<Object>> rows = getValuesList(resp);
                assertThat(rows, hasSize(limit));
                for (int row = 0; row < limit; row++) {
                    assertThat("limit " + limit, rows.get(row).get(0), equalTo(row));
                    assertThat("limit " + limit, rows.get(row).get(1), equalTo(rows.get(0).get(1)));
                }
            }
        }
    }

    /**
     * All shards are on one data node and run in one batch, so their drivers share one bound, and the limit is below the
     * documents of a shard, so the first shard to finish fills the top N. The rows must match the unlimited query, and the
     * profile shows that Lucene ran the top N. That a shard skips documents is not visible from here:
     * {@code LuceneTopNSourceOperatorCollectorTests#testSecondShardCollectsNothing} covers it.
     */
    public void testTopNOverShardsOfOneNode() {
        String dataNode = internalCluster().getRandomDataNodeName();
        int shards = between(2, 5);
        int docsPerShard = between(50, 200);
        assertAcked(
            client().admin()
                .indices()
                .prepareCreate("one-node")
                .setSettings(
                    Settings.builder()
                        .put("index.number_of_shards", shards)
                        .put("index.number_of_replicas", 0)
                        .put("index.routing.allocation.include._name", dataNode)
                )
                .setMapping("id", "type=integer", "content", "type=text")
        );
        // Routing by the id spreads the documents unevenly, so index many more than shards * docsPerShard would need
        int docs = shards * docsPerShard * 3;
        BulkRequestBuilder bulk = client().prepareBulk();
        for (int id = docs - 1; id >= 0; id--) {
            String content = "fox ".repeat(between(1, 4)) + "dog ".repeat(between(0, 4));
            bulk.add(new IndexRequest("one-node").id(Integer.toString(id)).source("id", id, "content", content));
        }
        bulk.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE).get();
        ensureGreen("one-node");

        String query = """
            FROM one-node METADATA _score
            | WHERE match(content, "fox")
            | SORT _score DESC, id ASC
            | LIMIT %d
            | KEEP id, _score
            """;
        List<List<Object>> all;
        try (var resp = run(EsqlQueryRequest.syncEsqlQueryRequest(String.format(Locale.ROOT, query, NO_LIMIT)))) {
            all = getValuesList(resp);
            assertThat(all, hasSize(docs));
        }
        int limit = between(1, docsPerShard);
        // No pragmas: by default a node runs up to 10 shards in one batch
        try (var resp = run(EsqlQueryRequest.syncEsqlQueryRequest(String.format(Locale.ROOT, query, limit)).profile(true))) {
            assertThat(getValuesList(resp), equalTo(all.subList(0, limit)));
            long topNSources = resp.profile()
                .drivers()
                .stream()
                .flatMap(driver -> driver.operators().stream())
                .filter(operator -> operator.operator().startsWith("LuceneTopNSourceOperator"))
                .count();
            assertThat("the top N runs in Lucene", topNSources, greaterThan(0L));
        }
    }

    private void assertTopNIsPrefixOfAllRows(String sort, boolean compareIds) {
        int docs = createAndPopulateIndex();
        String query = """
            FROM test METADATA _score
            | WHERE match(content, "fox")
            | SORT %s
            | LIMIT %d
            | KEEP id, _score
            """;
        List<List<Object>> all;
        try (var resp = run(String.format(Locale.ROOT, query, sort, NO_LIMIT))) {
            all = getValuesList(resp);
            assertThat(all, hasSize(docs));
        }
        for (int i = 0; i < 5; i++) {
            int limit = between(1, docs);
            try (var resp = run(String.format(Locale.ROOT, query, sort, limit))) {
                List<List<Object>> top = getValuesList(resp);
                assertThat(top, hasSize(limit));
                for (int row = 0; row < limit; row++) {
                    String reason = "limit " + limit + " row " + row;
                    assertThat(reason, top.get(row).get(1), equalTo(all.get(row).get(1)));
                    if (compareIds) {
                        assertThat(reason, top.get(row).get(0), equalTo(all.get(row).get(0)));
                    }
                }
            }
        }
    }

    /**
     * Every document matches {@code fox}, with few distinct scores per shard, so many rows tie on the score.
     */
    private int createAndPopulateIndex() {
        assertAcked(
            client().admin()
                .indices()
                .prepareCreate("test")
                .setSettings(Settings.builder().put("index.number_of_shards", between(2, 5)))
                .setMapping("id", "type=integer", "ts", "type=date", "kw", "type=keyword", "content", "type=text")
        );
        int docs = between(200, 1000);
        BulkRequestBuilder bulk = client().prepareBulk();
        for (int id = 0; id < docs; id++) {
            String content = "fox ".repeat(between(1, 4)) + "dog ".repeat(between(0, 4));
            bulk.add(
                new IndexRequest("test").id(Integer.toString(id))
                    .source("id", id, "ts", between(0, 50) * 1000L, "kw", "k" + between(0, 9), "content", content)
            );
        }
        bulk.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE).get();
        ensureYellow("test");
        return docs;
    }
}
