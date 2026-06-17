/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.Build;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;
import org.junit.Before;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.hamcrest.Matchers.hasSize;

/**
 * Integration tests for {@code WHERE x IN (subquery)} and {@code WHERE x NOT IN (subquery)}
 * across clusters (CCS). Exercises SemiJoin (top-level IN), AntiJoin (top-level NOT IN),
 * and MarkJoin (IN/NOT IN under OR) with the subquery and/or outer FROM referencing remote
 * indices, plus filter-path vs hash-join-path parity.
 *
 * <p>Data layout: each cluster hosts an {@code events} index with 6 docs —
 * ids 1–6, {@code color} alternates red (odd) / blue (even), {@code tag} set to the cluster
 * alias ("local", "cluster-a", "remote-b") so rows are attributable in aggregation results.
 */
public class CrossClusterInSubqueryIT extends AbstractCrossClusterTestCase {

    private static final String EVENTS = "events";

    @Before
    public void checkCapabilityAndSetup() throws IOException {
        assumeTrue("Requires IN subquery support", EsqlCapabilities.Cap.WHERE_IN_SUBQUERY_WITHOUT_VIEW.isEnabled());
        setupClusters(3);
        setupInSubqueryIndices();
    }

    // ---- SEMI join (top-level IN) ----

    /**
     * Outer FROM targets both remotes; subquery runs on the local cluster.
     * The subquery returns the red ids {1,3,5}; each remote has 3 matching rows.
     */
    public void testSemiJoinRemoteOuterLocalSubquery() {
        try (EsqlQueryResponse resp = runQuery("""
            FROM *:events
            | WHERE id IN (FROM events | WHERE color == "red" | KEEP id)
            | STATS c = COUNT(*) BY tag
            | SORT tag
            """, randomBoolean())) {
            List<List<Object>> values = getValuesList(resp);
            assertThat(values, hasSize(2));
            assertEquals(List.of(3L, REMOTE_CLUSTER_1), values.get(0));
            assertEquals(List.of(3L, REMOTE_CLUSTER_2), values.get(1));
            assertCCSExecutionInfoDetails(resp.getExecutionInfo());
        }
    }

    /**
     * Outer FROM targets the local cluster; subquery runs on remote cluster-a.
     * The subquery returns red ids {1,3,5}; the local cluster has 3 matching rows.
     */
    public void testSemiJoinLocalOuterRemoteSubquery() {
        try (EsqlQueryResponse resp = runQuery("""
            FROM events
            | WHERE id IN (FROM cluster-a:events | WHERE color == "red" | KEEP id)
            | SORT id
            | KEEP id, color
            """, randomBoolean())) {
            List<List<Object>> values = getValuesList(resp);
            assertThat(values, hasSize(3));
            assertEquals(List.of(1, "red"), values.get(0));
            assertEquals(List.of(3, "red"), values.get(1));
            assertEquals(List.of(5, "red"), values.get(2));
            // outer FROM is local-only — no CCS execution info to assert
        }
    }

    /**
     * Both outer FROM and subquery target remote clusters.
     * Subquery on cluster-a returns red ids {1,3,5}; both remotes contribute 3 matches each.
     */
    public void testSemiJoinBothRemote() {
        try (EsqlQueryResponse resp = runQuery("""
            FROM *:events
            | WHERE id IN (FROM cluster-a:events | WHERE color == "red" | KEEP id)
            | STATS c = COUNT(*) BY tag
            | SORT tag
            """, randomBoolean())) {
            List<List<Object>> values = getValuesList(resp);
            assertThat(values, hasSize(2));
            assertEquals(List.of(3L, REMOTE_CLUSTER_1), values.get(0));
            assertEquals(List.of(3L, REMOTE_CLUSTER_2), values.get(1));
            assertCCSExecutionInfoDetails(resp.getExecutionInfo());
        }
    }

    // ---- ANTI join (top-level NOT IN) ----

    /**
     * Outer FROM targets both remotes; subquery is local and returns red ids {1,3,5}.
     * Each remote contributes 3 blue rows (ids 2,4,6) that are NOT IN the subquery.
     */
    public void testAntiJoinRemoteOuterLocalSubquery() {
        try (EsqlQueryResponse resp = runQuery("""
            FROM *:events
            | WHERE id NOT IN (FROM events | WHERE color == "red" | KEEP id)
            | STATS c = COUNT(*) BY tag
            | SORT tag
            """, randomBoolean())) {
            List<List<Object>> values = getValuesList(resp);
            assertThat(values, hasSize(2));
            assertEquals(List.of(3L, REMOTE_CLUSTER_1), values.get(0));
            assertEquals(List.of(3L, REMOTE_CLUSTER_2), values.get(1));
            assertCCSExecutionInfoDetails(resp.getExecutionInfo());
        }
    }

    /**
     * Outer FROM is local; subquery runs on cluster-a and returns red ids {1,3,5}.
     * Local cluster yields the blue rows (ids 2,4,6) that are NOT IN the subquery.
     */
    public void testAntiJoinLocalOuterRemoteSubquery() {
        try (EsqlQueryResponse resp = runQuery("""
            FROM events
            | WHERE id NOT IN (FROM cluster-a:events | WHERE color == "red" | KEEP id)
            | SORT id
            | KEEP id, color
            """, randomBoolean())) {
            List<List<Object>> values = getValuesList(resp);
            assertThat(values, hasSize(3));
            assertEquals(List.of(2, "blue"), values.get(0));
            assertEquals(List.of(4, "blue"), values.get(1));
            assertEquals(List.of(6, "blue"), values.get(2));
        }
    }

    /**
     * Both outer FROM and subquery target remote clusters.
     * Subquery on cluster-a returns red ids {1,3,5}; NOT IN yields the blue rows on each remote.
     */
    public void testAntiJoinBothRemote() {
        try (EsqlQueryResponse resp = runQuery("""
            FROM *:events
            | WHERE id NOT IN (FROM cluster-a:events | WHERE color == "red" | KEEP id)
            | STATS c = COUNT(*) BY tag
            | SORT tag
            """, randomBoolean())) {
            List<List<Object>> values = getValuesList(resp);
            assertThat(values, hasSize(2));
            assertEquals(List.of(3L, REMOTE_CLUSTER_1), values.get(0));
            assertEquals(List.of(3L, REMOTE_CLUSTER_2), values.get(1));
            assertCCSExecutionInfoDetails(resp.getExecutionInfo());
        }
    }

    // ---- MARK join (IN under OR) ----

    /**
     * Outer FROM targets both remotes; subquery is local.
     * {@code id IN {1,3,5} OR id > 5} matches ids {1,3,5,6} on each remote (4 rows each).
     */
    public void testMarkJoinRemoteOuterLocalSubquery() {
        try (EsqlQueryResponse resp = runQuery("""
            FROM *:events
            | WHERE id IN (FROM events | WHERE color == "red" | KEEP id) OR id > 5
            | STATS c = COUNT(*) BY tag
            | SORT tag
            """, randomBoolean())) {
            List<List<Object>> values = getValuesList(resp);
            assertThat(values, hasSize(2));
            assertEquals(List.of(4L, REMOTE_CLUSTER_1), values.get(0));
            assertEquals(List.of(4L, REMOTE_CLUSTER_2), values.get(1));
            assertCCSExecutionInfoDetails(resp.getExecutionInfo());
        }
    }

    /**
     * Outer FROM is local; subquery runs on cluster-a.
     * {@code id IN {1,3,5} OR id > 5} matches ids {1,3,5,6} on the local cluster.
     */
    public void testMarkJoinLocalOuterRemoteSubquery() {
        try (EsqlQueryResponse resp = runQuery("""
            FROM events
            | WHERE id IN (FROM cluster-a:events | WHERE color == "red" | KEEP id) OR id > 5
            | SORT id
            | KEEP id
            """, randomBoolean())) {
            List<List<Object>> values = getValuesList(resp);
            assertThat(values, hasSize(4));
            assertEquals(List.of(1), values.get(0));
            assertEquals(List.of(3), values.get(1));
            assertEquals(List.of(5), values.get(2));
            assertEquals(List.of(6), values.get(3));
        }
    }

    /**
     * Both outer FROM and subquery target remote clusters.
     * {@code NOT IN} under OR: {@code id NOT IN {1,3,5} OR id < 2} matches ids {1,2,4,6} on each remote.
     */
    public void testMarkJoinNotInBothRemote() {
        try (EsqlQueryResponse resp = runQuery("""
            FROM *:events
            | WHERE id NOT IN (FROM cluster-a:events | WHERE color == "red" | KEEP id) OR id < 2
            | STATS c = COUNT(*) BY tag
            | SORT tag
            """, randomBoolean())) {
            List<List<Object>> values = getValuesList(resp);
            // NOT IN {1,3,5}: matches 2,4,6 (blue). OR id < 2: also includes id=1. Total: {1,2,4,6} → 4 rows each remote.
            assertThat(values, hasSize(2));
            assertEquals(List.of(4L, REMOTE_CLUSTER_1), values.get(0));
            assertEquals(List.of(4L, REMOTE_CLUSTER_2), values.get(1));
            assertCCSExecutionInfoDetails(resp.getExecutionInfo());
        }
    }

    // ---- empty subquery ----

    /**
     * Subquery returns no rows (SEMI with empty right side → no matches).
     */
    public void testSemiJoinEmptySubquery() {
        try (EsqlQueryResponse resp = runQuery("""
            FROM *:events
            | WHERE id IN (FROM events | WHERE id > 100 | KEEP id)
            | STATS c = COUNT(*)
            """, randomBoolean())) {
            List<List<Object>> values = getValuesList(resp);
            assertThat(values, hasSize(1));
            assertEquals(0L, values.get(0).get(0));
            assertCCSExecutionInfoDetails(resp.getExecutionInfo());
        }
    }

    /**
     * Subquery returns no rows (ANTI with empty right side → all left rows pass).
     * Both remotes have 6 docs each, so the total is 12.
     */
    public void testAntiJoinEmptySubquery() {
        try (EsqlQueryResponse resp = runQuery("""
            FROM *:events
            | WHERE id NOT IN (FROM events | WHERE id > 100 | KEEP id)
            | STATS c = COUNT(*)
            """, randomBoolean())) {
            List<List<Object>> values = getValuesList(resp);
            assertThat(values, hasSize(1));
            assertEquals(12L, values.get(0).get(0));
            assertCCSExecutionInfoDetails(resp.getExecutionInfo());
        }
    }

    // ---- nested IN subquery spanning clusters ----

    /**
     * Three-level nesting: the innermost subquery is local, the middle is on cluster-a,
     * and the outer FROM spans both remotes.
     * Inner (local): red ids {1,3,5}. Middle (cluster-a): {1,3,5} ∩ {id≤3} = {1,3}.
     * Outer (both remotes): id IN {1,3} → 2 rows each.
     */
    public void testNestedInSubqueryAcrossClusters() {
        try (EsqlQueryResponse resp = runQuery("""
            FROM *:events
            | WHERE id IN (
                FROM cluster-a:events
                | WHERE id IN (FROM events | WHERE color == "red" | KEEP id)
                | WHERE id <= 3
                | KEEP id
              )
            | STATS c = COUNT(*) BY tag
            | SORT tag
            """, randomBoolean())) {
            List<List<Object>> values = getValuesList(resp);
            assertThat(values, hasSize(2));
            assertEquals(List.of(2L, REMOTE_CLUSTER_1), values.get(0));
            assertEquals(List.of(2L, REMOTE_CLUSTER_2), values.get(1));
            assertCCSExecutionInfoDetails(resp.getExecutionInfo());
        }
    }

    // ---- filter-path vs hash-join-path parity ----

    /**
     * Runs representative SEMI, ANTI, and MARK CCS queries with both the default threshold
     * (filter-of-literals path) and the forced hash-join threshold (0), asserting that both
     * paths produce identical results. Any divergence indicates a hash-join implementation bug
     * in a cross-cluster scenario.
     */
    public void testFilterAndHashJoinPathsAgree() {
        assumeTrue("requires query pragmas", Build.current().isSnapshot());

        String[] queries = new String[] {
            // SEMI: local outer, remote subquery
            """
                FROM events
                | WHERE id IN (FROM cluster-a:events | WHERE color == "red" | KEEP id)
                | SORT id
                | KEEP id, color
                """,
            // ANTI: both remote
            """
                FROM *:events
                | WHERE id NOT IN (FROM cluster-a:events | WHERE color == "red" | KEEP id)
                | SORT id
                | KEEP id, color
                """,
            // MARK: remote outer, local subquery
            """
                FROM *:events
                | WHERE id IN (FROM events | WHERE color == "red" | KEEP id) OR id > 5
                | STATS c = COUNT(*) BY tag
                | SORT tag
                """ };

        for (String query : queries) {
            EsqlQueryRequest defaultReq = EsqlQueryRequest.syncEsqlQueryRequest(query);
            defaultReq.pragmas(QueryPragmas.EMPTY);
            EsqlQueryRequest forcedReq = EsqlQueryRequest.syncEsqlQueryRequest(query);
            forcedReq.pragmas(forceHashJoin());

            try (EsqlQueryResponse defaultResp = runQuery(defaultReq); EsqlQueryResponse forcedResp = runQuery(forcedReq)) {
                assertEquals(
                    "filter and hash-join paths disagree for CCS query:\n" + query,
                    getValuesList(defaultResp),
                    getValuesList(forcedResp)
                );
            }
        }
    }

    // ---- helpers ----

    private static QueryPragmas forceHashJoin() {
        return new QueryPragmas(Settings.builder().put("in_subquery_hash_join_threshold", 0).build());
    }

    /**
     * Creates the {@code events} index on each cluster with the following schema:
     * {@code id integer, color keyword, tag keyword}.
     * Six documents per cluster: ids 1–6, color alternates red (odd) / blue (even),
     * tag set to the cluster alias ("local", "cluster-a", "remote-b").
     */
    private void setupInSubqueryIndices() {
        for (String cluster : new String[] { LOCAL_CLUSTER, REMOTE_CLUSTER_1, REMOTE_CLUSTER_2 }) {
            String tag = cluster.isEmpty() ? "local" : cluster;
            assertAcked(
                client(cluster).admin()
                    .indices()
                    .prepareCreate(EVENTS)
                    .setSettings(Settings.builder().put("index.number_of_shards", 1))
                    .setMapping("id", "type=integer", "color", "type=keyword", "tag", "type=keyword")
            );
            BulkRequestBuilder bulk = client(cluster).prepareBulk();
            bulk.add(new IndexRequest(EVENTS).id("1").source("id", 1, "color", "red", "tag", tag));
            bulk.add(new IndexRequest(EVENTS).id("2").source("id", 2, "color", "blue", "tag", tag));
            bulk.add(new IndexRequest(EVENTS).id("3").source("id", 3, "color", "red", "tag", tag));
            bulk.add(new IndexRequest(EVENTS).id("4").source("id", 4, "color", "blue", "tag", tag));
            bulk.add(new IndexRequest(EVENTS).id("5").source("id", 5, "color", "red", "tag", tag));
            bulk.add(new IndexRequest(EVENTS).id("6").source("id", 6, "color", "blue", "tag", tag));
            bulk.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE).get();
        }
    }
}
