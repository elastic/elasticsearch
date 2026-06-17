/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.cluster.metadata.View;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.xpack.esql.view.PutViewAction;
import org.junit.Before;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasSize;

/**
 * CCS integration tests for {@code WHERE x IN (subquery)} combined with ES|QL views.
 *
 * <p>All views are local (created on {@link #LOCAL_CLUSTER}); the IN subquery or FROM-union
 * source references remote clusters. Scenarios covered:
 * <ol>
 *   <li>Local view as outer {@code FROM} source, IN subquery targeting a remote cluster.</li>
 *   <li>FROM-union nested inside the IN subquery, spanning both remote clusters.</li>
 *   <li>View body itself contains a remote IN subquery
 *       (CCS variant of the {@code viewContainingInSubquery} csv-spec test).</li>
 *   <li>View body is a FROM-union over all three clusters, combined with an outer IN subquery.</li>
 *   <li>Remote-view rejection guard: a view stored on a remote cluster is rejected with
 *       {@code RemoteViewNotSupportedException} even when an IN subquery is present.</li>
 * </ol>
 *
 * <p>Data: each cluster holds an {@code events} index with 6 docs (ids 1–6, odd=red/even=blue,
 * {@code tag} set to the cluster alias so rows are attributable in aggregation results).
 */
public class CrossClusterInSubqueryViewIT extends AbstractCrossClusterTestCase {

    private static final String EVENTS = "events";

    @Before
    public void checkCapabilityAndSetup() throws IOException {
        assumeTrue("Requires IN subquery support", EsqlCapabilities.Cap.WHERE_IN_SUBQUERY_WITHOUT_VIEW.isEnabled());
        setupClusters(3);
        setupInSubqueryIndices();
        // Local view: red events from the local cluster (ids 1, 3, 5).
        createViewOnCluster(LOCAL_CLUSTER, "events_red", "FROM events | WHERE color == \"red\"");
        // Local view whose body contains a remote IN subquery (scenario 3).
        createViewOnCluster(
            LOCAL_CLUSTER,
            "events_via_remote",
            "FROM events | WHERE id IN (FROM cluster-a:events | WHERE color == \"red\" | KEEP id)"
        );
        // Remote view stored on cluster-a for the rejection-guard test (scenario 5).
        createViewOnCluster(REMOTE_CLUSTER_1, "remote_events_view", "FROM events | LIMIT 1");
    }

    // ---- Scenario 1: local view in outer FROM + remote IN subquery ----

    /**
     * The outer {@code FROM} uses the local view {@code events_red} (ids {1,3,5}).
     * The IN subquery hits {@code cluster-a:events} with {@code id <= 3} → ids {1,2,3}.
     * Intersection = {1,3} → 2 rows.
     */
    public void testViewInOuterFromWithRemoteInSubquery() {
        try (EsqlQueryResponse resp = runQuery("""
            FROM events_red
            | WHERE id IN (FROM cluster-a:events | WHERE id <= 3 | KEEP id)
            | SORT id
            | KEEP id, color
            """, randomBoolean())) {
            List<List<Object>> values = getValuesList(resp);
            assertThat(values, hasSize(2));
            assertEquals(List.of(1, "red"), values.get(0));
            assertEquals(List.of(3, "red"), values.get(1));
        }
    }

    // ---- Scenario 2: FROM-union nested inside the IN subquery ----

    /**
     * The outer FROM uses the local view {@code events_red} (ids {1,3,5}).
     * The IN subquery contains a FROM-union of both remote clusters:
     * {@code cluster-a:events | WHERE id <= 3} → {1,2,3} and
     * {@code remote-b:events | WHERE color == "blue"} → {2,4,6}.
     * Union = {1,2,3,4,6}; {1,3,5} ∩ union = {1,3} → 2 rows.
     */
    public void testFromUnionInsideInSubquery() {
        assumeTrue("Requires FROM-subquery support", EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND.isEnabled());
        try (EsqlQueryResponse resp = runQuery("""
            FROM events_red
            | WHERE id IN (
                FROM (FROM cluster-a:events | WHERE id <= 3),
                     (FROM remote-b:events | WHERE color == "blue")
                | KEEP id
              )
            | SORT id
            | KEEP id, color
            """, randomBoolean())) {
            List<List<Object>> values = getValuesList(resp);
            assertThat(values, hasSize(2));
            assertEquals(List.of(1, "red"), values.get(0));
            assertEquals(List.of(3, "red"), values.get(1));
        }
    }

    // ---- Scenario 3: view body contains a remote IN subquery ----

    /**
     * The view {@code events_via_remote} has body
     * {@code FROM events | WHERE id IN (FROM cluster-a:events | WHERE color == "red" | KEEP id)}.
     * The view expands first (ViewResolver); the remote IN subquery in the body is then
     * resolved by InSubqueryResolver: local events where id ∈ {1,3,5} → 3 rows.
     *
     * <p>This is the CCS equivalent of the {@code viewContainingInSubquery} csv-spec test.
     */
    public void testViewBodyContainsRemoteInSubquery() {
        try (EsqlQueryResponse resp = runQuery("""
            FROM events_via_remote
            | SORT id
            | KEEP id, color
            """, randomBoolean())) {
            List<List<Object>> values = getValuesList(resp);
            assertThat(values, hasSize(3));
            assertEquals(List.of(1, "red"), values.get(0));
            assertEquals(List.of(3, "red"), values.get(1));
            assertEquals(List.of(5, "red"), values.get(2));
        }
    }

    // ---- Scenario 4: view body spans all clusters (multi-index FROM) + outer IN subquery ----

    /**
     * The view {@code all_events} uses a standard multi-index {@code FROM} spanning local and
     * both remote clusters — 18 rows total (6 per cluster).
     * The outer IN subquery is local and returns red ids {1,3,5}.
     * Each cluster contributes 3 matching rows → 9 rows split evenly by tag.
     *
     * <p>Note: using a FROM-union in the view body ({@code FROM (FROM events),(FROM cluster-a:events),...})
     * exposes a product gap: the IN subquery resolver conflates its own {@code FROM events} source
     * with the view body's inner FROM-union subquery, causing "Unknown column [color]". That
     * combination is therefore not tested here; this test uses a plain multi-index view body to
     * exercise the supported path — a local view spanning all clusters paired with a WHERE IN
     * subquery in the outer pipeline.
     */
    public void testMultiClusterViewWithOuterInSubquery() {
        createViewOnCluster(LOCAL_CLUSTER, "all_events", "FROM events, cluster-a:events, remote-b:events");
        try (EsqlQueryResponse resp = runQuery("""
            FROM all_events
            | WHERE id IN (FROM events | WHERE color == "red" | KEEP id)
            | STATS c = COUNT(*) BY tag
            | SORT tag
            """, randomBoolean())) {
            List<List<Object>> values = getValuesList(resp);
            assertThat(values, hasSize(3));
            assertEquals(List.of(3L, REMOTE_CLUSTER_1), values.get(0));
            assertEquals(List.of(3L, "local"), values.get(1));
            assertEquals(List.of(3L, REMOTE_CLUSTER_2), values.get(2));
            assertCCSExecutionInfoDetails(resp.getExecutionInfo());
        }
    }

    // ---- Scenario 5: remote-view rejection guard ----

    /**
     * A view stored on a remote cluster ({@code cluster-a:remote_events_view}) must still be
     * rejected with {@code RemoteViewNotSupportedException} even when the query has an IN
     * subquery — confirming that the remote-view guard in
     * {@code EsqlResolveFieldsAction} runs before IN-subquery resolution and cannot be bypassed.
     */
    public void testRemoteViewRejectedWithInSubquery() {
        expectThrows(
            Exception.class,
            containsString("ES|QL queries with remote views are not supported. Matched [cluster-a:remote_events_view]."),
            () -> runQuery("""
                FROM cluster-a:remote_events_view
                | WHERE id IN (FROM events | KEEP id)
                """, null)
        );
    }

    // ---- helpers ----

    private void createViewOnCluster(String clusterAlias, String viewName, String query) {
        assertAcked(
            client(clusterAlias).execute(
                PutViewAction.INSTANCE,
                new PutViewAction.Request(TimeValue.THIRTY_SECONDS, TimeValue.THIRTY_SECONDS, new View(viewName, query))
            ).actionGet(30, TimeUnit.SECONDS)
        );
    }

    /**
     * Creates the {@code events} index on each cluster with schema
     * {@code id integer, color keyword, tag keyword}. Six docs per cluster:
     * ids 1–6, odd ids are red, even ids are blue.
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
