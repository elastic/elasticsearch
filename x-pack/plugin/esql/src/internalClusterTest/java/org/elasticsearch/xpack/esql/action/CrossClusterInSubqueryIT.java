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
import org.elasticsearch.cluster.metadata.View;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.xpack.esql.VerificationException;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;
import org.elasticsearch.xpack.esql.view.PutViewAction;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

/**
 * Integration tests for {@code WHERE x IN (subquery)} and {@code WHERE x NOT IN (subquery)}
 * across clusters (CCS). Covers:
 * <ul>
 *   <li>SemiJoin (top-level IN), AntiJoin (top-level NOT IN), and MarkJoin (IN/NOT IN under OR)
 *       with the subquery and/or outer FROM referencing remote indices.</li>
 *   <li>Empty subquery edge cases, nested IN spanning three clusters, and filter-path vs
 *       hash-join-path parity.</li>
 *   <li>Local views as the outer {@code FROM} source with a remote IN subquery, and view bodies
 *       that themselves contain remote IN subqueries.</li>
 *   <li>Remote-view rejection guard: a view stored on a remote cluster is rejected with
 *       {@code RemoteViewNotSupportedException} even when an IN subquery is present.</li>
 * </ul>
 *
 * <p>Data layout: each cluster hosts an {@code events} index with 6 docs —
 * ids 1–6, {@code color} alternates red (odd) / blue (even), {@code tag} set to the cluster
 * alias ("local", "cluster-a", "remote-b") so rows are attributable in aggregation results.
 */
public class CrossClusterInSubqueryIT extends AbstractCrossClusterTestCase implements EnrichClusterPluginSupport {

    private static final String EVENTS = "events";

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins(String clusterAlias) {
        List<Class<? extends Plugin>> plugins = new ArrayList<>(super.nodePlugins(clusterAlias));
        plugins.remove(EsqlAsyncActionIT.LocalStateEsqlAsync.class);
        return addEnrichPlugins(plugins);
    }

    @Override
    protected Settings nodeSettings() {
        return enrichNodeSettings(super.nodeSettings());
    }

    @Before
    public void checkCapabilityAndSetup() throws IOException {
        assumeTrue("Requires IN subquery support", EsqlCapabilities.Cap.WHERE_IN_SUBQUERY_WITHOUT_VIEW.isEnabled());
        setupClusters(3);
        setupInSubqueryIndices();
        // Local view: red events from the local cluster (ids 1, 3, 5).
        createViewOnCluster(LOCAL_CLUSTER, "events_red", "FROM events | WHERE color == \"red\"");
        // Local view whose body contains a remote IN subquery.
        createViewOnCluster(
            LOCAL_CLUSTER,
            "events_via_remote",
            "FROM events | WHERE id IN (FROM cluster-a:events | WHERE color == \"red\" | KEEP id)"
        );
        // Remote view stored on cluster-a for the rejection-guard test.
        createViewOnCluster(REMOTE_CLUSTER_1, "remote_events_view", "FROM events | LIMIT 1");
    }

    private static void checkSubqueryWithRowSupport() {
        assumeTrue("Requires subquery with ROW as source command support", EsqlCapabilities.Cap.SUBQUERY_WITH_ROW.isEnabled());
    }

    private static void checkSubqueryWithTSSupport() {
        assumeTrue("Requires subquery with TS as source command support", EsqlCapabilities.Cap.SUBQUERY_WITH_TS.isEnabled());
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
            // subquery is local-only (1 shard); main plan hits both remotes each with 1 shard
            assertCCSExecutionInfoDetailsWithShards(
                resp.getExecutionInfo(),
                Map.of(LOCAL_CLUSTER, 1, REMOTE_CLUSTER_1, 1, REMOTE_CLUSTER_2, 1)
            );
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
            | KEEP id, color, tag
            """, randomBoolean())) {
            List<List<Object>> values = getValuesList(resp);
            assertThat(values, hasSize(3));
            assertEquals(List.of(1, "red", "local"), values.get(0));
            assertEquals(List.of(3, "red", "local"), values.get(1));
            assertEquals(List.of(5, "red", "local"), values.get(2));
            // subquery hits cluster-a (1 shard); main plan is local-only (1 shard)
            assertCCSExecutionInfoDetailsWithShards(resp.getExecutionInfo(), Map.of(LOCAL_CLUSTER, 1, REMOTE_CLUSTER_1, 1));
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
            assertCCSExecutionInfoDetailsWithShards(resp.getExecutionInfo(), Map.of(REMOTE_CLUSTER_1, 1, REMOTE_CLUSTER_2, 1));
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
            // subquery is local-only (1 shard); main plan hits both remotes each with 1 shard
            assertCCSExecutionInfoDetailsWithShards(
                resp.getExecutionInfo(),
                Map.of(LOCAL_CLUSTER, 1, REMOTE_CLUSTER_1, 1, REMOTE_CLUSTER_2, 1)
            );
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
            | KEEP id, color, tag
            """, randomBoolean())) {
            List<List<Object>> values = getValuesList(resp);
            assertThat(values, hasSize(3));
            assertEquals(List.of(2, "blue", "local"), values.get(0));
            assertEquals(List.of(4, "blue", "local"), values.get(1));
            assertEquals(List.of(6, "blue", "local"), values.get(2));
            // subquery hits cluster-a (1 shard); main plan is local-only (1 shard)
            assertCCSExecutionInfoDetailsWithShards(resp.getExecutionInfo(), Map.of(LOCAL_CLUSTER, 1, REMOTE_CLUSTER_1, 1));
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
            assertCCSExecutionInfoDetailsWithShards(resp.getExecutionInfo(), Map.of(REMOTE_CLUSTER_1, 1, REMOTE_CLUSTER_2, 1));
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
            // subquery is local-only (1 shard); main plan hits both remotes each with 1 shard
            assertCCSExecutionInfoDetailsWithShards(
                resp.getExecutionInfo(),
                Map.of(LOCAL_CLUSTER, 1, REMOTE_CLUSTER_1, 1, REMOTE_CLUSTER_2, 1)
            );
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
            | KEEP id, tag
            """, randomBoolean())) {
            List<List<Object>> values = getValuesList(resp);
            assertThat(values, hasSize(4));
            assertEquals(List.of(1, "local"), values.get(0));
            assertEquals(List.of(3, "local"), values.get(1));
            assertEquals(List.of(5, "local"), values.get(2));
            assertEquals(List.of(6, "local"), values.get(3));
            // subquery hits cluster-a (1 shard); main plan is local-only (1 shard)
            assertCCSExecutionInfoDetailsWithShards(resp.getExecutionInfo(), Map.of(LOCAL_CLUSTER, 1, REMOTE_CLUSTER_1, 1));
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
            assertCCSExecutionInfoDetailsWithShards(resp.getExecutionInfo(), Map.of(REMOTE_CLUSTER_1, 1, REMOTE_CLUSTER_2, 1));
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
            // subquery is local-only (1 shard); main plan hits both remotes each with 1 shard
            assertCCSExecutionInfoDetailsWithShards(
                resp.getExecutionInfo(),
                Map.of(LOCAL_CLUSTER, 1, REMOTE_CLUSTER_1, 1, REMOTE_CLUSTER_2, 1)
            );
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
            // subquery is local-only (1 shard); main plan hits both remotes each with 1 shard
            assertCCSExecutionInfoDetailsWithShards(
                resp.getExecutionInfo(),
                Map.of(LOCAL_CLUSTER, 1, REMOTE_CLUSTER_1, 1, REMOTE_CLUSTER_2, 1)
            );
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
            // innermost subquery is local (1 shard); middle subplan hits cluster-a (1 shard); main plan hits both remotes
            assertCCSExecutionInfoDetailsWithShards(
                resp.getExecutionInfo(),
                Map.of(LOCAL_CLUSTER, 1, REMOTE_CLUSTER_1, 1, REMOTE_CLUSTER_2, 1)
            );
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

    // ---- views: local view as outer FROM source ----

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

    // ---- views: view body contains a remote IN subquery ----

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

    // ---- views: multi-cluster view body + outer IN subquery ----

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
     * exercise the supported path.
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
            // subquery is local-only; main plan (view all_events = events + cluster-a:events + remote-b:events) hits all clusters
            assertCCSExecutionInfoDetailsWithShards(
                resp.getExecutionInfo(),
                Map.of(LOCAL_CLUSTER, 1, REMOTE_CLUSTER_1, 1, REMOTE_CLUSTER_2, 1)
            );
        }
    }

    // ---- views: remote-view rejection guard ----

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
                FROM events
                | WHERE id IN (FROM cluster-a:remote_events_view | KEEP id)
                """, null)
        );
    }

    // ---- LOOKUP JOIN inside WHERE IN subquery body (issue #149877) ----

    /**
     * A WHERE IN subquery whose body contains a LOOKUP JOIN referencing a remote lookup index.
     * The lookup index ({@code values_lookup}) exists only on {@code remote-b}, which is the
     * cluster targeted by the subquery. The subquery filters {@code remote-b:logs-2} to {@code v=4}
     * via the lookup join, and the outer query selects that value from {@code cluster-a:logs-2}.
     */
    public void testInSubqueryWithLookupJoinInSubqueryBodySkipUnavailableFalse() {
        populateLookupIndex(REMOTE_CLUSTER_2, "values_lookup", 10);
        setSkipUnavailable(REMOTE_CLUSTER_1, false);
        setSkipUnavailable(REMOTE_CLUSTER_2, false);
        try {
            try (EsqlQueryResponse resp = runQuery("""
                FROM cluster-a:logs-*
                | WHERE v IN (
                    FROM remote-b:logs-*
                    | WHERE v > 1 AND v < 7
                    | LOOKUP JOIN values_lookup ON v == lookup_key
                    | KEEP v
                  )
                | KEEP v
                """, false)) {
                assertThat(getValuesList(resp), equalTo(List.of(List.of(4L))));
            }
        } finally {
            clearSkipUnavailable(3);
        }
    }

    /**
     * A WHERE IN subquery whose body contains a LOOKUP JOIN referencing a remote lookup index,
     * with {@code skipUnavailable=true}. The subquery filters {@code remote-b:logs-2} to
     * {@code v=4} via the lookup join, and the outer query selects that value from
     * {@code cluster-a:logs-2}.
     */
    public void testInSubqueryWithLookupJoinInSubqueryBodySkipUnavailableTrue() {
        populateLookupIndex(REMOTE_CLUSTER_2, "values_lookup", 10);
        setSkipUnavailable(REMOTE_CLUSTER_1, true);
        setSkipUnavailable(REMOTE_CLUSTER_2, true);
        try {
            try (EsqlQueryResponse resp = runQuery("""
                FROM cluster-a:logs-*
                | WHERE v IN (
                    FROM remote-b:logs-*
                    | WHERE v > 1 AND v < 7
                    | LOOKUP JOIN values_lookup ON v == lookup_key
                    | KEEP v
                  )
                | KEEP v
                """, true)) {
                assertThat(getValuesList(resp), equalTo(List.of(List.of(4L))));
            }
        } finally {
            clearSkipUnavailable(3);
        }
    }

    // ---- ROW + LOOKUP JOIN combinations with WHERE IN subqueries ----

    /**
     * Two placements of a LOOKUP JOIN relative to a WHERE IN subquery whose source is ROW.
     *
     * <ol>
     *   <li>The LOOKUP JOIN lives <b>inside</b> the IN subquery, on top of a ROW source. A ROW produces data on the
     *       coordinator and carries no index relation, so the lookup index must be resolved on the local cluster rather
     *       than on the remote outer FROM. The subquery yields {@code v=4} and {@code cluster-a:logs-*} keeps the matching
     *       row.</li>
     *   <li>The LOOKUP JOIN lives in the main query <b>after</b> the WHERE whose IN subquery is a plain ROW. The lookup
     *       then enriches the (single-remote) outer rows, resolving against {@code cluster-a}.</li>
     * </ol>
     *
     * {@code values_lookup} is populated on both the local cluster and cluster-a so either placement resolves regardless of
     * whether the ROW-based IN is folded to a literal filter or kept as a join.
     */
    public void testRowLookupJoinInsideAndAfterWhereInSubquery() {
        checkSubqueryWithRowSupport();
        populateLookupIndex(LOCAL_CLUSTER, "values_lookup", 10);
        populateLookupIndex(REMOTE_CLUSTER_1, "values_lookup", 10);

        // (1) LOOKUP JOIN inside the IN subquery, on top of a ROW source -> the lookup is resolved on the local cluster.
        try (EsqlQueryResponse resp = runQuery("""
            FROM cluster-a:logs-*
            | WHERE v IN (
                ROW v = TO_LONG(4)
                | LOOKUP JOIN values_lookup ON v == lookup_key
                | KEEP v
              )
            | KEEP v
            """, randomBoolean())) {
            assertThat(getValuesList(resp), equalTo(List.of(List.of(4L))));
        }

        // (2) LOOKUP JOIN in the main query after a WHERE whose IN subquery is a plain ROW -> the lookup is resolved on
        // cluster-a (the single remote outer source).
        try (EsqlQueryResponse resp = runQuery("""
            FROM cluster-a:logs-*
            | WHERE v IN (ROW v = TO_LONG(4) | KEEP v)
            | LOOKUP JOIN values_lookup ON v == lookup_key
            | KEEP v, lookup_name
            """, randomBoolean())) {
            assertThat(getValuesList(resp), equalTo(List.of(List.of(4L, "lookup_4"))));
        }
    }

    // ---- negative cases: LOOKUP JOIN referencing a missing index with ROW/FROM/TS IN-subquery sources ----

    /**
     * The lookup index ({@code missing_lookup}) does not exist on any cluster. With the LOOKUP JOIN placed <b>inside</b> the
     * IN subquery (on top of the subquery's own source), the lookup is scoped to that source's cluster(s), so the reported
     * missing-index name is qualified accordingly: local (no prefix) for a ROW source, {@code remote-b} for a remote FROM
     * source, and {@code cluster-a} for a TS source.
     */
    public void testMissingLookupIndexInsideWhereInSubquery() {
        checkSubqueryWithRowSupport();
        checkSubqueryWithTSSupport();
        setupTsMetricsIndex(REMOTE_CLUSTER_1, "cluster-a");

        // ROW source -> lookup scoped to the local cluster
        VerificationException ex = expectThrows(VerificationException.class, () -> runQuery("""
            FROM cluster-a:logs-*
            | WHERE v IN (
                ROW v = TO_LONG(4)
                | LOOKUP JOIN missing_lookup ON v == lookup_key
                | KEEP v
              )
            | KEEP v
            """, randomBoolean()));
        assertThat(ex.getMessage(), containsString("Unknown index [missing_lookup]"));

        // remote FROM source -> lookup scoped to remote-b
        ex = expectThrows(VerificationException.class, () -> runQuery("""
            FROM cluster-a:logs-*
            | WHERE v IN (
                FROM remote-b:logs-*
                | LOOKUP JOIN missing_lookup ON v == lookup_key
                | KEEP v
              )
            | KEEP v
            """, randomBoolean()));
        assertThat(ex.getMessage(), containsString("Unknown index [remote-b:missing_lookup]"));

        // TS source -> lookup scoped to cluster-a
        ex = expectThrows(VerificationException.class, () -> runQuery("""
            FROM *:events
            | WHERE tag IN (
                TS cluster-a:ts_metrics
                | STATS top_bytes = max(max_bytes) BY cluster
                | LOOKUP JOIN missing_lookup ON cluster == lookup_key
                | KEEP cluster
              )
            | KEEP tag
            """, randomBoolean()));
        assertThat(ex.getMessage(), containsString("Unknown index [cluster-a:missing_lookup]"));
    }

    /**
     * Same missing lookup index, but with the LOOKUP JOIN placed in the main query <b>after</b> the WHERE IN. The lookup
     * reads only from the outer FROM (the IN subquery is a row filter whose source does not feed the lookup), so the
     * missing-index error names only the outer cluster(s) - never the IN-subquery's ROW (local) or remote-b source.
     */
    public void testMissingLookupIndexAfterWhereInSubquery() {
        checkSubqueryWithRowSupport();
        checkSubqueryWithTSSupport();
        setupTsMetricsIndex(REMOTE_CLUSTER_1, "cluster-a");

        // ROW IN-subquery + lookup after -> scoped to the outer cluster-a only (the ROW filter does not add the local cluster)
        VerificationException ex = expectThrows(VerificationException.class, () -> runQuery("""
            FROM cluster-a:logs-*
            | WHERE v IN (ROW v = TO_LONG(4) | KEEP v)
            | LOOKUP JOIN missing_lookup ON v == lookup_key
            | KEEP v
            """, randomBoolean()));
        assertThat(ex.getMessage(), containsString("Unknown index [cluster-a:missing_lookup]"));

        // remote FROM IN-subquery + lookup after -> scoped to the outer cluster-a only (the remote-b filter source is not added)
        ex = expectThrows(VerificationException.class, () -> runQuery("""
            FROM cluster-a:logs-*
            | WHERE v IN (FROM remote-b:logs-* | KEEP v)
            | LOOKUP JOIN missing_lookup ON v == lookup_key
            | KEEP v
            """, randomBoolean()));
        assertThat(ex.getMessage(), containsString("Unknown index [cluster-a:missing_lookup]"));

        // TS IN-subquery + lookup after -> scoped to the outer source *:events, which spans both remotes
        ex = expectThrows(VerificationException.class, () -> runQuery("""
            FROM *:events
            | WHERE tag IN (TS cluster-a:ts_metrics | STATS top_bytes = max(max_bytes) BY cluster | KEEP cluster)
            | LOOKUP JOIN missing_lookup ON tag == lookup_key
            | KEEP tag
            """, randomBoolean()));
        assertThat(ex.getMessage(), allOf(containsString("cluster-a:missing_lookup"), containsString("remote-b:missing_lookup")));
    }

    /**
     * Negative (missing lookup index) counterparts to {@code EsqlSessionTests#testComputeLookupJoinIndexScopeMixedSubqueries},
     * exercised over the three real clusters (local {@code logs-1}, {@code cluster-a:logs-2}, {@code remote-b:logs-2}). The
     * lookup index {@code missing_lookup} exists nowhere, so each query fails analysis with an {@code Unknown index} error
     * whose qualified name(s) reveal the computed lookup scope - confirming a lookup is scoped only to the clusters that feed
     * rows into it, never to a sibling FROM-union branch or an IN subquery used purely as a row filter.
     */
    public void testMissingLookupIndexMixedSubqueries() {
        // (1) FROM subquery has a WHERE IN subquery, the LOOKUP JOIN sits AFTER that WHERE inside the same FROM subquery.
        // Scoped to cluster-a (the FROM subquery's own source); the IN-filter source remote-b and the sibling local branch
        // do not feed the lookup.
        VerificationException ex = expectThrows(VerificationException.class, () -> runQuery("""
            FROM (FROM cluster-a:logs-*
                  | WHERE v IN (FROM remote-b:logs-* | KEEP v)
                  | LOOKUP JOIN missing_lookup ON v == lookup_key
                  | KEEP v),
                 (FROM logs-* | KEEP v)
            """, randomBoolean()));
        assertThat(ex.getMessage(), containsString("Unknown index [cluster-a:missing_lookup]"));

        // (2) FROM subquery has a WHERE IN subquery, the LOOKUP JOIN sits INSIDE that IN subquery -> scoped to remote-b.
        ex = expectThrows(VerificationException.class, () -> runQuery("""
            FROM (FROM cluster-a:logs-*
                  | WHERE v IN (FROM remote-b:logs-* | LOOKUP JOIN missing_lookup ON v == lookup_key | KEEP v)
                  | KEEP v),
                 (FROM logs-* | KEEP v)
            """, randomBoolean()));
        assertThat(ex.getMessage(), containsString("Unknown index [remote-b:missing_lookup]"));

        // (3) WHERE IN subquery is a union of two FROM subqueries, each carrying its own LOOKUP JOIN -> union of both sources
        // cluster-a and remote-b; the outer local source is only filtered, not a lookup source.
        ex = expectThrows(VerificationException.class, () -> runQuery("""
            FROM logs-*
            | WHERE v IN (FROM (FROM cluster-a:logs-* | LOOKUP JOIN missing_lookup ON v == lookup_key | KEEP v),
                               (FROM remote-b:logs-* | LOOKUP JOIN missing_lookup ON v == lookup_key | KEEP v))
            | KEEP v
            """, randomBoolean()));
        assertThat(ex.getMessage(), allOf(containsString("cluster-a:missing_lookup"), containsString("remote-b:missing_lookup")));

        // (4) WHERE IN subquery is a union of two FROM subqueries, the LOOKUP JOIN sits in the main query AFTER the WHERE ->
        // scoped to the outer cluster-a only; the IN-filter sources remote-b and local are excluded.
        ex = expectThrows(VerificationException.class, () -> runQuery("""
            FROM cluster-a:logs-*
            | WHERE v IN (FROM (FROM remote-b:logs-* | KEEP v), (FROM logs-* | KEEP v))
            | LOOKUP JOIN missing_lookup ON v == lookup_key
            | KEEP v
            """, randomBoolean()));
        assertThat(ex.getMessage(), containsString("Unknown index [cluster-a:missing_lookup]"));

        // (5) Nested IN subqueries, the LOOKUP JOIN sits INSIDE the innermost (local) subquery -> scoped to the local cluster.
        ex = expectThrows(VerificationException.class, () -> runQuery("""
            FROM cluster-a:logs-*
            | WHERE v IN (FROM remote-b:logs-*
                          | WHERE v IN (FROM logs-* | LOOKUP JOIN missing_lookup ON v == lookup_key | KEEP v)
                          | KEEP v)
            | KEEP v
            """, randomBoolean()));
        assertThat(ex.getMessage(), containsString("Unknown index [missing_lookup]"));

        // (6) Nested IN subqueries, the LOOKUP JOIN sits AFTER the inner WHERE but inside the outer IN subquery -> scoped to
        // remote-b (the outer IN subquery's own source); the inner-filter source local and the outermost cluster-a are excluded.
        ex = expectThrows(VerificationException.class, () -> runQuery("""
            FROM cluster-a:logs-*
            | WHERE v IN (FROM remote-b:logs-*
                          | WHERE v IN (FROM logs-* | KEEP v)
                          | LOOKUP JOIN missing_lookup ON v == lookup_key
                          | KEEP v)
            | KEEP v
            """, randomBoolean()));
        assertThat(ex.getMessage(), containsString("Unknown index [remote-b:missing_lookup]"));

        // (7) Everything at once: a FROM-union whose first branch carries a LOOKUP JOIN (local), a WHERE IN subquery carrying
        // another LOOKUP JOIN (remote-b), and a top-level LOOKUP JOIN after the WHERE that reads the whole FROM-union
        // (local + cluster-a). The combined field-caps request reports the missing index on all three clusters.
        ex = expectThrows(VerificationException.class, () -> runQuery("""
            FROM (FROM logs-* | LOOKUP JOIN missing_lookup ON v == lookup_key | KEEP v),
                 (FROM cluster-a:logs-* | KEEP v)
            | WHERE v IN (FROM remote-b:logs-* | LOOKUP JOIN missing_lookup ON v == lookup_key | KEEP v)
            | LOOKUP JOIN missing_lookup ON v == lookup_key
            | KEEP v
            """, randomBoolean()));
        assertThat(
            ex.getMessage(),
            allOf(
                containsString("cluster-a:missing_lookup"),
                containsString("remote-b:missing_lookup"),
                // the local cluster contributes the unqualified name; its position in the comma-joined list is not guaranteed
                anyOf(containsString("[missing_lookup,"), containsString(",missing_lookup"))
            )
        );
    }

    // ---- additional source types: TS, ROW, FROM-union in outer query ----

    /**
     * TS as the IN subquery source — executes a time series query on a remote cluster and uses the
     * results to filter events from both remote clusters.
     *
     * <p>A {@code ts_metrics} index lives on {@code cluster-a} with dimension {@code cluster="cluster-a"}
     * and gauge metric {@code max_bytes}. The subplan aggregates max bytes per cluster from that TS index,
     * returning {@code ["cluster-a"]}. The outer FROM queries both remote {@code events} indices; only
     * events with {@code tag == "cluster-a"} match, yielding 6 rows.
     */
    public void testTsSourceInSubquery() {
        checkSubqueryWithTSSupport();
        setupTsMetricsIndex(REMOTE_CLUSTER_1, "cluster-a");
        try (EsqlQueryResponse resp = runQuery("""
            FROM *:events
            | WHERE tag IN (
                TS cluster-a:ts_metrics
                | STATS top_bytes = max(max_bytes) BY cluster
                | WHERE top_bytes > 0
                | KEEP cluster
              )
            | STATS c = COUNT(*) BY tag
            | SORT tag
            """, randomBoolean())) {
            List<List<Object>> values = getValuesList(resp);
            assertThat(values, hasSize(1));
            assertEquals(List.of(6L, REMOTE_CLUSTER_1), values.get(0));
            // subplan queries cluster-a:ts_metrics (1 shard); main plan queries both remote events indices
            assertCCSExecutionInfoDetailsWithShards(resp.getExecutionInfo(), Map.of(REMOTE_CLUSTER_1, 1, REMOTE_CLUSTER_2, 1));
        }
    }

    /**
     * ROW as the IN subquery source — produces a constant single-row result set with no cluster I/O.
     * The subplan executes ROW locally; the main plan queries events from all three clusters.
     * Events where id == 1 (one per cluster) are returned.
     */
    public void testRowSourceInSubquery() {
        checkSubqueryWithRowSupport();
        try (EsqlQueryResponse resp = runQuery("""
            FROM *:events, events
            | WHERE id IN (ROW id = 1 | KEEP id)
            | STATS c = COUNT(*) BY tag
            | SORT tag
            """, randomBoolean())) {
            List<List<Object>> values = getValuesList(resp);
            assertThat(values, hasSize(3));
            assertEquals(List.of(1L, REMOTE_CLUSTER_1), values.get(0));
            assertEquals(List.of(1L, "local"), values.get(1));
            assertEquals(List.of(1L, REMOTE_CLUSTER_2), values.get(2));
            // ROW subplan touches no remote clusters; all shard counts come from the main plan
            assertCCSExecutionInfoDetailsWithShards(
                resp.getExecutionInfo(),
                Map.of(LOCAL_CLUSTER, 1, REMOTE_CLUSTER_1, 1, REMOTE_CLUSTER_2, 1)
            );
        }
    }

    /**
     * FROM union (UnionAll) in the main plan where one branch contains a WHERE IN subquery.
     * The subplan (WHERE IN part) queries remote-b only; the main plan queries local and cluster-a.
     * Remote-b is only in the subplan so {@code finalizeSubPlanOnlyRemoteClusters} marks it SUCCESSFUL.
     *
     * <p>Data: source 1 → local events with id &lt; 3 (2 rows); source 2 → cluster-a events where
     * id IN (remote-b red ids {1,3,5}) → 3 matching rows. STATS: cluster-a 3, local 2.
     */
    public void testFromUnionInMainPlanWithWhereInSubquery() {
        assumeTrue("Requires FROM-subquery support", EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND.isEnabled());
        try (EsqlQueryResponse resp = runQuery("""
            FROM (FROM events | WHERE id < 3 | KEEP id, tag),
                 (FROM cluster-a:events
                  | WHERE id IN (FROM remote-b:events | WHERE color == "red" | KEEP id)
                  | KEEP id, tag)
            | STATS c = COUNT(*) BY tag
            | SORT c DESC, tag
            """, randomBoolean())) {
            List<List<Object>> values = getValuesList(resp);
            assertThat(values, hasSize(2));
            assertEquals(List.of(3L, REMOTE_CLUSTER_1), values.get(0));
            assertEquals(List.of(2L, "local"), values.get(1));
            // subplan hits remote-b (1 shard); main plan hits local (1 shard) and cluster-a (1 shard)
            assertCCSExecutionInfoDetailsWithShards(
                resp.getExecutionInfo(),
                Map.of(LOCAL_CLUSTER, 1, REMOTE_CLUSTER_1, 1, REMOTE_CLUSTER_2, 1)
            );
        }
    }

    // ---- ENRICH interaction with WHERE IN subqueries ----

    /**
     * Happy path, ANY mode ENRICH used inside WHERE IN subquery and policy existing in all clusters the query uses
     * */
    public void testEnrichWithAnyModePolicyPresentOnAllClustersSucceeds() {
        setupEnrichPolicy(client(LOCAL_CLUSTER), "values_enrich", 10);
        setupEnrichPolicy(client(REMOTE_CLUSTER_1), "values_enrich", 10);
        setupEnrichPolicy(client(REMOTE_CLUSTER_2), "values_enrich", 10);
        setSkipUnavailable(REMOTE_CLUSTER_1, false);
        setSkipUnavailable(REMOTE_CLUSTER_2, false);
        try {
            try (EsqlQueryResponse resp = runQuery("""
                FROM logs-*
                | WHERE v IN (
                    FROM *:logs-*
                    | WHERE v > 1 AND v < 7
                    | ENRICH values_enrich ON v WITH enrich_name
                    | KEEP v
                  )
                | KEEP v
                """, false)) {
                assertThat(getValuesList(resp), equalTo(List.of(List.of(4L))));
            }
        } finally {
            deleteEnrichPolicy(client(LOCAL_CLUSTER), "values_enrich");
            deleteEnrichPolicy(client(REMOTE_CLUSTER_1), "values_enrich");
            deleteEnrichPolicy(client(REMOTE_CLUSTER_2), "values_enrich");
            clearSkipUnavailable(3);
        }
    }

    /**
     * ANY mode ENRICH used inside remote cluster WHERE IN subquery: policy must exist in local and that remote cluster
     * */
    public void testEnrichWithAnyModePolicyPresentOnRelevantClustersSucceeds() {
        setupEnrichPolicy(client(LOCAL_CLUSTER), "values_enrich", 10);
        setupEnrichPolicy(client(REMOTE_CLUSTER_1), "values_enrich", 10);
        setSkipUnavailable(REMOTE_CLUSTER_1, false);
        setSkipUnavailable(REMOTE_CLUSTER_2, false);
        try {
            try (EsqlQueryResponse resp = runQuery("""
                FROM *:logs-*
                | WHERE v IN (
                    FROM cluster-a:logs-*
                    | WHERE v > 1 AND v < 7
                    | ENRICH values_enrich ON v WITH enrich_name
                    | KEEP v
                  )
                | KEEP v
                """, false)) {
                assertThat(getValuesList(resp), equalTo(List.of(List.of(4L), List.of(4L))));
            }
        } finally {
            deleteEnrichPolicy(client(LOCAL_CLUSTER), "values_enrich");
            deleteEnrichPolicy(client(REMOTE_CLUSTER_1), "values_enrich");
            clearSkipUnavailable(3);
        }
    }

    /**
     * ANY mode ENRICH used in the outer and the inner subquery: the scope will be a union of both
     */
    public void testEnrichWithAnyModePolicyFailsWithMixedScopes() {
        setupEnrichPolicy(client(LOCAL_CLUSTER), "values_enrich", 10);
        setupEnrichPolicy(client(REMOTE_CLUSTER_1), "values_enrich", 10);
        setSkipUnavailable(REMOTE_CLUSTER_1, false);
        setSkipUnavailable(REMOTE_CLUSTER_2, false);
        try {
            VerificationException ex = expectThrows(VerificationException.class, () -> runQuery("""
                FROM remote-b:logs-*
                | WHERE v IN (
                    FROM cluster-a:logs-*
                    | WHERE v > 1 AND v < 7
                    | ENRICH values_enrich ON v WITH enrich_name
                    | KEEP v
                  )
                | ENRICH values_enrich ON v WITH enrich_name
                | KEEP v
                """, false));
            assertThat(ex.getMessage(), containsString("cannot find enrich policy [values_enrich] on clusters [remote-b]"));
        } finally {
            deleteEnrichPolicy(client(LOCAL_CLUSTER), "values_enrich");
            deleteEnrichPolicy(client(REMOTE_CLUSTER_1), "values_enrich");
            clearSkipUnavailable(3);
        }
    }

    /**
     * ANY mode ENRICH used in the outer and the inner subquery: the scope will be a union of both. Which means the policy will have
     * to be defined in all remote clusters plus local
     */
    public void testEnrichWithAnyModePolicySucceedsWithMixedScopes() {
        setupEnrichPolicy(client(LOCAL_CLUSTER), "values_enrich", 10);
        setupEnrichPolicy(client(REMOTE_CLUSTER_1), "values_enrich", 10);
        setupEnrichPolicy(client(REMOTE_CLUSTER_2), "values_enrich", 10);
        setSkipUnavailable(REMOTE_CLUSTER_1, false);
        setSkipUnavailable(REMOTE_CLUSTER_2, false);

        try {
            try (EsqlQueryResponse resp = runQuery("""
                FROM remote-b:logs-*
                | WHERE v IN (
                    FROM cluster-a:logs-*
                    | WHERE v > 1 AND v < 7
                    | ENRICH values_enrich ON v WITH enrich_name
                    | KEEP v
                  )
                | ENRICH values_enrich ON v WITH enrich_name
                | KEEP v
                """, false)) {
                assertThat(getValuesList(resp), equalTo(List.of(List.of(4L))));
            }
        } finally {
            deleteEnrichPolicy(client(LOCAL_CLUSTER), "values_enrich");
            deleteEnrichPolicy(client(REMOTE_CLUSTER_1), "values_enrich");
            deleteEnrichPolicy(client(REMOTE_CLUSTER_2), "values_enrich");
            clearSkipUnavailable(3);
        }
    }

    /**
     * COORDINATOR mode ENRICH used in the outer and the inner subquery: the policy has to exist in local cluster
     */
    public void testEnrichWithCoordinatorModePolicyPresentOnRelevantClustersSucceeds() {
        setupEnrichPolicy(client(LOCAL_CLUSTER), "values_enrich", 10);
        setSkipUnavailable(REMOTE_CLUSTER_1, false);
        setSkipUnavailable(REMOTE_CLUSTER_2, false);
        try {
            try (EsqlQueryResponse resp = runQuery("""
                FROM *:logs-*
                | WHERE v IN (
                    FROM cluster-a:logs-*
                    | WHERE v > 1 AND v < 7
                    | ENRICH _coordinator:values_enrich ON v WITH enrich_name
                    | KEEP v
                  )
                | ENRICH _coordinator:values_enrich ON v WITH enrich_name
                | KEEP v
                | SORT v
                """, false)) {
                assertThat(getValuesList(resp), equalTo(List.of(List.of(4L), List.of(4L))));
            }
        } finally {
            deleteEnrichPolicy(client(LOCAL_CLUSTER), "values_enrich");
            clearSkipUnavailable(3);
        }
    }

    /**
     * COORDINATOR mode ENRICH used in the outer and the inner subquery: the policy has to exist in local cluster
     */
    public void testEnrichWithCoordinatorModePolicyMissingOnRelevantClustersFails() {
        setupEnrichPolicy(client(REMOTE_CLUSTER_1), "values_enrich", 10);
        setupEnrichPolicy(client(REMOTE_CLUSTER_2), "values_enrich", 10);
        setSkipUnavailable(REMOTE_CLUSTER_1, false);
        setSkipUnavailable(REMOTE_CLUSTER_2, false);
        try {
            VerificationException ex = expectThrows(VerificationException.class, () -> runQuery("""
                FROM logs-*
                | WHERE v IN (
                    FROM cluster-a:logs-*
                    | WHERE v > 1 AND v < 7
                    | ENRICH _coordinator:values_enrich ON v WITH enrich_name
                    | KEEP v
                  )
                | KEEP v
                """, false));
            assertThat(ex.getMessage(), containsString("cannot find enrich policy [values_enrich]"));
        } finally {
            deleteEnrichPolicy(client(REMOTE_CLUSTER_1), "values_enrich");
            deleteEnrichPolicy(client(REMOTE_CLUSTER_2), "values_enrich");
            clearSkipUnavailable(3);
        }
    }

    /**
     * REMOTE mode ENRICH used in the outer and the inner subquery: the policy has to exist in the remote clusters being targeted
     */
    public void testEnrichWithRemoteModePolicyPresentOnRelevantClustersSucceeds() {
        setupEnrichPolicy(client(REMOTE_CLUSTER_1), "values_enrich", 10);
        setSkipUnavailable(REMOTE_CLUSTER_1, false);
        setSkipUnavailable(REMOTE_CLUSTER_2, false);
        try {
            try (EsqlQueryResponse resp = runQuery("""
                FROM cluster-a:logs-*
                | WHERE v IN (
                    FROM cluster-a:logs-*
                    | WHERE v > 1 AND v < 7
                    | ENRICH _remote:values_enrich ON v WITH enrich_name
                    | KEEP v
                  )
                | KEEP v
                """, false)) {
                assertThat(getValuesList(resp), equalTo(List.of(List.of(4L))));
            }
        } finally {
            deleteEnrichPolicy(client(REMOTE_CLUSTER_1), "values_enrich");
            clearSkipUnavailable(3);
        }
    }

    /**
     * REMOTE mode ENRICH used in the inner subquery with a remote cluster: the policy has to exist in that remote cluster
     */
    public void testEnrichWithRemoteModePolicySucceedsEvenIfMissingOnUnrelatedCluster() {
        setupEnrichPolicy(client(REMOTE_CLUSTER_1), "values_enrich", 10);
        setSkipUnavailable(REMOTE_CLUSTER_1, false);
        setSkipUnavailable(REMOTE_CLUSTER_2, false);
        try {
            try (EsqlQueryResponse resp = runQuery("""
                FROM logs-*
                | WHERE v IN (
                    FROM cluster-a:logs-*
                    | WHERE v > 1 AND v < 7
                    | ENRICH _remote:values_enrich ON v WITH enrich_name
                    | KEEP v
                  )
                | KEEP v
                """, false)) {
                assertThat(getValuesList(resp), equalTo(List.of(List.of(4L))));
            }
        } finally {
            deleteEnrichPolicy(client(REMOTE_CLUSTER_1), "values_enrich");
            clearSkipUnavailable(3);
        }
    }

    /**
     * REMOTE mode ENRICH used in the inner subquery with different remote clusters and policy: each policy has to exist in the
     * corresponding cluster it's used with
     */
    public void testEnrichWithRemoteModePolicyMixedScopesSucceeds() {
        setupEnrichPolicy(client(REMOTE_CLUSTER_1), "values_enrich_a", 10);
        setupEnrichPolicy(client(REMOTE_CLUSTER_2), "values_enrich_b", 10);
        setSkipUnavailable(REMOTE_CLUSTER_1, false);
        setSkipUnavailable(REMOTE_CLUSTER_2, false);
        try {
            try (EsqlQueryResponse resp = runQuery("""
                FROM remote-b:logs-*
                | WHERE v IN (
                    FROM cluster-a:logs-*
                    | WHERE v > 1 AND v < 7
                    | ENRICH _remote:values_enrich_a ON v WITH enrich_name
                    | KEEP v
                  )
                | ENRICH _remote:values_enrich_b ON v WITH enrich_name
                | KEEP v
                """, false)) {
                assertThat(getValuesList(resp), equalTo(List.of(List.of(4L))));
            }
        } finally {
            deleteEnrichPolicy(client(REMOTE_CLUSTER_1), "values_enrich_a");
            deleteEnrichPolicy(client(REMOTE_CLUSTER_2), "values_enrich_b");
            clearSkipUnavailable(3);
        }
    }

    /**
     * Two occurrences of the same ENRICH command - same policy name, same mode - sharing the exact same {@code Source}
     */
    public void testEnrichWithViewReferencedTwiceSharesSource() {
        assumeTrue("Requires FROM-subquery support", EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND.isEnabled());
        setupEnrichPolicy(client(LOCAL_CLUSTER), "values_enrich", 10);
        setSkipUnavailable(REMOTE_CLUSTER_1, false);
        setSkipUnavailable(REMOTE_CLUSTER_2, false);
        createViewOnCluster(
            LOCAL_CLUSTER,
            "enrich_view",
            "FROM logs-* | WHERE v > 1 AND v < 7 | ENRICH values_enrich ON v WITH enrich_name | KEEP v"
        );
        try {
            try (EsqlQueryResponse resp = runQuery("""
                FROM (FROM enrich_view), (FROM cluster-a:logs-*)
                | WHERE v IN (FROM enrich_view)
                | KEEP v
                | SORT v
                """, false)) {
                assertThat(
                    getValuesList(resp),
                    equalTo(List.of(List.of(2L), List.of(3L), List.of(4L), List.of(4L), List.of(5L), List.of(6L)))
                );
            }
        } finally {
            deleteEnrichPolicy(client(LOCAL_CLUSTER), "values_enrich");
            clearSkipUnavailable(3);
        }
    }

    /**
     * The same policy name used with three different modes (ANY, COORDINATOR, REMOTE) in one query: each occurrence has
     * its own mode-specific target-cluster requirement (see {@code EnrichPolicyResolver#calculateTargetClusters}), so all
     * three must resolve independently even though they share a policy name - the REMOTE occurrence only needs
     * {@code cluster-a} (its own scope), the ANY occurrence needs both remotes the outer {@code *:logs-*} touches (that
     * pattern spans {@code cluster-a} and {@code remote-b}, not the local cluster) plus {@code _local} (ANY always
     * requires it), and the COORDINATOR occurrence only ever needs {@code _local}.
     */
    public void testEnrichSamePolicyDifferentModesAllSucceed() {
        setupEnrichPolicy(client(LOCAL_CLUSTER), "values_enrich", 10);
        setupEnrichPolicy(client(REMOTE_CLUSTER_1), "values_enrich", 10);
        setupEnrichPolicy(client(REMOTE_CLUSTER_2), "values_enrich", 10);
        setSkipUnavailable(REMOTE_CLUSTER_1, false);
        setSkipUnavailable(REMOTE_CLUSTER_2, false);
        try {
            try (EsqlQueryResponse resp = runQuery("""
                FROM *:logs-*
                | WHERE v IN (
                    FROM cluster-a:logs-*
                    | WHERE v > 1 AND v < 7
                    | ENRICH _remote:values_enrich ON v WITH enrich_name
                    | KEEP v
                  )
                | ENRICH values_enrich ON v WITH enrich_name
                | ENRICH _coordinator:values_enrich ON v WITH enrich_name
                | KEEP v
                | SORT v
                """, false)) {
                // *:logs-* spans cluster-a and remote-b only; each contributes a single v=4 row (i=2 -> 2^2=4).
                assertThat(getValuesList(resp), equalTo(List.of(List.of(4L), List.of(4L))));
            }
        } finally {
            deleteEnrichPolicy(client(LOCAL_CLUSTER), "values_enrich");
            deleteEnrichPolicy(client(REMOTE_CLUSTER_1), "values_enrich");
            deleteEnrichPolicy(client(REMOTE_CLUSTER_2), "values_enrich");
            clearSkipUnavailable(3);
        }
    }

    /**
     * The same policy name used with two different modes where only one occurrence's requirement is satisfied: REMOTE
     * mode (scoped to {@code cluster-a} only) fails because the policy is missing there, while COORDINATOR mode
     * (scoped to {@code _local} only, elsewhere in the same query) would succeed on its own since the policy exists
     * locally. The failure must report exactly the REMOTE occurrence's own missing cluster ({@code cluster-a}), proving
     * the two occurrences are resolved independently rather than one's requirement leaking into the other's error.
     */
    public void testEnrichSamePolicyDifferentModesFailureIsolatedPerOccurrence() {
        setupEnrichPolicy(client(LOCAL_CLUSTER), "values_enrich", 10);
        setSkipUnavailable(REMOTE_CLUSTER_1, false);
        setSkipUnavailable(REMOTE_CLUSTER_2, false);
        try {
            VerificationException ex = expectThrows(VerificationException.class, () -> runQuery("""
                FROM logs-*
                | WHERE v IN (
                    FROM cluster-a:logs-*
                    | WHERE v > 1 AND v < 7
                    | ENRICH _remote:values_enrich ON v WITH enrich_name
                    | KEEP v
                  )
                | ENRICH _coordinator:values_enrich ON v WITH enrich_name
                | KEEP v
                """, false));
            assertThat(ex.getMessage(), containsString("cannot find enrich policy [values_enrich] on clusters [cluster-a]"));
        } finally {
            deleteEnrichPolicy(client(LOCAL_CLUSTER), "values_enrich");
            clearSkipUnavailable(3);
        }
    }

    /**
     * Three ENRICH occurrences in one query, each with a different mode, each in a different subquery scope:
     * <ul>
     *   <li>REMOTE mode inside the {@code cluster-a} WHERE IN subquery — scoped to {@code cluster-a}, needs only that remote.</li>
     *   <li>ANY mode in the outer query — scoped to {@code _local} (outer source is {@code FROM logs-*}), needs only local.</li>
     *   <li>COORDINATOR mode in the outer query — always runs on the coordinator, needs only local.</li>
     * </ul>
     * Each ENRICH uses a distinct policy name, so each resolves independently with no cross-contamination.
     */
    public void testEnrichWithMixedModesInSubqueries() {
        setupEnrichPolicy(client(REMOTE_CLUSTER_1), "values_enrich_a", 10);
        setupEnrichPolicy(client(LOCAL_CLUSTER), "values_enrich_b", 10);
        setupEnrichPolicy(client(LOCAL_CLUSTER), "values_enrich_c", 10);
        setSkipUnavailable(REMOTE_CLUSTER_1, false);
        try {
            try (EsqlQueryResponse resp = runQuery("""
                FROM logs-*
                | WHERE v IN (
                    FROM cluster-a:logs-*
                    | WHERE v > 1 AND v < 7
                    | ENRICH _remote:values_enrich_a ON v WITH enrich_name
                    | KEEP v
                  )
                | ENRICH values_enrich_b ON v WITH enrich_name
                | ENRICH _coordinator:values_enrich_c ON v WITH enrich_name
                | KEEP v
                | SORT v
                """, false)) {
                // cluster-a has v = i*i for i in [0,9]: only v=4 passes the > 1 AND < 7 filter, so IN set = {4}.
                // Local logs has exactly one row with v=4; after the filter, enrichments, and KEEP v: [[4]].
                assertThat(getValuesList(resp), equalTo(List.of(List.of(4L))));
            }
        } finally {
            deleteEnrichPolicy(client(REMOTE_CLUSTER_1), "values_enrich_a");
            deleteEnrichPolicy(client(LOCAL_CLUSTER), "values_enrich_b");
            deleteEnrichPolicy(client(LOCAL_CLUSTER), "values_enrich_c");
            clearSkipUnavailable(3);
        }
    }

    // ---- helpers ----

    /**
     * Creates a {@code ts_metrics} time-series index on {@code clusterAlias} with a single
     * {@code cluster} dimension (set to {@code clusterValue}) and a {@code max_bytes} gauge metric.
     * Two documents are inserted so the index is non-empty and the TS aggregation has data to process.
     */
    private void setupTsMetricsIndex(String clusterAlias, String clusterValue) {
        assertAcked(
            client(clusterAlias).admin()
                .indices()
                .prepareCreate("ts_metrics")
                .setSettings(
                    Settings.builder()
                        .put("mode", "time_series")
                        .putList("routing_path", List.of("cluster"))
                        .put("index.number_of_shards", 1)
                        .put("index.number_of_replicas", 0)
                )
                .setMapping(
                    "@timestamp",
                    "type=date",
                    "cluster",
                    "type=keyword,time_series_dimension=true",
                    "max_bytes",
                    "type=long,time_series_metric=gauge"
                )
        );
        client(clusterAlias).prepareIndex("ts_metrics")
            .setSource("@timestamp", "2024-01-01T00:00:00Z", "cluster", clusterValue, "max_bytes", 1000L)
            .get();
        client(clusterAlias).prepareIndex("ts_metrics")
            .setSource("@timestamp", "2024-01-01T01:00:00Z", "cluster", clusterValue, "max_bytes", 2000L)
            .get();
        client(clusterAlias).admin().indices().prepareRefresh("ts_metrics").get();
    }

    private static QueryPragmas forceHashJoin() {
        return new QueryPragmas(Settings.builder().put("in_subquery_hash_join_threshold", 0).build());
    }

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
