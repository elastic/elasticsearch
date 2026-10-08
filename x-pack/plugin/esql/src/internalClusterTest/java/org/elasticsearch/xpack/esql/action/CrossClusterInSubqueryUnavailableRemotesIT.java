/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.cluster.metadata.View;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.xpack.esql.view.PutViewAction;
import org.junit.Before;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.esql.action.CrossClusterSubqueryIT.assertClusterEsqlExecutionInfo;
import static org.elasticsearch.xpack.esql.action.CrossClusterSubqueryIT.assertClusterEsqlExecutionInfoFailureReason;

/**
 * CCS tests for {@code WHERE x IN (subquery)} / {@code WHERE x NOT IN (subquery)} when
 * a remote cluster is unavailable (fully disconnected). Covers both
 * {@code skip_unavailable=true} and {@code skip_unavailable=false} for each case.
 *
 * <p>Note: simulated transport-layer failures via {@code EsqlResolveFieldsAction.RESOLVE_REMOTE_TYPE}
 * are NOT tested here. That mock intercepts the ESQL-specific resolution path used by the
 * union-FROM-subquery feature ({@code FROM (subquery)}), but WHERE IN subqueries use the
 * standard field-caps CCS resolution path for the outer FROM clause. Full-disconnect tests
 * (via {@code cluster.close()}) remain the appropriate way to test unavailability for this
 * feature.
 */
public class CrossClusterInSubqueryUnavailableRemotesIT extends AbstractCrossClusterTestCase {

    private static final String EVENTS = "events";

    @Before
    public void checkCapabilityAndSetup() throws IOException {
        assumeTrue("Requires IN subquery support", EsqlCapabilities.Cap.WHERE_IN_SUBQUERY_WITHOUT_VIEW.isEnabled());
        setupClusters(3);
        setupInSubqueryIndices();
        // Local view whose body spans all remote clusters — used by the view+skip_unavailable tests.
        createViewOnCluster(LOCAL_CLUSTER, "remote_events_red", "FROM *:events | WHERE color == \"red\"");
    }

    /**
     * Each test closes {@link #REMOTE_CLUSTER_1} and does not restart it. Returning {@code false}
     * here causes the framework to tear down and recreate all clusters between test methods, so a
     * closed cluster in one test cannot affect the next.
     */
    @Override
    protected boolean reuseClusters() {
        return false;
    }

    /**
     * When {@code skip_unavailable=true} and a remote cluster is fully disconnected, it is
     * marked SKIPPED; the other clusters return their data normally.
     */
    public void testInSubqueryWithDisconnectedRemoteClusterWithSkipUnavailableTrue() throws IOException {
        setSkipUnavailable(REMOTE_CLUSTER_1, true);
        setSkipUnavailable(REMOTE_CLUSTER_2, true);

        try {
            cluster(REMOTE_CLUSTER_1).close();

            try (EsqlQueryResponse resp = runQuery("""
                FROM *:events
                | WHERE id IN (FROM events | WHERE color == "red" | KEEP id)
                | STATS c = COUNT(*) BY tag
                | SORT tag
                """, randomBoolean())) {
                EsqlExecutionInfo executionInfo = resp.getExecutionInfo();
                assertClusterEsqlExecutionInfo(executionInfo, LOCAL_CLUSTER, EsqlExecutionInfo.Cluster.Status.SUCCESSFUL);
                assertClusterEsqlExecutionInfo(executionInfo, REMOTE_CLUSTER_1, EsqlExecutionInfo.Cluster.Status.SKIPPED);
                assertClusterEsqlExecutionInfo(executionInfo, REMOTE_CLUSTER_2, EsqlExecutionInfo.Cluster.Status.SUCCESSFUL);
                assertClusterEsqlExecutionInfoFailureReason(
                    executionInfo,
                    REMOTE_CLUSTER_1,
                    "Remote cluster [cluster-a] (with setting skip_unavailable=true) is not available"
                );
            }
        } finally {
            clearSkipUnavailable(3);
        }
    }

    /**
     * When {@code skip_unavailable=false} and a remote cluster is fully disconnected, the query
     * fails with a remote-unavailable exception.
     */
    public void testInSubqueryWithDisconnectedRemoteClusterWithSkipUnavailableFalse() throws IOException {
        setSkipUnavailable(REMOTE_CLUSTER_1, false);
        setSkipUnavailable(REMOTE_CLUSTER_2, false);

        try {
            cluster(REMOTE_CLUSTER_1).close();

            Exception ex = expectThrows(ElasticsearchException.class, () -> runQuery("""
                FROM *:events
                | WHERE id IN (FROM events | WHERE color == "red" | KEEP id)
                | STATS c = COUNT(*) BY tag
                | SORT tag
                """, randomBoolean()));
            assertTrue(ExceptionsHelper.isRemoteUnavailableException(ex));
        } finally {
            clearSkipUnavailable(3);
        }
    }

    // ---- view + skip_unavailable ----

    /**
     * The outer {@code FROM} uses the local view {@code remote_events_red}, whose body is
     * {@code FROM *:events | WHERE color == "red"} — this expands to a CCS query spanning all
     * configured remote clusters. When {@code cluster-a} is disconnected and
     * {@code skip_unavailable=true}, it is marked {@code SKIPPED} and the remaining clusters
     * continue normally.
     *
     * <p>The IN subquery runs on the local cluster and is unaffected by the remote disconnect.
     */
    public void testViewInOuterFromWithDisconnectedRemoteClusterSkipUnavailableTrue() throws IOException {
        setSkipUnavailable(REMOTE_CLUSTER_1, true);
        setSkipUnavailable(REMOTE_CLUSTER_2, true);

        try {
            cluster(REMOTE_CLUSTER_1).close();

            try (EsqlQueryResponse resp = runQuery("""
                FROM remote_events_red
                | WHERE id IN (FROM events | WHERE id <= 4 | KEEP id)
                | STATS c = COUNT(*) BY tag
                | SORT tag
                """, randomBoolean())) {
                EsqlExecutionInfo executionInfo = resp.getExecutionInfo();
                assertClusterEsqlExecutionInfo(executionInfo, LOCAL_CLUSTER, EsqlExecutionInfo.Cluster.Status.SUCCESSFUL);
                assertClusterEsqlExecutionInfo(executionInfo, REMOTE_CLUSTER_1, EsqlExecutionInfo.Cluster.Status.SKIPPED);
                assertClusterEsqlExecutionInfo(executionInfo, REMOTE_CLUSTER_2, EsqlExecutionInfo.Cluster.Status.SUCCESSFUL);
                assertClusterEsqlExecutionInfoFailureReason(
                    executionInfo,
                    REMOTE_CLUSTER_1,
                    "Remote cluster [cluster-a] (with setting skip_unavailable=true) is not available"
                );
            }
        } finally {
            clearSkipUnavailable(3);
        }
    }

    /**
     * Same setup as {@link #testViewInOuterFromWithDisconnectedRemoteClusterSkipUnavailableTrue},
     * but with {@code skip_unavailable=false}: the query must fail with a remote-unavailable
     * exception when {@code cluster-a} is disconnected.
     */
    public void testViewInOuterFromWithDisconnectedRemoteClusterSkipUnavailableFalse() throws IOException {
        setSkipUnavailable(REMOTE_CLUSTER_1, false);
        setSkipUnavailable(REMOTE_CLUSTER_2, false);

        try {
            cluster(REMOTE_CLUSTER_1).close();

            Exception ex = expectThrows(ElasticsearchException.class, () -> runQuery("""
                FROM remote_events_red
                | WHERE id IN (FROM events | WHERE id <= 4 | KEEP id)
                | STATS c = COUNT(*) BY tag
                | SORT tag
                """, randomBoolean()));
            assertTrue(ExceptionsHelper.isRemoteUnavailableException(ex));
        } finally {
            clearSkipUnavailable(3);
        }
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
