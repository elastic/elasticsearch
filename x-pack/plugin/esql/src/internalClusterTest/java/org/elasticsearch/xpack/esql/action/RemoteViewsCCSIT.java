/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.cluster.metadata.View;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.xpack.esql.view.PutViewAction;
import org.junit.Before;
import org.junit.BeforeClass;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

/**
 * Integration tests for ES|QL remote-view resolution (the
 * {@link EsqlCapabilities.Cap#REMOTE_VIEW_RESOLUTION} feature).
 * <p>
 * These tests require two running clusters and verify that views created on a remote cluster can be
 * queried by the coordinating cluster by expanding the view body into a cluster-scoped sub-plan
 * before field-caps runs.
 * <p>
 * The legacy "remote views are not supported" behaviour is tested in {@link CrossClusterViewIT}
 * (which skips when this capability is active).
 */
public class RemoteViewsCCSIT extends AbstractCrossClusterTestCase {

    @BeforeClass
    public static void requireRemoteViewResolution() {
        assumeTrue(
            "Requires REMOTE_VIEW_RESOLUTION capability (snapshot builds only)",
            EsqlCapabilities.Cap.REMOTE_VIEW_RESOLUTION.isEnabled()
        );
    }

    @Before
    public void setupClustersAndViews() throws IOException {
        setupClusters(3);
        // Create views on cluster-a referencing logs-2 (the remote data index)
        createViewOnCluster(REMOTE_CLUSTER_1, "logs-web", "FROM logs-2 | WHERE tag == \"remote\" | LIMIT 100");
        createViewOnCluster(REMOTE_CLUSTER_1, "logs-mobile", "FROM logs-2 | LIMIT 5");
    }

    /**
     * Exact remote view reference: {@code FROM cluster-a:logs-web} must be expanded into the
     * view body and return rows from the underlying index on the remote cluster.
     */
    public void testExactRemoteViewReturnsData() {
        try (var resp = runQuery("FROM cluster-a:logs-web", null)) {
            assertThat(resp.isPartial(), equalTo(false));
            List<List<Object>> values = getValuesList(resp);
            assertThat("Expected rows from cluster-a:logs-web view body", values.size(), greaterThan(0));
        }
    }

    /**
     * Query that references a remote view alongside local data: both branches must execute and
     * return rows.
     */
    public void testLocalIndexAndRemoteViewReturnsData() {
        try (var resp = runQuery("FROM logs-1, cluster-a:logs-mobile | KEEP tag", null)) {
            assertThat(resp.isPartial(), equalTo(false));
            List<List<Object>> values = getValuesList(resp);
            assertThat("Expected rows from both local index and remote view", values.size(), greaterThan(0));
        }
    }

    /**
     * Wildcard pattern that matches remote views: the view bodies are expanded and executed;
     * the query must succeed and return data.
     */
    public void testRemoteViewWildcardMatchSucceeds() {
        try (var resp = runQuery("FROM cluster-a:logs-*", null)) {
            assertThat(resp.isPartial(), equalTo(false));
            List<List<Object>> values = getValuesList(resp);
            assertThat("Expected rows when wildcard matches remote views", values.size(), greaterThan(0));
        }
    }

    /**
     * A remote cluster with no views matching the requested pattern: the query succeeds,
     * returning data only from the concrete indices on that cluster.
     */
    public void testRemoteClusterWithNoMatchingViewsSucceeds() {
        try (var resp = runQuery("FROM remote-b:logs-*", null)) {
            assertThat(resp.isPartial(), equalTo(false));
            List<List<Object>> values = getValuesList(resp);
            assertThat("Expected rows from remote-b concrete index", values.size(), greaterThan(0));
        }
    }

    /**
     * Queries both clusters; one (cluster-a) has views matching the pattern while the other
     * (remote-b) has only concrete indices. Both must contribute rows.
     */
    public void testMixedViewsAndConcreteIndicesAcrossClusters() {
        try (var resp = runQuery("FROM cluster-a:logs-web, remote-b:logs-*", null)) {
            assertThat(resp.isPartial(), equalTo(false));
            List<List<Object>> values = getValuesList(resp);
            assertThat("Expected rows from view-expanded cluster-a and concrete remote-b", values.size(), greaterThan(0));
        }
    }

    // ------------------------------------------------------------------
    // Helpers
    // ------------------------------------------------------------------

    private void createViewOnCluster(String clusterAlias, String viewName, String query) {
        assertAcked(
            client(clusterAlias).execute(
                PutViewAction.INSTANCE,
                new PutViewAction.Request(TimeValue.THIRTY_SECONDS, TimeValue.THIRTY_SECONDS, new View(viewName, query))
            ).actionGet(30, TimeUnit.SECONDS)
        );
    }
}
