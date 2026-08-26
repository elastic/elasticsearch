/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.cluster.metadata.View;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.transport.NoSuchRemoteClusterException;
import org.elasticsearch.xpack.esql.view.PutViewAction;
import org.junit.Before;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class CrossClusterViewIT extends AbstractCrossClusterTestCase {

    @Before
    public void setupClustersAndViews() throws IOException {
        setupClusters(3);
        createViewOnCluster(REMOTE_CLUSTER_1, "logs-web", "FROM logs-2 | LIMIT 10");
        createViewOnCluster(REMOTE_CLUSTER_1, "logs-mobile", "FROM logs-2 | LIMIT 10");
    }

    public void testRemoteViewWildcardMatchFailsQuery() {
        expectThrows(
            Exception.class,
            containsString(
                "ES|QL queries with remote views are not supported. Matched [cluster-a:logs-mobile, cluster-a:logs-web]."
                    + " Remove them from the query pattern or exclude them with"
                    + " [cluster-a:-logs-mobile,cluster-a:-logs-web] if matched by a wildcard."
            ),
            () -> runQuery("FROM cluster-a:logs-*", null)
        );
    }

    public void testRemoteViewConcreteMatchFailsQuery() {
        expectThrows(
            Exception.class,
            containsString(
                "ES|QL queries with remote views are not supported. Matched [cluster-a:logs-web]."
                    + " Remove them from the query pattern or exclude them with [cluster-a:-logs-web] if matched by a wildcard."
            ),
            () -> runQuery("FROM cluster-a:logs-web", null)
        );
    }

    public void testRemoteViewExcludedSucceeds() {
        try (var resp = runQuery("FROM cluster-a:logs-*,cluster-a:-logs-web,cluster-a:-logs-mobile", null)) {
            assertOk(resp);
        }
    }

    public void testAllViewsOnRemoteExcludedSucceeds() {
        try (var resp = runQuery("FROM cluster*:logs-*,-cluster-a:*,remote-b:*", null)) {
            assertOk(resp);
        }
    }

    public void testRemoteViewFailsOnOneCluster() {
        expectThrows(
            Exception.class,
            containsString(
                "ES|QL queries with remote views are not supported. Matched [cluster-a:logs-mobile, cluster-a:logs-web]."
                    + " Remove them from the query pattern or exclude them with"
                    + " [cluster-a:-logs-mobile,cluster-a:-logs-web] if matched by a wildcard."
            ),
            () -> runQuery("FROM cluster-a:logs-*,remote-b:logs-*", null)
        );
    }

    public void testNoRemoteViewsQuerySucceeds() {
        try (var resp = runQuery("FROM remote-b:logs-*", null)) {
            assertOk(resp);
        }
    }

    public void testUnknownRemote() {
        expectThrows(
            NoSuchRemoteClusterException.class,
            containsString("no such remote cluster: [no_such_remote]"),
            () -> runQuery("FROM no_such_remote:logs-web", null)
        );
        try (var resp = runQuery("FROM no_such_*:logs-web", null)) {
            assertOk(resp);
        }
    }

    private void createViewOnCluster(String clusterAlias, String viewName, String query) {
        assertAcked(
            client(clusterAlias).execute(
                PutViewAction.INSTANCE,
                new PutViewAction.Request(TimeValue.THIRTY_SECONDS, TimeValue.THIRTY_SECONDS, new View(viewName, query))
            ).actionGet(30, TimeUnit.SECONDS)
        );
    }

    private static void assertOk(EsqlQueryResponse response) {
        assertThat(response.isPartial(), equalTo(false));
    }
}
