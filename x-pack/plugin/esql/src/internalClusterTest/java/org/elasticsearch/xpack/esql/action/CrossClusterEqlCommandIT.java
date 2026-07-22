/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.Build;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.xpack.eql.plugin.EqlPlugin;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

/**
 * Verifies that the {@code EQL} source command works across clusters. The command passes its
 * {@code WITH {"indices": ...}} option through to {@code EqlSearchAction} unchanged, so a
 * {@code remote_cluster:index} pattern is resolved by the EQL engine's own cross-cluster support:
 * the query is issued on the local cluster but the events are read from a remote cluster.
 */
public class CrossClusterEqlCommandIT extends AbstractCrossClusterTestCase {

    @Override
    protected List<String> remoteClusterAlias() {
        return List.of(REMOTE_CLUSTER_1);
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins(String clusterAlias) {
        List<Class<? extends Plugin>> plugins = new ArrayList<>(super.nodePlugins(clusterAlias));
        // The EQL command delegates to the EQL engine, so every participating cluster needs the EQL plugin:
        // the local cluster initiates the EqlSearchAction and the remote cluster executes its part.
        plugins.add(EqlPlugin.class);
        return plugins;
    }

    public void testEqlEventQueryReadsFromRemoteCluster() throws Exception {
        assumeTrue("EQL command is snapshot-only", Build.current().isSnapshot());
        setupClusters(2); // registers the remote connection and skip_unavailable settings

        // The EQL-shaped events live only on the remote cluster.
        Client remote = client(REMOTE_CLUSTER_1);
        assertAcked(
            remote.admin()
                .indices()
                .prepareCreate("eql_events")
                .setSettings(Settings.builder().put("index.number_of_shards", 1))
                .setMapping("@timestamp", "type=date", "event.category", "type=keyword", "process.name", "type=keyword")
        );
        remote.prepareBulk()
            .add(
                new IndexRequest("eql_events").id("p1")
                    .source("@timestamp", "2026-07-22T10:00:00Z", "event.category", "process", "process.name", "cmd.exe")
            )
            .add(new IndexRequest("eql_events").id("n1").source("@timestamp", "2026-07-22T10:00:01Z", "event.category", "network"))
            .add(
                new IndexRequest("eql_events").id("p2")
                    .source("@timestamp", "2026-07-22T10:00:02Z", "event.category", "process", "process.name", "powershell.exe")
            )
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            .get();

        // Run the EQL command from the LOCAL cluster, targeting the REMOTE index via cluster-qualified pattern.
        String query = "EQL \"process where true\" WITH {\"indices\": \""
            + REMOTE_CLUSTER_1
            + ":eql_events\"} | STATS count = COUNT(*)";
        try (EsqlQueryResponse resp = runQuery(query, false)) {
            List<List<Object>> rows = getValuesList(resp);
            assertThat(rows, hasSize(1));
            assertThat(rows.get(0).get(0), equalTo(2L)); // the two remote process events (not the network event)
        }
    }
}
