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
 * Verifies that the {@code EQL} source command works across clusters. Its leading index-pattern argument may be
 * cluster-qualified ({@code remote_cluster:index}); that pattern is resolved through field-caps and handed to
 * {@code EqlSearchAction}, whose own cross-cluster support reads the events from the remote cluster while the
 * query is issued on the local cluster. This is also the canary for coordinator-only cross-cluster execution-info
 * finalization, so it must actually run, not merely compile.
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
        String query = "EQL " + REMOTE_CLUSTER_1 + ":eql_events \"process where true\" | STATS count = COUNT(*)";
        try (EsqlQueryResponse resp = runQuery(query, false)) {
            List<List<Object>> rows = getValuesList(resp);
            assertThat(rows, hasSize(1));
            assertThat(rows.get(0).get(0), equalTo(2L)); // the two remote process events (not the network event)

            // The EQL pattern registers the remote cluster in the execution info, but the plan is coordinator-only
            // (no per-cluster compute reports back). Pin that the coordinator-only finalization still marks the
            // remote cluster SUCCESSFUL rather than leaving it stuck RUNNING.
            EsqlExecutionInfo.Cluster remoteCluster = resp.getExecutionInfo().getCluster(REMOTE_CLUSTER_1);
            assertThat(remoteCluster.getStatus(), equalTo(EsqlExecutionInfo.Cluster.Status.SUCCESSFUL));
        }

        // METADATA _index over a remote pattern must render the cluster-qualified index (matching FROM's CCS _index).
        String metadataQuery = "EQL " + REMOTE_CLUSTER_1 + ":eql_events \"process where true\" METADATA _index | KEEP _index | SORT _index";
        try (EsqlQueryResponse resp = runQuery(metadataQuery, false)) {
            List<List<Object>> rows = getValuesList(resp);
            assertThat(rows, hasSize(2));
            String qualified = REMOTE_CLUSTER_1 + ":eql_events";
            assertThat(rows.get(0).get(0).toString(), equalTo(qualified));
            assertThat(rows.get(1).get(0).toString(), equalTo(qualified));
        }
    }
}
