/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.indexlifecycle;

import static org.elasticsearch.client.Requests.clusterHealthRequest;
import static org.elasticsearch.client.Requests.createIndexRequest;
import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_NUMBER_OF_REPLICAS;
import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_NUMBER_OF_SHARDS;
import static org.elasticsearch.cluster.routing.ShardRoutingState.STARTED;
import static org.elasticsearch.test.ESIntegTestCase.Scope;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.core.IsNull.nullValue;

import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.get.GetIndexResponse;
import org.elasticsearch.analysis.common.CommonAnalysisPlugin;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.reindex.ReindexPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.XPackPlugin;
import org.elasticsearch.xpack.XPackSettings;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;

@ESIntegTestCase.ClusterScope(scope = Scope.TEST, numDataNodes = 0)
public class IndexLifecycleInitialisationIT extends ESIntegTestCase {
    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        Settings.Builder settings = Settings.builder().put(super.nodeSettings(nodeOrdinal));
        settings.put(XPackSettings.INDEX_LIFECYCLE_ENABLED.getKey(),  true);
        settings.put(XPackSettings.MACHINE_LEARNING_ENABLED.getKey(), false);
        settings.put(XPackSettings.SECURITY_ENABLED.getKey(), false);
        settings.put(XPackSettings.WATCHER_ENABLED.getKey(), false);
        settings.put(XPackSettings.MONITORING_ENABLED.getKey(), false);
        settings.put(XPackSettings.GRAPH_ENABLED.getKey(), false);
        return settings.build();
    }

    @Override
    protected Settings transportClientSettings() {
        Settings.Builder settings = Settings.builder().put(super.transportClientSettings());
        settings.put(XPackSettings.INDEX_LIFECYCLE_ENABLED.getKey(), true);
        settings.put(XPackSettings.MACHINE_LEARNING_ENABLED.getKey(), false);
        settings.put(XPackSettings.SECURITY_ENABLED.getKey(), false);
        settings.put(XPackSettings.WATCHER_ENABLED.getKey(), false);
        settings.put(XPackSettings.MONITORING_ENABLED.getKey(), false);
        settings.put(XPackSettings.GRAPH_ENABLED.getKey(), false);
        return settings.build();
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(XPackPlugin.class, CommonAnalysisPlugin.class, ReindexPlugin.class);
    }

    @Override
    protected Collection<Class<? extends Plugin>> transportClientPlugins() {
        return nodePlugins();
    }

    public void test() throws IOException {
        Settings settings = Settings.builder()
            .put(indexSettings())
            .put(SETTING_NUMBER_OF_SHARDS, 1)
            .put(SETTING_NUMBER_OF_REPLICAS, 0)
            .put("index.lifecycle.timeseries.new", "baz")
            .put("index.lifecycle.timeseries.delete.after", "3s")
            .build();

        // start one server
        logger.info("Starting sever1");
        final String server_1 = internalCluster().startNode();
        final String node1 = getLocalNodeId(server_1);

        logger.info("Creating index [test]");
        CreateIndexResponse createIndexResponse = client().admin().indices().create(createIndexRequest("test").settings(settings)).actionGet();
        assertAcked(createIndexResponse);

        ClusterState clusterState = client().admin().cluster().prepareState().get().getState();
        RoutingNode routingNodeEntry1 = clusterState.getRoutingNodes().node(node1);
        assertThat(routingNodeEntry1.numberOfShardsWithState(STARTED), equalTo(1));

        logger.info("Starting server2");
        // start another server
        String server_2 = internalCluster().startNode();

        // first wait for 2 nodes in the cluster
        logger.info("Waiting for replicas to be assigned");
        ClusterHealthResponse clusterHealth =
            client().admin().cluster().health(clusterHealthRequest().waitForGreenStatus().waitForNodes("2")).actionGet();
        logger.info("Done Cluster Health, status {}", clusterHealth.getStatus());
        assertThat(clusterHealth.isTimedOut(), equalTo(false));
        assertThat(clusterHealth.getStatus(), equalTo(ClusterHealthStatus.GREEN));
        final String node2 = getLocalNodeId(server_2);

        // explicitly call reroute, so shards will get relocated to the new node (we delay it in ES in case other nodes join)
        client().admin().cluster().prepareReroute().execute().actionGet();

        clusterHealth = client().admin().cluster().health(clusterHealthRequest().waitForGreenStatus().waitForNodes("2").waitForNoRelocatingShards(true)).actionGet();
        assertThat(clusterHealth.isTimedOut(), equalTo(false));
        assertThat(clusterHealth.getStatus(), equalTo(ClusterHealthStatus.GREEN));
        assertThat(clusterHealth.getNumberOfDataNodes(), equalTo(2));
        assertThat(clusterHealth.getInitializingShards(), equalTo(0));
        assertThat(clusterHealth.getUnassignedShards(), equalTo(0));
        assertThat(clusterHealth.getRelocatingShards(), equalTo(0));
        assertThat(clusterHealth.getActiveShards(), equalTo(1));
        assertThat(clusterHealth.getActivePrimaryShards(), equalTo(1));

        logger.info("Closing server1");
        // kill the first server
        internalCluster().stopCurrentMasterNode();
        // verify health
        logger.info("Running Cluster Health");
        clusterHealth = client().admin().cluster().health(clusterHealthRequest().waitForGreenStatus()
            .waitForNodes("1").waitForActiveShards(0)).actionGet();
        logger.info("Done Cluster Health, status {}", clusterHealth.getStatus());
        assertThat(clusterHealth.isTimedOut(), equalTo(false));
        assertThat(clusterHealth.getStatus(), equalTo(ClusterHealthStatus.GREEN));
        assertThat(clusterHealth.getNumberOfNodes(), equalTo(1));
        assertThat(clusterHealth.getActivePrimaryShards(), equalTo(0));
        expectThrows(IndexNotFoundException.class,
            () -> client().admin().indices().prepareGetIndex().addIndices("test").get());
    }

    private String getLocalNodeId(String name) {
        TransportService transportService = internalCluster().getInstance(TransportService.class, name);
        String nodeId = transportService.getLocalNode().getId();
        assertThat(nodeId, not(nullValue()));
        return nodeId;
    }
}
