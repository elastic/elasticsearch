/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.indexlifecycle;

import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.ESIntegTestCase.Scope;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.LocalStateCompositeXPackPlugin;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.indexlifecycle.DeleteAction;
import org.elasticsearch.xpack.core.indexlifecycle.ForceMergeAction;
import org.elasticsearch.xpack.core.indexlifecycle.LifecycleAction;
import org.elasticsearch.xpack.core.indexlifecycle.LifecyclePolicy;
import org.elasticsearch.xpack.core.indexlifecycle.LifecycleSettings;
import org.elasticsearch.xpack.core.indexlifecycle.Phase;
import org.elasticsearch.xpack.core.indexlifecycle.TimeseriesLifecycleType;
import org.elasticsearch.xpack.core.indexlifecycle.action.PutLifecycleAction;
import org.junit.Before;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.client.Requests.clusterHealthRequest;
import static org.elasticsearch.client.Requests.createIndexRequest;
import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_NUMBER_OF_REPLICAS;
import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_NUMBER_OF_SHARDS;
import static org.elasticsearch.cluster.routing.ShardRoutingState.STARTED;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.core.IsNull.nullValue;

@ESIntegTestCase.ClusterScope(scope = Scope.TEST, numDataNodes = 0)
public class IndexLifecycleInitialisationIT extends ESIntegTestCase {
    private Settings settings;
    private LifecyclePolicy lifecyclePolicy;

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        Settings.Builder settings = Settings.builder().put(super.nodeSettings(nodeOrdinal));
        settings.put(XPackSettings.INDEX_LIFECYCLE_ENABLED.getKey(), true);
        settings.put(XPackSettings.MACHINE_LEARNING_ENABLED.getKey(), false);
        settings.put(XPackSettings.SECURITY_ENABLED.getKey(), false);
        settings.put(XPackSettings.WATCHER_ENABLED.getKey(), false);
        settings.put(XPackSettings.MONITORING_ENABLED.getKey(), false);
        settings.put(XPackSettings.GRAPH_ENABLED.getKey(), false);
        settings.put(XPackSettings.LOGSTASH_ENABLED.getKey(), false);
        settings.put(LifecycleSettings.LIFECYCLE_POLL_INTERVAL, "1s");
        return settings.build();
    }

    @Override
    protected boolean ignoreExternalCluster() {
        return true;
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
        settings.put(XPackSettings.LOGSTASH_ENABLED.getKey(), false);
        return settings.build();
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(LocalStateCompositeXPackPlugin.class, IndexLifecycle.class);
    }

    @Override
    protected Collection<Class<? extends Plugin>> transportClientPlugins() {
        return nodePlugins();
    }

    @Before
    public void init() {
        settings = Settings.builder().put(indexSettings()).put(SETTING_NUMBER_OF_SHARDS, 1)
            .put(SETTING_NUMBER_OF_REPLICAS, 0).put(LifecycleSettings.LIFECYCLE_NAME, "test").build();
        Map<String, Phase> phases = new HashMap<>();

        Map<String, LifecycleAction> warmPhaseActions = Collections.singletonMap(ForceMergeAction.NAME, new ForceMergeAction(10000));
        phases.put("warm", new Phase("warm", TimeValue.timeValueSeconds(2), warmPhaseActions));

        Map<String, LifecycleAction> deletePhaseActions = Collections.singletonMap(DeleteAction.NAME, new DeleteAction());
        phases.put("delete", new Phase("delete", TimeValue.timeValueSeconds(3), deletePhaseActions));
        lifecyclePolicy = new LifecyclePolicy(TimeseriesLifecycleType.INSTANCE, "test", phases);
    }

    public void testSingleNodeCluster() throws Exception {
        // start master node
        logger.info("Starting server1");
        final String server_1 = internalCluster().startNode();
        final String node1 = getLocalNodeId(server_1);
        logger.info("Creating lifecycle [test_lifecycle]");
        PutLifecycleAction.Request putLifecycleRequest = new PutLifecycleAction.Request(lifecyclePolicy);
        PutLifecycleAction.Response putLifecycleResponse = client().execute(PutLifecycleAction.INSTANCE, putLifecycleRequest).get();
        assertAcked(putLifecycleResponse);
        logger.info("Creating index [test]");
        CreateIndexResponse createIndexResponse = client().admin().indices().create(createIndexRequest("test").settings(settings))
                .actionGet();
        assertAcked(createIndexResponse);
        ClusterState clusterState = client().admin().cluster().prepareState().get().getState();
        RoutingNode routingNodeEntry1 = clusterState.getRoutingNodes().node(node1);
        assertThat(routingNodeEntry1.numberOfShardsWithState(STARTED), equalTo(1));
        assertBusy(() -> {
            assertEquals(false, client().admin().indices().prepareExists("test").get().isExists());
        });
    }

    // NORELEASE re-enable when we re-visit integration testing
    @AwaitsFix(bugUrl = "Fails because of timing")
    public void testMasterDedicatedDataDedicated() throws Exception {
        // start master node
        logger.info("Starting sever1");
        internalCluster().startMasterOnlyNode();
        // start data node
        logger.info("Starting sever1");
        final String server_2 = internalCluster().startDataOnlyNode();
        final String node2 = getLocalNodeId(server_2);

        logger.info("Creating lifecycle [test_lifecycle]");
        PutLifecycleAction.Request putLifecycleRequest = new PutLifecycleAction.Request(lifecyclePolicy);
        PutLifecycleAction.Response putLifecycleResponse = client().execute(PutLifecycleAction.INSTANCE, putLifecycleRequest).get();
        assertAcked(putLifecycleResponse);
        logger.info("Creating index [test]");
        CreateIndexResponse createIndexResponse = client().admin().indices().create(createIndexRequest("test").settings(settings))
                .actionGet();
        assertAcked(createIndexResponse);

        ClusterState clusterState = client().admin().cluster().prepareState().get().getState();
        RoutingNode routingNodeEntry1 = clusterState.getRoutingNodes().node(node2);
        assertThat(routingNodeEntry1.numberOfShardsWithState(STARTED), equalTo(1));

        assertBusy(() -> {
            assertEquals(false, client().admin().indices().prepareExists("test").get().isExists());
        });
    }

    // NORELEASE re-enable when force merge action bug is fixed
    @AwaitsFix(bugUrl = "Fails because force merge action expect shards to be assigned")
    public void testMasterFailover() throws Exception {
        // start one server
        logger.info("Starting sever1");
        final String server_1 = internalCluster().startNode();
        final String node1 = getLocalNodeId(server_1);

        logger.info("Creating lifecycle [test_lifecycle]");
        PutLifecycleAction.Request putLifecycleRequest = new PutLifecycleAction.Request(lifecyclePolicy);
        PutLifecycleAction.Response putLifecycleResponse = client().execute(PutLifecycleAction.INSTANCE, putLifecycleRequest).get();
        assertAcked(putLifecycleResponse);

        logger.info("Creating index [test]");
        CreateIndexResponse createIndexResponse = client().admin().indices().create(createIndexRequest("test").settings(settings))
                .actionGet();
        assertAcked(createIndexResponse);

        ClusterState clusterState = client().admin().cluster().prepareState().get().getState();
        RoutingNode routingNodeEntry1 = clusterState.getRoutingNodes().node(node1);
        assertThat(routingNodeEntry1.numberOfShardsWithState(STARTED), equalTo(1));

        logger.info("Starting server2");
        // start another server
        internalCluster().startNode();

        // first wait for 2 nodes in the cluster
        logger.info("Waiting for replicas to be assigned");
        ClusterHealthResponse clusterHealth = client().admin().cluster()
                .health(clusterHealthRequest().waitForGreenStatus().waitForNodes("2")).actionGet();
        logger.info("Done Cluster Health, status {}", clusterHealth.getStatus());
        assertThat(clusterHealth.isTimedOut(), equalTo(false));
        assertThat(clusterHealth.getStatus(), equalTo(ClusterHealthStatus.GREEN));

        logger.info("Closing server1");
        // kill the first server
        internalCluster().stopCurrentMasterNode();

        assertBusy(() -> {
            assertEquals(false, client().admin().indices().prepareExists("test").get().isExists());
        });
    }

    private String getLocalNodeId(String name) {
        TransportService transportService = internalCluster().getInstance(TransportService.class, name);
        String nodeId = transportService.getLocalNode().getId();
        assertThat(nodeId, not(nullValue()));
        return nodeId;
    }
}
