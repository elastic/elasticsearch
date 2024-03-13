/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.indexlifecycle;

import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequest;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.cluster.reroute.ClusterRerouteResponse;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.RoutingNodes;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.ESIntegTestCase.ClusterScope;
import org.elasticsearch.test.ESIntegTestCase.Scope;
import org.elasticsearch.transport.TransportService;

import java.util.Set;
import java.util.stream.Collectors;

import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_REPLICAS;
import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_SHARDS;
import static org.elasticsearch.cluster.routing.ShardRoutingState.INITIALIZING;
import static org.elasticsearch.cluster.routing.ShardRoutingState.RELOCATING;
import static org.elasticsearch.cluster.routing.ShardRoutingState.STARTED;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;

@ClusterScope(scope = Scope.TEST, numDataNodes = 0)
public class IndexLifecycleActionIT extends ESIntegTestCase {
    public void testIndexLifecycleActionsWith11Shards1Backup() throws Exception {
        CreateIndexResponse createIndexResponse = null;
        ClusterHealthResponse clusterHealth1 = null;
        ClusterHealthResponse clusterHealth2 = null;
        ClusterHealthResponse clusterHealth3 = null;
        ClusterHealthResponse clusterHealth4 = null;
        ClusterHealthResponse clusterHealth5 = null;
        ClusterHealthResponse clusterHealth6 = null;
        ClusterRerouteResponse clusterRerouteResponse1 = null;
        ClusterRerouteResponse clusterRerouteResponse2 = null;
        ClusterRerouteResponse clusterRerouteResponse3 = null;
        AcknowledgedResponse deleteIndexResponse = null;
        try {
            Settings settings = Settings.builder()
                .put(indexSettings())
                .put(SETTING_NUMBER_OF_SHARDS, 11)
                .put(SETTING_NUMBER_OF_REPLICAS, 1)
                .build();

            // start one server
            logger.info("Starting sever1");
            final String server_1 = internalCluster().startNode();
            final String node1 = getLocalNodeId(server_1);

            logger.info("Creating index [test]");
            createIndexResponse = indicesAdmin().create(new CreateIndexRequest("test").settings(settings)).actionGet();
            assertAcked(createIndexResponse);

            ClusterState clusterState = clusterAdmin().prepareState().get().getState();
            RoutingNode routingNodeEntry1 = clusterState.getRoutingNodes().node(node1);
            assertThat(routingNodeEntry1.numberOfShardsWithState(STARTED), equalTo(11));

            logger.info("Starting server2");
            // start another server
            String server_2 = internalCluster().startNode();

            // first wait for 2 nodes in the cluster
            logger.info("Waiting for replicas to be assigned");
            clusterHealth1 = clusterAdmin().prepareHealth()
                .setWaitForGreenStatus()
                .setWaitForNodes("2")
                .setWaitForNoRelocatingShards(true)
                .setWaitForEvents(Priority.LANGUID)
                .get();
            logger.info("Done Cluster Health, status {}", clusterHealth1.getStatus());
            assertThat(clusterHealth1.isTimedOut(), equalTo(false));
            assertThat(clusterHealth1.getStatus(), equalTo(ClusterHealthStatus.GREEN));
            final String node2 = getLocalNodeId(server_2);

            // explicitly call reroute, so shards will get relocated to the new node (we delay it in ES in case other nodes join)
            clusterRerouteResponse1 = clusterAdmin().prepareReroute().get();

            clusterHealth2 = clusterAdmin().health(
                new ClusterHealthRequest(new String[] {}).waitForGreenStatus().waitForNodes("2").waitForNoRelocatingShards(true)
            ).actionGet();
            assertThat(clusterHealth2.isTimedOut(), equalTo(false));
            assertThat(clusterHealth2.getStatus(), equalTo(ClusterHealthStatus.GREEN));
            assertThat(clusterHealth2.getNumberOfDataNodes(), equalTo(2));
            assertThat(clusterHealth2.getInitializingShards(), equalTo(0));
            assertThat(clusterHealth2.getUnassignedShards(), equalTo(0));
            assertThat(clusterHealth2.getRelocatingShards(), equalTo(0));
            assertThat(clusterHealth2.getActiveShards(), equalTo(22));
            assertThat(clusterHealth2.getActivePrimaryShards(), equalTo(11));

            clusterState = clusterAdmin().prepareState().get().getState();
            assertNodesPresent(clusterState.getRoutingNodes(), node1, node2);
            routingNodeEntry1 = clusterState.getRoutingNodes().node(node1);
            assertThat(routingNodeEntry1.numberOfShardsWithState(RELOCATING), equalTo(0));
            assertThat(routingNodeEntry1.numberOfShardsWithState(STARTED), equalTo(11));
            RoutingNode routingNodeEntry2 = clusterState.getRoutingNodes().node(node2);
            assertThat(routingNodeEntry2.numberOfShardsWithState(INITIALIZING), equalTo(0));
            assertThat(routingNodeEntry2.numberOfShardsWithState(STARTED), equalTo(11));

            logger.info("Starting server3");
            // start another server
            String server_3 = internalCluster().startNode();

            // first wait for 3 nodes in the cluster
            logger.info("Waiting for replicas to be assigned");
            clusterHealth3 = clusterAdmin().prepareHealth()
                .setWaitForGreenStatus()
                .setWaitForNodes("3")
                .setWaitForNoRelocatingShards(true)
                .setWaitForEvents(Priority.LANGUID)
                .get();
            assertThat(clusterHealth3.isTimedOut(), equalTo(false));
            assertThat(clusterHealth3.getStatus(), equalTo(ClusterHealthStatus.GREEN));

            final String node3 = getLocalNodeId(server_3);

            // explicitly call reroute, so shards will get relocated to the new node (we delay it in ES in case other nodes join)
            clusterRerouteResponse2 = clusterAdmin().prepareReroute().get();

            clusterHealth4 = clusterAdmin().prepareHealth()
                .setWaitForGreenStatus()
                .setWaitForNodes("3")
                .setWaitForNoRelocatingShards(true)
                .setWaitForEvents(Priority.LANGUID)
                .get();
            assertThat(clusterHealth4.isTimedOut(), equalTo(false));
            assertThat(clusterHealth4.getStatus(), equalTo(ClusterHealthStatus.GREEN));
            assertThat(clusterHealth4.getNumberOfDataNodes(), equalTo(3));
            assertThat(clusterHealth4.getInitializingShards(), equalTo(0));
            assertThat(clusterHealth4.getUnassignedShards(), equalTo(0));
            assertThat(clusterHealth4.getRelocatingShards(), equalTo(0));
            assertThat(clusterHealth4.getActiveShards(), equalTo(22));
            assertThat(clusterHealth4.getActivePrimaryShards(), equalTo(11));

            clusterState = clusterAdmin().prepareState().get().getState();
            assertNodesPresent(clusterState.getRoutingNodes(), node1, node2, node3);

            routingNodeEntry1 = clusterState.getRoutingNodes().node(node1);
            routingNodeEntry2 = clusterState.getRoutingNodes().node(node2);
            RoutingNode routingNodeEntry3 = clusterState.getRoutingNodes().node(node3);

            assertThat(
                routingNodeEntry1.numberOfShardsWithState(STARTED) + routingNodeEntry2.numberOfShardsWithState(STARTED) + routingNodeEntry3
                    .numberOfShardsWithState(STARTED),
                equalTo(22)
            );

            assertThat(routingNodeEntry1.numberOfShardsWithState(RELOCATING), equalTo(0));
            assertThat(routingNodeEntry1.numberOfShardsWithState(STARTED), anyOf(equalTo(7), equalTo(8)));

            assertThat(routingNodeEntry2.numberOfShardsWithState(RELOCATING), equalTo(0));
            assertThat(routingNodeEntry2.numberOfShardsWithState(STARTED), anyOf(equalTo(7), equalTo(8)));

            assertThat(routingNodeEntry3.numberOfShardsWithState(INITIALIZING), equalTo(0));
            assertThat(routingNodeEntry3.numberOfShardsWithState(STARTED), equalTo(7));

            logger.info("Closing server1");
            // kill the first server
            internalCluster().stopNode(server_1);
            // verify health
            logger.info("Running Cluster Health");
            clusterHealth5 = clusterAdmin().prepareHealth()
                .setWaitForGreenStatus()
                .setWaitForNodes("2")
                .setWaitForNoRelocatingShards(true)
                .setWaitForEvents(Priority.LANGUID)
                .get();
            logger.info("Done Cluster Health, status {}", clusterHealth5.getStatus());
            assertThat(clusterHealth5.isTimedOut(), equalTo(false));
            assertThat(clusterHealth5.getStatus(), equalTo(ClusterHealthStatus.GREEN));

            clusterRerouteResponse3 = clusterAdmin().prepareReroute().get();

            clusterHealth6 = clusterAdmin().prepareHealth()
                .setWaitForGreenStatus()
                .setWaitForNodes("2")
                .setWaitForNoRelocatingShards(true)
                .setWaitForEvents(Priority.LANGUID)
                .get();
            assertThat(clusterHealth6.isTimedOut(), equalTo(false));
            assertThat(clusterHealth6.getStatus(), equalTo(ClusterHealthStatus.GREEN));
            assertThat(clusterHealth6.getRelocatingShards(), equalTo(0));
            assertThat(clusterHealth6.getActiveShards(), equalTo(22));
            assertThat(clusterHealth6.getActivePrimaryShards(), equalTo(11));

            clusterState = clusterAdmin().prepareState().get().getState();
            assertNodesPresent(clusterState.getRoutingNodes(), node3, node2);
            routingNodeEntry2 = clusterState.getRoutingNodes().node(node2);
            routingNodeEntry3 = clusterState.getRoutingNodes().node(node3);

            assertThat(
                routingNodeEntry2.numberOfShardsWithState(STARTED) + routingNodeEntry3.numberOfShardsWithState(STARTED),
                equalTo(22)
            );

            assertThat(routingNodeEntry2.numberOfShardsWithState(RELOCATING), equalTo(0));
            assertThat(routingNodeEntry2.numberOfShardsWithState(STARTED), equalTo(11));

            assertThat(routingNodeEntry3.numberOfShardsWithState(RELOCATING), equalTo(0));
            assertThat(routingNodeEntry3.numberOfShardsWithState(STARTED), equalTo(11));

            logger.info("Deleting index [test]");
            // last, lets delete the index
            deleteIndexResponse = indicesAdmin().prepareDelete("test").get();
            assertThat(deleteIndexResponse.isAcknowledged(), equalTo(true));

            clusterState = clusterAdmin().prepareState().get().getState();
            assertNodesPresent(clusterState.getRoutingNodes(), node3, node2);
            routingNodeEntry2 = clusterState.getRoutingNodes().node(node2);
            assertThat(routingNodeEntry2.isEmpty(), equalTo(true));

            routingNodeEntry3 = clusterState.getRoutingNodes().node(node3);
            assertThat(routingNodeEntry3.isEmpty(), equalTo(true));
        } finally {
            if (createIndexResponse != null) {
                createIndexResponse.decRef();
            }
            if (clusterHealth1 != null) {
                clusterHealth1.decRef();
            }
            if (clusterHealth2 != null) {
                clusterHealth2.decRef();
            }
            if (clusterHealth3 != null) {
                clusterHealth3.decRef();
            }
            if (clusterHealth4 != null) {
                clusterHealth4.decRef();
            }
            if (clusterHealth4 != null) {
                clusterHealth4.decRef();
            }
            if (clusterHealth5 != null) {
                clusterHealth5.decRef();
            }
            if (clusterHealth6 != null) {
                clusterHealth6.decRef();
            }
            if (clusterRerouteResponse1 != null) {
                clusterRerouteResponse1.decRef();
            }
            if (deleteIndexResponse != null) {
                deleteIndexResponse.decRef();
            }
            if (clusterRerouteResponse2 != null) {
                clusterRerouteResponse2.decRef();
            }
            if (clusterRerouteResponse3 != null) {
                clusterRerouteResponse3.decRef();
            }
        }

    }

    private String getLocalNodeId(String name) {
        TransportService transportService = internalCluster().getInstance(TransportService.class, name);
        String nodeId = transportService.getLocalNode().getId();
        assertThat(nodeId, not(nullValue()));
        return nodeId;
    }

    private void assertNodesPresent(RoutingNodes routingNodes, String... nodes) {
        final Set<String> keySet = routingNodes.stream().map(RoutingNode::nodeId).collect(Collectors.toSet());
        assertThat(keySet, containsInAnyOrder(nodes));
    }
}
