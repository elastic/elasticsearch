/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.indexlifecycle;

import com.carrotsearch.randomizedtesting.annotations.Nightly;
import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import org.apache.lucene.util.LuceneTestCase.Slow;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthStatus;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.RoutingNodes;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.discovery.Discovery;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.elasticsearch.test.ElasticsearchIntegrationTest.ClusterScope;
import org.elasticsearch.test.InternalTestCluster;
import org.junit.Test;

import java.util.Set;

import static org.elasticsearch.client.Requests.*;
import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_NUMBER_OF_REPLICAS;
import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_NUMBER_OF_SHARDS;
import static org.elasticsearch.cluster.routing.ShardRoutingState.*;
import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;
import static org.elasticsearch.test.ElasticsearchIntegrationTest.*;
import static org.hamcrest.Matchers.*;


/**
 *
 */
@ClusterScope(scope = Scope.TEST, numDataNodes = 0)
public class IndexLifecycleActionTests extends ElasticsearchIntegrationTest {

    @Slow
    @Test
    public void testIndexLifecycleActions() throws Exception {
        if (randomBoolean()) { // both run with @Nightly
            testIndexLifecycleActionsWith11Shards0Backup();
        } else {
            testIndexLifecycleActionsWith11Shards1Backup();
        }
    }

    @Slow
    @Nightly
    @Test
    public void testIndexLifecycleActionsWith11Shards1Backup() throws Exception {
        Settings settings = settingsBuilder()
                .put(SETTING_NUMBER_OF_SHARDS, 11)
                .put(SETTING_NUMBER_OF_REPLICAS, 1)
                .put("cluster.routing.schedule", "20ms") // reroute every 20ms so we identify new nodes fast
                .build();

        // start one server
        logger.info("Starting sever1");
        final String server_1 = internalCluster().startNode(settings);
        final String node1 = getLocalNodeId(server_1);

        logger.info("Creating index [test]");
        CreateIndexResponse createIndexResponse = client().admin().indices().create(createIndexRequest("test")).actionGet();
        assertThat(createIndexResponse.isAcknowledged(), equalTo(true));

        logger.info("Running Cluster Health");
        ClusterHealthResponse clusterHealth = client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setWaitForYellowStatus().execute().actionGet();
        logger.info("Done Cluster Health, status " + clusterHealth.getStatus());
        assertThat(clusterHealth.isTimedOut(), equalTo(false));
        assertThat(clusterHealth.getStatus(), equalTo(ClusterHealthStatus.YELLOW));

        ClusterState clusterState = client().admin().cluster().prepareState().get().getState();
        RoutingNode routingNodeEntry1 = clusterState.readOnlyRoutingNodes().node(node1);
        assertThat(routingNodeEntry1.numberOfShardsWithState(STARTED), equalTo(11));

        logger.info("Starting server2");
        // start another server
        String server_2 = internalCluster().startNode(settings);

        // first wait for 2 nodes in the cluster
        logger.info("Running Cluster Health");
        clusterHealth = client().admin().cluster().health(clusterHealthRequest().waitForGreenStatus().waitForNodes("2")).actionGet();
        logger.info("Done Cluster Health, status " + clusterHealth.getStatus());
        assertThat(clusterHealth.isTimedOut(), equalTo(false));
        assertThat(clusterHealth.getStatus(), equalTo(ClusterHealthStatus.GREEN));
        setMinimumMasterNodes(2);
        final String node2 = getLocalNodeId(server_2);

        // explicitly call reroute, so shards will get relocated to the new node (we delay it in ES in case other nodes join)
        client().admin().cluster().prepareReroute().execute().actionGet();

        clusterHealth = client().admin().cluster().health(clusterHealthRequest().waitForGreenStatus().waitForNodes("2").waitForRelocatingShards(0)).actionGet();
        assertThat(clusterHealth.isTimedOut(), equalTo(false));
        assertThat(clusterHealth.getStatus(), equalTo(ClusterHealthStatus.GREEN));
        assertThat(clusterHealth.getNumberOfDataNodes(), equalTo(2));
        assertThat(clusterHealth.getInitializingShards(), equalTo(0));
        assertThat(clusterHealth.getUnassignedShards(), equalTo(0));
        assertThat(clusterHealth.getRelocatingShards(), equalTo(0));
        assertThat(clusterHealth.getActiveShards(), equalTo(22));
        assertThat(clusterHealth.getActivePrimaryShards(), equalTo(11));


        clusterState = client().admin().cluster().prepareState().get().getState();
        assertNodesPresent(clusterState.readOnlyRoutingNodes(), node1, node2);
        routingNodeEntry1 = clusterState.readOnlyRoutingNodes().node(node1);
        assertThat(routingNodeEntry1.numberOfShardsWithState(RELOCATING), equalTo(0));
        assertThat(routingNodeEntry1.numberOfShardsWithState(STARTED), equalTo(11));
        RoutingNode routingNodeEntry2 = clusterState.readOnlyRoutingNodes().node(node2);
        assertThat(routingNodeEntry2.numberOfShardsWithState(INITIALIZING), equalTo(0));
        assertThat(routingNodeEntry2.numberOfShardsWithState(STARTED), equalTo(11));

        logger.info("Starting server3");
        // start another server
        String server_3 = internalCluster().startNode(settings);

        // first wait for 3 nodes in the cluster
        clusterHealth = client().admin().cluster().health(clusterHealthRequest().waitForGreenStatus().waitForNodes("3")).actionGet();
        assertThat(clusterHealth.isTimedOut(), equalTo(false));
        assertThat(clusterHealth.getStatus(), equalTo(ClusterHealthStatus.GREEN));


        final String node3 = getLocalNodeId(server_3);


        // explicitly call reroute, so shards will get relocated to the new node (we delay it in ES in case other nodes join)
        client().admin().cluster().prepareReroute().execute().actionGet();

        clusterHealth = client().admin().cluster().health(clusterHealthRequest().waitForGreenStatus().waitForNodes("3").waitForRelocatingShards(0)).actionGet();
        assertThat(clusterHealth.isTimedOut(), equalTo(false));
        assertThat(clusterHealth.getStatus(), equalTo(ClusterHealthStatus.GREEN));
        assertThat(clusterHealth.getNumberOfDataNodes(), equalTo(3));
        assertThat(clusterHealth.getInitializingShards(), equalTo(0));
        assertThat(clusterHealth.getUnassignedShards(), equalTo(0));
        assertThat(clusterHealth.getRelocatingShards(), equalTo(0));
        assertThat(clusterHealth.getActiveShards(), equalTo(22));
        assertThat(clusterHealth.getActivePrimaryShards(), equalTo(11));


        clusterState = client().admin().cluster().prepareState().get().getState();
        assertNodesPresent(clusterState.readOnlyRoutingNodes(), node1, node2, node3);

        routingNodeEntry1 = clusterState.readOnlyRoutingNodes().node(node1);
        routingNodeEntry2 = clusterState.readOnlyRoutingNodes().node(node2);
        RoutingNode routingNodeEntry3 = clusterState.readOnlyRoutingNodes().node(node3);

        assertThat(routingNodeEntry1.numberOfShardsWithState(STARTED) + routingNodeEntry2.numberOfShardsWithState(STARTED) + routingNodeEntry3.numberOfShardsWithState(STARTED), equalTo(22));

        assertThat(routingNodeEntry1.numberOfShardsWithState(RELOCATING), equalTo(0));
        assertThat(routingNodeEntry1.numberOfShardsWithState(STARTED), anyOf(equalTo(7), equalTo(8)));

        assertThat(routingNodeEntry2.numberOfShardsWithState(RELOCATING), equalTo(0));
        assertThat(routingNodeEntry2.numberOfShardsWithState(STARTED), anyOf(equalTo(7), equalTo(8)));

        assertThat(routingNodeEntry3.numberOfShardsWithState(INITIALIZING), equalTo(0));
        assertThat(routingNodeEntry3.numberOfShardsWithState(STARTED), equalTo(7));

        logger.info("Closing server1");
        // kill the first server
        internalCluster().stopRandomNode(InternalTestCluster.nameFilter(server_1));
        // verify health
        logger.info("Running Cluster Health");
        clusterHealth = client().admin().cluster().health(clusterHealthRequest().waitForGreenStatus().waitForNodes("2")).actionGet();
        logger.info("Done Cluster Health, status " + clusterHealth.getStatus());
        assertThat(clusterHealth.isTimedOut(), equalTo(false));
        assertThat(clusterHealth.getStatus(), equalTo(ClusterHealthStatus.GREEN));

        client().admin().cluster().prepareReroute().get();

        clusterHealth = client().admin().cluster().health(clusterHealthRequest().waitForGreenStatus().waitForRelocatingShards(0).waitForNodes("2")).actionGet();
        assertThat(clusterHealth.isTimedOut(), equalTo(false));
        assertThat(clusterHealth.getStatus(), equalTo(ClusterHealthStatus.GREEN));
        assertThat(clusterHealth.getRelocatingShards(), equalTo(0));
        assertThat(clusterHealth.getActiveShards(), equalTo(22));
        assertThat(clusterHealth.getActivePrimaryShards(), equalTo(11));

        clusterState = client().admin().cluster().prepareState().get().getState();
        assertNodesPresent(clusterState.readOnlyRoutingNodes(), node3, node2);
        routingNodeEntry2 = clusterState.readOnlyRoutingNodes().node(node2);
        routingNodeEntry3 = clusterState.readOnlyRoutingNodes().node(node3);

        assertThat(routingNodeEntry2.numberOfShardsWithState(STARTED) + routingNodeEntry3.numberOfShardsWithState(STARTED), equalTo(22));

        assertThat(routingNodeEntry2.numberOfShardsWithState(RELOCATING), equalTo(0));
        assertThat(routingNodeEntry2.numberOfShardsWithState(STARTED), equalTo(11));

        assertThat(routingNodeEntry3.numberOfShardsWithState(RELOCATING), equalTo(0));
        assertThat(routingNodeEntry3.numberOfShardsWithState(STARTED), equalTo(11));


        logger.info("Deleting index [test]");
        // last, lets delete the index
        DeleteIndexResponse deleteIndexResponse = client().admin().indices().prepareDelete("test").execute().actionGet();
        assertThat(deleteIndexResponse.isAcknowledged(), equalTo(true));

        clusterState = client().admin().cluster().prepareState().get().getState();
        assertNodesPresent(clusterState.readOnlyRoutingNodes(), node3, node2);
        routingNodeEntry2 = clusterState.readOnlyRoutingNodes().node(node2);
        assertThat(routingNodeEntry2.isEmpty(), equalTo(true));

        routingNodeEntry3 = clusterState.readOnlyRoutingNodes().node(node3);
        assertThat(routingNodeEntry3.isEmpty(), equalTo(true));
    }

    private String getLocalNodeId(String name) {
        Discovery discovery = internalCluster().getInstance(Discovery.class, name);
        String nodeId = discovery.localNode().getId();
        assertThat(nodeId, not(nullValue()));
        return nodeId;
    }

    @Slow
    @Nightly
    @Test
    public void testIndexLifecycleActionsWith11Shards0Backup() throws Exception {

        Settings settings = settingsBuilder()
                .put(SETTING_NUMBER_OF_SHARDS, 11)
                .put(SETTING_NUMBER_OF_REPLICAS, 0)
                .put("cluster.routing.schedule", "20ms") // reroute every 20ms so we identify new nodes fast
                .build();

        // start one server
        logger.info("Starting server1");
        final String server_1 = internalCluster().startNode(settings);

        final String node1 = getLocalNodeId(server_1);

        logger.info("Creating index [test]");
        CreateIndexResponse createIndexResponse = client().admin().indices().create(createIndexRequest("test")).actionGet();
        assertThat(createIndexResponse.isAcknowledged(), equalTo(true));

        logger.info("Running Cluster Health");
        ClusterHealthResponse clusterHealth = client().admin().cluster().health(clusterHealthRequest().waitForGreenStatus()).actionGet();
        logger.info("Done Cluster Health, status " + clusterHealth.getStatus());
        assertThat(clusterHealth.isTimedOut(), equalTo(false));
        assertThat(clusterHealth.getStatus(), equalTo(ClusterHealthStatus.GREEN));
        assertThat(clusterHealth.getRelocatingShards(), equalTo(0));
        assertThat(clusterHealth.getActiveShards(), equalTo(11));
        assertThat(clusterHealth.getActivePrimaryShards(), equalTo(11));

        ClusterState clusterState = client().admin().cluster().prepareState().get().getState();
        assertNodesPresent(clusterState.readOnlyRoutingNodes(), node1);
        RoutingNode routingNodeEntry1 = clusterState.readOnlyRoutingNodes().node(node1);
        assertThat(routingNodeEntry1.numberOfShardsWithState(STARTED), equalTo(11));

        // start another server
        logger.info("Starting server2");
        final String server_2 = internalCluster().startNode(settings);

        // first wait for 2 nodes in the cluster
        clusterHealth = client().admin().cluster().health(clusterHealthRequest().waitForGreenStatus().waitForNodes("2")).actionGet();
        assertThat(clusterHealth.isTimedOut(), equalTo(false));
        assertThat(clusterHealth.getStatus(), equalTo(ClusterHealthStatus.GREEN));
        setMinimumMasterNodes(2);
        final String node2 = getLocalNodeId(server_2);

        // explicitly call reroute, so shards will get relocated to the new node (we delay it in ES in case other nodes join)
        client().admin().cluster().prepareReroute().execute().actionGet();

        clusterHealth = client().admin().cluster().health(clusterHealthRequest().waitForGreenStatus().waitForRelocatingShards(0).waitForNodes("2")).actionGet();
        assertThat(clusterHealth.isTimedOut(), equalTo(false));
        assertThat(clusterHealth.getStatus(), equalTo(ClusterHealthStatus.GREEN));
        assertThat(clusterHealth.getNumberOfDataNodes(), equalTo(2));
        assertThat(clusterHealth.getInitializingShards(), equalTo(0));
        assertThat(clusterHealth.getUnassignedShards(), equalTo(0));
        assertThat(clusterHealth.getRelocatingShards(), equalTo(0));
        assertThat(clusterHealth.getActiveShards(), equalTo(11));
        assertThat(clusterHealth.getActivePrimaryShards(), equalTo(11));


        clusterState = client().admin().cluster().prepareState().get().getState();
        assertNodesPresent(clusterState.readOnlyRoutingNodes(), node1, node2);
        routingNodeEntry1 = clusterState.readOnlyRoutingNodes().node(node1);
        assertThat(routingNodeEntry1.numberOfShardsWithState(RELOCATING), equalTo(0));
        assertThat(routingNodeEntry1.numberOfShardsWithState(STARTED), anyOf(equalTo(6), equalTo(5)));
        RoutingNode routingNodeEntry2 = clusterState.readOnlyRoutingNodes().node(node2);
        assertThat(routingNodeEntry2.numberOfShardsWithState(INITIALIZING), equalTo(0));
        assertThat(routingNodeEntry2.numberOfShardsWithState(STARTED), anyOf(equalTo(5), equalTo(6)));

        // start another server
        logger.info("Starting server3");
        final String server_3 = internalCluster().startNode();

        // first wait for 3 nodes in the cluster
        clusterHealth = client().admin().cluster().health(clusterHealthRequest().waitForGreenStatus().waitForNodes("3")).actionGet();
        assertThat(clusterHealth.isTimedOut(), equalTo(false));
        assertThat(clusterHealth.getStatus(), equalTo(ClusterHealthStatus.GREEN));

        final String node3 = getLocalNodeId(server_3);
        // explicitly call reroute, so shards will get relocated to the new node (we delay it in ES in case other nodes join)
        client().admin().cluster().prepareReroute().execute().actionGet();

        clusterHealth = client().admin().cluster().health(clusterHealthRequest().waitForGreenStatus().waitForNodes("3").waitForRelocatingShards(0)).actionGet();
        assertThat(clusterHealth.isTimedOut(), equalTo(false));
        assertThat(clusterHealth.getStatus(), equalTo(ClusterHealthStatus.GREEN));
        assertThat(clusterHealth.getNumberOfDataNodes(), equalTo(3));
        assertThat(clusterHealth.getInitializingShards(), equalTo(0));
        assertThat(clusterHealth.getUnassignedShards(), equalTo(0));
        assertThat(clusterHealth.getRelocatingShards(), equalTo(0));
        assertThat(clusterHealth.getActiveShards(), equalTo(11));
        assertThat(clusterHealth.getActivePrimaryShards(), equalTo(11));


        clusterState = client().admin().cluster().prepareState().get().getState();
        assertNodesPresent(clusterState.readOnlyRoutingNodes(), node1, node2, node3);
        routingNodeEntry1 = clusterState.readOnlyRoutingNodes().node(node1);
        routingNodeEntry2 = clusterState.readOnlyRoutingNodes().node(node2);
        RoutingNode routingNodeEntry3 = clusterState.readOnlyRoutingNodes().node(node3);

        assertThat(routingNodeEntry1.numberOfShardsWithState(STARTED) + routingNodeEntry2.numberOfShardsWithState(STARTED) + routingNodeEntry3.numberOfShardsWithState(STARTED), equalTo(11));

        assertThat(routingNodeEntry1.numberOfShardsWithState(RELOCATING), equalTo(0));
        assertThat(routingNodeEntry1.numberOfShardsWithState(STARTED), anyOf(equalTo(5), equalTo(4), equalTo(3)));

        assertThat(routingNodeEntry2.numberOfShardsWithState(RELOCATING), equalTo(0));
        assertThat(routingNodeEntry2.numberOfShardsWithState(STARTED), anyOf(equalTo(5), equalTo(4), equalTo(3)));

        assertThat(routingNodeEntry3.numberOfShardsWithState(INITIALIZING), equalTo(0));
        assertThat(routingNodeEntry3.numberOfShardsWithState(STARTED), equalTo(3));

        logger.info("Closing server1");
        // kill the first server
        internalCluster().stopRandomNode(InternalTestCluster.nameFilter(server_1));

        logger.info("Running Cluster Health");
        clusterHealth = client().admin().cluster().health(clusterHealthRequest().waitForGreenStatus().waitForNodes("2")).actionGet();
        logger.info("Done Cluster Health, status " + clusterHealth.getStatus());
        assertThat(clusterHealth.isTimedOut(), equalTo(false));
        assertThat(clusterHealth.getStatus(), equalTo(ClusterHealthStatus.GREEN));

        client().admin().cluster().prepareReroute().get();

        logger.info("Running Cluster Health");
        clusterHealth = client().admin().cluster().health(clusterHealthRequest().waitForGreenStatus().waitForNodes("2").waitForRelocatingShards(0)).actionGet();
        logger.info("Done Cluster Health, status " + clusterHealth.getStatus());
        assertThat(clusterHealth.isTimedOut(), equalTo(false));
        assertThat(clusterHealth.getStatus(), equalTo(ClusterHealthStatus.GREEN));
        assertThat(clusterHealth.getRelocatingShards(), equalTo(0));
        assertThat(clusterHealth.getActiveShards(), equalTo(11));
        assertThat(clusterHealth.getActivePrimaryShards(), equalTo(11));

        clusterState = client().admin().cluster().prepareState().get().getState();
        assertNodesPresent(clusterState.readOnlyRoutingNodes(), node3, node2);

        routingNodeEntry2 = clusterState.readOnlyRoutingNodes().node(node2);
        routingNodeEntry3 = clusterState.readOnlyRoutingNodes().node(node3);

        assertThat(routingNodeEntry2.numberOfShardsWithState(STARTED) + routingNodeEntry3.numberOfShardsWithState(STARTED), equalTo(11));

        assertThat(routingNodeEntry2.numberOfShardsWithState(RELOCATING), equalTo(0));
        assertThat(routingNodeEntry2.numberOfShardsWithState(INITIALIZING), equalTo(0));
        assertThat(routingNodeEntry2.numberOfShardsWithState(STARTED), anyOf(equalTo(5), equalTo(6)));

        assertThat(routingNodeEntry3.numberOfShardsWithState(RELOCATING), equalTo(0));
        assertThat(routingNodeEntry3.numberOfShardsWithState(INITIALIZING), equalTo(0));
        assertThat(routingNodeEntry3.numberOfShardsWithState(STARTED), anyOf(equalTo(5), equalTo(6)));

        logger.info("Deleting index [test]");
        // last, lets delete the index
        DeleteIndexResponse deleteIndexResponse = client().admin().indices().delete(deleteIndexRequest("test")).actionGet();
        assertThat(deleteIndexResponse.isAcknowledged(), equalTo(true));

        clusterState = client().admin().cluster().prepareState().get().getState();
        assertNodesPresent(clusterState.readOnlyRoutingNodes(), node3, node2);

        routingNodeEntry2 = clusterState.readOnlyRoutingNodes().node(node2);
        assertThat(routingNodeEntry2.isEmpty(), equalTo(true));

        routingNodeEntry3 = clusterState.readOnlyRoutingNodes().node(node3);
        assertThat(routingNodeEntry3.isEmpty(), equalTo(true));
    }

    private void assertNodesPresent(RoutingNodes routingNodes, String... nodes) {
        final Set<String> keySet = Sets.newHashSet(Iterables.transform(routingNodes, new Function<RoutingNode, String>() {
            @Override
            public String apply(RoutingNode input) {
                return input.nodeId();
            }
        }));
        assertThat(keySet, containsInAnyOrder(nodes));
    }
}
