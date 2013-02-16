/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package org.elasticsearch.test.integration.indexlifecycle;

import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthStatus;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.node.internal.InternalNode;
import org.elasticsearch.test.integration.AbstractNodesTests;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

import static org.elasticsearch.client.Requests.*;
import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_NUMBER_OF_REPLICAS;
import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_NUMBER_OF_SHARDS;
import static org.elasticsearch.cluster.routing.ShardRoutingState.STARTED;
import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.equalTo;

/**
 *
 */
public class IndexLifecycleActionTests extends AbstractNodesTests {

    private final ESLogger logger = Loggers.getLogger(IndexLifecycleActionTests.class);

    @AfterMethod
    public void closeNodes() {
        closeAllNodes();
    }

    @Test
    public void testIndexLifecycleActionsWith11Shards1Backup() throws Exception {
        Settings settings = settingsBuilder()
                .put(SETTING_NUMBER_OF_SHARDS, 11)
                .put(SETTING_NUMBER_OF_REPLICAS, 1)
                .put("cluster.routing.schedule", "20ms") // reroute every 20ms so we identify new nodes fast
                .build();

        // start one server
        logger.info("Starting sever1");
        startNode("server1", settings);

        ClusterService clusterService1 = ((InternalNode) node("server1")).injector().getInstance(ClusterService.class);

        logger.info("Creating index [test]");
        CreateIndexResponse createIndexResponse = client("server1").admin().indices().create(createIndexRequest("test")).actionGet();
        assertThat(createIndexResponse.isAcknowledged(), equalTo(true));

        logger.info("Running Cluster Health");
        ClusterHealthResponse clusterHealth = client("server1").admin().cluster().prepareHealth().setWaitForYellowStatus().execute().actionGet();
        logger.info("Done Cluster Health, status " + clusterHealth.getStatus());
        assertThat(clusterHealth.isTimedOut(), equalTo(false));
        assertThat(clusterHealth.getStatus(), equalTo(ClusterHealthStatus.YELLOW));

        // sleep till the cluster state gets published, since we check the master
        Thread.sleep(200);


        ClusterState clusterState1 = clusterService1.state();
        RoutingNode routingNodeEntry1 = clusterState1.readOnlyRoutingNodes().nodesToShards().get(clusterState1.nodes().localNodeId());
        assertThat(routingNodeEntry1.numberOfShardsWithState(STARTED), equalTo(11));

        clusterState1 = client("server1").admin().cluster().state(clusterStateRequest()).actionGet().getState();
        routingNodeEntry1 = clusterState1.readOnlyRoutingNodes().nodesToShards().get(clusterState1.nodes().localNodeId());
        assertThat(routingNodeEntry1.numberOfShardsWithState(STARTED), equalTo(11));

        logger.info("Starting server2");
        // start another server
        startNode("server2", settings);
        ClusterService clusterService2 = ((InternalNode) node("server2")).injector().getInstance(ClusterService.class);

        logger.info("Running Cluster Health");
        clusterHealth = client("server1").admin().cluster().health(clusterHealthRequest().setWaitForGreenStatus().setWaitForNodes("2")).actionGet();
        logger.info("Done Cluster Health, status " + clusterHealth.getStatus());
        assertThat(clusterHealth.isTimedOut(), equalTo(false));
        assertThat(clusterHealth.getStatus(), equalTo(ClusterHealthStatus.GREEN));

        // sleep till the cluster state gets published, since we check the master
        Thread.sleep(200);

        clusterState1 = clusterService1.state();
        routingNodeEntry1 = clusterState1.readOnlyRoutingNodes().nodesToShards().get(clusterState1.nodes().localNodeId());
        assertThat(routingNodeEntry1.numberOfShardsWithState(STARTED), equalTo(11));

        ClusterState clusterState2 = clusterService2.state();
        RoutingNode routingNodeEntry2 = clusterState2.readOnlyRoutingNodes().nodesToShards().get(clusterState2.nodes().localNodeId());
        assertThat(routingNodeEntry2.numberOfShardsWithState(STARTED), equalTo(11));

        logger.info("Starting server3");
        // start another server
        startNode("server3", settings);
        Thread.sleep(200);

        ClusterService clusterService3 = ((InternalNode) node("server3")).injector().getInstance(ClusterService.class);

        logger.info("Running Cluster Health");
        clusterHealth = client("server1").admin().cluster().health(clusterHealthRequest().setWaitForGreenStatus().setWaitForNodes("3").setWaitForRelocatingShards(0)).actionGet();
        logger.info("Done Cluster Health, status " + clusterHealth.getStatus());
        assertThat(clusterHealth.isTimedOut(), equalTo(false));
        assertThat(clusterHealth.getStatus(), equalTo(ClusterHealthStatus.GREEN));
        assertThat(clusterHealth.getRelocatingShards(), equalTo(0));
        assertThat(clusterHealth.getActiveShards(), equalTo(22));
        assertThat(clusterHealth.getActivePrimaryShards(), equalTo(11));

        // sleep till the cluster state gets published, since we check the master
        Thread.sleep(200);

        clusterState1 = clusterService1.state();
        routingNodeEntry1 = clusterState1.readOnlyRoutingNodes().nodesToShards().get(clusterState1.nodes().localNodeId());
        assertThat(routingNodeEntry1.numberOfShardsWithState(STARTED), anyOf(equalTo(7), equalTo(8)));

        clusterState2 = clusterService2.state();
        routingNodeEntry2 = clusterState2.readOnlyRoutingNodes().nodesToShards().get(clusterState2.nodes().localNodeId());
        assertThat(routingNodeEntry2.numberOfShardsWithState(STARTED), anyOf(equalTo(7), equalTo(8)));

        ClusterState clusterState3 = clusterService3.state();
        RoutingNode routingNodeEntry3 = clusterState3.readOnlyRoutingNodes().nodesToShards().get(clusterState3.nodes().localNodeId());
        assertThat(routingNodeEntry3.numberOfShardsWithState(STARTED), equalTo(7));

        assertThat(routingNodeEntry1.numberOfShardsWithState(STARTED) + routingNodeEntry2.numberOfShardsWithState(STARTED) + routingNodeEntry3.numberOfShardsWithState(STARTED), equalTo(22));

        logger.info("Closing server1");
        // kill the first server
        closeNode("server1");
        // verify health
        logger.info("Running Cluster Health");
        clusterHealth = client("server2").admin().cluster().health(clusterHealthRequest().setWaitForGreenStatus().setWaitForRelocatingShards(0).setWaitForNodes("2")).actionGet();
        logger.info("Done Cluster Health, status " + clusterHealth.getStatus());
        assertThat(clusterHealth.isTimedOut(), equalTo(false));
        assertThat(clusterHealth.getStatus(), equalTo(ClusterHealthStatus.GREEN));
        assertThat(clusterHealth.getRelocatingShards(), equalTo(0));
        assertThat(clusterHealth.getActiveShards(), equalTo(22));
        assertThat(clusterHealth.getActivePrimaryShards(), equalTo(11));

        // sleep till the cluster state gets published, since we check the master
        Thread.sleep(200);

        clusterState2 = clusterService2.state();
        routingNodeEntry2 = clusterState2.readOnlyRoutingNodes().nodesToShards().get(clusterState2.nodes().localNodeId());
        assertThat(routingNodeEntry2.numberOfShardsWithState(STARTED), equalTo(11));

        clusterState3 = clusterService3.state();
        routingNodeEntry3 = clusterState3.readOnlyRoutingNodes().nodesToShards().get(clusterState3.nodes().localNodeId());
        assertThat(routingNodeEntry3.numberOfShardsWithState(STARTED), equalTo(11));

        assertThat(routingNodeEntry2.numberOfShardsWithState(STARTED) + routingNodeEntry3.numberOfShardsWithState(STARTED), equalTo(22));

        logger.info("Deleting index [test]");
        // last, lets delete the index
        DeleteIndexResponse deleteIndexResponse = client("server2").admin().indices().prepareDelete("test").execute().actionGet();
        assertThat(deleteIndexResponse.isAcknowledged(), equalTo(true));

        Thread.sleep(500); // wait till the cluster state gets published

        clusterState2 = clusterService2.state();
        routingNodeEntry2 = clusterState2.readOnlyRoutingNodes().nodesToShards().get(clusterState2.nodes().localNodeId());
        assertThat(routingNodeEntry2.shards().isEmpty(), equalTo(true));

        clusterState3 = clusterService3.state();
        routingNodeEntry3 = clusterState3.readOnlyRoutingNodes().nodesToShards().get(clusterState3.nodes().localNodeId());
        assertThat(routingNodeEntry3.shards().isEmpty(), equalTo(true));
    }

    @Test
    public void testIndexLifecycleActionsWith11Shards0Backup() throws Exception {

        Settings settings = settingsBuilder()
                .put(SETTING_NUMBER_OF_SHARDS, 11)
                .put(SETTING_NUMBER_OF_REPLICAS, 0)
                .put("cluster.routing.schedule", "20ms") // reroute every 20ms so we identify new nodes fast
                .build();

        // start one server
        logger.info("Starting server1");
        startNode("server1", settings);

        ClusterService clusterService1 = ((InternalNode) node("server1")).injector().getInstance(ClusterService.class);

        logger.info("Creating index [test]");
        CreateIndexResponse createIndexResponse = client("server1").admin().indices().create(createIndexRequest("test")).actionGet();
        assertThat(createIndexResponse.isAcknowledged(), equalTo(true));

        logger.info("Running Cluster Health");
        ClusterHealthResponse clusterHealth = client("server1").admin().cluster().health(clusterHealthRequest().setWaitForGreenStatus()).actionGet();
        logger.info("Done Cluster Health, status " + clusterHealth.getStatus());
        assertThat(clusterHealth.isTimedOut(), equalTo(false));
        assertThat(clusterHealth.getStatus(), equalTo(ClusterHealthStatus.GREEN));
        assertThat(clusterHealth.getRelocatingShards(), equalTo(0));
        assertThat(clusterHealth.getActiveShards(), equalTo(11));
        assertThat(clusterHealth.getActivePrimaryShards(), equalTo(11));

        ClusterState clusterState1 = clusterService1.state();
        RoutingNode routingNodeEntry1 = clusterState1.readOnlyRoutingNodes().nodesToShards().get(clusterState1.nodes().localNodeId());
        assertThat(routingNodeEntry1.numberOfShardsWithState(STARTED), equalTo(11));

        // start another server
        logger.info("Starting server2");
        startNode("server2", settings);
        Thread.sleep(200);

        logger.info("Running Cluster Health");
        clusterHealth = client("server1").admin().cluster().health(clusterHealthRequest().setWaitForGreenStatus().setWaitForRelocatingShards(0).setWaitForNodes("2")).actionGet();
        logger.info("Done Cluster Health, status " + clusterHealth.getStatus());
        assertThat(clusterHealth.isTimedOut(), equalTo(false));
        assertThat(clusterHealth.getStatus(), equalTo(ClusterHealthStatus.GREEN));
        assertThat(clusterHealth.getRelocatingShards(), equalTo(0));
        assertThat(clusterHealth.getActiveShards(), equalTo(11));
        assertThat(clusterHealth.getActivePrimaryShards(), equalTo(11));

        // sleep till the cluster state gets published, since we check the master
        Thread.sleep(200);

        ClusterService clusterService2 = ((InternalNode) node("server2")).injector().getInstance(ClusterService.class);

        clusterState1 = clusterService1.state();
        routingNodeEntry1 = clusterState1.readOnlyRoutingNodes().nodesToShards().get(clusterState1.nodes().localNodeId());
        assertThat(routingNodeEntry1.numberOfShardsWithState(STARTED), anyOf(equalTo(6), equalTo(5)));

        ClusterState clusterState2 = clusterService2.state();
        RoutingNode routingNodeEntry2 = clusterState2.readOnlyRoutingNodes().nodesToShards().get(clusterState2.nodes().localNodeId());
        assertThat(routingNodeEntry2.numberOfShardsWithState(STARTED), anyOf(equalTo(5), equalTo(6)));

        // start another server
        logger.info("Starting server3");
        startNode("server3");
        Thread.sleep(200);

        ClusterService clusterService3 = ((InternalNode) node("server3")).injector().getInstance(ClusterService.class);

        logger.info("Running Cluster Health");
        clusterHealth = client("server1").admin().cluster().health(clusterHealthRequest().setWaitForGreenStatus().setWaitForRelocatingShards(0).setWaitForNodes("3")).actionGet();
        logger.info("Done Cluster Health, status " + clusterHealth.getStatus());
        assertThat(clusterHealth.isTimedOut(), equalTo(false));
        assertThat(clusterHealth.getStatus(), equalTo(ClusterHealthStatus.GREEN));
        assertThat(clusterHealth.getRelocatingShards(), equalTo(0));
        assertThat(clusterHealth.getActiveShards(), equalTo(11));
        assertThat(clusterHealth.getActivePrimaryShards(), equalTo(11));

        // sleep till the cluster state gets published, since we check the master
        Thread.sleep(200);

        clusterState1 = clusterService1.state();
        routingNodeEntry1 = clusterState1.readOnlyRoutingNodes().nodesToShards().get(clusterState1.nodes().localNodeId());
        assertThat(routingNodeEntry1.numberOfShardsWithState(STARTED), anyOf(equalTo(5), equalTo(4), equalTo(3)));

        clusterState2 = clusterService2.state();
        routingNodeEntry2 = clusterState2.readOnlyRoutingNodes().nodesToShards().get(clusterState2.nodes().localNodeId());
        assertThat(routingNodeEntry2.numberOfShardsWithState(STARTED), anyOf(equalTo(5), equalTo(4), equalTo(3)));

        ClusterState clusterState3 = clusterService3.state();
        RoutingNode routingNodeEntry3 = clusterState3.readOnlyRoutingNodes().nodesToShards().get(clusterState3.nodes().localNodeId());
        assertThat(routingNodeEntry3.numberOfShardsWithState(STARTED), equalTo(3));

        assertThat(routingNodeEntry1.numberOfShardsWithState(STARTED) + routingNodeEntry2.numberOfShardsWithState(STARTED) + routingNodeEntry3.numberOfShardsWithState(STARTED), equalTo(11));

        logger.info("Closing server1");
        // kill the first server
        closeNode("server1");

        logger.info("Running Cluster Health");
        clusterHealth = client("server3").admin().cluster().health(clusterHealthRequest().setWaitForGreenStatus().setWaitForNodes("2").setWaitForRelocatingShards(0)).actionGet();
        logger.info("Done Cluster Health, status " + clusterHealth.getStatus());
        assertThat(clusterHealth.isTimedOut(), equalTo(false));
        assertThat(clusterHealth.getStatus(), equalTo(ClusterHealthStatus.GREEN));
        assertThat(clusterHealth.getRelocatingShards(), equalTo(0));
        assertThat(clusterHealth.getActiveShards(), equalTo(11));
        assertThat(clusterHealth.getActivePrimaryShards(), equalTo(11));

        // sleep till the cluster state gets published, since we check the master
        Thread.sleep(200);

        clusterState2 = clusterService2.state();
        routingNodeEntry2 = clusterState2.readOnlyRoutingNodes().nodesToShards().get(clusterState2.nodes().localNodeId());
        assertThat(routingNodeEntry2.numberOfShardsWithState(STARTED), anyOf(equalTo(5), equalTo(6)));

        clusterState3 = clusterService3.state();
        routingNodeEntry3 = clusterState3.readOnlyRoutingNodes().nodesToShards().get(clusterState3.nodes().localNodeId());
        assertThat(routingNodeEntry3.numberOfShardsWithState(STARTED), anyOf(equalTo(5), equalTo(6)));

        assertThat(routingNodeEntry2.numberOfShardsWithState(STARTED) + routingNodeEntry3.numberOfShardsWithState(STARTED), equalTo(11));

        logger.info("Deleting index [test]");
        // last, lets delete the index
        DeleteIndexResponse deleteIndexResponse = client("server2").admin().indices().delete(deleteIndexRequest("test")).actionGet();
        assertThat(deleteIndexResponse.isAcknowledged(), equalTo(true));

        Thread.sleep(500); // wait till the cluster state gets published

        clusterState2 = clusterService2.state();
        routingNodeEntry2 = clusterState2.readOnlyRoutingNodes().nodesToShards().get(clusterState2.nodes().localNodeId());
        assertThat(routingNodeEntry2.shards().isEmpty(), equalTo(true));

        clusterState3 = clusterService3.state();
        routingNodeEntry3 = clusterState3.readOnlyRoutingNodes().nodesToShards().get(clusterState3.nodes().localNodeId());
        assertThat(routingNodeEntry3.shards().isEmpty(), equalTo(true));
    }

    @Test
    public void testTwoIndicesCreation() throws Exception {

        Settings settings = settingsBuilder()
                .put(SETTING_NUMBER_OF_SHARDS, 11)
                .put(SETTING_NUMBER_OF_REPLICAS, 0)
                .put("cluster.routing.schedule", "20ms") // reroute every 20ms so we identify new nodes fast
                .build();

        // start one server
        startNode("server1", settings);
        client("server1").admin().indices().create(createIndexRequest("test1")).actionGet();
        client("server1").admin().indices().create(createIndexRequest("test2")).actionGet();
    }
}
