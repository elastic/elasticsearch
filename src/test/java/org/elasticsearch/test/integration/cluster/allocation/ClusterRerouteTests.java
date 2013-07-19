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

package org.elasticsearch.test.integration.cluster.allocation;

import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthStatus;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.allocation.command.AllocateAllocationCommand;
import org.elasticsearch.cluster.routing.allocation.command.MoveAllocationCommand;
import org.elasticsearch.cluster.routing.allocation.decider.DisableAllocationDecider;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.io.FileSystemUtils;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.gateway.Gateway;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.node.internal.InternalNode;
import org.elasticsearch.test.integration.AbstractNodesTests;
import org.junit.After;
import org.junit.Test;

import java.io.File;

import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

/**
 */
public class ClusterRerouteTests extends AbstractNodesTests {

    private final ESLogger logger = Loggers.getLogger(ClusterRerouteTests.class);

    @After
    public void cleanAndCloseNodes() throws Exception {
        for (int i = 0; i < 10; i++) {
            if (node("node" + i) != null) {
                node("node" + i).stop();
                // since we store (by default) the index snapshot under the gateway, resetting it will reset the index data as well
                ((InternalNode) node("node" + i)).injector().getInstance(Gateway.class).reset();
            }
        }
        closeAllNodes();
    }

    @Test
    public void rerouteWithCommands() throws Exception {
        Settings commonSettings = settingsBuilder()
                .put(DisableAllocationDecider.CLUSTER_ROUTING_ALLOCATION_DISABLE_NEW_ALLOCATION, true)
                .put(DisableAllocationDecider.CLUSTER_ROUTING_ALLOCATION_DISABLE_ALLOCATION, true)
                .build();

        startNode("node1", commonSettings);
        startNode("node2", commonSettings);

        logger.info("--> create an index with 1 shard, 1 replica, nothing should allocate");
        client("node1").admin().indices().prepareCreate("test")
                .setSettings(settingsBuilder().put("index.number_of_shards", 1))
                .execute().actionGet();

        ClusterState state = client("node1").admin().cluster().prepareState().execute().actionGet().getState();
        assertThat(state.routingNodes().unassigned().size(), equalTo(2));

        logger.info("--> explicitly allocate shard 1, *under dry_run*");
        state = client("node1").admin().cluster().prepareReroute()
                .add(new AllocateAllocationCommand(new ShardId("test", 0), "node1", true))
                .setDryRun(true)
                .execute().actionGet().getState();
        assertThat(state.routingNodes().unassigned().size(), equalTo(1));
        assertThat(state.routingNodes().node(state.nodes().resolveNode("node1").id()).shards().get(0).state(), equalTo(ShardRoutingState.INITIALIZING));

        logger.info("--> get the state, verify nothing changed because of the dry run");
        state = client("node1").admin().cluster().prepareState().execute().actionGet().getState();
        assertThat(state.routingNodes().unassigned().size(), equalTo(2));

        logger.info("--> explicitly allocate shard 1, actually allocating, no dry run");
        state = client("node1").admin().cluster().prepareReroute()
                .add(new AllocateAllocationCommand(new ShardId("test", 0), "node1", true))
                .execute().actionGet().getState();
        assertThat(state.routingNodes().unassigned().size(), equalTo(1));
        assertThat(state.routingNodes().node(state.nodes().resolveNode("node1").id()).shards().get(0).state(), equalTo(ShardRoutingState.INITIALIZING));

        ClusterHealthResponse healthResponse = client("node1").admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setWaitForYellowStatus().execute().actionGet();
        assertThat(healthResponse.isTimedOut(), equalTo(false));

        logger.info("--> get the state, verify shard 1 primary allocated");
        state = client("node1").admin().cluster().prepareState().execute().actionGet().getState();
        assertThat(state.routingNodes().unassigned().size(), equalTo(1));
        assertThat(state.routingNodes().node(state.nodes().resolveNode("node1").id()).shards().get(0).state(), equalTo(ShardRoutingState.STARTED));

        logger.info("--> move shard 1 primary from node1 to node2");
        state = client("node1").admin().cluster().prepareReroute()
                .add(new MoveAllocationCommand(new ShardId("test", 0), "node1", "node2"))
                .execute().actionGet().getState();

        assertThat(state.routingNodes().node(state.nodes().resolveNode("node1").id()).shards().get(0).state(), equalTo(ShardRoutingState.RELOCATING));
        assertThat(state.routingNodes().node(state.nodes().resolveNode("node2").id()).shards().get(0).state(), equalTo(ShardRoutingState.INITIALIZING));


        healthResponse = client("node1").admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setWaitForYellowStatus().setWaitForRelocatingShards(0).execute().actionGet();
        assertThat(healthResponse.isTimedOut(), equalTo(false));

        logger.info("--> get the state, verify shard 1 primary moved from node1 to node2");
        state = client("node1").admin().cluster().prepareState().execute().actionGet().getState();
        assertThat(state.routingNodes().unassigned().size(), equalTo(1));
        assertThat(state.routingNodes().node(state.nodes().resolveNode("node2").id()).shards().get(0).state(), equalTo(ShardRoutingState.STARTED));
    }

    @Test
    public void rerouteWithAllocateLocalGateway() throws Exception {
        Settings commonSettings = settingsBuilder()
                .put(DisableAllocationDecider.CLUSTER_ROUTING_ALLOCATION_DISABLE_NEW_ALLOCATION, true)
                .put(DisableAllocationDecider.CLUSTER_ROUTING_ALLOCATION_DISABLE_ALLOCATION, true)
                .put("gateway.type", "local")
                .build();

        // clean three nodes
        logger.info("--> cleaning nodes");
        buildNode("node1", settingsBuilder().put("gateway.type", "local").build());
        buildNode("node2", settingsBuilder().put("gateway.type", "local").build());
        cleanAndCloseNodes();

        logger.info("--> starting 2 nodes");
        startNode("node1", commonSettings);
        startNode("node2", commonSettings);

        logger.info("--> create an index with 1 shard, 1 replica, nothing should allocate");
        client("node1").admin().indices().prepareCreate("test")
                .setSettings(settingsBuilder().put("index.number_of_shards", 1))
                .execute().actionGet();

        ClusterState state = client("node1").admin().cluster().prepareState().execute().actionGet().getState();
        assertThat(state.routingNodes().unassigned().size(), equalTo(2));

        logger.info("--> explicitly allocate shard 1, actually allocating, no dry run");
        state = client("node1").admin().cluster().prepareReroute()
                .add(new AllocateAllocationCommand(new ShardId("test", 0), "node1", true))
                .execute().actionGet().getState();
        assertThat(state.routingNodes().unassigned().size(), equalTo(1));
        assertThat(state.routingNodes().node(state.nodes().resolveNode("node1").id()).shards().get(0).state(), equalTo(ShardRoutingState.INITIALIZING));

        ClusterHealthResponse healthResponse = client("node1").admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setWaitForYellowStatus().execute().actionGet();
        assertThat(healthResponse.isTimedOut(), equalTo(false));

        logger.info("--> get the state, verify shard 1 primary allocated");
        state = client("node1").admin().cluster().prepareState().execute().actionGet().getState();
        assertThat(state.routingNodes().unassigned().size(), equalTo(1));
        assertThat(state.routingNodes().node(state.nodes().resolveNode("node1").id()).shards().get(0).state(), equalTo(ShardRoutingState.STARTED));

        client("node1").prepareIndex("test", "type", "1").setSource("field", "value").setRefresh(true).execute().actionGet();

        logger.info("--> closing all nodes");
        File shardLocation = ((InternalNode) node("node1")).injector().getInstance(NodeEnvironment.class).shardLocations(new ShardId("test", 0))[0];
        closeAllNodes();

        logger.info("--> deleting the shard data");
        FileSystemUtils.deleteRecursively(shardLocation);

        logger.info("--> starting nodes back, will not allocate the shard since it has no data, but the index will be there");
        startNode("node1", commonSettings);
        startNode("node2", commonSettings);
        // wait a bit for the cluster to realize that the shard is not there...
        // TODO can we get around this? the cluster is RED, so what do we wait for?
        Thread.sleep(300);
        assertThat(client("node1").admin().cluster().prepareHealth().execute().actionGet().getStatus(), equalTo(ClusterHealthStatus.RED));
        logger.info("--> explicitly allocate primary");
        state = client("node1").admin().cluster().prepareReroute()
                .add(new AllocateAllocationCommand(new ShardId("test", 0), "node1", true))
                .execute().actionGet().getState();
        assertThat(state.routingNodes().unassigned().size(), equalTo(1));
        assertThat(state.routingNodes().node(state.nodes().resolveNode("node1").id()).shards().get(0).state(), equalTo(ShardRoutingState.INITIALIZING));

        healthResponse = client("node1").admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setWaitForYellowStatus().execute().actionGet();
        assertThat(healthResponse.isTimedOut(), equalTo(false));

        logger.info("--> get the state, verify shard 1 primary allocated");
        state = client("node1").admin().cluster().prepareState().execute().actionGet().getState();
        assertThat(state.routingNodes().unassigned().size(), equalTo(1));
        assertThat(state.routingNodes().node(state.nodes().resolveNode("node1").id()).shards().get(0).state(), equalTo(ShardRoutingState.STARTED));

    }

}