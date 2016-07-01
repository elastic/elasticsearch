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

package org.elasticsearch.cluster.routing.allocation;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.RoutingNodes;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESAllocationTestCase;

import static org.elasticsearch.cluster.routing.ShardRoutingState.INITIALIZING;
import static org.elasticsearch.cluster.routing.ShardRoutingState.RELOCATING;
import static org.elasticsearch.cluster.routing.ShardRoutingState.STARTED;
import static org.elasticsearch.cluster.routing.ShardRoutingState.UNASSIGNED;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

public class ConcurrentRebalanceRoutingTests extends ESAllocationTestCase {
    private final ESLogger logger = Loggers.getLogger(ConcurrentRebalanceRoutingTests.class);

    public void testClusterConcurrentRebalance() {
        AllocationService strategy = createAllocationService(Settings.builder()
                .put("cluster.routing.allocation.node_concurrent_recoveries", 10)
                .put("cluster.routing.allocation.cluster_concurrent_rebalance", 3)
                .build());

        logger.info("Building initial routing table");

        MetaData metaData = MetaData.builder()
                .put(IndexMetaData.builder("test").settings(settings(Version.CURRENT)).numberOfShards(5).numberOfReplicas(1))
                .build();

        RoutingTable routingTable = RoutingTable.builder()
                .addAsNew(metaData.index("test"))
                .build();

        ClusterState clusterState = ClusterState.builder(org.elasticsearch.cluster.ClusterName.CLUSTER_NAME_SETTING.getDefault(Settings.EMPTY)).metaData(metaData).routingTable(routingTable).build();

        assertThat(routingTable.index("test").shards().size(), equalTo(5));
        for (int i = 0; i < routingTable.index("test").shards().size(); i++) {
            assertThat(routingTable.index("test").shard(i).shards().size(), equalTo(2));
            assertThat(routingTable.index("test").shard(i).shards().get(0).state(), equalTo(UNASSIGNED));
            assertThat(routingTable.index("test").shard(i).shards().get(1).state(), equalTo(UNASSIGNED));
            assertThat(routingTable.index("test").shard(i).shards().get(0).currentNodeId(), nullValue());
            assertThat(routingTable.index("test").shard(i).shards().get(1).currentNodeId(), nullValue());
        }

        logger.info("start two nodes and fully start the shards");
        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder().put(newNode("node1")).put(newNode("node2"))).build();
        routingTable = strategy.reroute(clusterState, "reroute").routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();

        for (int i = 0; i < routingTable.index("test").shards().size(); i++) {
            assertThat(routingTable.index("test").shard(i).shards().size(), equalTo(2));
            assertThat(routingTable.index("test").shard(i).primaryShard().state(), equalTo(INITIALIZING));
            assertThat(routingTable.index("test").shard(i).replicaShards().get(0).state(), equalTo(UNASSIGNED));
        }

        logger.info("start all the primary shards, replicas will start initializing");
        RoutingNodes routingNodes = clusterState.getRoutingNodes();
        routingTable = strategy.applyStartedShards(clusterState, routingNodes.shardsWithState(INITIALIZING)).routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();
        routingNodes = clusterState.getRoutingNodes();

        for (int i = 0; i < routingTable.index("test").shards().size(); i++) {
            assertThat(routingTable.index("test").shard(i).shards().size(), equalTo(2));
            assertThat(routingTable.index("test").shard(i).primaryShard().state(), equalTo(STARTED));
            assertThat(routingTable.index("test").shard(i).replicaShards().get(0).state(), equalTo(INITIALIZING));
        }

        logger.info("now, start 8 more nodes, and check that no rebalancing/relocation have happened");
        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder(clusterState.nodes())
                .put(newNode("node3")).put(newNode("node4")).put(newNode("node5")).put(newNode("node6")).put(newNode("node7")).put(newNode("node8")).put(newNode("node9")).put(newNode("node10")))
                .build();
        routingTable = strategy.reroute(clusterState, "reroute").routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();
        routingNodes = clusterState.getRoutingNodes();

        for (int i = 0; i < routingTable.index("test").shards().size(); i++) {
            assertThat(routingTable.index("test").shard(i).shards().size(), equalTo(2));
            assertThat(routingTable.index("test").shard(i).primaryShard().state(), equalTo(STARTED));
            assertThat(routingTable.index("test").shard(i).replicaShards().get(0).state(), equalTo(INITIALIZING));
        }

        logger.info("start the replica shards, rebalancing should start, but, only 3 should be rebalancing");
        routingNodes = clusterState.getRoutingNodes();
        routingTable = strategy.applyStartedShards(clusterState, routingNodes.shardsWithState(INITIALIZING)).routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();
        routingNodes = clusterState.getRoutingNodes();

        // we only allow one relocation at a time
        assertThat(routingTable.shardsWithState(STARTED).size(), equalTo(7));
        assertThat(routingTable.shardsWithState(RELOCATING).size(), equalTo(3));

        logger.info("finalize this session relocation, 3 more should relocate now");
        routingNodes = clusterState.getRoutingNodes();
        routingTable = strategy.applyStartedShards(clusterState, routingNodes.shardsWithState(INITIALIZING)).routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();
        routingNodes = clusterState.getRoutingNodes();

        // we only allow one relocation at a time
        assertThat(routingTable.shardsWithState(STARTED).size(), equalTo(7));
        assertThat(routingTable.shardsWithState(RELOCATING).size(), equalTo(3));

        logger.info("finalize this session relocation, 2 more should relocate now");
        routingNodes = clusterState.getRoutingNodes();
        routingTable = strategy.applyStartedShards(clusterState, routingNodes.shardsWithState(INITIALIZING)).routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();
        routingNodes = clusterState.getRoutingNodes();

        // we only allow one relocation at a time
        assertThat(routingTable.shardsWithState(STARTED).size(), equalTo(8));
        assertThat(routingTable.shardsWithState(RELOCATING).size(), equalTo(2));

        logger.info("finalize this session relocation, no more relocation");
        routingNodes = clusterState.getRoutingNodes();
        routingTable = strategy.applyStartedShards(clusterState, routingNodes.shardsWithState(INITIALIZING)).routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();
        routingNodes = clusterState.getRoutingNodes();

        // we only allow one relocation at a time
        assertThat(routingTable.shardsWithState(STARTED).size(), equalTo(10));
        assertThat(routingTable.shardsWithState(RELOCATING).size(), equalTo(0));
    }
}
