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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ESAllocationTestCase;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.common.settings.Settings;

import static org.elasticsearch.cluster.routing.ShardRoutingState.INITIALIZING;
import static org.elasticsearch.cluster.routing.ShardRoutingState.RELOCATING;
import static org.elasticsearch.cluster.routing.ShardRoutingState.STARTED;
import static org.elasticsearch.cluster.routing.ShardRoutingState.UNASSIGNED;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

public class ConcurrentRebalanceRoutingTests extends ESAllocationTestCase {
    private final Logger logger = LogManager.getLogger(ConcurrentRebalanceRoutingTests.class);

    public void testClusterConcurrentRebalance() {
        AllocationService strategy = createAllocationService(Settings.builder()
                .put("cluster.routing.allocation.node_concurrent_recoveries", 10)
                .put("cluster.routing.allocation.cluster_concurrent_rebalance", 3)
                .build());

        logger.info("Building initial routing table");

        Metadata metadata = Metadata.builder()
                .put(IndexMetadata.builder("test").settings(settings(Version.CURRENT)).numberOfShards(5).numberOfReplicas(1))
                .build();

        RoutingTable initialRoutingTable = RoutingTable.builder()
                .addAsNew(metadata.index("test"))
                .build();

        ClusterState clusterState = ClusterState.builder(org.elasticsearch.cluster.ClusterName.CLUSTER_NAME_SETTING
            .getDefault(Settings.EMPTY)).metadata(metadata).routingTable(initialRoutingTable).build();

        assertThat(clusterState.routingTable().index("test").shards().size(), equalTo(5));
        for (int i = 0; i < clusterState.routingTable().index("test").shards().size(); i++) {
            assertThat(clusterState.routingTable().index("test").shard(i).shards().size(), equalTo(2));
            assertThat(clusterState.routingTable().index("test").shard(i).shards().get(0).state(), equalTo(UNASSIGNED));
            assertThat(clusterState.routingTable().index("test").shard(i).shards().get(1).state(), equalTo(UNASSIGNED));
            assertThat(clusterState.routingTable().index("test").shard(i).shards().get(0).currentNodeId(), nullValue());
            assertThat(clusterState.routingTable().index("test").shard(i).shards().get(1).currentNodeId(), nullValue());
        }

        logger.info("start two nodes and fully start the shards");
        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder()
            .add(newNode("node1")).add(newNode("node2"))).build();
        clusterState = strategy.reroute(clusterState, "reroute");

        for (int i = 0; i < clusterState.routingTable().index("test").shards().size(); i++) {
            assertThat(clusterState.routingTable().index("test").shard(i).shards().size(), equalTo(2));
            assertThat(clusterState.routingTable().index("test").shard(i).primaryShard().state(), equalTo(INITIALIZING));
            assertThat(clusterState.routingTable().index("test").shard(i).replicaShards().get(0).state(), equalTo(UNASSIGNED));
        }

        logger.info("start all the primary shards, replicas will start initializing");
        clusterState = startInitializingShardsAndReroute(strategy, clusterState);

        for (int i = 0; i < clusterState.routingTable().index("test").shards().size(); i++) {
            assertThat(clusterState.routingTable().index("test").shard(i).shards().size(), equalTo(2));
            assertThat(clusterState.routingTable().index("test").shard(i).primaryShard().state(), equalTo(STARTED));
            assertThat(clusterState.routingTable().index("test").shard(i).replicaShards().get(0).state(), equalTo(INITIALIZING));
        }

        logger.info("now, start 8 more nodes, and check that no rebalancing/relocation have happened");
        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder(clusterState.nodes())
                .add(newNode("node3")).add(newNode("node4")).add(newNode("node5"))
            .add(newNode("node6")).add(newNode("node7")).add(newNode("node8"))
            .add(newNode("node9")).add(newNode("node10")))
                .build();
        clusterState = strategy.reroute(clusterState, "reroute");

        for (int i = 0; i < clusterState.routingTable().index("test").shards().size(); i++) {
            assertThat(clusterState.routingTable().index("test").shard(i).shards().size(), equalTo(2));
            assertThat(clusterState.routingTable().index("test").shard(i).primaryShard().state(), equalTo(STARTED));
            assertThat(clusterState.routingTable().index("test").shard(i).replicaShards().get(0).state(), equalTo(INITIALIZING));
        }

        logger.info("start the replica shards, rebalancing should start, but, only 3 should be rebalancing");
        clusterState = startInitializingShardsAndReroute(strategy, clusterState);

        // we only allow one relocation at a time
        assertThat(clusterState.routingTable().shardsWithState(STARTED).size(), equalTo(7));
        assertThat(clusterState.routingTable().shardsWithState(RELOCATING).size(), equalTo(3));

        logger.info("finalize this session relocation, 3 more should relocate now");
        clusterState = startInitializingShardsAndReroute(strategy, clusterState);

        // we only allow one relocation at a time
        assertThat(clusterState.routingTable().shardsWithState(STARTED).size(), equalTo(7));
        assertThat(clusterState.routingTable().shardsWithState(RELOCATING).size(), equalTo(3));

        logger.info("finalize this session relocation, 2 more should relocate now");
        clusterState = startInitializingShardsAndReroute(strategy, clusterState);

        // we only allow one relocation at a time
        assertThat(clusterState.routingTable().shardsWithState(STARTED).size(), equalTo(8));
        assertThat(clusterState.routingTable().shardsWithState(RELOCATING).size(), equalTo(2));

        logger.info("finalize this session relocation, no more relocation");
        clusterState = startInitializingShardsAndReroute(strategy, clusterState);

        // we only allow one relocation at a time
        assertThat(clusterState.routingTable().shardsWithState(STARTED).size(), equalTo(10));
        assertThat(clusterState.routingTable().shardsWithState(RELOCATING).size(), equalTo(0));
    }
}
