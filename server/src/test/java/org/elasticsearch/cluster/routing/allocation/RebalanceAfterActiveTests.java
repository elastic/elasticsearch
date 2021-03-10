/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.routing.allocation;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterInfo;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ESAllocationTestCase;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.RoutingNodes;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.allocation.decider.ClusterRebalanceAllocationDecider;
import org.elasticsearch.common.settings.Settings;

import static org.elasticsearch.cluster.routing.ShardRoutingState.INITIALIZING;
import static org.elasticsearch.cluster.routing.ShardRoutingState.RELOCATING;
import static org.elasticsearch.cluster.routing.ShardRoutingState.STARTED;
import static org.elasticsearch.cluster.routing.ShardRoutingState.UNASSIGNED;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

public class RebalanceAfterActiveTests extends ESAllocationTestCase {
    private final Logger logger = LogManager.getLogger(RebalanceAfterActiveTests.class);

    public void testRebalanceOnlyAfterAllShardsAreActive() {
        final long[] sizes = new long[5];
        for (int i =0; i < sizes.length; i++) {
            sizes[i] = randomIntBetween(0, Integer.MAX_VALUE);
        }

        AllocationService strategy = createAllocationService(Settings.builder()
                        .put("cluster.routing.allocation.node_concurrent_recoveries", 10)
                        .put(ClusterRebalanceAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ALLOW_REBALANCE_SETTING.getKey(), "always")
                        .put("cluster.routing.allocation.cluster_concurrent_rebalance", -1)
                        .build(),
            () -> new ClusterInfo() {
                @Override
                public Long getShardSize(ShardRouting shardRouting) {
                    if (shardRouting.getIndexName().equals("test")) {
                        return sizes[shardRouting.getId()];
                    }
                    return null;
                }
            });
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
            assertEquals(clusterState.routingTable().index("test").shard(i).replicaShards().get(0).getExpectedShardSize(), sizes[i]);
        }

        logger.info("now, start 8 more nodes, and check that no rebalancing/relocation have happened");
        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder(clusterState.nodes())
                .add(newNode("node3")).add(newNode("node4")).add(newNode("node5")).add(newNode("node6"))
                .add(newNode("node7")).add(newNode("node8")).add(newNode("node9")).add(newNode("node10")))
                .build();
        clusterState = strategy.reroute(clusterState, "reroute");

        for (int i = 0; i < clusterState.routingTable().index("test").shards().size(); i++) {
            assertThat(clusterState.routingTable().index("test").shard(i).shards().size(), equalTo(2));
            assertThat(clusterState.routingTable().index("test").shard(i).primaryShard().state(), equalTo(STARTED));
            assertThat(clusterState.routingTable().index("test").shard(i).replicaShards().get(0).state(), equalTo(INITIALIZING));
            assertEquals(clusterState.routingTable().index("test").shard(i).replicaShards().get(0).getExpectedShardSize(), sizes[i]);

        }

        logger.info("start the replica shards, rebalancing should start");
        clusterState = startInitializingShardsAndReroute(strategy, clusterState);

        // we only allow one relocation at a time
        assertThat(clusterState.routingTable().shardsWithState(STARTED).size(), equalTo(5));
        assertThat(clusterState.routingTable().shardsWithState(RELOCATING).size(), equalTo(5));
        for (int i = 0; i < clusterState.routingTable().index("test").shards().size(); i++) {
            int num = 0;
            for (ShardRouting routing : clusterState.routingTable().index("test").shard(i).shards()) {
                if (routing.state() == RELOCATING || routing.state() == INITIALIZING) {
                    assertEquals(routing.getExpectedShardSize(), sizes[i]);
                    num++;
                }
            }
            assertTrue(num > 0);
        }

        logger.info("complete relocation, other half of relocation should happen");
        clusterState = startInitializingShardsAndReroute(strategy, clusterState);

        // we now only relocate 3, since 2 remain where they are!
        assertThat(clusterState.routingTable().shardsWithState(STARTED).size(), equalTo(7));
        assertThat(clusterState.routingTable().shardsWithState(RELOCATING).size(), equalTo(3));
        for (int i = 0; i < clusterState.routingTable().index("test").shards().size(); i++) {
            for (ShardRouting routing : clusterState.routingTable().index("test").shard(i).shards()) {
                if (routing.state() == RELOCATING || routing.state() == INITIALIZING) {
                    assertEquals(routing.getExpectedShardSize(), sizes[i]);
                }
            }
        }


        logger.info("complete relocation, that's it!");
        clusterState = startInitializingShardsAndReroute(strategy, clusterState);
        RoutingNodes routingNodes = clusterState.getRoutingNodes();

        assertThat(clusterState.routingTable().shardsWithState(STARTED).size(), equalTo(10));
        // make sure we have an even relocation
        for (RoutingNode routingNode : routingNodes) {
            assertThat(routingNode.size(), equalTo(1));
        }
    }
}
