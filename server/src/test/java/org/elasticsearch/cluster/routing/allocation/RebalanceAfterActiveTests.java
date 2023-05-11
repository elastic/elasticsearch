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
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.ClusterInfo;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ESAllocationTestCase;
import org.elasticsearch.cluster.TestShardRoutingRoleStrategies;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.RoutingNodes;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.allocation.decider.ClusterRebalanceAllocationDecider;
import org.elasticsearch.common.settings.Settings;

import static org.elasticsearch.cluster.routing.RoutingNodesHelper.shardsWithState;
import static org.elasticsearch.cluster.routing.ShardRoutingState.INITIALIZING;
import static org.elasticsearch.cluster.routing.ShardRoutingState.RELOCATING;
import static org.elasticsearch.cluster.routing.ShardRoutingState.STARTED;
import static org.elasticsearch.cluster.routing.ShardRoutingState.UNASSIGNED;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.nullValue;

public class RebalanceAfterActiveTests extends ESAllocationTestCase {
    private final Logger logger = LogManager.getLogger(RebalanceAfterActiveTests.class);

    public void testRebalanceOnlyAfterAllShardsAreActive() {
        final long[] sizes = new long[5];
        for (int i = 0; i < sizes.length; i++) {
            sizes[i] = randomIntBetween(0, Integer.MAX_VALUE);
        }

        AllocationService strategy = createAllocationService(
            Settings.builder()
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
            }
        );
        logger.info("Building initial routing table");

        var indexMetadata = IndexMetadata.builder("test").settings(indexSettings(Version.CURRENT, 5, 1)).build();

        ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT)
            .metadata(Metadata.builder().put(indexMetadata, false))
            .routingTable(RoutingTable.builder(TestShardRoutingRoleStrategies.DEFAULT_ROLE_ONLY).addAsNew(indexMetadata))
            .build();

        assertThat(clusterState.routingTable().index("test").size(), equalTo(5));
        for (int i = 0; i < clusterState.routingTable().index("test").size(); i++) {
            assertThat(clusterState.routingTable().index("test").shard(i).size(), equalTo(2));
            assertThat(clusterState.routingTable().index("test").shard(i).shard(0).state(), equalTo(UNASSIGNED));
            assertThat(clusterState.routingTable().index("test").shard(i).shard(1).state(), equalTo(UNASSIGNED));
            assertThat(clusterState.routingTable().index("test").shard(i).shard(0).currentNodeId(), nullValue());
            assertThat(clusterState.routingTable().index("test").shard(i).shard(1).currentNodeId(), nullValue());
        }

        logger.info("start two nodes and fully start the shards");
        clusterState = ClusterState.builder(clusterState)
            .nodes(DiscoveryNodes.builder().add(newNode("node1")).add(newNode("node2")))
            .build();
        clusterState = strategy.reroute(clusterState, "reroute", ActionListener.noop());

        for (int i = 0; i < clusterState.routingTable().index("test").size(); i++) {
            assertThat(clusterState.routingTable().index("test").shard(i).size(), equalTo(2));
            assertThat(clusterState.routingTable().index("test").shard(i).primaryShard().state(), equalTo(INITIALIZING));
            assertThat(clusterState.routingTable().index("test").shard(i).primaryShard().getExpectedShardSize(), equalTo(sizes[i]));
            assertThat(clusterState.routingTable().index("test").shard(i).replicaShards().get(0).state(), equalTo(UNASSIGNED));
        }

        logger.info("start all the primary shards, replicas will start initializing");
        clusterState = startInitializingShardsAndReroute(strategy, clusterState);

        for (int i = 0; i < clusterState.routingTable().index("test").size(); i++) {
            assertThat(clusterState.routingTable().index("test").shard(i).size(), equalTo(2));
            assertThat(clusterState.routingTable().index("test").shard(i).primaryShard().state(), equalTo(STARTED));
            assertThat(clusterState.routingTable().index("test").shard(i).replicaShards().get(0).state(), equalTo(INITIALIZING));
            assertThat(clusterState.routingTable().index("test").shard(i).replicaShards().get(0).getExpectedShardSize(), equalTo(sizes[i]));
        }

        logger.info("now, start 8 more nodes, and check that no rebalancing/relocation have happened");
        clusterState = ClusterState.builder(clusterState)
            .nodes(
                DiscoveryNodes.builder(clusterState.nodes())
                    .add(newNode("node3"))
                    .add(newNode("node4"))
                    .add(newNode("node5"))
                    .add(newNode("node6"))
                    .add(newNode("node7"))
                    .add(newNode("node8"))
                    .add(newNode("node9"))
                    .add(newNode("node10"))
            )
            .build();
        clusterState = strategy.reroute(clusterState, "reroute", ActionListener.noop());

        for (int i = 0; i < clusterState.routingTable().index("test").size(); i++) {
            assertThat(clusterState.routingTable().index("test").shard(i).size(), equalTo(2));
            assertThat(clusterState.routingTable().index("test").shard(i).primaryShard().state(), equalTo(STARTED));
            assertThat(clusterState.routingTable().index("test").shard(i).replicaShards().get(0).state(), equalTo(INITIALIZING));
            assertThat(clusterState.routingTable().index("test").shard(i).replicaShards().get(0).getExpectedShardSize(), equalTo(sizes[i]));
        }

        logger.info("start the replica shards, rebalancing should start");
        clusterState = startInitializingShardsAndReroute(strategy, clusterState);

        assertThat(shardsWithState(clusterState.getRoutingNodes(), STARTED), hasSize(2));
        assertThat(shardsWithState(clusterState.getRoutingNodes(), RELOCATING), hasSize(8));

        logger.info("complete all relocations");
        clusterState = applyStartedShardsUntilNoChange(clusterState, strategy);

        logger.info("complete relocation, that's it!");
        clusterState = startInitializingShardsAndReroute(strategy, clusterState);
        RoutingNodes routingNodes = clusterState.getRoutingNodes();

        assertThat(shardsWithState(clusterState.getRoutingNodes(), STARTED), hasSize(10));
        // make sure we have an even relocation
        for (RoutingNode routingNode : routingNodes) {
            assertThat(routingNode.size(), equalTo(1));
        }
    }
}
