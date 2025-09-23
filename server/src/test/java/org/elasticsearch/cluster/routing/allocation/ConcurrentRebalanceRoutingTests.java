/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.routing.allocation;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ESAllocationTestCase;
import org.elasticsearch.cluster.TestShardRoutingRoleStrategies;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.routing.allocation.DataTier;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexVersion;

import static org.elasticsearch.cluster.routing.allocation.DataTier.DATA_FROZEN;
import java.util.Collections;

import static org.elasticsearch.cluster.routing.RoutingNodesHelper.shardsWithState;
import static org.elasticsearch.cluster.routing.ShardRoutingState.INITIALIZING;
import static org.elasticsearch.cluster.routing.ShardRoutingState.RELOCATING;
import static org.elasticsearch.cluster.routing.ShardRoutingState.STARTED;
import static org.elasticsearch.cluster.routing.ShardRoutingState.UNASSIGNED;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

public class ConcurrentRebalanceRoutingTests extends ESAllocationTestCase {

    public void testClusterConcurrentRebalance() {
        boolean testFrozen = randomBoolean();
        AllocationService strategy;
        if (testFrozen) {
            strategy = createAllocationService(
                Settings.builder()
                    .put("cluster.routing.allocation.node_concurrent_recoveries", 10)
                    .put("cluster.routing.allocation.cluster_concurrent_frozen_rebalance", 3)
                    .build()
            );
        } else {
            strategy = createAllocationService(
                Settings.builder()
                    .put("cluster.routing.allocation.node_concurrent_recoveries", 10)
                    .put("cluster.routing.allocation.cluster_concurrent_rebalance", 3)
                    .build()
                );
        }

        logger.info("Building initial routing table");

        Metadata metadata;
        if (testFrozen) {
            metadata = Metadata.builder()
                .put(IndexMetadata.builder("test").settings(settings(IndexVersion.current()).put(DataTier.TIER_PREFERENCE, DataTier.DATA_FROZEN)).numberOfShards(5).numberOfReplicas(1))
                .build();
        } else {
            metadata = Metadata.builder()
                .put(IndexMetadata.builder("test").settings(settings(IndexVersion.current())).numberOfShards(5).numberOfReplicas(1))
                .build();
        }

        RoutingTable initialRoutingTable = RoutingTable.builder(TestShardRoutingRoleStrategies.DEFAULT_ROLE_ONLY)
            .addAsNew(metadata.getProject().index("test"))
            .build();

        ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT).metadata(metadata).routingTable(initialRoutingTable).build();

        assertThat(clusterState.routingTable().index("test").size(), equalTo(5));
        for (int i = 0; i < clusterState.routingTable().index("test").size(); i++) {
            assertThat(clusterState.routingTable().index("test").shard(i).size(), equalTo(2));
            assertThat(clusterState.routingTable().index("test").shard(i).shard(0).state(), equalTo(UNASSIGNED));
            assertThat(clusterState.routingTable().index("test").shard(i).shard(1).state(), equalTo(UNASSIGNED));
            assertThat(clusterState.routingTable().index("test").shard(i).shard(0).currentNodeId(), nullValue());
            assertThat(clusterState.routingTable().index("test").shard(i).shard(1).currentNodeId(), nullValue());
        }

        logger.info("start two nodes and fully start the shards");
        if (testFrozen) {
            clusterState = ClusterState.builder(clusterState)
                .nodes(DiscoveryNodes.builder().add(newNode("node1", Collections.singleton(DiscoveryNodeRole.DATA_FROZEN_NODE_ROLE))).add(newNode("node2", Collections.singleton(DiscoveryNodeRole.DATA_FROZEN_NODE_ROLE))))
                .build();
        } else {
            clusterState = ClusterState.builder(clusterState)
                .nodes(DiscoveryNodes.builder().add(newNode("node1")).add(newNode("node2")))
                .build();
        }
        clusterState = strategy.reroute(clusterState, "reroute", ActionListener.noop());

        for (int i = 0; i < clusterState.routingTable().index("test").size(); i++) {
            assertThat(clusterState.routingTable().index("test").shard(i).size(), equalTo(2));
            assertThat(clusterState.routingTable().index("test").shard(i).primaryShard().state(), equalTo(INITIALIZING));
            assertThat(clusterState.routingTable().index("test").shard(i).replicaShards().get(0).state(), equalTo(UNASSIGNED));
        }

        logger.info("start all the primary shards, replicas will start initializing");
        clusterState = startInitializingShardsAndReroute(strategy, clusterState);

        for (int i = 0; i < clusterState.routingTable().index("test").size(); i++) {
            assertThat(clusterState.routingTable().index("test").shard(i).size(), equalTo(2));
            assertThat(clusterState.routingTable().index("test").shard(i).primaryShard().state(), equalTo(STARTED));
            assertThat(clusterState.routingTable().index("test").shard(i).replicaShards().get(0).state(), equalTo(INITIALIZING));
        }

        logger.info("now, start 8 more nodes, and check that no rebalancing/relocation have happened");
        if (testFrozen) {
            clusterState = ClusterState.builder(clusterState)
                .nodes(
                    DiscoveryNodes.builder(clusterState.nodes())
                        .add(newNode("node3", Collections.singleton(DiscoveryNodeRole.DATA_FROZEN_NODE_ROLE)))
                        .add(newNode("node4", Collections.singleton(DiscoveryNodeRole.DATA_FROZEN_NODE_ROLE)))
                        .add(newNode("node5", Collections.singleton(DiscoveryNodeRole.DATA_FROZEN_NODE_ROLE)))
                        .add(newNode("node6", Collections.singleton(DiscoveryNodeRole.DATA_FROZEN_NODE_ROLE)))
                        .add(newNode("node7", Collections.singleton(DiscoveryNodeRole.DATA_FROZEN_NODE_ROLE)))
                        .add(newNode("node8", Collections.singleton(DiscoveryNodeRole.DATA_FROZEN_NODE_ROLE)))
                        .add(newNode("node9", Collections.singleton(DiscoveryNodeRole.DATA_FROZEN_NODE_ROLE)))
                        .add(newNode("node10", Collections.singleton(DiscoveryNodeRole.DATA_FROZEN_NODE_ROLE)))
                )
                .build();
        } else {
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
        }
        clusterState = strategy.reroute(clusterState, "reroute", ActionListener.noop());

        for (int i = 0; i < clusterState.routingTable().index("test").size(); i++) {
            assertThat(clusterState.routingTable().index("test").shard(i).size(), equalTo(2));
            assertThat(clusterState.routingTable().index("test").shard(i).primaryShard().state(), equalTo(STARTED));
            assertThat(clusterState.routingTable().index("test").shard(i).replicaShards().get(0).state(), equalTo(INITIALIZING));
        }

        logger.info("start the replica shards, rebalancing should start, but, only 3 should be rebalancing");
        clusterState = startInitializingShardsAndReroute(strategy, clusterState);

        // we only allow one relocation at a time
        assertThat(shardsWithState(clusterState.getRoutingNodes(), STARTED).size(), equalTo(7));
        assertThat(shardsWithState(clusterState.getRoutingNodes(), RELOCATING).size(), equalTo(3));

        logger.info("finalize this session relocation, 3 more should relocate now");
        clusterState = startInitializingShardsAndReroute(strategy, clusterState);

        // we only allow one relocation at a time
        assertThat(shardsWithState(clusterState.getRoutingNodes(), STARTED).size(), equalTo(7));
        assertThat(shardsWithState(clusterState.getRoutingNodes(), RELOCATING).size(), equalTo(3));

        logger.info("finalize this session relocation, 2 more should relocate now");
        clusterState = startInitializingShardsAndReroute(strategy, clusterState);

        // we only allow one relocation at a time
        assertThat(shardsWithState(clusterState.getRoutingNodes(), STARTED).size(), equalTo(8));
        assertThat(shardsWithState(clusterState.getRoutingNodes(), RELOCATING).size(), equalTo(2));

        logger.info("finalize this session relocation, no more relocation");
        clusterState = startInitializingShardsAndReroute(strategy, clusterState);

        // we only allow one relocation at a time
        assertThat(shardsWithState(clusterState.getRoutingNodes(), STARTED).size(), equalTo(10));
        assertThat(shardsWithState(clusterState.getRoutingNodes(), RELOCATING).size(), equalTo(0));
    }
}
