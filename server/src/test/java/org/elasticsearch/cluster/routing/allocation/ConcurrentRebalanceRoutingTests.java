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
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.RoutingNodes;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexVersion;

import java.util.Collections;
import java.util.Set;
import java.util.function.Supplier;

import static org.elasticsearch.cluster.routing.RoutingNodesHelper.shardsWithState;
import static org.elasticsearch.cluster.routing.ShardRoutingState.INITIALIZING;
import static org.elasticsearch.cluster.routing.ShardRoutingState.RELOCATING;
import static org.elasticsearch.cluster.routing.ShardRoutingState.STARTED;
import static org.elasticsearch.cluster.routing.ShardRoutingState.UNASSIGNED;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

public class ConcurrentRebalanceRoutingTests extends ESAllocationTestCase {

    public void testClusterConcurrentRebalance() {
        AllocationService allocationService = createAllocationService(
            Settings.builder()
                .put("cluster.routing.allocation.node_concurrent_recoveries", 10)
                .put("cluster.routing.allocation.cluster_concurrent_rebalance", 3)
                .put("cluster.routing.allocation.cluster_concurrent_frozen_rebalance", 1)
                .build()
        );

        Metadata metadata = Metadata.builder()
            .put(IndexMetadata.builder("test").settings(settings(IndexVersion.current())).numberOfShards(5).numberOfReplicas(1))
            .build();

        Supplier<DiscoveryNode> nodeFactory = new Supplier<DiscoveryNode>() {
            int count = 1;

            @Override
            public DiscoveryNode get() {
                return ESAllocationTestCase.newNode("node" + count++);
            }
        };

        testClusterConcurrentInternal(allocationService, metadata, nodeFactory);
    }

    public void testClusterConcurrentRebalanceFrozen() {
        AllocationService allocationService = createAllocationService(
            Settings.builder()
                .put("cluster.routing.allocation.node_concurrent_recoveries", 10)
                .put("cluster.routing.allocation.cluster_concurrent_frozen_rebalance", 3)
                .put("cluster.routing.allocation.cluster_concurrent_rebalance", 1)
                .build()
        );

        Metadata metadata = Metadata.builder()
            .put(
                IndexMetadata.builder("test")
                    .settings(settings(IndexVersion.current()).put(DataTier.TIER_PREFERENCE, DataTier.DATA_FROZEN))
                    .numberOfShards(5)
                    .numberOfReplicas(1)
            )
            .build();

        Supplier<DiscoveryNode> nodeFactory = new Supplier<DiscoveryNode>() {
            int count = 1;
            Set<DiscoveryNodeRole> frozenRole = Collections.singleton(DiscoveryNodeRole.DATA_FROZEN_NODE_ROLE);

            @Override
            public DiscoveryNode get() {
                return ESAllocationTestCase.newNode("node" + count++, frozenRole);
            }
        };

        testClusterConcurrentInternal(allocationService, metadata, nodeFactory);
    }

    /**
     * Run a series of concurrent rebalance checks on an index as nodes are created and the index changes state.
     * Index must be named "test"
     */
    void testClusterConcurrentInternal(AllocationService allocationService, Metadata metadata, Supplier<DiscoveryNode> nodeFactory) {
        RoutingTable initialRoutingTable = RoutingTable.builder(TestShardRoutingRoleStrategies.DEFAULT_ROLE_ONLY)
            .addAsNew(metadata.getProject().index("test"))
            .build();

        ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT).metadata(metadata).routingTable(initialRoutingTable).build();

        assertShardsUnassigned(clusterState.routingTable().index("test"));

        logger.info("start two nodes and fully start the shards");
        clusterState = ClusterState.builder(clusterState)
            .nodes(DiscoveryNodes.builder().add(nodeFactory.get()).add(nodeFactory.get()))
            .build();
        clusterState = allocationService.reroute(clusterState, "reroute", ActionListener.noop());

        assertPrimariesInitializing(clusterState.routingTable().index("test"));

        logger.info("start all the primary shards, replicas will start initializing");
        clusterState = startInitializingShardsAndReroute(allocationService, clusterState);

        assertReplicasInitializing(clusterState.routingTable().index("test"));

        logger.info("now, start 8 more nodes, and check that no rebalancing/relocation have happened");
        clusterState = ClusterState.builder(clusterState)
            .nodes(
                DiscoveryNodes.builder(clusterState.nodes())
                    .add(nodeFactory.get())
                    .add(nodeFactory.get())
                    .add(nodeFactory.get())
                    .add(nodeFactory.get())
                    .add(nodeFactory.get())
                    .add(nodeFactory.get())
                    .add(nodeFactory.get())
                    .add(nodeFactory.get())
            )
            .build();

        clusterState = allocationService.reroute(clusterState, "reroute", ActionListener.noop());

        assertReplicasInitializing(clusterState.routingTable().index("test"));

        logger.info("start the replica shards, rebalancing should start, but, only 3 should be rebalancing");
        clusterState = startInitializingShardsAndReroute(allocationService, clusterState);
        RoutingNodes routingNodes = clusterState.getRoutingNodes();

        // we only allow one relocation at a time
        assertThat(shardsWithState(routingNodes, STARTED).size(), equalTo(7));
        assertThat(shardsWithState(routingNodes, RELOCATING).size(), equalTo(3));

        logger.info("finalize this session relocation, 3 more should relocate now");
        clusterState = startInitializingShardsAndReroute(allocationService, clusterState);
        routingNodes = clusterState.getRoutingNodes();

        // we only allow one relocation at a time
        assertThat(shardsWithState(routingNodes, STARTED).size(), equalTo(7));
        assertThat(shardsWithState(routingNodes, RELOCATING).size(), equalTo(3));

        logger.info("finalize this session relocation, 2 more should relocate now");
        clusterState = startInitializingShardsAndReroute(allocationService, clusterState);
        routingNodes = clusterState.getRoutingNodes();

        // we only allow one relocation at a time
        assertThat(shardsWithState(routingNodes, STARTED).size(), equalTo(8));
        assertThat(shardsWithState(routingNodes, RELOCATING).size(), equalTo(2));

        logger.info("finalize this session relocation, no more relocation");
        clusterState = startInitializingShardsAndReroute(allocationService, clusterState);
        routingNodes = clusterState.getRoutingNodes();

        // we only allow one relocation at a time
        assertThat(shardsWithState(routingNodes, STARTED).size(), equalTo(10));
        assertThat(shardsWithState(routingNodes, RELOCATING).size(), equalTo(0));
    }

    public void testClusterConcurrentRebalanceFrozenUnlimited() {
        Set<DiscoveryNodeRole> frozenRole = Collections.singleton(DiscoveryNodeRole.DATA_FROZEN_NODE_ROLE);

        AllocationService allocationService = createAllocationService(
            Settings.builder()
                .put("cluster.routing.allocation.node_concurrent_recoveries", 10)
                .put("cluster.routing.allocation.cluster_concurrent_rebalance", 0)
                .put("cluster.routing.allocation.cluster_concurrent_frozen_rebalance", -1)
                .build()
        );

        logger.info("Building initial routing table");

        Metadata metadata = Metadata.builder()
            .put(
                IndexMetadata.builder("test")
                    .settings(settings(IndexVersion.current()).put(DataTier.TIER_PREFERENCE, DataTier.DATA_FROZEN))
                    .numberOfShards(5)
                    .numberOfReplicas(1)
            )
            .build();

        RoutingTable initialRoutingTable = RoutingTable.builder(TestShardRoutingRoleStrategies.DEFAULT_ROLE_ONLY)
            .addAsNew(metadata.getProject().index("test"))
            .build();

        ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT).metadata(metadata).routingTable(initialRoutingTable).build();

        assertShardsUnassigned(clusterState.routingTable().index("test"));

        logger.info("start two nodes and fully start the shards");
        clusterState = ClusterState.builder(clusterState)
            .nodes(DiscoveryNodes.builder().add(newNode("node1", frozenRole)).add(newNode("node2", frozenRole)))
            .build();
        clusterState = allocationService.reroute(clusterState, "reroute", ActionListener.noop());

        logger.info("start all the primary shards, replicas will start initializing");
        clusterState = startInitializingShardsAndReroute(allocationService, clusterState);

        var clusterStateBuilder = ClusterState.builder(clusterState);

        int nodeCount = randomIntBetween(8, 20);
        var nodeBuilder = DiscoveryNodes.builder(clusterStateBuilder.nodes());

        logger.info("now, start " + nodeCount + " more nodes, and check that no rebalancing/relocation have happened");

        for (int i = 0; i < nodeCount; i++) {
            int nodeId = 3 + i;
            nodeBuilder = nodeBuilder.add(newNode("node" + nodeId, frozenRole));
        }

        clusterState = clusterStateBuilder.nodes(nodeBuilder).build();

        clusterState = allocationService.reroute(clusterState, "reroute", ActionListener.noop());

        logger.info("start the replica shards, rebalancing should start, but with a limit " + nodeCount + " should be rebalancing");
        clusterState = startInitializingShardsAndReroute(allocationService, clusterState);

        // we allow unlimited relocations in the settings -- 8 shards should be moving to spread out evenly
        assertThat(shardsWithState(clusterState.getRoutingNodes(), STARTED).size(), equalTo(2));
        assertThat(shardsWithState(clusterState.getRoutingNodes(), RELOCATING).size(), equalTo(8));
    }

    void assertShardsUnassigned(IndexRoutingTable indexRoutingTable) {
        assertShardStates(indexRoutingTable, UNASSIGNED, UNASSIGNED);

        for (int i = 0; i < indexRoutingTable.size(); i++) {
            IndexShardRoutingTable shardRouting = indexRoutingTable.shard(i);
            assertThat(shardRouting.shard(0).currentNodeId(), nullValue());
            assertThat(shardRouting.shard(1).currentNodeId(), nullValue());
        }
    }

    void assertPrimariesInitializing(IndexRoutingTable indexRoutingTable) {
        assertShardStates(indexRoutingTable, INITIALIZING, UNASSIGNED);
    }

    void assertReplicasInitializing(IndexRoutingTable indexRoutingTable) {
        assertShardStates(indexRoutingTable, STARTED, INITIALIZING);
    }

    void assertShardStates(IndexRoutingTable indexRoutingTable, ShardRoutingState primaryState, ShardRoutingState replicaState) {
        for (int i = 0; i < indexRoutingTable.size(); i++) {
            IndexShardRoutingTable shardRouting = indexRoutingTable.shard(i);
            assertThat(shardRouting.size(), equalTo(2));
            assertThat(shardRouting.primaryShard().state(), equalTo(primaryState));
            assertThat(shardRouting.replicaShards().get(0).state(), equalTo(replicaState));
        }
    }
}
