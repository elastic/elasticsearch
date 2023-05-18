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
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ESAllocationTestCase;
import org.elasticsearch.cluster.TestShardRoutingRoleStrategies;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.AllocationId;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.RoutingNodes;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.TestShardRouting;
import org.elasticsearch.cluster.routing.allocation.allocator.DesiredBalanceShardsAllocator;
import org.elasticsearch.cluster.routing.allocation.decider.ClusterRebalanceAllocationDecider;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.shard.ShardId;

import java.util.Set;
import java.util.function.IntFunction;

import static org.elasticsearch.cluster.routing.ShardRoutingState.INITIALIZING;
import static org.elasticsearch.cluster.routing.ShardRoutingState.STARTED;
import static org.elasticsearch.cluster.routing.ShardRoutingState.UNASSIGNED;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;

public class IndexBalanceTests extends ESAllocationTestCase {
    private final Logger logger = LogManager.getLogger(IndexBalanceTests.class);

    public void testBalanceAllNodesStarted() {
        AllocationService strategy = createAllocationService(
            Settings.builder()
                .put("cluster.routing.allocation.node_concurrent_recoveries", 10)
                .put("cluster.routing.allocation.node_initial_primaries_recoveries", 10)
                .put(ClusterRebalanceAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ALLOW_REBALANCE_SETTING.getKey(), "always")
                .put("cluster.routing.allocation.cluster_concurrent_rebalance", -1)
                .build()
        );

        logger.info("Building initial routing table");

        Metadata metadata = Metadata.builder()
            .put(IndexMetadata.builder("test").settings(settings(Version.CURRENT)).numberOfShards(3).numberOfReplicas(1))
            .put(IndexMetadata.builder("test1").settings(settings(Version.CURRENT)).numberOfShards(3).numberOfReplicas(1))
            .build();

        RoutingTable initialRoutingTable = RoutingTable.builder(TestShardRoutingRoleStrategies.DEFAULT_ROLE_ONLY)
            .addAsNew(metadata.index("test"))
            .addAsNew(metadata.index("test1"))
            .build();

        ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT).metadata(metadata).routingTable(initialRoutingTable).build();

        assertThat(clusterState.routingTable().index("test").size(), equalTo(3));
        for (int i = 0; i < clusterState.routingTable().index("test").size(); i++) {
            assertThat(clusterState.routingTable().index("test").shard(i).size(), equalTo(2));
            assertThat(clusterState.routingTable().index("test").shard(i).shard(0).state(), equalTo(UNASSIGNED));
            assertThat(clusterState.routingTable().index("test").shard(i).shard(1).state(), equalTo(UNASSIGNED));
            assertThat(clusterState.routingTable().index("test").shard(i).shard(0).currentNodeId(), nullValue());
            assertThat(clusterState.routingTable().index("test").shard(i).shard(1).currentNodeId(), nullValue());
        }

        assertThat(clusterState.routingTable().index("test1").size(), equalTo(3));
        for (int i = 0; i < clusterState.routingTable().index("test1").size(); i++) {
            assertThat(clusterState.routingTable().index("test1").shard(i).size(), equalTo(2));
            assertThat(clusterState.routingTable().index("test1").shard(i).shard(0).state(), equalTo(UNASSIGNED));
            assertThat(clusterState.routingTable().index("test1").shard(i).shard(1).state(), equalTo(UNASSIGNED));
            assertThat(clusterState.routingTable().index("test1").shard(i).shard(0).currentNodeId(), nullValue());
            assertThat(clusterState.routingTable().index("test1").shard(i).shard(1).currentNodeId(), nullValue());
        }

        logger.info("Adding three node and performing rerouting");
        clusterState = ClusterState.builder(clusterState)
            .nodes(DiscoveryNodes.builder().add(newNode("node1")).add(newNode("node2")).add(newNode("node3")))
            .build();

        ClusterState newState = strategy.reroute(clusterState, "reroute", ActionListener.noop());
        assertThat(newState, not(equalTo(clusterState)));
        clusterState = newState;

        assertThat(clusterState.routingTable().index("test").size(), equalTo(3));
        for (int i = 0; i < clusterState.routingTable().index("test").size(); i++) {
            assertThat(clusterState.routingTable().index("test").shard(i).size(), equalTo(2));
            assertThat(clusterState.routingTable().index("test").shard(i).primaryShard().state(), equalTo(INITIALIZING));
            assertThat(clusterState.routingTable().index("test").shard(i).replicaShards().size(), equalTo(1));
            assertThat(clusterState.routingTable().index("test").shard(i).replicaShards().get(0).state(), equalTo(UNASSIGNED));
            assertThat(clusterState.routingTable().index("test").shard(i).replicaShards().get(0).currentNodeId(), nullValue());
        }

        logger.info("Another round of rebalancing");
        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder(clusterState.nodes())).build();
        newState = strategy.reroute(clusterState, "reroute", ActionListener.noop());
        assertThat(newState, equalTo(clusterState));

        newState = startInitializingShardsAndReroute(strategy, clusterState);
        assertThat(newState, not(equalTo(clusterState)));
        clusterState = newState;
        assertThat(clusterState.routingTable().index("test").size(), equalTo(3));
        for (int i = 0; i < clusterState.routingTable().index("test").size(); i++) {
            assertThat(clusterState.routingTable().index("test").shard(i).size(), equalTo(2));
            assertThat(clusterState.routingTable().index("test").shard(i).primaryShard().state(), equalTo(STARTED));
            assertThat(clusterState.routingTable().index("test").shard(i).replicaShards().size(), equalTo(1));
            // backup shards are initializing as well, we make sure that they
            // recover from primary *started* shards in the
            // IndicesClusterStateService
            assertThat(clusterState.routingTable().index("test").shard(i).replicaShards().get(0).state(), equalTo(INITIALIZING));
        }

        logger.info("Reroute, nothing should change");
        newState = strategy.reroute(clusterState, "reroute", ActionListener.noop());
        assertThat(newState, equalTo(clusterState));

        logger.info("Start the more shards");
        newState = startInitializingShardsAndReroute(strategy, clusterState);
        assertThat(newState, not(equalTo(clusterState)));
        clusterState = newState;
        RoutingNodes routingNodes = clusterState.getRoutingNodes();

        assertThat(clusterState.routingTable().index("test").size(), equalTo(3));
        for (int i = 0; i < clusterState.routingTable().index("test").size(); i++) {
            assertThat(clusterState.routingTable().index("test").shard(i).size(), equalTo(2));
            assertThat(clusterState.routingTable().index("test").shard(i).primaryShard().state(), equalTo(STARTED));
            assertThat(clusterState.routingTable().index("test").shard(i).replicaShards().size(), equalTo(1));
            assertThat(clusterState.routingTable().index("test").shard(i).replicaShards().get(0).state(), equalTo(STARTED));
        }
        assertThat(clusterState.routingTable().index("test1").size(), equalTo(3));
        for (int i = 0; i < clusterState.routingTable().index("test1").size(); i++) {
            assertThat(clusterState.routingTable().index("test1").shard(i).size(), equalTo(2));
            assertThat(clusterState.routingTable().index("test1").shard(i).primaryShard().state(), equalTo(STARTED));
            assertThat(clusterState.routingTable().index("test1").shard(i).replicaShards().size(), equalTo(1));
            assertThat(clusterState.routingTable().index("test1").shard(i).replicaShards().get(0).state(), equalTo(STARTED));
        }

        assertThat(routingNodes.node("node1").numberOfShardsWithState(STARTED), equalTo(4));
        assertThat(routingNodes.node("node2").numberOfShardsWithState(STARTED), equalTo(4));
        assertThat(routingNodes.node("node3").numberOfShardsWithState(STARTED), equalTo(4));

        assertThat(routingNodes.node("node1").shardsWithState("test", STARTED).count(), equalTo(2L));
        assertThat(routingNodes.node("node2").shardsWithState("test", STARTED).count(), equalTo(2L));
        assertThat(routingNodes.node("node3").shardsWithState("test", STARTED).count(), equalTo(2L));

        assertThat(routingNodes.node("node1").shardsWithState("test1", STARTED).count(), equalTo(2L));
        assertThat(routingNodes.node("node2").shardsWithState("test1", STARTED).count(), equalTo(2L));
        assertThat(routingNodes.node("node3").shardsWithState("test1", STARTED).count(), equalTo(2L));
    }

    public void testBalanceIncrementallyStartNodes() {
        AllocationService strategy = createAllocationService(
            Settings.builder()
                .put("cluster.routing.allocation.node_concurrent_recoveries", 10)
                .put("cluster.routing.allocation.node_initial_primaries_recoveries", 10)
                .put(ClusterRebalanceAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ALLOW_REBALANCE_SETTING.getKey(), "always")
                .put("cluster.routing.allocation.cluster_concurrent_rebalance", -1)
                .build()
        );

        logger.info("Building initial routing table");

        Metadata metadata = Metadata.builder()
            .put(IndexMetadata.builder("test").settings(settings(Version.CURRENT)).numberOfShards(3).numberOfReplicas(1))
            .put(IndexMetadata.builder("test1").settings(settings(Version.CURRENT)).numberOfShards(3).numberOfReplicas(1))
            .build();

        RoutingTable initialRoutingTable = RoutingTable.builder(TestShardRoutingRoleStrategies.DEFAULT_ROLE_ONLY)
            .addAsNew(metadata.index("test"))
            .addAsNew(metadata.index("test1"))
            .build();

        ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT).metadata(metadata).routingTable(initialRoutingTable).build();

        assertThat(clusterState.routingTable().index("test").size(), equalTo(3));
        for (int i = 0; i < clusterState.routingTable().index("test").size(); i++) {
            assertThat(clusterState.routingTable().index("test").shard(i).size(), equalTo(2));
            assertThat(clusterState.routingTable().index("test").shard(i).shard(0).state(), equalTo(UNASSIGNED));
            assertThat(clusterState.routingTable().index("test").shard(i).shard(1).state(), equalTo(UNASSIGNED));
            assertThat(clusterState.routingTable().index("test").shard(i).shard(0).currentNodeId(), nullValue());
            assertThat(clusterState.routingTable().index("test").shard(i).shard(1).currentNodeId(), nullValue());
        }

        assertThat(clusterState.routingTable().index("test1").size(), equalTo(3));
        for (int i = 0; i < clusterState.routingTable().index("test1").size(); i++) {
            assertThat(clusterState.routingTable().index("test1").shard(i).size(), equalTo(2));
            assertThat(clusterState.routingTable().index("test1").shard(i).shard(0).state(), equalTo(UNASSIGNED));
            assertThat(clusterState.routingTable().index("test1").shard(i).shard(1).state(), equalTo(UNASSIGNED));
            assertThat(clusterState.routingTable().index("test1").shard(i).shard(0).currentNodeId(), nullValue());
            assertThat(clusterState.routingTable().index("test1").shard(i).shard(1).currentNodeId(), nullValue());
        }

        logger.info("Adding one node and performing rerouting");
        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder().add(newNode("node1"))).build();

        ClusterState newState = strategy.reroute(clusterState, "reroute", ActionListener.noop());
        assertThat(newState, not(equalTo(clusterState)));
        clusterState = newState;
        assertThat(clusterState.routingTable().index("test").size(), equalTo(3));
        for (int i = 0; i < clusterState.routingTable().index("test").size(); i++) {
            assertThat(clusterState.routingTable().index("test").shard(i).size(), equalTo(2));
            assertThat(clusterState.routingTable().index("test").shard(i).primaryShard().state(), equalTo(INITIALIZING));
            assertThat(clusterState.routingTable().index("test").shard(i).replicaShards().size(), equalTo(1));
            assertThat(clusterState.routingTable().index("test").shard(i).replicaShards().get(0).state(), equalTo(UNASSIGNED));
            assertThat(clusterState.routingTable().index("test").shard(i).replicaShards().get(0).currentNodeId(), nullValue());
        }

        logger.info("Add another node and perform rerouting, nothing will happen since primary not started");
        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder(clusterState.nodes()).add(newNode("node2"))).build();
        newState = strategy.reroute(clusterState, "reroute", ActionListener.noop());
        assertThat(newState, equalTo(clusterState));

        logger.info("Start the primary shard");
        newState = startInitializingShardsAndReroute(strategy, clusterState);
        assertThat(newState, not(equalTo(clusterState)));
        clusterState = newState;
        assertThat(clusterState.routingTable().index("test").size(), equalTo(3));
        for (int i = 0; i < clusterState.routingTable().index("test").size(); i++) {
            assertThat(clusterState.routingTable().index("test").shard(i).size(), equalTo(2));
            assertThat(clusterState.routingTable().index("test").shard(i).primaryShard().state(), equalTo(STARTED));
            assertThat(clusterState.routingTable().index("test").shard(i).replicaShards().size(), equalTo(1));
            // backup shards are initializing as well, we make sure that they
            // recover from primary *started* shards in the
            // IndicesClusterStateService
            assertThat(clusterState.routingTable().index("test").shard(i).replicaShards().get(0).state(), equalTo(INITIALIZING));
        }

        logger.info("Reroute, nothing should change");
        newState = strategy.reroute(clusterState, "reroute", ActionListener.noop());
        assertThat(newState, equalTo(clusterState));

        logger.info("Start the backup shard");
        newState = startInitializingShardsAndReroute(strategy, clusterState);
        assertThat(newState, not(equalTo(clusterState)));
        clusterState = newState;

        assertThat(clusterState.routingTable().index("test").size(), equalTo(3));
        for (int i = 0; i < clusterState.routingTable().index("test").size(); i++) {
            assertThat(clusterState.routingTable().index("test").shard(i).size(), equalTo(2));
            assertThat(clusterState.routingTable().index("test").shard(i).primaryShard().state(), equalTo(STARTED));
            assertThat(clusterState.routingTable().index("test").shard(i).replicaShards().size(), equalTo(1));
            assertThat(clusterState.routingTable().index("test").shard(i).replicaShards().get(0).state(), equalTo(STARTED));
        }
        assertThat(clusterState.routingTable().index("test1").size(), equalTo(3));
        for (int i = 0; i < clusterState.routingTable().index("test1").size(); i++) {
            assertThat(clusterState.routingTable().index("test1").shard(i).size(), equalTo(2));
            assertThat(clusterState.routingTable().index("test1").shard(i).primaryShard().state(), equalTo(STARTED));
            assertThat(clusterState.routingTable().index("test1").shard(i).replicaShards().size(), equalTo(1));
            assertThat(clusterState.routingTable().index("test1").shard(i).replicaShards().get(0).state(), equalTo(STARTED));
        }

        logger.info("Add another node and perform reroute to relocate shards to the new node");
        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder(clusterState.nodes()).add(newNode("node3"))).build();
        clusterState = applyStartedShardsUntilNoChange(clusterState, strategy);

        assertThat(clusterState.routingTable().index("test").size(), equalTo(3));
        assertThat(clusterState.routingTable().index("test1").size(), equalTo(3));

        assertThat(clusterState.getRoutingNodes().node("node1").numberOfShardsWithState(STARTED), equalTo(4));
        assertThat(clusterState.getRoutingNodes().node("node2").numberOfShardsWithState(STARTED), equalTo(4));
        assertThat(clusterState.getRoutingNodes().node("node3").numberOfShardsWithState(STARTED), equalTo(4));

        assertThat(clusterState.getRoutingNodes().node("node1").shardsWithState("test", STARTED).count(), equalTo(2L));
        assertThat(clusterState.getRoutingNodes().node("node2").shardsWithState("test", STARTED).count(), equalTo(2L));
        assertThat(clusterState.getRoutingNodes().node("node3").shardsWithState("test", STARTED).count(), equalTo(2L));

        assertThat(clusterState.getRoutingNodes().node("node1").shardsWithState("test1", STARTED).count(), equalTo(2L));
        assertThat(clusterState.getRoutingNodes().node("node2").shardsWithState("test1", STARTED).count(), equalTo(2L));
        assertThat(clusterState.getRoutingNodes().node("node3").shardsWithState("test1", STARTED).count(), equalTo(2L));
    }

    public void testBalanceAllNodesStartedAddIndex() {
        AllocationService strategy = createAllocationService(
            Settings.builder()
                .put("cluster.routing.allocation.node_concurrent_recoveries", 10)
                .put("cluster.routing.allocation.node_initial_primaries_recoveries", 10)
                .put(ClusterRebalanceAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ALLOW_REBALANCE_SETTING.getKey(), "always")
                .put("cluster.routing.allocation.cluster_concurrent_rebalance", -1)
                .build()
        );

        logger.info("Building initial routing table");

        Metadata metadata = Metadata.builder()
            .put(IndexMetadata.builder("test").settings(settings(Version.CURRENT)).numberOfShards(3).numberOfReplicas(1))
            .build();

        RoutingTable initialRoutingTable = RoutingTable.builder(TestShardRoutingRoleStrategies.DEFAULT_ROLE_ONLY)
            .addAsNew(metadata.index("test"))
            .build();

        ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT).metadata(metadata).routingTable(initialRoutingTable).build();

        assertThat(clusterState.routingTable().index("test").size(), equalTo(3));
        for (int i = 0; i < clusterState.routingTable().index("test").size(); i++) {
            assertThat(clusterState.routingTable().index("test").shard(i).size(), equalTo(2));
            assertThat(clusterState.routingTable().index("test").shard(i).shard(0).state(), equalTo(UNASSIGNED));
            assertThat(clusterState.routingTable().index("test").shard(i).shard(1).state(), equalTo(UNASSIGNED));
            assertThat(clusterState.routingTable().index("test").shard(i).shard(0).currentNodeId(), nullValue());
            assertThat(clusterState.routingTable().index("test").shard(i).shard(1).currentNodeId(), nullValue());
        }

        logger.info("Adding three node and performing rerouting");
        clusterState = ClusterState.builder(clusterState)
            .nodes(DiscoveryNodes.builder().add(newNode("node1")).add(newNode("node2")).add(newNode("node3")))
            .build();

        ClusterState newState = strategy.reroute(clusterState, "reroute", ActionListener.noop());
        assertThat(newState, not(equalTo(clusterState)));
        clusterState = newState;
        assertThat(clusterState.routingTable().index("test").size(), equalTo(3));
        for (int i = 0; i < clusterState.routingTable().index("test").size(); i++) {
            assertThat(clusterState.routingTable().index("test").shard(i).size(), equalTo(2));
            assertThat(clusterState.routingTable().index("test").shard(i).primaryShard().state(), equalTo(INITIALIZING));
            assertThat(clusterState.routingTable().index("test").shard(i).replicaShards().size(), equalTo(1));
            assertThat(clusterState.routingTable().index("test").shard(i).replicaShards().get(0).state(), equalTo(UNASSIGNED));
            assertThat(clusterState.routingTable().index("test").shard(i).replicaShards().get(0).currentNodeId(), nullValue());
        }

        logger.info("Another round of rebalancing");
        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder(clusterState.nodes())).build();
        newState = strategy.reroute(clusterState, "reroute", ActionListener.noop());
        assertThat(newState, equalTo(clusterState));

        newState = startInitializingShardsAndReroute(strategy, clusterState);
        assertThat(newState, not(equalTo(clusterState)));
        clusterState = newState;
        assertThat(clusterState.routingTable().index("test").size(), equalTo(3));
        for (int i = 0; i < clusterState.routingTable().index("test").size(); i++) {
            assertThat(clusterState.routingTable().index("test").shard(i).size(), equalTo(2));
            assertThat(clusterState.routingTable().index("test").shard(i).primaryShard().state(), equalTo(STARTED));
            assertThat(clusterState.routingTable().index("test").shard(i).replicaShards().size(), equalTo(1));
            // backup shards are initializing as well, we make sure that they
            // recover from primary *started* shards in the
            // IndicesClusterStateService
            assertThat(clusterState.routingTable().index("test").shard(i).replicaShards().get(0).state(), equalTo(INITIALIZING));
        }

        logger.info("Reroute, nothing should change");
        newState = strategy.reroute(clusterState, "reroute", ActionListener.noop());
        assertThat(newState, equalTo(clusterState));

        logger.info("Start the more shards");
        newState = startInitializingShardsAndReroute(strategy, clusterState);
        assertThat(newState, not(equalTo(clusterState)));
        clusterState = newState;
        RoutingNodes routingNodes = clusterState.getRoutingNodes();
        assertThat(clusterState.routingTable().index("test").size(), equalTo(3));
        for (int i = 0; i < clusterState.routingTable().index("test").size(); i++) {
            assertThat(clusterState.routingTable().index("test").shard(i).size(), equalTo(2));
            assertThat(clusterState.routingTable().index("test").shard(i).primaryShard().state(), equalTo(STARTED));
            assertThat(clusterState.routingTable().index("test").shard(i).replicaShards().size(), equalTo(1));
        }
        assertThat(routingNodes.node("node1").numberOfShardsWithState(STARTED), equalTo(2));
        assertThat(routingNodes.node("node2").numberOfShardsWithState(STARTED), equalTo(2));
        assertThat(routingNodes.node("node3").numberOfShardsWithState(STARTED), equalTo(2));

        assertThat(routingNodes.node("node1").shardsWithState("test", STARTED).count(), equalTo(2L));
        assertThat(routingNodes.node("node2").shardsWithState("test", STARTED).count(), equalTo(2L));
        assertThat(routingNodes.node("node3").shardsWithState("test", STARTED).count(), equalTo(2L));

        logger.info("Add new index 3 shards 1 replica");

        Metadata updatedMetadata = Metadata.builder(clusterState.metadata())
            .put(IndexMetadata.builder("test1").settings(indexSettings(Version.CURRENT, 3, 1)))
            .build();
        RoutingTable updatedRoutingTable = RoutingTable.builder(
            TestShardRoutingRoleStrategies.DEFAULT_ROLE_ONLY,
            clusterState.routingTable()
        ).addAsNew(updatedMetadata.index("test1")).build();
        clusterState = ClusterState.builder(clusterState).metadata(updatedMetadata).routingTable(updatedRoutingTable).build();

        assertThat(clusterState.routingTable().index("test1").size(), equalTo(3));

        newState = strategy.reroute(clusterState, "reroute", ActionListener.noop());
        assertThat(newState, not(equalTo(clusterState)));
        clusterState = newState;
        assertThat(clusterState.routingTable().index("test1").size(), equalTo(3));
        for (int i = 0; i < clusterState.routingTable().index("test1").size(); i++) {
            assertThat(clusterState.routingTable().index("test1").shard(i).size(), equalTo(2));
            assertThat(clusterState.routingTable().index("test1").shard(i).primaryShard().state(), equalTo(INITIALIZING));
            assertThat(clusterState.routingTable().index("test1").shard(i).replicaShards().size(), equalTo(1));
            assertThat(clusterState.routingTable().index("test1").shard(i).replicaShards().get(0).state(), equalTo(UNASSIGNED));
            assertThat(clusterState.routingTable().index("test1").shard(i).replicaShards().get(0).currentNodeId(), nullValue());
        }

        logger.info("Another round of rebalancing");
        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder(clusterState.nodes())).build();
        newState = strategy.reroute(clusterState, "reroute", ActionListener.noop());
        assertThat(newState, equalTo(clusterState));

        newState = startInitializingShardsAndReroute(strategy, clusterState);
        assertThat(newState, not(equalTo(clusterState)));
        clusterState = newState;
        assertThat(clusterState.routingTable().index("test1").size(), equalTo(3));
        for (int i = 0; i < clusterState.routingTable().index("test1").size(); i++) {
            assertThat(clusterState.routingTable().index("test1").shard(i).size(), equalTo(2));
            assertThat(clusterState.routingTable().index("test1").shard(i).primaryShard().state(), equalTo(STARTED));
            assertThat(clusterState.routingTable().index("test1").shard(i).replicaShards().size(), equalTo(1));
            // backup shards are initializing as well, we make sure that they
            // recover from primary *started* shards in the
            // IndicesClusterStateService
            assertThat(clusterState.routingTable().index("test1").shard(i).replicaShards().get(0).state(), equalTo(INITIALIZING));
        }

        logger.info("Reroute, nothing should change");
        newState = strategy.reroute(clusterState, "reroute", ActionListener.noop());
        assertThat(newState, equalTo(clusterState));

        logger.info("Start the more shards");
        newState = startInitializingShardsAndReroute(strategy, clusterState);
        assertThat(newState, not(equalTo(clusterState)));
        clusterState = newState;
        routingNodes = clusterState.getRoutingNodes();
        assertThat(clusterState.routingTable().index("test1").size(), equalTo(3));
        for (int i = 0; i < clusterState.routingTable().index("test1").size(); i++) {
            assertThat(clusterState.routingTable().index("test1").shard(i).size(), equalTo(2));
            assertThat(clusterState.routingTable().index("test1").shard(i).primaryShard().state(), equalTo(STARTED));
            assertThat(clusterState.routingTable().index("test1").shard(i).replicaShards().size(), equalTo(1));
        }
        assertThat(routingNodes.node("node1").numberOfShardsWithState(STARTED), equalTo(4));
        assertThat(routingNodes.node("node2").numberOfShardsWithState(STARTED), equalTo(4));
        assertThat(routingNodes.node("node3").numberOfShardsWithState(STARTED), equalTo(4));

        assertThat(routingNodes.node("node1").shardsWithState("test1", STARTED).count(), equalTo(2L));
        assertThat(routingNodes.node("node2").shardsWithState("test1", STARTED).count(), equalTo(2L));
        assertThat(routingNodes.node("node3").shardsWithState("test1", STARTED).count(), equalTo(2L));
    }

    /**
     * {@see https://github.com/elastic/elasticsearch/issues/87279}
     */
    public void testRebalanceShouldNotPerformUnnecessaryMovesWithMultipleConcurrentRebalances() {
        final var settings = Settings.builder()
            .put("cluster.routing.allocation.type", "desired_balance")
            .put("cluster.routing.allocation.cluster_concurrent_rebalance", randomIntBetween(3, 9))
            .build();

        final var allocationService = createAllocationService(settings);
        assertCriticalWarnings(
            "[cluster.routing.allocation.type] setting was deprecated in Elasticsearch and will be removed in a future release."
        );

        assertTrue(
            "Only fixed in DesiredBalanceShardsAllocator",
            allocationService.shardsAllocator instanceof DesiredBalanceShardsAllocator
        );

        final var discoveryNodesBuilder = DiscoveryNodes.builder();
        for (int nodeIndex = 0; nodeIndex < 3; nodeIndex++) {
            discoveryNodesBuilder.add(newNode("node-" + nodeIndex));
        }

        final var metadataBuilder = Metadata.builder();
        final var routingTableBuilder = RoutingTable.builder();

        addIndex(metadataBuilder, routingTableBuilder, "index-0", 4, shardId -> "node-" + (shardId / 2));
        addIndex(metadataBuilder, routingTableBuilder, "index-1", 4, shardId -> "node-" + (shardId / 2));
        addIndex(metadataBuilder, routingTableBuilder, "index-2", 1, shardId -> "node-2");

        final var clusterState = ClusterState.builder(ClusterName.DEFAULT)
            .nodes(discoveryNodesBuilder)
            .metadata(metadataBuilder)
            .routingTable(routingTableBuilder)
            .build();

        final var reroutedState = applyStartedShardsUntilNoChange(clusterState, allocationService);
        // node-0 and node-1 has 4 shards each. node-2 has only [index-2][0]. It should not be moved to achieve even balance.
        assertThat(reroutedState.getRoutingTable().index("index-2").shard(0).primaryShard().currentNodeId(), equalTo("node-2"));
    }

    private static void addIndex(
        Metadata.Builder metadataBuilder,
        RoutingTable.Builder routingTableBuilder,
        String indexName,
        int numberOfShards,
        IntFunction<String> assignmentFunction
    ) {
        final var inSyncIds = randomList(numberOfShards, numberOfShards, () -> UUIDs.randomBase64UUID(random()));
        final var indexMetadataBuilder = IndexMetadata.builder(indexName).settings(indexSettings(Version.CURRENT, numberOfShards, 0));
        for (int shardId = 0; shardId < numberOfShards; shardId++) {
            indexMetadataBuilder.putInSyncAllocationIds(shardId, Set.of(inSyncIds.get(shardId)));
        }
        metadataBuilder.put(indexMetadataBuilder);
        final var indexId = metadataBuilder.get(indexName).getIndex();
        final var indexRoutingTableBuilder = IndexRoutingTable.builder(indexId);

        for (int shardId = 0; shardId < numberOfShards; shardId++) {
            indexRoutingTableBuilder.addShard(
                TestShardRouting.newShardRouting(
                    new ShardId(indexId, shardId),
                    assignmentFunction.apply(shardId),
                    null,
                    true,
                    ShardRoutingState.STARTED,
                    AllocationId.newInitializing(inSyncIds.get(shardId))
                )
            );
        }

        routingTableBuilder.add(indexRoutingTableBuilder);
    }
}
