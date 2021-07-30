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
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ESAllocationTestCase;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.RoutingNodes;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.allocation.decider.ClusterRebalanceAllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.ThrottlingAllocationDecider;
import org.elasticsearch.common.settings.Settings;

import static org.elasticsearch.cluster.routing.ShardRoutingState.INITIALIZING;
import static org.elasticsearch.cluster.routing.ShardRoutingState.STARTED;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;

public class RoutingNodesIntegrityTests extends ESAllocationTestCase {
    private final Logger logger = LogManager.getLogger(IndexBalanceTests.class);

    public void testBalanceAllNodesStarted() {
        AllocationService strategy = createAllocationService(Settings.builder()
                .put("cluster.routing.allocation.node_concurrent_recoveries", 10)
                .put("cluster.routing.allocation.node_initial_primaries_recoveries", 10)
                .put(ClusterRebalanceAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ALLOW_REBALANCE_SETTING.getKey(), "always")
                .put("cluster.routing.allocation.cluster_concurrent_rebalance", -1).build());

        logger.info("Building initial routing table");

        Metadata metadata = Metadata.builder()
            .put(IndexMetadata.builder("test").settings(settings(Version.CURRENT)).numberOfShards(3).numberOfReplicas(1))
            .put(IndexMetadata.builder("test1").settings(settings(Version.CURRENT)).numberOfShards(3).numberOfReplicas(1)).build();

        RoutingTable initialRoutingTable = RoutingTable.builder()
            .addAsNew(metadata.index("test")).addAsNew(metadata.index("test1")).build();

        ClusterState clusterState = ClusterState.builder(org.elasticsearch.cluster.ClusterName.CLUSTER_NAME_SETTING
            .getDefault(Settings.EMPTY)).metadata(metadata).routingTable(initialRoutingTable).build();

        logger.info("Adding three node and performing rerouting");
        clusterState = ClusterState.builder(clusterState)
                .nodes(DiscoveryNodes.builder().add(newNode("node1")).add(newNode("node2")).add(newNode("node3"))).build();
        RoutingNodes routingNodes = clusterState.getRoutingNodes();

        assertThat(assertShardStats(routingNodes), equalTo(true));
        // all shards are unassigned. so no inactive shards or primaries.
        assertThat(routingNodes.hasInactiveShards(), equalTo(false));
        assertThat(routingNodes.hasInactivePrimaries(), equalTo(false));
        assertThat(routingNodes.hasUnassignedPrimaries(), equalTo(true));

        clusterState = strategy.reroute(clusterState, "reroute");
        routingNodes = clusterState.getRoutingNodes();

        assertThat(assertShardStats(routingNodes), equalTo(true));
        assertThat(routingNodes.hasInactiveShards(), equalTo(true));
        assertThat(routingNodes.hasInactivePrimaries(), equalTo(true));
        assertThat(routingNodes.hasUnassignedPrimaries(), equalTo(false));

        logger.info("Another round of rebalancing");
        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder(clusterState.nodes())).build();
        clusterState = strategy.reroute(clusterState, "reroute");

        clusterState = startInitializingShardsAndReroute(strategy, clusterState);

        logger.info("Reroute, nothing should change");
        ClusterState newState = strategy.reroute(clusterState, "reroute");
        assertThat(newState, equalTo(clusterState));

        logger.info("Start the more shards");
        clusterState = startInitializingShardsAndReroute(strategy, clusterState);
        routingNodes = clusterState.getRoutingNodes();

        assertThat(assertShardStats(routingNodes), equalTo(true));
        assertThat(routingNodes.hasInactiveShards(), equalTo(false));
        assertThat(routingNodes.hasInactivePrimaries(), equalTo(false));
        assertThat(routingNodes.hasUnassignedPrimaries(), equalTo(false));

        startInitializingShardsAndReroute(strategy, clusterState);
    }

    public void testBalanceIncrementallyStartNodes() {
        AllocationService strategy = createAllocationService(Settings.builder()
                .put("cluster.routing.allocation.node_concurrent_recoveries", 10)
                .put("cluster.routing.allocation.node_initial_primaries_recoveries", 10)
                .put(ClusterRebalanceAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ALLOW_REBALANCE_SETTING.getKey(), "always")
                .put("cluster.routing.allocation.cluster_concurrent_rebalance", -1).build());

        logger.info("Building initial routing table");

        Metadata metadata = Metadata.builder()
            .put(IndexMetadata.builder("test").settings(settings(Version.CURRENT)).numberOfShards(3).numberOfReplicas(1))
            .put(IndexMetadata.builder("test1").settings(settings(Version.CURRENT)).numberOfShards(3).numberOfReplicas(1)).build();

        RoutingTable initialRoutingTable = RoutingTable.builder()
            .addAsNew(metadata.index("test")).addAsNew(metadata.index("test1")).build();

        ClusterState clusterState = ClusterState.builder(org.elasticsearch.cluster.ClusterName.CLUSTER_NAME_SETTING
            .getDefault(Settings.EMPTY)).metadata(metadata).routingTable(initialRoutingTable).build();

        logger.info("Adding one node and performing rerouting");
        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder().add(newNode("node1"))).build();

        clusterState = strategy.reroute(clusterState, "reroute");

        logger.info("Add another node and perform rerouting, nothing will happen since primary not started");
        clusterState = ClusterState.builder(clusterState)
                .nodes(DiscoveryNodes.builder(clusterState.nodes()).add(newNode("node2"))).build();
        clusterState = strategy.reroute(clusterState, "reroute");

        logger.info("Start the primary shard");
        clusterState = startInitializingShardsAndReroute(strategy, clusterState);

        logger.info("Reroute, nothing should change");
        clusterState = strategy.reroute(clusterState, "reroute");

        logger.info("Start the backup shard");
        clusterState = startInitializingShardsAndReroute(strategy, clusterState);

        clusterState = startInitializingShardsAndReroute(strategy, clusterState);

        logger.info("Add another node and perform rerouting, nothing will happen since primary not started");
        clusterState = ClusterState.builder(clusterState)
                .nodes(DiscoveryNodes.builder(clusterState.nodes()).add(newNode("node3"))).build();
        clusterState = strategy.reroute(clusterState, "reroute");

        logger.info("Reroute, nothing should change");
        ClusterState newState = strategy.reroute(clusterState, "reroute");
        assertThat(newState, equalTo(clusterState));

        logger.info("Start the backup shard");
        newState = startInitializingShardsAndReroute(strategy, clusterState);
        assertThat(newState, not(equalTo(clusterState)));
        clusterState = newState;

        assertThat(clusterState.routingTable().index("test").shards().size(), equalTo(3));

        newState = startInitializingShardsAndReroute(strategy, clusterState);
        assertThat(newState, not(equalTo(clusterState)));
        clusterState = newState;
        RoutingNodes routingNodes = clusterState.getRoutingNodes();

        assertThat(clusterState.routingTable().index("test1").shards().size(), equalTo(3));

        assertThat(routingNodes.node("node1").numberOfShardsWithState(STARTED), equalTo(4));
        assertThat(routingNodes.node("node2").numberOfShardsWithState(STARTED), equalTo(4));
        assertThat(routingNodes.node("node3").numberOfShardsWithState(STARTED), equalTo(4));

        assertThat(routingNodes.node("node1").shardsWithState("test", STARTED).size(), equalTo(2));
        assertThat(routingNodes.node("node2").shardsWithState("test", STARTED).size(), equalTo(2));
        assertThat(routingNodes.node("node3").shardsWithState("test", STARTED).size(), equalTo(2));

        assertThat(routingNodes.node("node1").shardsWithState("test1", STARTED).size(), equalTo(2));
        assertThat(routingNodes.node("node2").shardsWithState("test1", STARTED).size(), equalTo(2));
        assertThat(routingNodes.node("node3").shardsWithState("test1", STARTED).size(), equalTo(2));
    }

    public void testBalanceAllNodesStartedAddIndex() {
        AllocationService strategy = createAllocationService(Settings.builder()
                .put("cluster.routing.allocation.node_concurrent_recoveries", 1)
                .put("cluster.routing.allocation.node_initial_primaries_recoveries", 3)
                .put(ThrottlingAllocationDecider.CLUSTER_ROUTING_ALLOCATION_NODE_CONCURRENT_OUTGOING_RECOVERIES_SETTING.getKey(), 10)
                .put(ClusterRebalanceAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ALLOW_REBALANCE_SETTING.getKey(), "always")
                .put("cluster.routing.allocation.cluster_concurrent_rebalance", -1).build());

        logger.info("Building initial routing table");

        Metadata metadata = Metadata.builder()
            .put(IndexMetadata.builder("test").settings(settings(Version.CURRENT)).numberOfShards(3).numberOfReplicas(1)).build();

        RoutingTable initialRoutingTable = RoutingTable.builder().addAsNew(metadata.index("test")).build();

        ClusterState clusterState = ClusterState.builder(org.elasticsearch.cluster.ClusterName.CLUSTER_NAME_SETTING
            .getDefault(Settings.EMPTY)).metadata(metadata).routingTable(initialRoutingTable).build();

        logger.info("Adding three node and performing rerouting");
        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder().add(newNode("node1"))
            .add(newNode("node2")).add(newNode("node3"))).build();

        RoutingNodes routingNodes = clusterState.getRoutingNodes();
        assertThat(assertShardStats(routingNodes), equalTo(true));
        assertThat(routingNodes.hasInactiveShards(), equalTo(false));
        assertThat(routingNodes.hasInactivePrimaries(), equalTo(false));
        assertThat(routingNodes.hasUnassignedPrimaries(), equalTo(true));

        clusterState = strategy.reroute(clusterState, "reroute");
        routingNodes = clusterState.getRoutingNodes();

        assertThat(assertShardStats(routingNodes), equalTo(true));
        assertThat(routingNodes.hasInactiveShards(), equalTo(true));
        assertThat(routingNodes.hasInactivePrimaries(), equalTo(true));
        assertThat(routingNodes.hasUnassignedPrimaries(), equalTo(false));

        logger.info("Another round of rebalancing");
        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder(clusterState.nodes())).build();
        ClusterState newState = strategy.reroute(clusterState, "reroute");
        assertThat(newState, equalTo(clusterState));

        routingNodes = clusterState.getRoutingNodes();
        assertThat(routingNodes.node("node1").numberOfShardsWithState(INITIALIZING), equalTo(1));
        assertThat(routingNodes.node("node2").numberOfShardsWithState(INITIALIZING), equalTo(1));
        assertThat(routingNodes.node("node3").numberOfShardsWithState(INITIALIZING), equalTo(1));

        clusterState = startInitializingShardsAndReroute(strategy, clusterState);
        routingNodes = clusterState.getRoutingNodes();

        assertThat(assertShardStats(routingNodes), equalTo(true));
        assertThat(routingNodes.hasInactiveShards(), equalTo(true));
        assertThat(routingNodes.hasInactivePrimaries(), equalTo(false));
        assertThat(routingNodes.hasUnassignedPrimaries(), equalTo(false));
        assertThat(routingNodes.node("node1").numberOfShardsWithState(STARTED), equalTo(1));
        assertThat(routingNodes.node("node2").numberOfShardsWithState(STARTED), equalTo(1));
        assertThat(routingNodes.node("node3").numberOfShardsWithState(STARTED), equalTo(1));

        logger.info("Reroute, nothing should change");
        newState = strategy.reroute(clusterState, "reroute");
        assertThat(newState, equalTo(clusterState));

        logger.info("Start the more shards");
        clusterState = startInitializingShardsAndReroute(strategy, clusterState);
        routingNodes = clusterState.getRoutingNodes();

        assertThat(assertShardStats(routingNodes), equalTo(true));
        assertThat(routingNodes.hasInactiveShards(), equalTo(false));
        assertThat(routingNodes.hasInactivePrimaries(), equalTo(false));
        assertThat(routingNodes.hasUnassignedPrimaries(), equalTo(false));

        assertThat(routingNodes.node("node1").numberOfShardsWithState(STARTED), equalTo(2));
        assertThat(routingNodes.node("node2").numberOfShardsWithState(STARTED), equalTo(2));
        assertThat(routingNodes.node("node3").numberOfShardsWithState(STARTED), equalTo(2));

        assertThat(routingNodes.node("node1").shardsWithState("test", STARTED).size(), equalTo(2));
        assertThat(routingNodes.node("node2").shardsWithState("test", STARTED).size(), equalTo(2));
        assertThat(routingNodes.node("node3").shardsWithState("test", STARTED).size(), equalTo(2));

        logger.info("Add new index 3 shards 1 replica");

        metadata = Metadata.builder(clusterState.metadata())
                .put(IndexMetadata.builder("test1").settings(settings(Version.CURRENT)
                        .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 3)
                        .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1)
                ))
                .build();
        RoutingTable updatedRoutingTable = RoutingTable.builder(clusterState.routingTable())
            .addAsNew(metadata.index("test1"))
            .build();
        clusterState = ClusterState.builder(clusterState).metadata(metadata).routingTable(updatedRoutingTable).build();
        routingNodes = clusterState.getRoutingNodes();

        assertThat(assertShardStats(routingNodes), equalTo(true));
        assertThat(routingNodes.hasInactiveShards(), equalTo(false));
        assertThat(routingNodes.hasInactivePrimaries(), equalTo(false));
        assertThat(routingNodes.hasUnassignedPrimaries(), equalTo(true));

        assertThat(clusterState.routingTable().index("test1").shards().size(), equalTo(3));

        clusterState = strategy.reroute(clusterState, "reroute");

        logger.info("Reroute, assign");
        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder(clusterState.nodes())).build();
        newState = strategy.reroute(clusterState, "reroute");
        assertThat(newState, equalTo(clusterState));
        routingNodes = clusterState.getRoutingNodes();

        assertThat(assertShardStats(routingNodes), equalTo(true));
        assertThat(routingNodes.hasInactiveShards(), equalTo(true));
        assertThat(routingNodes.hasInactivePrimaries(), equalTo(true));
        assertThat(routingNodes.hasUnassignedPrimaries(), equalTo(false));


        logger.info("Reroute, start the primaries");
        clusterState = startInitializingShardsAndReroute(strategy, clusterState);
        routingNodes = clusterState.getRoutingNodes();

        assertThat(assertShardStats(routingNodes), equalTo(true));
        assertThat(routingNodes.hasInactiveShards(), equalTo(true));
        assertThat(routingNodes.hasInactivePrimaries(), equalTo(false));
        assertThat(routingNodes.hasUnassignedPrimaries(), equalTo(false));

        logger.info("Reroute, start the replicas");
        clusterState = startInitializingShardsAndReroute(strategy, clusterState);
        routingNodes = clusterState.getRoutingNodes();

        assertThat(assertShardStats(routingNodes), equalTo(true));
        assertThat(routingNodes.hasInactiveShards(), equalTo(false));
        assertThat(routingNodes.hasInactivePrimaries(), equalTo(false));
        assertThat(routingNodes.hasUnassignedPrimaries(), equalTo(false));


        assertThat(routingNodes.node("node1").numberOfShardsWithState(STARTED), equalTo(4));
        assertThat(routingNodes.node("node2").numberOfShardsWithState(STARTED), equalTo(4));
        assertThat(routingNodes.node("node3").numberOfShardsWithState(STARTED), equalTo(4));

        assertThat(routingNodes.node("node1").shardsWithState("test1", STARTED).size(), equalTo(2));
        assertThat(routingNodes.node("node2").shardsWithState("test1", STARTED).size(), equalTo(2));
        assertThat(routingNodes.node("node3").shardsWithState("test1", STARTED).size(), equalTo(2));

        logger.info("kill one node");
        IndexShardRoutingTable indexShardRoutingTable = clusterState.routingTable().index("test").shard(0);
        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder(clusterState.nodes())
            .remove(indexShardRoutingTable.primaryShard().currentNodeId())).build();
        clusterState = strategy.disassociateDeadNodes(clusterState, true, "reroute");
        routingNodes = clusterState.getRoutingNodes();

        assertThat(assertShardStats(routingNodes), equalTo(true));
        assertThat(routingNodes.hasInactiveShards(), equalTo(true));
        // replica got promoted to primary
        assertThat(routingNodes.hasInactivePrimaries(), equalTo(false));
        assertThat(routingNodes.hasUnassignedPrimaries(), equalTo(false));

        logger.info("Start Recovering shards round 1");
        clusterState = startInitializingShardsAndReroute(strategy, clusterState);
        routingNodes = clusterState.getRoutingNodes();

        assertThat(assertShardStats(routingNodes), equalTo(true));
        assertThat(routingNodes.hasInactiveShards(), equalTo(true));
        assertThat(routingNodes.hasInactivePrimaries(), equalTo(false));
        assertThat(routingNodes.hasUnassignedPrimaries(), equalTo(false));

        logger.info("Start Recovering shards round 2");
        clusterState = startInitializingShardsAndReroute(strategy, clusterState);
        routingNodes = clusterState.getRoutingNodes();

        assertThat(assertShardStats(routingNodes), equalTo(true));
        assertThat(routingNodes.hasInactiveShards(), equalTo(false));
        assertThat(routingNodes.hasInactivePrimaries(), equalTo(false));
        assertThat(routingNodes.hasUnassignedPrimaries(), equalTo(false));

    }

    private boolean assertShardStats(RoutingNodes routingNodes) {
        return RoutingNodes.assertShardStats(routingNodes);
    }
}
