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
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.RoutingChangesObserver;
import org.elasticsearch.cluster.routing.RoutingNodes;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.TestShardRouting;
import org.elasticsearch.cluster.routing.allocation.decider.ClusterRebalanceAllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.ThrottlingAllocationDecider;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.index.shard.ShardId;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.elasticsearch.cluster.routing.ShardRoutingState.INITIALIZING;
import static org.elasticsearch.cluster.routing.ShardRoutingState.RELOCATING;
import static org.elasticsearch.cluster.routing.ShardRoutingState.STARTED;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.oneOf;

public class RoutingNodesTests extends ESAllocationTestCase {
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

        logger.info("Adding three node and performing rerouting");
        clusterState = ClusterState.builder(clusterState)
            .nodes(DiscoveryNodes.builder().add(newNode("node1")).add(newNode("node2")).add(newNode("node3")))
            .build();
        RoutingNodes routingNodes = clusterState.getRoutingNodes();

        assertThat(assertShardStats(routingNodes), equalTo(true));
        // all shards are unassigned. so no inactive shards or primaries.
        assertThat(routingNodes.hasInactiveShards(), equalTo(false));
        assertThat(routingNodes.hasInactivePrimaries(), equalTo(false));
        assertThat(routingNodes.hasUnassignedPrimaries(), equalTo(true));

        clusterState = strategy.reroute(clusterState, "reroute", ActionListener.noop());
        routingNodes = clusterState.getRoutingNodes();

        assertThat(assertShardStats(routingNodes), equalTo(true));
        assertThat(routingNodes.hasInactiveShards(), equalTo(true));
        assertThat(routingNodes.hasInactivePrimaries(), equalTo(true));
        assertThat(routingNodes.hasUnassignedPrimaries(), equalTo(false));

        logger.info("Another round of rebalancing");
        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder(clusterState.nodes())).build();
        clusterState = strategy.reroute(clusterState, "reroute", ActionListener.noop());

        clusterState = startInitializingShardsAndReroute(strategy, clusterState);

        logger.info("Reroute, nothing should change");
        ClusterState newState = strategy.reroute(clusterState, "reroute", ActionListener.noop());
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

        logger.info("Adding node-1 and performing reroute");
        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder().add(newNode("node1"))).build();
        clusterState = strategy.reroute(clusterState, "reroute", ActionListener.noop());

        logger.info("Add node-2 and perform reroute, nothing will happen since primary not started");
        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder(clusterState.nodes()).add(newNode("node2"))).build();
        clusterState = strategy.reroute(clusterState, "reroute", ActionListener.noop());

        logger.info("Start the all shards");
        clusterState = startInitializingShardsAndReroute(strategy, clusterState); // primaries
        clusterState = startInitializingShardsAndReroute(strategy, clusterState); // replicas

        logger.info("Add node-3 and perform reroute, relocate shards to new node");
        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder(clusterState.nodes()).add(newNode("node3"))).build();
        clusterState = strategy.reroute(clusterState, "reroute", ActionListener.noop());

        logger.info("Await all shards reallocate");
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
                .put("cluster.routing.allocation.node_concurrent_recoveries", 1)
                .put("cluster.routing.allocation.node_initial_primaries_recoveries", 3)
                .put(ThrottlingAllocationDecider.CLUSTER_ROUTING_ALLOCATION_NODE_CONCURRENT_OUTGOING_RECOVERIES_SETTING.getKey(), 10)
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

        logger.info("Adding three node and performing rerouting");
        clusterState = ClusterState.builder(clusterState)
            .nodes(DiscoveryNodes.builder().add(newNode("node1")).add(newNode("node2")).add(newNode("node3")))
            .build();

        RoutingNodes routingNodes = clusterState.getRoutingNodes();
        assertThat(assertShardStats(routingNodes), equalTo(true));
        assertThat(routingNodes.hasInactiveShards(), equalTo(false));
        assertThat(routingNodes.hasInactivePrimaries(), equalTo(false));
        assertThat(routingNodes.hasUnassignedPrimaries(), equalTo(true));

        clusterState = strategy.reroute(clusterState, "reroute", ActionListener.noop());
        routingNodes = clusterState.getRoutingNodes();

        assertThat(assertShardStats(routingNodes), equalTo(true));
        assertThat(routingNodes.hasInactiveShards(), equalTo(true));
        assertThat(routingNodes.hasInactivePrimaries(), equalTo(true));
        assertThat(routingNodes.hasUnassignedPrimaries(), equalTo(false));

        logger.info("Another round of rebalancing");
        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder(clusterState.nodes())).build();
        ClusterState newState = strategy.reroute(clusterState, "reroute", ActionListener.noop());
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
        newState = strategy.reroute(clusterState, "reroute", ActionListener.noop());
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

        assertThat(routingNodes.node("node1").shardsWithState("test", STARTED).count(), equalTo(2L));
        assertThat(routingNodes.node("node2").shardsWithState("test", STARTED).count(), equalTo(2L));
        assertThat(routingNodes.node("node3").shardsWithState("test", STARTED).count(), equalTo(2L));

        logger.info("Add new index 3 shards 1 replica");

        metadata = Metadata.builder(clusterState.metadata())
            .put(IndexMetadata.builder("test1").settings(indexSettings(Version.CURRENT, 3, 1)))
            .build();
        RoutingTable updatedRoutingTable = RoutingTable.builder(
            TestShardRoutingRoleStrategies.DEFAULT_ROLE_ONLY,
            clusterState.routingTable()
        ).addAsNew(metadata.index("test1")).build();
        clusterState = ClusterState.builder(clusterState).metadata(metadata).routingTable(updatedRoutingTable).build();
        routingNodes = clusterState.getRoutingNodes();

        assertThat(assertShardStats(routingNodes), equalTo(true));
        assertThat(routingNodes.hasInactiveShards(), equalTo(false));
        assertThat(routingNodes.hasInactivePrimaries(), equalTo(false));
        assertThat(routingNodes.hasUnassignedPrimaries(), equalTo(true));

        assertThat(clusterState.routingTable().index("test1").size(), equalTo(3));

        clusterState = strategy.reroute(clusterState, "reroute", ActionListener.noop());

        logger.info("Reroute, assign");
        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder(clusterState.nodes())).build();
        newState = strategy.reroute(clusterState, "reroute", ActionListener.noop());
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

        assertThat(routingNodes.node("node1").shardsWithState("test1", STARTED).count(), equalTo(2L));
        assertThat(routingNodes.node("node2").shardsWithState("test1", STARTED).count(), equalTo(2L));
        assertThat(routingNodes.node("node3").shardsWithState("test1", STARTED).count(), equalTo(2L));

        logger.info("kill one node");
        IndexShardRoutingTable indexShardRoutingTable = clusterState.routingTable().index("test").shard(0);
        clusterState = ClusterState.builder(clusterState)
            .nodes(DiscoveryNodes.builder(clusterState.nodes()).remove(indexShardRoutingTable.primaryShard().currentNodeId()))
            .build();
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

    public void testNodeInterleavedShardIterator() {
        final var numberOfShards = between(1, 5);
        final var numberOfReplicas = between(0, 4);
        final var indexMetadata = IndexMetadata.builder("index")
            .settings(indexSettings(Version.CURRENT, numberOfShards, numberOfReplicas))
            .build();
        final var metadata = Metadata.builder().put(indexMetadata, true).build();
        final var routingTable = RoutingTable.builder(TestShardRoutingRoleStrategies.DEFAULT_ROLE_ONLY).addAsNew(indexMetadata).build();

        final var nodeCount = between(numberOfReplicas + 1, 6);
        final var discoveryNodes = DiscoveryNodes.builder();
        for (var i = 0; i < nodeCount; i++) {
            discoveryNodes.add(
                DiscoveryNodeUtils.builder("node-" + i)
                    .name("node-" + i)
                    .ephemeralId(UUIDs.randomBase64UUID(random()))
                    .roles(Set.of(DiscoveryNodeRole.MASTER_ROLE, DiscoveryNodeRole.DATA_ROLE))
                    .build()
            );
        }
        discoveryNodes.masterNodeId("node-0").localNodeId("node-0");

        final var allocationService = createAllocationService(Settings.EMPTY);
        var clusterState = ClusterState.builder(ClusterName.DEFAULT)
            .nodes(discoveryNodes)
            .metadata(metadata)
            .routingTable(routingTable)
            .build();
        boolean changed;
        do {
            final var newState = startInitializingShardsAndReroute(allocationService, clusterState);
            changed = newState != clusterState;
            clusterState = newState;
        } while (changed);

        final Map<String, Set<ShardId>> shardsByNode = Maps.newMapWithExpectedSize(nodeCount);
        for (final var indexRoutingTable : clusterState.routingTable()) {
            for (int shardId = 0; shardId < indexRoutingTable.size(); shardId++) {
                final IndexShardRoutingTable indexShardRoutingTable = indexRoutingTable.shard(shardId);
                for (int copy = 0; copy < indexShardRoutingTable.size(); copy++) {
                    ShardRouting shardRouting = indexShardRoutingTable.shard(copy);
                    assertTrue(shardRouting.started());
                    shardsByNode.computeIfAbsent(shardRouting.currentNodeId(), ignored -> new HashSet<>()).add(shardRouting.shardId());
                }
            }
        }

        final var iterationCountsByNode = shardsByNode.keySet().stream().collect(Collectors.toMap(Function.identity(), ignored -> 0));
        final var interleavingIterator = clusterState.getRoutingNodes().nodeInterleavedShardIterator();
        while (interleavingIterator.hasNext()) {
            final var shardRouting = interleavingIterator.next();
            final var expectedShards = shardsByNode.get(shardRouting.currentNodeId());
            assertTrue(expectedShards.remove(shardRouting.shardId()));
            iterationCountsByNode.computeIfPresent(shardRouting.currentNodeId(), (ignored, i) -> i + 1);
            final var minNodeCount = iterationCountsByNode.values().stream().mapToInt(i -> i).min().orElseThrow();
            final var maxNodeCount = iterationCountsByNode.values().stream().mapToInt(i -> i).max().orElseThrow();
            assertThat(maxNodeCount - minNodeCount, oneOf(0, 1));
            if (expectedShards.isEmpty()) {
                iterationCountsByNode.remove(shardRouting.currentNodeId());
            }
        }
        assertTrue(shardsByNode.values().stream().allMatch(Set::isEmpty));
    }

    public void testMoveShardWithDefaultRole() {

        var inSync = randomList(2, 2, UUIDs::randomBase64UUID);
        var indexMetadata = IndexMetadata.builder("index")
            .settings(indexSettings(Version.CURRENT, 1, 1))
            .putInSyncAllocationIds(0, Set.copyOf(inSync))
            .build();

        var shardId = new ShardId(indexMetadata.getIndex(), 0);

        var indexRoutingTable = IndexRoutingTable.builder(indexMetadata.getIndex())
            .addShard(TestShardRouting.newShardRouting(shardId, "node-1", null, true, STARTED, ShardRouting.Role.DEFAULT))
            .addShard(TestShardRouting.newShardRouting(shardId, "node-2", null, false, STARTED, ShardRouting.Role.DEFAULT))
            .build();

        var node1 = newNode("node-1");
        var node2 = newNode("node-2");
        var node3 = newNode("node-3");

        var clusterState = ClusterState.builder(ClusterName.DEFAULT)
            .metadata(Metadata.builder().put(indexMetadata, false).build())
            .nodes(DiscoveryNodes.builder().add(node1).add(node2).add(node3).build())
            .routingTable(RoutingTable.builder().add(indexRoutingTable).build())
            .build();

        var routingNodes = clusterState.getRoutingNodes().mutableCopy();

        routingNodes.relocateShard(routingNodes.node("node-1").getByShardId(shardId), "node-3", 0L, new RoutingChangesObserver() {
        });

        assertThat(routingNodes.node("node-1").getByShardId(shardId).state(), equalTo(RELOCATING));
        assertThat(routingNodes.node("node-2").getByShardId(shardId).state(), equalTo(STARTED));
        assertThat(routingNodes.node("node-3").getByShardId(shardId).state(), equalTo(INITIALIZING));
    }

    public void testMoveShardWithPromotableOnlyRole() {

        var inSync = randomList(2, 2, UUIDs::randomBase64UUID);
        var indexMetadata = IndexMetadata.builder("index")
            .settings(indexSettings(Version.CURRENT, 1, 1))
            .putInSyncAllocationIds(0, Set.copyOf(inSync))
            .build();

        var shardId = new ShardId(indexMetadata.getIndex(), 0);

        var indexRoutingTable = IndexRoutingTable.builder(indexMetadata.getIndex())
            .addShard(TestShardRouting.newShardRouting(shardId, "node-1", null, true, STARTED, ShardRouting.Role.INDEX_ONLY))
            .addShard(TestShardRouting.newShardRouting(shardId, "node-2", null, false, STARTED, ShardRouting.Role.SEARCH_ONLY))
            .build();

        var node1 = newNode("node-1");
        var node2 = newNode("node-2");
        var node3 = newNode("node-3");

        var clusterState = ClusterState.builder(ClusterName.DEFAULT)
            .metadata(Metadata.builder().put(indexMetadata, false).build())
            .nodes(DiscoveryNodes.builder().add(node1).add(node2).add(node3).build())
            .routingTable(RoutingTable.builder().add(indexRoutingTable).build())
            .build();

        var routingNodes = clusterState.getRoutingNodes().mutableCopy();

        routingNodes.relocateShard(routingNodes.node("node-1").getByShardId(shardId), "node-3", 0L, new RoutingChangesObserver() {
        });

        assertThat(routingNodes.node("node-1").getByShardId(shardId), nullValue());
        assertThat(routingNodes.node("node-2").getByShardId(shardId), nullValue());
        assertThat(routingNodes.node("node-3").getByShardId(shardId).state(), equalTo(INITIALIZING));
        assertThat(routingNodes.unassigned().ignored(), hasSize(1));
    }

    private boolean assertShardStats(RoutingNodes routingNodes) {
        return RoutingNodes.assertShardStats(routingNodes);
    }
}
