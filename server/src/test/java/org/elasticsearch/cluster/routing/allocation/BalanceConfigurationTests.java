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
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.RoutingNodes;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.allocation.allocator.BalancedShardsAllocator;
import org.elasticsearch.cluster.routing.allocation.allocator.BalancerSettings;
import org.elasticsearch.cluster.routing.allocation.decider.ClusterRebalanceAllocationDecider;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexVersion;
import org.hamcrest.Matchers;

import java.util.stream.Collectors;

import static org.elasticsearch.cluster.routing.RoutingNodesHelper.shardsWithState;
import static org.elasticsearch.cluster.routing.ShardRoutingState.STARTED;
import static org.elasticsearch.cluster.routing.ShardRoutingState.UNASSIGNED;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

public class BalanceConfigurationTests extends ESAllocationTestCase {

    // TODO maybe we can randomize these numbers somehow
    final int numberOfNodes = 25;
    final int numberOfIndices = 12;
    final int numberOfShards = 2;
    final int numberOfReplicas = 2;

    public void testIndexBalance() {
        /* Tests balance over indices only */
        final float indexBalance = 1.0f;
        final float replicaBalance = 0.0f;
        final float balanceThreshold = 1.0f;

        Settings.Builder settings = Settings.builder();
        settings.put(
            ClusterRebalanceAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ALLOW_REBALANCE_SETTING.getKey(),
            ClusterRebalanceAllocationDecider.ClusterRebalanceType.ALWAYS.toString()
        );
        settings.put(BalancedShardsAllocator.INDEX_BALANCE_FACTOR_SETTING.getKey(), indexBalance);
        settings.put(BalancedShardsAllocator.SHARD_BALANCE_FACTOR_SETTING.getKey(), replicaBalance);
        settings.put(BalancedShardsAllocator.THRESHOLD_SETTING.getKey(), balanceThreshold);

        AllocationService strategy = createAllocationService(settings.build());

        ClusterState clusterState = initCluster(strategy);
        assertIndexBalance(
            clusterState.getRoutingTable(),
            clusterState.getRoutingNodes(),
            numberOfNodes,
            numberOfIndices,
            numberOfReplicas,
            numberOfShards,
            balanceThreshold
        );

        clusterState = addNode(clusterState, strategy);
        assertIndexBalance(
            clusterState.getRoutingTable(),
            clusterState.getRoutingNodes(),
            numberOfNodes + 1,
            numberOfIndices,
            numberOfReplicas,
            numberOfShards,
            balanceThreshold
        );

        clusterState = removeNodes(clusterState, strategy);
        assertIndexBalance(
            clusterState.getRoutingTable(),
            clusterState.getRoutingNodes(),
            (numberOfNodes + 1) - (numberOfNodes + 1) / 2,
            numberOfIndices,
            numberOfReplicas,
            numberOfShards,
            balanceThreshold
        );
    }

    public void testReplicaBalance() {
        /* Tests balance over replicas only */
        final float indexBalance = 0.0f;
        final float replicaBalance = 1.0f;
        final float balanceThreshold = 1.0f;

        Settings.Builder settings = Settings.builder();
        settings.put(
            ClusterRebalanceAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ALLOW_REBALANCE_SETTING.getKey(),
            ClusterRebalanceAllocationDecider.ClusterRebalanceType.ALWAYS.toString()
        );
        settings.put(BalancedShardsAllocator.INDEX_BALANCE_FACTOR_SETTING.getKey(), indexBalance);
        settings.put(BalancedShardsAllocator.SHARD_BALANCE_FACTOR_SETTING.getKey(), replicaBalance);
        settings.put(BalancedShardsAllocator.THRESHOLD_SETTING.getKey(), balanceThreshold);

        AllocationService strategy = createAllocationService(settings.build());

        ClusterState clusterState = initCluster(strategy);
        assertReplicaBalance(
            clusterState.getRoutingNodes(),
            numberOfNodes,
            numberOfIndices,
            numberOfReplicas,
            numberOfShards,
            balanceThreshold
        );

        clusterState = addNode(clusterState, strategy);
        assertReplicaBalance(
            clusterState.getRoutingNodes(),
            numberOfNodes + 1,
            numberOfIndices,
            numberOfReplicas,
            numberOfShards,
            balanceThreshold
        );

        clusterState = removeNodes(clusterState, strategy);
        assertReplicaBalance(
            clusterState.getRoutingNodes(),
            numberOfNodes + 1 - (numberOfNodes + 1) / 2,
            numberOfIndices,
            numberOfReplicas,
            numberOfShards,
            balanceThreshold
        );
    }

    private ClusterState initCluster(AllocationService strategy) {
        Metadata.Builder metadataBuilder = Metadata.builder();
        RoutingTable.Builder routingTableBuilder = RoutingTable.builder(TestShardRoutingRoleStrategies.DEFAULT_ROLE_ONLY);

        for (int i = 0; i < numberOfIndices; i++) {
            IndexMetadata.Builder index = IndexMetadata.builder("test" + i)
                .settings(settings(IndexVersion.current()))
                .numberOfShards(numberOfShards)
                .numberOfReplicas(numberOfReplicas);
            metadataBuilder = metadataBuilder.put(index);
        }

        Metadata metadata = metadataBuilder.build();

        for (IndexMetadata indexMetadata : metadata.getProject().indices().values()) {
            routingTableBuilder.addAsNew(indexMetadata);
        }

        RoutingTable initialRoutingTable = routingTableBuilder.build();

        logger.info("start " + numberOfNodes + " nodes");
        DiscoveryNodes.Builder nodes = DiscoveryNodes.builder();
        for (int i = 0; i < numberOfNodes; i++) {
            nodes.add(newNode("node" + i));
        }
        ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT)
            .nodes(nodes)
            .metadata(metadata)
            .routingTable(initialRoutingTable)
            .build();
        clusterState = strategy.reroute(clusterState, "reroute", ActionListener.noop());

        logger.info("restart all the primary shards, replicas will start initializing");
        clusterState = startInitializingShardsAndReroute(strategy, clusterState);

        logger.info("start the replica shards");
        clusterState = startInitializingShardsAndReroute(strategy, clusterState);

        logger.info("complete rebalancing");
        return applyStartedShardsUntilNoChange(clusterState, strategy);
    }

    private ClusterState addNode(ClusterState clusterState, AllocationService strategy) {
        logger.info("now, start 1 more node, check that rebalancing will happen because we set it to always");
        clusterState = ClusterState.builder(clusterState)
            .nodes(DiscoveryNodes.builder(clusterState.nodes()).add(newNode("node" + numberOfNodes)))
            .build();

        RoutingTable routingTable = strategy.reroute(clusterState, "reroute", ActionListener.noop()).routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();

        // move initializing to started
        return applyStartedShardsUntilNoChange(clusterState, strategy);
    }

    private ClusterState removeNodes(ClusterState clusterState, AllocationService strategy) {
        logger.info("Removing half the nodes (" + (numberOfNodes + 1) / 2 + ")");
        DiscoveryNodes.Builder nodes = DiscoveryNodes.builder(clusterState.nodes());

        boolean removed = false;
        for (int i = (numberOfNodes + 1) / 2; i <= numberOfNodes; i++) {
            nodes.remove("node" + i);
            removed = true;
        }

        clusterState = ClusterState.builder(clusterState).nodes(nodes.build()).build();
        if (removed) {
            clusterState = strategy.disassociateDeadNodes(clusterState, randomBoolean(), "removed nodes");
        }

        logger.info("start all the primary shards, replicas will start initializing");
        clusterState = startInitializingShardsAndReroute(strategy, clusterState);

        logger.info("start the replica shards");
        clusterState = startInitializingShardsAndReroute(strategy, clusterState);

        logger.info("rebalancing");
        clusterState = strategy.reroute(clusterState, "reroute", ActionListener.noop());

        logger.info("complete rebalancing");
        return applyStartedShardsUntilNoChange(clusterState, strategy);
    }

    private void assertReplicaBalance(
        RoutingNodes nodes,
        int numberOfNodes,
        int numberOfIndices,
        int numberOfReplicas,
        int numberOfShards,
        float threshold
    ) {
        final int unassigned = nodes.unassigned().size();

        if (unassigned > 0) {
            // Ensure that if there any unassigned shards, all of their replicas are unassigned as well
            // (i.e. unassigned count is always [replicas] + 1 for each shard unassigned shardId)
            shardsWithState(nodes, UNASSIGNED).stream()
                .collect(Collectors.toMap(ShardRouting::shardId, s -> 1, (a, b) -> a + b))
                .values()
                .forEach(count -> assertEquals(numberOfReplicas + 1, count.longValue()));
        }
        assertEquals(numberOfNodes, nodes.size());

        final int numShards = numberOfIndices * numberOfShards * (numberOfReplicas + 1) - unassigned;
        final float avgNumShards = (float) (numShards) / (float) (numberOfNodes);
        final int minAvgNumberOfShards = Math.round(Math.round(Math.floor(avgNumShards - threshold)));
        final int maxAvgNumberOfShards = Math.round(Math.round(Math.ceil(avgNumShards + threshold)));

        for (RoutingNode node : nodes) {
            assertThat(node.numberOfShardsWithState(STARTED), greaterThanOrEqualTo(minAvgNumberOfShards));
            assertThat(node.numberOfShardsWithState(STARTED), lessThanOrEqualTo(maxAvgNumberOfShards));
        }
    }

    private void assertIndexBalance(
        RoutingTable routingTable,
        RoutingNodes nodes,
        int numberOfNodes,
        int numberOfIndices,
        int numberOfReplicas,
        int numberOfShards,
        float threshold
    ) {

        final int numShards = numberOfShards * (numberOfReplicas + 1);
        final float avgNumShards = (float) (numShards) / (float) (numberOfNodes);
        final int minAvgNumberOfShards = Math.round(Math.round(Math.floor(avgNumShards - threshold)));
        final int maxAvgNumberOfShards = Math.round(Math.round(Math.ceil(avgNumShards + threshold)));

        for (String index : routingTable.indicesRouting().keySet()) {
            for (RoutingNode node : nodes) {
                assertThat(Math.toIntExact(node.shardsWithState(index, STARTED).count()), greaterThanOrEqualTo(minAvgNumberOfShards));
                assertThat(Math.toIntExact(node.shardsWithState(index, STARTED).count()), lessThanOrEqualTo(maxAvgNumberOfShards));
            }
        }
    }

    public void testPersistedSettings() {
        Settings.Builder settings = Settings.builder();
        settings.put(BalancedShardsAllocator.INDEX_BALANCE_FACTOR_SETTING.getKey(), 0.2);
        settings.put(BalancedShardsAllocator.SHARD_BALANCE_FACTOR_SETTING.getKey(), 0.3);
        settings.put(BalancedShardsAllocator.THRESHOLD_SETTING.getKey(), 2.0);
        ClusterSettings clusterSettings = new ClusterSettings(settings.build(), ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        BalancerSettings balancerSettings = new BalancerSettings(clusterSettings);
        assertThat(balancerSettings.getIndexBalanceFactor(), Matchers.equalTo(0.2f));
        assertThat(balancerSettings.getShardBalanceFactor(), Matchers.equalTo(0.3f));
        assertThat(balancerSettings.getThreshold(), Matchers.equalTo(2.0f));

        settings = Settings.builder();
        settings.put(BalancedShardsAllocator.INDEX_BALANCE_FACTOR_SETTING.getKey(), 0.2);
        settings.put(BalancedShardsAllocator.SHARD_BALANCE_FACTOR_SETTING.getKey(), 0.3);
        settings.put(BalancedShardsAllocator.THRESHOLD_SETTING.getKey(), 2.0);
        settings.put(
            ClusterRebalanceAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ALLOW_REBALANCE_SETTING.getKey(),
            ClusterRebalanceAllocationDecider.ClusterRebalanceType.ALWAYS.toString()
        );
        clusterSettings.applySettings(settings.build());
        assertThat(balancerSettings.getIndexBalanceFactor(), Matchers.equalTo(0.2f));
        assertThat(balancerSettings.getShardBalanceFactor(), Matchers.equalTo(0.3f));
        assertThat(balancerSettings.getThreshold(), Matchers.equalTo(2.0f));

        settings = Settings.builder();
        settings.put(BalancedShardsAllocator.INDEX_BALANCE_FACTOR_SETTING.getKey(), 0.5);
        settings.put(BalancedShardsAllocator.SHARD_BALANCE_FACTOR_SETTING.getKey(), 0.1);
        settings.put(BalancedShardsAllocator.THRESHOLD_SETTING.getKey(), 3.0);
        clusterSettings.applySettings(settings.build());
        assertThat(balancerSettings.getIndexBalanceFactor(), Matchers.equalTo(0.5f));
        assertThat(balancerSettings.getShardBalanceFactor(), Matchers.equalTo(0.1f));
        assertThat(balancerSettings.getThreshold(), Matchers.equalTo(3.0f));
    }
}
