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

import com.carrotsearch.hppc.cursors.ObjectCursor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.util.ArrayUtil;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ESAllocationTestCase;
import org.elasticsearch.cluster.EmptyClusterInfoService;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.RoutingNodes;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.allocation.allocator.BalancedShardsAllocator;
import org.elasticsearch.cluster.routing.allocation.allocator.ShardsAllocator;
import org.elasticsearch.cluster.routing.allocation.decider.ClusterRebalanceAllocationDecider;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.gateway.TestGatewayAllocator;
import org.hamcrest.Matchers;

import java.util.stream.Collectors;

import static org.elasticsearch.cluster.routing.ShardRoutingState.STARTED;
import static org.elasticsearch.cluster.routing.ShardRoutingState.UNASSIGNED;

public class BalanceConfigurationTests extends ESAllocationTestCase {

    private final Logger logger = LogManager.getLogger(BalanceConfigurationTests.class);
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
        settings.put(ClusterRebalanceAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ALLOW_REBALANCE_SETTING.getKey(),
            ClusterRebalanceAllocationDecider.ClusterRebalanceType.ALWAYS.toString());
        settings.put(BalancedShardsAllocator.INDEX_BALANCE_FACTOR_SETTING.getKey(), indexBalance);
        settings.put(BalancedShardsAllocator.SHARD_BALANCE_FACTOR_SETTING.getKey(), replicaBalance);
        settings.put(BalancedShardsAllocator.THRESHOLD_SETTING.getKey(), balanceThreshold);

        AllocationService strategy = createAllocationService(settings.build(), new TestGatewayAllocator());

        ClusterState clusterState = initCluster(strategy);
        assertIndexBalance(clusterState.getRoutingTable(), clusterState.getRoutingNodes(), numberOfNodes, numberOfIndices,
            numberOfReplicas, numberOfShards, balanceThreshold);

        clusterState = addNode(clusterState, strategy);
        assertIndexBalance(clusterState.getRoutingTable(), clusterState.getRoutingNodes(), numberOfNodes + 1,
            numberOfIndices, numberOfReplicas, numberOfShards, balanceThreshold);

        clusterState = removeNodes(clusterState, strategy);
        assertIndexBalance(clusterState.getRoutingTable(), clusterState.getRoutingNodes(),
            (numberOfNodes + 1) - (numberOfNodes + 1) / 2, numberOfIndices, numberOfReplicas, numberOfShards, balanceThreshold);
    }

    public void testReplicaBalance() {
        /* Tests balance over replicas only */
        final float indexBalance = 0.0f;
        final float replicaBalance = 1.0f;
        final float balanceThreshold = 1.0f;

        Settings.Builder settings = Settings.builder();
        settings.put(ClusterRebalanceAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ALLOW_REBALANCE_SETTING.getKey(),
            ClusterRebalanceAllocationDecider.ClusterRebalanceType.ALWAYS.toString());
        settings.put(BalancedShardsAllocator.INDEX_BALANCE_FACTOR_SETTING.getKey(), indexBalance);
        settings.put(BalancedShardsAllocator.SHARD_BALANCE_FACTOR_SETTING.getKey(), replicaBalance);
        settings.put(BalancedShardsAllocator.THRESHOLD_SETTING.getKey(), balanceThreshold);

        AllocationService strategy = createAllocationService(settings.build(), new TestGatewayAllocator());

        ClusterState clusterState = initCluster(strategy);
        assertReplicaBalance(clusterState.getRoutingNodes(), numberOfNodes, numberOfIndices,
            numberOfReplicas, numberOfShards, balanceThreshold);

        clusterState = addNode(clusterState, strategy);
        assertReplicaBalance(clusterState.getRoutingNodes(), numberOfNodes + 1,
            numberOfIndices, numberOfReplicas, numberOfShards, balanceThreshold);

        clusterState = removeNodes(clusterState, strategy);
        assertReplicaBalance(clusterState.getRoutingNodes(),
            numberOfNodes + 1 - (numberOfNodes + 1) / 2, numberOfIndices, numberOfReplicas, numberOfShards, balanceThreshold);
    }

    private ClusterState initCluster(AllocationService strategy) {
        Metadata.Builder metadataBuilder = Metadata.builder();
        RoutingTable.Builder routingTableBuilder = RoutingTable.builder();

        for (int i = 0; i < numberOfIndices; i++) {
            IndexMetadata.Builder index = IndexMetadata.builder("test" + i).settings(settings(Version.CURRENT))
                .numberOfShards(numberOfShards).numberOfReplicas(numberOfReplicas);
            metadataBuilder = metadataBuilder.put(index);
        }

        Metadata metadata = metadataBuilder.build();

        for (ObjectCursor<IndexMetadata> cursor : metadata.indices().values()) {
            routingTableBuilder.addAsNew(cursor.value);
        }

        RoutingTable initialRoutingTable = routingTableBuilder.build();


        logger.info("start " + numberOfNodes + " nodes");
        DiscoveryNodes.Builder nodes = DiscoveryNodes.builder();
        for (int i = 0; i < numberOfNodes; i++) {
            nodes.add(newNode("node" + i));
        }
        ClusterState clusterState = ClusterState.builder(org.elasticsearch.cluster.ClusterName.CLUSTER_NAME_SETTING
            .getDefault(Settings.EMPTY)).nodes(nodes).metadata(metadata).routingTable(initialRoutingTable).build();
        clusterState = strategy.reroute(clusterState, "reroute");

        logger.info("restart all the primary shards, replicas will start initializing");
        clusterState = startInitializingShardsAndReroute(strategy, clusterState);

        logger.info("start the replica shards");
        clusterState = startInitializingShardsAndReroute(strategy, clusterState);

        logger.info("complete rebalancing");
        return applyStartedShardsUntilNoChange(clusterState, strategy);
    }

    private ClusterState addNode(ClusterState clusterState, AllocationService strategy) {
        logger.info("now, start 1 more node, check that rebalancing will happen because we set it to always");
        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder(clusterState.nodes())
                .add(newNode("node" + numberOfNodes)))
                .build();

        RoutingTable routingTable = strategy.reroute(clusterState, "reroute").routingTable();
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
        clusterState = strategy.reroute(clusterState, "reroute");

        logger.info("complete rebalancing");
        return applyStartedShardsUntilNoChange(clusterState, strategy);
    }


    private void assertReplicaBalance(RoutingNodes nodes, int numberOfNodes, int numberOfIndices, int numberOfReplicas,
        int numberOfShards, float threshold) {
        final int unassigned = nodes.unassigned().size();

        if (unassigned > 0) {
            // Ensure that if there any unassigned shards, all of their replicas are unassigned as well
            // (i.e. unassigned count is always [replicas] + 1 for each shard unassigned shardId)
            nodes.shardsWithState(UNASSIGNED).stream().collect(
                Collectors.toMap(
                    ShardRouting::shardId,
                    s -> 1,
                    (a, b) -> a + b
            )).values().forEach(
                count -> assertEquals(numberOfReplicas + 1, count.longValue())
            );
        }
        assertEquals(numberOfNodes, nodes.size());

        final int numShards = numberOfIndices * numberOfShards * (numberOfReplicas + 1) - unassigned;
        final float avgNumShards = (float) (numShards) / (float) (numberOfNodes);
        final int minAvgNumberOfShards = Math.round(Math.round(Math.floor(avgNumShards - threshold)));
        final int maxAvgNumberOfShards = Math.round(Math.round(Math.ceil(avgNumShards + threshold)));

        for (RoutingNode node : nodes) {
            assertThat(node.shardsWithState(STARTED).size(), Matchers.greaterThanOrEqualTo(minAvgNumberOfShards));
            assertThat(node.shardsWithState(STARTED).size(), Matchers.lessThanOrEqualTo(maxAvgNumberOfShards));
        }
    }

    private void assertIndexBalance(RoutingTable routingTable, RoutingNodes nodes, int numberOfNodes, int numberOfIndices,
                                    int numberOfReplicas, int numberOfShards, float threshold) {

        final int numShards = numberOfShards * (numberOfReplicas + 1);
        final float avgNumShards = (float) (numShards) / (float) (numberOfNodes);
        final int minAvgNumberOfShards = Math.round(Math.round(Math.floor(avgNumShards - threshold)));
        final int maxAvgNumberOfShards = Math.round(Math.round(Math.ceil(avgNumShards + threshold)));

        for (ObjectCursor<String> index : routingTable.indicesRouting().keys()) {
            for (RoutingNode node : nodes) {
                assertThat(node.shardsWithState(index.value, STARTED).size(), Matchers.greaterThanOrEqualTo(minAvgNumberOfShards));
                assertThat(node.shardsWithState(index.value, STARTED).size(), Matchers.lessThanOrEqualTo(maxAvgNumberOfShards));
            }
        }
    }

    public void testPersistedSettings() {
        Settings.Builder settings = Settings.builder();
        settings.put(BalancedShardsAllocator.INDEX_BALANCE_FACTOR_SETTING.getKey(), 0.2);
        settings.put(BalancedShardsAllocator.SHARD_BALANCE_FACTOR_SETTING.getKey(), 0.3);
        settings.put(BalancedShardsAllocator.THRESHOLD_SETTING.getKey(), 2.0);
        ClusterSettings service = new ClusterSettings(Settings.builder().build(), ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        BalancedShardsAllocator allocator = new BalancedShardsAllocator(settings.build(), service);
        assertThat(allocator.getIndexBalance(), Matchers.equalTo(0.2f));
        assertThat(allocator.getShardBalance(), Matchers.equalTo(0.3f));
        assertThat(allocator.getThreshold(), Matchers.equalTo(2.0f));

        settings = Settings.builder();
        settings.put(BalancedShardsAllocator.INDEX_BALANCE_FACTOR_SETTING.getKey(), 0.2);
        settings.put(BalancedShardsAllocator.SHARD_BALANCE_FACTOR_SETTING.getKey(), 0.3);
        settings.put(BalancedShardsAllocator.THRESHOLD_SETTING.getKey(), 2.0);
        settings.put(ClusterRebalanceAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ALLOW_REBALANCE_SETTING.getKey(),
            ClusterRebalanceAllocationDecider.ClusterRebalanceType.ALWAYS.toString());
        service.applySettings(settings.build());
        assertThat(allocator.getIndexBalance(), Matchers.equalTo(0.2f));
        assertThat(allocator.getShardBalance(), Matchers.equalTo(0.3f));
        assertThat(allocator.getThreshold(), Matchers.equalTo(2.0f));

        settings = Settings.builder();
        settings.put(BalancedShardsAllocator.INDEX_BALANCE_FACTOR_SETTING.getKey(), 0.5);
        settings.put(BalancedShardsAllocator.SHARD_BALANCE_FACTOR_SETTING.getKey(), 0.1);
        settings.put(BalancedShardsAllocator.THRESHOLD_SETTING.getKey(), 3.0);
        service.applySettings(settings.build());
        assertThat(allocator.getIndexBalance(), Matchers.equalTo(0.5f));
        assertThat(allocator.getShardBalance(), Matchers.equalTo(0.1f));
        assertThat(allocator.getThreshold(), Matchers.equalTo(3.0f));
    }

    public void testNoRebalanceOnPrimaryOverload() {
        Settings.Builder settings = Settings.builder();
        AllocationService strategy = new AllocationService(randomAllocationDeciders(settings.build(),
                new ClusterSettings(Settings.Builder.EMPTY_SETTINGS, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS), random()),
                new TestGatewayAllocator(), new ShardsAllocator() {
            /*
             *  // this allocator tries to rebuild this scenario where a rebalance is
             *  // triggered solely by the primary overload on node [1] where a shard
             *  // is rebalanced to node 0
                routing_nodes:
                -----node_id[0][V]
                --------[test][0], node[0], [R], s[STARTED]
                --------[test][4], node[0], [R], s[STARTED]
                -----node_id[1][V]
                --------[test][0], node[1], [P], s[STARTED]
                --------[test][1], node[1], [P], s[STARTED]
                --------[test][3], node[1], [R], s[STARTED]
                -----node_id[2][V]
                --------[test][1], node[2], [R], s[STARTED]
                --------[test][2], node[2], [R], s[STARTED]
                --------[test][4], node[2], [P], s[STARTED]
                -----node_id[3][V]
                --------[test][2], node[3], [P], s[STARTED]
                --------[test][3], node[3], [P], s[STARTED]
                ---- unassigned
            */
            public void allocate(RoutingAllocation allocation) {
                RoutingNodes.UnassignedShards unassigned = allocation.routingNodes().unassigned();
                ShardRouting[] drain = unassigned.drain();
                ArrayUtil.timSort(drain, (a, b) -> { return a.primary() ? -1 : 1; }); // we have to allocate primaries first
                for (ShardRouting sr : drain) {
                    switch (sr.id()) {
                        case 0:
                            if (sr.primary()) {
                                allocation.routingNodes().initializeShard(sr, "node1", null, -1, allocation.changes());
                            } else {
                                allocation.routingNodes().initializeShard(sr, "node0", null, -1, allocation.changes());
                            }
                            break;
                        case 1:
                            if (sr.primary()) {
                                allocation.routingNodes().initializeShard(sr, "node1", null, -1, allocation.changes());
                            } else {
                                allocation.routingNodes().initializeShard(sr, "node2", null, -1, allocation.changes());
                            }
                            break;
                        case 2:
                            if (sr.primary()) {
                                allocation.routingNodes().initializeShard(sr, "node3", null, -1, allocation.changes());
                            } else {
                                allocation.routingNodes().initializeShard(sr, "node2", null, -1, allocation.changes());
                            }
                            break;
                        case 3:
                            if (sr.primary()) {
                                allocation.routingNodes().initializeShard(sr, "node3", null, -1, allocation.changes());
                            } else {
                                allocation.routingNodes().initializeShard(sr, "node1", null, -1, allocation.changes());
                            }
                            break;
                        case 4:
                            if (sr.primary()) {
                                allocation.routingNodes().initializeShard(sr, "node2", null, -1, allocation.changes());
                            } else {
                                allocation.routingNodes().initializeShard(sr, "node0", null, -1, allocation.changes());
                            }
                            break;
                    }

                }
            }

            @Override
            public ShardAllocationDecision decideShardAllocation(ShardRouting shard, RoutingAllocation allocation) {
                throw new UnsupportedOperationException("explain not supported");
            }
        }, EmptyClusterInfoService.INSTANCE);
        Metadata.Builder metadataBuilder = Metadata.builder();
        RoutingTable.Builder routingTableBuilder = RoutingTable.builder();
        IndexMetadata.Builder indexMeta = IndexMetadata.builder("test")
            .settings(settings(Version.CURRENT)).numberOfShards(5).numberOfReplicas(1);
        metadataBuilder = metadataBuilder.put(indexMeta);
        Metadata metadata = metadataBuilder.build();
        for (ObjectCursor<IndexMetadata> cursor : metadata.indices().values()) {
            routingTableBuilder.addAsNew(cursor.value);
        }
        RoutingTable routingTable = routingTableBuilder.build();
        DiscoveryNodes.Builder nodes = DiscoveryNodes.builder();
        for (int i = 0; i < 4; i++) {
            DiscoveryNode node = newNode("node" + i);
            nodes.add(node);
        }

        ClusterState clusterState = ClusterState.builder(org.elasticsearch.cluster.ClusterName.CLUSTER_NAME_SETTING
            .getDefault(Settings.EMPTY)).nodes(nodes).metadata(metadata).routingTable(routingTable).build();
        routingTable = strategy.reroute(clusterState, "reroute").routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();
        RoutingNodes routingNodes = clusterState.getRoutingNodes();

        for (RoutingNode routingNode : routingNodes) {
            for (ShardRouting shardRouting : routingNode) {
                assertThat(shardRouting.state(), Matchers.equalTo(ShardRoutingState.INITIALIZING));
            }
        }
        strategy = createAllocationService(settings.build(), new TestGatewayAllocator());

        logger.info("use the new allocator and check if it moves shards");
        routingTable = startInitializingShardsAndReroute(strategy, clusterState).routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();
        routingNodes = clusterState.getRoutingNodes();
        for (RoutingNode routingNode : routingNodes) {
            for (ShardRouting shardRouting : routingNode) {
                assertThat(shardRouting.state(), Matchers.equalTo(ShardRoutingState.STARTED));
            }
        }

        logger.info("start the replica shards");
        routingTable = startInitializingShardsAndReroute(strategy, clusterState).routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();
        routingNodes = clusterState.getRoutingNodes();

        for (RoutingNode routingNode : routingNodes) {
            for (ShardRouting shardRouting : routingNode) {
                assertThat(shardRouting.state(), Matchers.equalTo(ShardRoutingState.STARTED));
            }
        }

        logger.info("rebalancing");
        routingTable = strategy.reroute(clusterState, "reroute").routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();
        routingNodes = clusterState.getRoutingNodes();

        for (RoutingNode routingNode : routingNodes) {
            for (ShardRouting shardRouting : routingNode) {
                assertThat(shardRouting.state(), Matchers.equalTo(ShardRoutingState.STARTED));
            }
        }
    }

}
