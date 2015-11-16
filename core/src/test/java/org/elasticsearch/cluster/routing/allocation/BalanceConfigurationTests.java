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

import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.EmptyClusterInfoService;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.RoutingNodes;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.allocation.allocator.BalancedShardsAllocator;
import org.elasticsearch.cluster.routing.allocation.allocator.ShardsAllocator;
import org.elasticsearch.cluster.routing.allocation.allocator.ShardsAllocators;
import org.elasticsearch.cluster.routing.allocation.decider.ClusterRebalanceAllocationDecider;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.node.settings.NodeSettingsService;
import org.elasticsearch.test.ESAllocationTestCase;
import org.elasticsearch.test.gateway.NoopGatewayAllocator;
import org.hamcrest.Matchers;

import static org.elasticsearch.cluster.routing.ShardRoutingState.INITIALIZING;
import static org.elasticsearch.cluster.routing.ShardRoutingState.STARTED;
import static org.elasticsearch.common.settings.Settings.settingsBuilder;

public class BalanceConfigurationTests extends ESAllocationTestCase {

    private final ESLogger logger = Loggers.getLogger(BalanceConfigurationTests.class);
    // TODO maybe we can randomize these numbers somehow
    final int numberOfNodes = 25;
    final int numberOfIndices = 12;
    final int numberOfShards = 2;
    final int numberOfReplicas = 2;

    public void testIndexBalance() {
        /* Tests balance over indices only */
        final float indexBalance = 1.0f;
        final float replicaBalance = 0.0f;
        final float balanceTreshold = 1.0f;

        Settings.Builder settings = settingsBuilder();
        settings.put(ClusterRebalanceAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ALLOW_REBALANCE, ClusterRebalanceAllocationDecider.ClusterRebalanceType.ALWAYS.toString());
        settings.put(BalancedShardsAllocator.SETTING_INDEX_BALANCE_FACTOR, indexBalance);
        settings.put(BalancedShardsAllocator.SETTING_SHARD_BALANCE_FACTOR, replicaBalance);
        settings.put(BalancedShardsAllocator.SETTING_THRESHOLD, balanceTreshold);

        AllocationService strategy = createAllocationService(settings.build());

        ClusterState clusterState = initCluster(strategy);
        assertIndexBalance(logger, clusterState.getRoutingNodes(), numberOfNodes, numberOfIndices, numberOfReplicas, numberOfShards, balanceTreshold);

        clusterState = addNode(clusterState, strategy);
        assertIndexBalance(logger, clusterState.getRoutingNodes(), numberOfNodes + 1, numberOfIndices, numberOfReplicas, numberOfShards, balanceTreshold);

        clusterState = removeNodes(clusterState, strategy);
        assertIndexBalance(logger, clusterState.getRoutingNodes(), (numberOfNodes + 1) - (numberOfNodes + 1) / 2, numberOfIndices, numberOfReplicas, numberOfShards, balanceTreshold);

    }

    public void testReplicaBalance() {
        /* Tests balance over replicas only */
        final float indexBalance = 0.0f;
        final float replicaBalance = 1.0f;
        final float balanceTreshold = 1.0f;

        Settings.Builder settings = settingsBuilder();
        settings.put(ClusterRebalanceAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ALLOW_REBALANCE, ClusterRebalanceAllocationDecider.ClusterRebalanceType.ALWAYS.toString());
        settings.put(BalancedShardsAllocator.SETTING_INDEX_BALANCE_FACTOR, indexBalance);
        settings.put(BalancedShardsAllocator.SETTING_SHARD_BALANCE_FACTOR, replicaBalance);
        settings.put(BalancedShardsAllocator.SETTING_THRESHOLD, balanceTreshold);

        AllocationService strategy = createAllocationService(settings.build());

        ClusterState clusterState = initCluster(strategy);
        assertReplicaBalance(logger, clusterState.getRoutingNodes(), numberOfNodes, numberOfIndices, numberOfReplicas, numberOfShards, balanceTreshold);

        clusterState = addNode(clusterState, strategy);
        assertReplicaBalance(logger, clusterState.getRoutingNodes(), numberOfNodes + 1, numberOfIndices, numberOfReplicas, numberOfShards, balanceTreshold);

        clusterState = removeNodes(clusterState, strategy);
        assertReplicaBalance(logger, clusterState.getRoutingNodes(), (numberOfNodes + 1) - (numberOfNodes + 1) / 2, numberOfIndices, numberOfReplicas, numberOfShards, balanceTreshold);

    }

    private ClusterState initCluster(AllocationService strategy) {
        MetaData.Builder metaDataBuilder = MetaData.builder();
        RoutingTable.Builder routingTableBuilder = RoutingTable.builder();

        for (int i = 0; i < numberOfIndices; i++) {
            IndexMetaData.Builder index = IndexMetaData.builder("test" + i).settings(settings(Version.CURRENT)).numberOfShards(numberOfShards).numberOfReplicas(numberOfReplicas);
            metaDataBuilder = metaDataBuilder.put(index);
        }

        MetaData metaData = metaDataBuilder.build();

        for (ObjectCursor<IndexMetaData> cursor : metaData.indices().values()) {
            routingTableBuilder.addAsNew(cursor.value);
        }

        RoutingTable routingTable = routingTableBuilder.build();


        logger.info("start " + numberOfNodes + " nodes");
        DiscoveryNodes.Builder nodes = DiscoveryNodes.builder();
        for (int i = 0; i < numberOfNodes; i++) {
            nodes.put(newNode("node" + i));
        }
        ClusterState clusterState = ClusterState.builder(org.elasticsearch.cluster.ClusterName.DEFAULT).nodes(nodes).metaData(metaData).routingTable(routingTable).build();
        routingTable = strategy.reroute(clusterState, "reroute").routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();
        RoutingNodes routingNodes = clusterState.getRoutingNodes();

        logger.info("restart all the primary shards, replicas will start initializing");
        routingNodes = clusterState.getRoutingNodes();
        routingTable = strategy.applyStartedShards(clusterState, routingNodes.shardsWithState(INITIALIZING)).routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();
        routingNodes = clusterState.getRoutingNodes();

        logger.info("start the replica shards");
        routingNodes = clusterState.getRoutingNodes();
        routingTable = strategy.applyStartedShards(clusterState, routingNodes.shardsWithState(INITIALIZING)).routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();
        routingNodes = clusterState.getRoutingNodes();

        logger.info("complete rebalancing");
        RoutingTable prev = routingTable;
        while (true) {
            routingTable = strategy.applyStartedShards(clusterState, routingNodes.shardsWithState(INITIALIZING)).routingTable();
            clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();
            routingNodes = clusterState.getRoutingNodes();
            if (routingTable == prev)
                break;
            prev = routingTable;
        }

        return clusterState;
    }

    private ClusterState addNode(ClusterState clusterState, AllocationService strategy) {
        logger.info("now, start 1 more node, check that rebalancing will happen because we set it to always");
        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder(clusterState.nodes())
                .put(newNode("node" + numberOfNodes)))
                .build();

        RoutingTable routingTable = strategy.reroute(clusterState, "reroute").routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();
        RoutingNodes routingNodes = clusterState.getRoutingNodes();

        // move initializing to started

        RoutingTable prev = routingTable;
        while (true) {
            routingTable = strategy.applyStartedShards(clusterState, routingNodes.shardsWithState(INITIALIZING)).routingTable();
            clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();
            routingNodes = clusterState.getRoutingNodes();
            if (routingTable == prev)
                break;
            prev = routingTable;
        }

        return clusterState;
    }

    private ClusterState removeNodes(ClusterState clusterState, AllocationService strategy) {
        logger.info("Removing half the nodes (" + (numberOfNodes + 1) / 2 + ")");
        DiscoveryNodes.Builder nodes = DiscoveryNodes.builder(clusterState.nodes());

        for (int i = (numberOfNodes + 1) / 2; i <= numberOfNodes; i++) {
            nodes.remove("node" + i);
        }

        clusterState = ClusterState.builder(clusterState).nodes(nodes.build()).build();
        RoutingNodes routingNodes = clusterState.getRoutingNodes();

        logger.info("start all the primary shards, replicas will start initializing");
        RoutingTable routingTable = strategy.applyStartedShards(clusterState, routingNodes.shardsWithState(INITIALIZING)).routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();
        routingNodes = clusterState.getRoutingNodes();

        logger.info("start the replica shards");
        routingTable = strategy.applyStartedShards(clusterState, routingNodes.shardsWithState(INITIALIZING)).routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();
        routingNodes = clusterState.getRoutingNodes();

        logger.info("rebalancing");
        routingTable = strategy.reroute(clusterState, "reroute").routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();
        routingNodes = clusterState.getRoutingNodes();

        logger.info("complete rebalancing");
        RoutingTable prev = routingTable;
        while (true) {
            routingTable = strategy.applyStartedShards(clusterState, routingNodes.shardsWithState(INITIALIZING)).routingTable();
            clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();
            routingNodes = clusterState.getRoutingNodes();
            if (routingTable == prev)
                break;
            prev = routingTable;
        }

        return clusterState;
    }


    private void assertReplicaBalance(ESLogger logger, RoutingNodes nodes, int numberOfNodes, int numberOfIndices, int numberOfReplicas, int numberOfShards, float treshold) {
        final int numShards = numberOfIndices * numberOfShards * (numberOfReplicas + 1);
        final float avgNumShards = (float) (numShards) / (float) (numberOfNodes);
        final int minAvgNumberOfShards = Math.round(Math.round(Math.floor(avgNumShards - treshold)));
        final int maxAvgNumberOfShards = Math.round(Math.round(Math.ceil(avgNumShards + treshold)));

        for (RoutingNode node : nodes) {
//            logger.info(node.nodeId() + ": " + node.shardsWithState(INITIALIZING, STARTED).size() + " shards ("+minAvgNumberOfShards+" to "+maxAvgNumberOfShards+")");
            assertThat(node.shardsWithState(STARTED).size(), Matchers.greaterThanOrEqualTo(minAvgNumberOfShards));
            assertThat(node.shardsWithState(STARTED).size(), Matchers.lessThanOrEqualTo(maxAvgNumberOfShards));
        }
    }

    private void assertIndexBalance(ESLogger logger, RoutingNodes nodes, int numberOfNodes, int numberOfIndices, int numberOfReplicas, int numberOfShards, float treshold) {

        final int numShards = numberOfShards * (numberOfReplicas + 1);
        final float avgNumShards = (float) (numShards) / (float) (numberOfNodes);
        final int minAvgNumberOfShards = Math.round(Math.round(Math.floor(avgNumShards - treshold)));
        final int maxAvgNumberOfShards = Math.round(Math.round(Math.ceil(avgNumShards + treshold)));

        for (ObjectCursor<String> index : nodes.getRoutingTable().indicesRouting().keys()) {
            for (RoutingNode node : nodes) {
//              logger.info(node.nodeId() +":"+index+ ": " + node.shardsWithState(index, INITIALIZING, STARTED).size() + " shards ("+minAvgNumberOfShards+" to "+maxAvgNumberOfShards+")");
                assertThat(node.shardsWithState(index.value, STARTED).size(), Matchers.greaterThanOrEqualTo(minAvgNumberOfShards));
                assertThat(node.shardsWithState(index.value, STARTED).size(), Matchers.lessThanOrEqualTo(maxAvgNumberOfShards));
            }
        }
    }

    private void assertPrimaryBalance(ESLogger logger, RoutingNodes nodes, int numberOfNodes, int numberOfIndices, int numberOfReplicas, int numberOfShards, float treshold) {

        final int numShards = numberOfShards;
        final float avgNumShards = (float) (numShards) / (float) (numberOfNodes);
        final int minAvgNumberOfShards = Math.round(Math.round(Math.floor(avgNumShards - treshold)));
        final int maxAvgNumberOfShards = Math.round(Math.round(Math.ceil(avgNumShards + treshold)));

        for (ObjectCursor<String> index : nodes.getRoutingTable().indicesRouting().keys()) {
            for (RoutingNode node : nodes) {
                int primaries = 0;
                for (ShardRouting shard : node.shardsWithState(index.value, STARTED)) {
                    primaries += shard.primary() ? 1 : 0;
                }
//                logger.info(node.nodeId() + ": " + primaries + " primaries ("+minAvgNumberOfShards+" to "+maxAvgNumberOfShards+")");
                assertThat(primaries, Matchers.greaterThanOrEqualTo(minAvgNumberOfShards));
                assertThat(primaries, Matchers.lessThanOrEqualTo(maxAvgNumberOfShards));
            }
        }
    }

    public void testPersistedSettings() {
        Settings.Builder settings = settingsBuilder();
        settings.put(BalancedShardsAllocator.SETTING_INDEX_BALANCE_FACTOR, 0.2);
        settings.put(BalancedShardsAllocator.SETTING_SHARD_BALANCE_FACTOR, 0.3);
        settings.put(BalancedShardsAllocator.SETTING_THRESHOLD, 2.0);
        final NodeSettingsService.Listener[] listeners = new NodeSettingsService.Listener[1];
        NodeSettingsService service = new NodeSettingsService(settingsBuilder().build()) {

            @Override
            public void addListener(Listener listener) {
                assertNull("addListener was called twice while only one time was expected", listeners[0]);
                listeners[0] = listener;
            }

        };
        BalancedShardsAllocator allocator = new BalancedShardsAllocator(settings.build(), service);
        assertThat(allocator.getIndexBalance(), Matchers.equalTo(0.2f));
        assertThat(allocator.getShardBalance(), Matchers.equalTo(0.3f));
        assertThat(allocator.getThreshold(), Matchers.equalTo(2.0f));

        settings = settingsBuilder();
        settings.put(ClusterRebalanceAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ALLOW_REBALANCE, ClusterRebalanceAllocationDecider.ClusterRebalanceType.ALWAYS.toString());
        listeners[0].onRefreshSettings(settings.build());
        assertThat(allocator.getIndexBalance(), Matchers.equalTo(0.2f));
        assertThat(allocator.getShardBalance(), Matchers.equalTo(0.3f));
        assertThat(allocator.getThreshold(), Matchers.equalTo(2.0f));

        settings = settingsBuilder();
        settings.put(BalancedShardsAllocator.SETTING_INDEX_BALANCE_FACTOR, 0.5);
        settings.put(BalancedShardsAllocator.SETTING_SHARD_BALANCE_FACTOR, 0.1);
        settings.put(BalancedShardsAllocator.SETTING_THRESHOLD, 3.0);
        listeners[0].onRefreshSettings(settings.build());
        assertThat(allocator.getIndexBalance(), Matchers.equalTo(0.5f));
        assertThat(allocator.getShardBalance(), Matchers.equalTo(0.1f));
        assertThat(allocator.getThreshold(), Matchers.equalTo(3.0f));
    }

    public void testNoRebalanceOnPrimaryOverload() {
        Settings.Builder settings = settingsBuilder();
        AllocationService strategy = new AllocationService(settings.build(), randomAllocationDeciders(settings.build(),
                new NodeSettingsService(Settings.Builder.EMPTY_SETTINGS), getRandom()), new ShardsAllocators(settings.build(),
                NoopGatewayAllocator.INSTANCE, new ShardsAllocator() {

            @Override
            public boolean rebalance(RoutingAllocation allocation) {
                return false;
            }

            @Override
            public boolean move(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
                return false;
            }

            @Override
            public void applyStartedShards(StartedRerouteAllocation allocation) {


            }

            @Override
            public void applyFailedShards(FailedRerouteAllocation allocation) {
            }

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
            @Override
            public boolean allocateUnassigned(RoutingAllocation allocation) {
                RoutingNodes.UnassignedShards unassigned = allocation.routingNodes().unassigned();
                boolean changed = !unassigned.isEmpty();
                for (ShardRouting sr : unassigned.drain()) {
                    switch (sr.id()) {
                        case 0:
                            if (sr.primary()) {
                                allocation.routingNodes().initialize(sr, "node1", -1);
                            } else {
                                allocation.routingNodes().initialize(sr, "node0", -1);
                            }
                            break;
                        case 1:
                            if (sr.primary()) {
                                allocation.routingNodes().initialize(sr, "node1", -1);
                            } else {
                                allocation.routingNodes().initialize(sr, "node2", -1);
                            }
                            break;
                        case 2:
                            if (sr.primary()) {
                                allocation.routingNodes().initialize(sr, "node3", -1);
                            } else {
                                allocation.routingNodes().initialize(sr, "node2", -1);
                            }
                            break;
                        case 3:
                            if (sr.primary()) {
                                allocation.routingNodes().initialize(sr, "node3", -1);
                            } else {
                                allocation.routingNodes().initialize(sr, "node1", -1);
                            }
                            break;
                        case 4:
                            if (sr.primary()) {
                                allocation.routingNodes().initialize(sr, "node2", -1);
                            } else {
                                allocation.routingNodes().initialize(sr, "node0", -1);
                            }
                            break;
                    }

                }
                return changed;
            }
        }), EmptyClusterInfoService.INSTANCE);
        MetaData.Builder metaDataBuilder = MetaData.builder();
        RoutingTable.Builder routingTableBuilder = RoutingTable.builder();
        IndexMetaData.Builder indexMeta = IndexMetaData.builder("test").settings(settings(Version.CURRENT)).numberOfShards(5).numberOfReplicas(1);
        metaDataBuilder = metaDataBuilder.put(indexMeta);
        MetaData metaData = metaDataBuilder.build();
        for (ObjectCursor<IndexMetaData> cursor : metaData.indices().values()) {
            routingTableBuilder.addAsNew(cursor.value);
        }
        RoutingTable routingTable = routingTableBuilder.build();
        DiscoveryNodes.Builder nodes = DiscoveryNodes.builder();
        for (int i = 0; i < 4; i++) {
            DiscoveryNode node = newNode("node" + i);
            nodes.put(node);
        }

        ClusterState clusterState = ClusterState.builder(org.elasticsearch.cluster.ClusterName.DEFAULT).nodes(nodes).metaData(metaData).routingTable(routingTable).build();
        routingTable = strategy.reroute(clusterState, "reroute").routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();
        RoutingNodes routingNodes = clusterState.getRoutingNodes();

        for (RoutingNode routingNode : routingNodes) {
            for (ShardRouting shardRouting : routingNode) {
                assertThat(shardRouting.state(), Matchers.equalTo(ShardRoutingState.INITIALIZING));
            }
        }
        strategy = createAllocationService(settings.build());

        logger.info("use the new allocator and check if it moves shards");
        routingNodes = clusterState.getRoutingNodes();
        routingTable = strategy.applyStartedShards(clusterState, routingNodes.shardsWithState(INITIALIZING)).routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();
        routingNodes = clusterState.getRoutingNodes();
        for (RoutingNode routingNode : routingNodes) {
            for (ShardRouting shardRouting : routingNode) {
                assertThat(shardRouting.state(), Matchers.equalTo(ShardRoutingState.STARTED));
            }
        }

        logger.info("start the replica shards");
        routingTable = strategy.applyStartedShards(clusterState, routingNodes.shardsWithState(INITIALIZING)).routingTable();
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
