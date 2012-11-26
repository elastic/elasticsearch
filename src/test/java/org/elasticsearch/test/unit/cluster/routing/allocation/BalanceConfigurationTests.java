/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package org.elasticsearch.test.unit.cluster.routing.allocation;

import static org.elasticsearch.cluster.ClusterState.newClusterStateBuilder;
import static org.elasticsearch.cluster.metadata.IndexMetaData.newIndexMetaDataBuilder;
import static org.elasticsearch.cluster.metadata.MetaData.newMetaDataBuilder;
import static org.elasticsearch.cluster.node.DiscoveryNodes.newNodesBuilder;
import static org.elasticsearch.cluster.routing.RoutingBuilders.routingTable;
import static org.elasticsearch.cluster.routing.ShardRoutingState.INITIALIZING;
import static org.elasticsearch.cluster.routing.ShardRoutingState.STARTED;
import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;
import static org.elasticsearch.test.unit.cluster.routing.allocation.RoutingAllocationTests.newNode;
import static org.hamcrest.MatcherAssert.assertThat;

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.RoutingNodes;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.cluster.routing.allocation.allocator.BalancedShardsAllocator;
import org.elasticsearch.cluster.routing.allocation.decider.ClusterRebalanceAllocationDecider;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.hamcrest.Matchers;
import org.testng.annotations.Test;

public class BalanceConfigurationTests {

    private final ESLogger logger = Loggers.getLogger(BalanceConfigurationTests.class);
    // TODO maybe we can randomize these numbers somehow
    final int numberOfNodes = 25;
    final int numberOfIndices = 12;
    final int numberOfShards = 2;
    final int numberOfReplicas = 2;
    
    @Test
    public void testIndexBalance() {
        /* Tests balance over indices only */
        final float indexBalance = 1.0f;
        final float replicaBalance = 0.0f;
        final float primaryBalance = 0.0f;
        final float balanceTreshold = 1.0f;

        ImmutableSettings.Builder settings = settingsBuilder();
        settings.put("cluster.routing.allocation.allow_rebalance", ClusterRebalanceAllocationDecider.ClusterRebalanceType.ALWAYS.toString());
        settings.put(BalancedShardsAllocator.SETTING_INDEX_BALANCE_FACTOR, indexBalance);
        settings.put(BalancedShardsAllocator.SETTING_SHARD_BALANCE_FACTOR, replicaBalance);
        settings.put(BalancedShardsAllocator.SETTING_PRIMARY_BALANCE_FACTOR, primaryBalance);
        settings.put(BalancedShardsAllocator.SETTING_THRESHOLD, balanceTreshold);
        
        AllocationService strategy = new AllocationService(settings.build());

        ClusterState clusterState = initCluster(strategy);
        assertIndexBalance(logger, clusterState.getRoutingNodes(), numberOfNodes, numberOfIndices, numberOfReplicas, numberOfShards, balanceTreshold);

        clusterState = addNode(clusterState, strategy);
        assertIndexBalance(logger, clusterState.getRoutingNodes(), numberOfNodes+1, numberOfIndices, numberOfReplicas, numberOfShards, balanceTreshold);

        clusterState = removeNodes(clusterState, strategy);
        assertIndexBalance(logger, clusterState.getRoutingNodes(), (numberOfNodes+1)-(numberOfNodes+1)/2, numberOfIndices, numberOfReplicas, numberOfShards, balanceTreshold);

    }
    
    @Test
    public void testReplicaBalance() {
        /* Tests balance over replicas only */
        final float indexBalance = 0.0f;
        final float replicaBalance = 1.0f;
        final float primaryBalance = 0.0f;
        final float balanceTreshold = 1.0f;
        
        ImmutableSettings.Builder settings = settingsBuilder();
        settings.put("cluster.routing.allocation.allow_rebalance", ClusterRebalanceAllocationDecider.ClusterRebalanceType.ALWAYS.toString());
        settings.put(BalancedShardsAllocator.SETTING_INDEX_BALANCE_FACTOR, indexBalance);
        settings.put(BalancedShardsAllocator.SETTING_SHARD_BALANCE_FACTOR, replicaBalance);
        settings.put(BalancedShardsAllocator.SETTING_PRIMARY_BALANCE_FACTOR, primaryBalance);
        settings.put(BalancedShardsAllocator.SETTING_THRESHOLD, balanceTreshold);
        
        AllocationService strategy = new AllocationService(settings.build());

        ClusterState clusterState = initCluster(strategy);
        assertReplicaBalance(logger, clusterState.getRoutingNodes(), numberOfNodes, numberOfIndices, numberOfReplicas, numberOfShards, balanceTreshold);

        clusterState = addNode(clusterState, strategy);
        assertReplicaBalance(logger, clusterState.getRoutingNodes(), numberOfNodes+1, numberOfIndices, numberOfReplicas, numberOfShards, balanceTreshold);

        clusterState = removeNodes(clusterState, strategy);
        assertReplicaBalance(logger, clusterState.getRoutingNodes(), (numberOfNodes+1)-(numberOfNodes+1)/2, numberOfIndices, numberOfReplicas, numberOfShards, balanceTreshold);
        
    }
    
    @Test
    public void testPrimaryBalance() {
        /* Tests balance over primaries only */
        final float indexBalance = 0.0f;
        final float replicaBalance = 0.0f;
        final float primaryBalance = 1.0f;
        final float balanceTreshold = 1.0f;
        
        ImmutableSettings.Builder settings = settingsBuilder();
        settings.put("cluster.routing.allocation.allow_rebalance", ClusterRebalanceAllocationDecider.ClusterRebalanceType.ALWAYS.toString());
        settings.put(BalancedShardsAllocator.SETTING_INDEX_BALANCE_FACTOR, indexBalance);
        settings.put(BalancedShardsAllocator.SETTING_SHARD_BALANCE_FACTOR, replicaBalance);
        settings.put(BalancedShardsAllocator.SETTING_PRIMARY_BALANCE_FACTOR, primaryBalance);
        settings.put(BalancedShardsAllocator.SETTING_THRESHOLD, balanceTreshold);
        
        AllocationService strategy = new AllocationService(settings.build());

        ClusterState clusterstate = initCluster(strategy); 
        assertPrimaryBalance(logger, clusterstate.getRoutingNodes(), numberOfNodes, numberOfIndices, numberOfReplicas, numberOfShards, balanceTreshold);
        
        clusterstate = addNode(clusterstate, strategy);
        assertPrimaryBalance(logger, clusterstate.getRoutingNodes(), numberOfNodes+1, numberOfIndices, numberOfReplicas, numberOfShards, balanceTreshold);

        clusterstate = removeNodes(clusterstate, strategy);
        assertPrimaryBalance(logger, clusterstate.getRoutingNodes(), numberOfNodes+1-(numberOfNodes+1)/2, numberOfIndices, numberOfReplicas, numberOfShards, balanceTreshold);
    }    

    private ClusterState initCluster(AllocationService strategy) {
        MetaData.Builder metaDataBuilder = newMetaDataBuilder();
        RoutingTable.Builder routingTableBuilder = routingTable();
        
        for (int i = 0; i < numberOfIndices; i++) {
            IndexMetaData.Builder index = newIndexMetaDataBuilder("test"+i).numberOfShards(numberOfShards).numberOfReplicas(numberOfReplicas);
            metaDataBuilder = metaDataBuilder.put(index);
        }
        
        MetaData metaData = metaDataBuilder.build();

        for (IndexMetaData index : metaData.indices().values()) {
            routingTableBuilder.addAsNew(index);
        }
        
        RoutingTable routingTable = routingTableBuilder.build();


        logger.info("start "+numberOfNodes+" nodes");
        DiscoveryNodes.Builder nodes = newNodesBuilder();
        for (int i = 0; i < numberOfNodes; i++) {
            nodes.put(newNode("node"+i));
        }
        ClusterState clusterState = newClusterStateBuilder().nodes(nodes).metaData(metaData).routingTable(routingTable).build();
        routingTable = strategy.reroute(clusterState).routingTable();
        clusterState = newClusterStateBuilder().state(clusterState).routingTable(routingTable).build();
        RoutingNodes routingNodes = clusterState.routingNodes();

        logger.info("restart all the primary shards, replicas will start initializing");
        routingNodes = clusterState.routingNodes();
        routingTable = strategy.applyStartedShards(clusterState, routingNodes.shardsWithState(INITIALIZING)).routingTable();
        clusterState = newClusterStateBuilder().state(clusterState).routingTable(routingTable).build();
        routingNodes = clusterState.routingNodes();

        logger.info("start the replica shards");
        routingNodes = clusterState.routingNodes();
        routingTable = strategy.applyStartedShards(clusterState, routingNodes.shardsWithState(INITIALIZING)).routingTable();
        clusterState = newClusterStateBuilder().state(clusterState).routingTable(routingTable).build();
        routingNodes = clusterState.routingNodes();
        
        logger.info("complete rebalancing");
        RoutingTable prev = routingTable;
        while(true) {
            routingTable = strategy.applyStartedShards(clusterState, routingNodes.shardsWithState(INITIALIZING)).routingTable();
            clusterState = newClusterStateBuilder().state(clusterState).routingTable(routingTable).build();
            routingNodes = clusterState.routingNodes();
            if (routingTable == prev) 
                break;
            prev = routingTable;
        }
     
        return clusterState;
    }
    
    private ClusterState addNode(ClusterState clusterState, AllocationService strategy) {
        logger.info("now, start 1 more node, check that rebalancing will happen because we set it to always");
        clusterState = newClusterStateBuilder().state(clusterState).nodes(newNodesBuilder().putAll(clusterState.nodes())
                .put(newNode("node"+numberOfNodes)))
                .build();

        RoutingTable routingTable = strategy.reroute(clusterState).routingTable();
        clusterState = newClusterStateBuilder().state(clusterState).routingTable(routingTable).build();
        RoutingNodes routingNodes = clusterState.routingNodes();

        // move initializing to started
        
        RoutingTable prev = routingTable;
        while(true) {
            routingTable = strategy.applyStartedShards(clusterState, routingNodes.shardsWithState(INITIALIZING)).routingTable();
            clusterState = newClusterStateBuilder().state(clusterState).routingTable(routingTable).build();
            routingNodes = clusterState.routingNodes();
            if (routingTable == prev) 
                break;
            prev = routingTable;
        }
        
        return clusterState;
    }
    
    private ClusterState removeNodes(ClusterState clusterState, AllocationService strategy) {
        logger.info("Removing half the nodes ("+(numberOfNodes+1)/2+")");
        DiscoveryNodes.Builder nodes = newNodesBuilder().putAll(clusterState.nodes());

        for(int i=(numberOfNodes+1)/2; i<=numberOfNodes; i++){
            nodes.remove("node"+i);
        }

        clusterState = newClusterStateBuilder().state(clusterState).nodes(nodes.build()).build();
        RoutingNodes routingNodes = clusterState.routingNodes();

        logger.info("start all the primary shards, replicas will start initializing");
        RoutingTable routingTable = strategy.applyStartedShards(clusterState, routingNodes.shardsWithState(INITIALIZING)).routingTable();
        clusterState = newClusterStateBuilder().state(clusterState).routingTable(routingTable).build();
        routingNodes = clusterState.routingNodes();

        logger.info("start the replica shards");
        routingTable = strategy.applyStartedShards(clusterState, routingNodes.shardsWithState(INITIALIZING)).routingTable();
        clusterState = newClusterStateBuilder().state(clusterState).routingTable(routingTable).build();
        routingNodes = clusterState.routingNodes();

        logger.info("rebalancing");
        routingTable = strategy.reroute(clusterState).routingTable();
        clusterState = newClusterStateBuilder().state(clusterState).routingTable(routingTable).build();
        routingNodes = clusterState.routingNodes();

        logger.info("complete rebalancing");
        RoutingTable prev = routingTable;
        while(true) {
            routingTable = strategy.applyStartedShards(clusterState, routingNodes.shardsWithState(INITIALIZING)).routingTable();
            clusterState = newClusterStateBuilder().state(clusterState).routingTable(routingTable).build();
            routingNodes = clusterState.routingNodes();
            if (routingTable == prev) 
                break;
            prev = routingTable;
        }
        
        return clusterState;
    }
    

    private void assertReplicaBalance(ESLogger logger, RoutingNodes nodes, int numberOfNodes, int numberOfIndices, int numberOfReplicas, int numberOfShards, float treshold) {
        final int numShards = numberOfIndices * numberOfShards * (numberOfReplicas+1);
        final float avgNumShards = (float)(numShards) / (float)(numberOfNodes);
        final int minAvgNumberOfShards = Math.round(Math.round(Math.floor(avgNumShards-treshold))); 
        final int maxAvgNumberOfShards = Math.round(Math.round(Math.ceil(avgNumShards+treshold))); 
        
        for (RoutingNode node : nodes) {
//            logger.info(node.nodeId() + ": " + node.shardsWithState(INITIALIZING, STARTED).size() + " shards ("+minAvgNumberOfShards+" to "+maxAvgNumberOfShards+")");
            assertThat(node.shardsWithState(STARTED).size(), Matchers.greaterThanOrEqualTo(minAvgNumberOfShards));
            assertThat(node.shardsWithState(STARTED).size(), Matchers.lessThanOrEqualTo(maxAvgNumberOfShards));
        }
    }

    private void assertIndexBalance(ESLogger logger, RoutingNodes nodes, int numberOfNodes, int numberOfIndices, int numberOfReplicas, int numberOfShards, float treshold) {
        
        final int numShards = numberOfShards * (numberOfReplicas+1);
        final float avgNumShards = (float)(numShards) / (float)(numberOfNodes);
        final int minAvgNumberOfShards = Math.round(Math.round(Math.floor(avgNumShards-treshold))); 
        final int maxAvgNumberOfShards = Math.round(Math.round(Math.ceil(avgNumShards+treshold))); 

        for(String index : nodes.getRoutingTable().indicesRouting().keySet()) {
            for (RoutingNode node : nodes) {
//              logger.info(node.nodeId() +":"+index+ ": " + node.shardsWithState(index, INITIALIZING, STARTED).size() + " shards ("+minAvgNumberOfShards+" to "+maxAvgNumberOfShards+")");
                assertThat(node.shardsWithState(index, STARTED).size(), Matchers.greaterThanOrEqualTo(minAvgNumberOfShards));
                assertThat(node.shardsWithState(index, STARTED).size(), Matchers.lessThanOrEqualTo(maxAvgNumberOfShards));
            }
        }
    }
    
    private void assertPrimaryBalance(ESLogger logger, RoutingNodes nodes, int numberOfNodes, int numberOfIndices, int numberOfReplicas, int numberOfShards, float treshold) {
        
        final int numShards = numberOfShards;
        final float avgNumShards = (float)(numShards) / (float)(numberOfNodes);
        final int minAvgNumberOfShards = Math.round(Math.round(Math.floor(avgNumShards-treshold))); 
        final int maxAvgNumberOfShards = Math.round(Math.round(Math.ceil(avgNumShards+treshold))); 

        for(String index : nodes.getRoutingTable().indicesRouting().keySet()) {
            for (RoutingNode node : nodes) {
                int primaries = 0;
                for(ShardRouting shard : node.shardsWithState(index, STARTED)) {
                    primaries += shard.primary()?1:0;
                }
//                logger.info(node.nodeId() + ": " + primaries + " primaries ("+minAvgNumberOfShards+" to "+maxAvgNumberOfShards+")");
                assertThat(primaries, Matchers.greaterThanOrEqualTo(minAvgNumberOfShards));
                assertThat(primaries, Matchers.lessThanOrEqualTo(maxAvgNumberOfShards));
            }
        }
    }
    
}