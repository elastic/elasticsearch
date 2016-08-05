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

import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.RoutingNodes;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.allocation.decider.ClusterRebalanceAllocationDecider;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESAllocationTestCase;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.elasticsearch.cluster.routing.ShardRoutingState.INITIALIZING;
import static org.elasticsearch.cluster.routing.ShardRoutingState.RELOCATING;
import static org.elasticsearch.cluster.routing.ShardRoutingState.STARTED;
import static org.elasticsearch.cluster.routing.ShardRoutingState.UNASSIGNED;
import static org.elasticsearch.cluster.routing.allocation.RoutingNodesUtils.numberOfShardsOfType;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;

/**
 *
 */
public class SingleShardNoReplicasRoutingTests extends ESAllocationTestCase {
    private final ESLogger logger = Loggers.getLogger(SingleShardNoReplicasRoutingTests.class);

    public void testSingleIndexStartedShard() {
        AllocationService strategy = createAllocationService(Settings.builder().put("cluster.routing.allocation.node_concurrent_recoveries", 10).build());

        logger.info("Building initial routing table");

        MetaData metaData = MetaData.builder()
                .put(IndexMetaData.builder("test").settings(settings(Version.CURRENT)).numberOfShards(1).numberOfReplicas(0))
                .build();

        RoutingTable routingTable = RoutingTable.builder()
                .addAsNew(metaData.index("test"))
                .build();

        ClusterState clusterState = ClusterState.builder(org.elasticsearch.cluster.ClusterName.CLUSTER_NAME_SETTING.getDefault(Settings.EMPTY)).metaData(metaData).routingTable(routingTable).build();

        assertThat(routingTable.index("test").shards().size(), equalTo(1));
        assertThat(routingTable.index("test").shard(0).size(), equalTo(1));
        assertThat(routingTable.index("test").shard(0).shards().size(), equalTo(1));
        assertThat(routingTable.index("test").shard(0).shards().get(0).state(), equalTo(UNASSIGNED));
        assertThat(routingTable.index("test").shard(0).shards().get(0).currentNodeId(), nullValue());

        logger.info("Adding one node and performing rerouting");
        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder().add(newNode("node1"))).build();
        RoutingTable prevRoutingTable = routingTable;
        routingTable = strategy.reroute(clusterState, "reroute").routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();

        assertThat(routingTable.index("test").shards().size(), equalTo(1));
        assertThat(routingTable.index("test").shard(0).size(), equalTo(1));
        assertThat(routingTable.index("test").shard(0).shards().size(), equalTo(1));
        assertThat(routingTable.index("test").shard(0).shards().get(0).state(), equalTo(INITIALIZING));
        assertThat(routingTable.index("test").shard(0).shards().get(0).currentNodeId(), equalTo("node1"));

        logger.info("Rerouting again, nothing should change");
        prevRoutingTable = routingTable;
        clusterState = ClusterState.builder(clusterState).build();
        routingTable = strategy.reroute(clusterState, "reroute").routingTable();
        assertThat(routingTable == prevRoutingTable, equalTo(true));
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();

        logger.info("Marking the shard as started");
        RoutingNodes routingNodes = clusterState.getRoutingNodes();
        prevRoutingTable = routingTable;
        routingTable = strategy.applyStartedShards(clusterState, routingNodes.node("node1").shardsWithState(INITIALIZING)).routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();

        assertThat(routingTable != prevRoutingTable, equalTo(true));
        assertThat(routingTable.index("test").shards().size(), equalTo(1));
        assertThat(routingTable.index("test").shard(0).size(), equalTo(1));
        assertThat(routingTable.index("test").shard(0).shards().size(), equalTo(1));
        assertThat(routingTable.index("test").shard(0).shards().get(0).state(), equalTo(STARTED));
        assertThat(routingTable.index("test").shard(0).shards().get(0).currentNodeId(), equalTo("node1"));

        logger.info("Starting another node and making sure nothing changed");
        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder(clusterState.nodes()).add(newNode("node2"))).build();
        prevRoutingTable = routingTable;
        routingTable = strategy.reroute(clusterState, "reroute").routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();

        assertThat(routingTable == prevRoutingTable, equalTo(true));
        assertThat(routingTable.index("test").shards().size(), equalTo(1));
        assertThat(routingTable.index("test").shard(0).size(), equalTo(1));
        assertThat(routingTable.index("test").shard(0).shards().size(), equalTo(1));
        assertThat(routingTable.index("test").shard(0).shards().get(0).state(), equalTo(STARTED));
        assertThat(routingTable.index("test").shard(0).shards().get(0).currentNodeId(), equalTo("node1"));

        logger.info("Killing node1 where the shard is, checking the shard is relocated");

        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder(clusterState.nodes()).remove("node1")).build();
        prevRoutingTable = routingTable;
        routingTable = strategy.deassociateDeadNodes(clusterState, true, "reroute").routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();

        assertThat(routingTable != prevRoutingTable, equalTo(true));
        assertThat(routingTable.index("test").shards().size(), equalTo(1));
        assertThat(routingTable.index("test").shard(0).size(), equalTo(1));
        assertThat(routingTable.index("test").shard(0).shards().size(), equalTo(1));
        assertThat(routingTable.index("test").shard(0).shards().get(0).state(), equalTo(INITIALIZING));
        assertThat(routingTable.index("test").shard(0).shards().get(0).currentNodeId(), equalTo("node2"));

        logger.info("Start another node, make sure that things remain the same (shard is in node2 and initializing)");
        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder(clusterState.nodes()).add(newNode("node3"))).build();
        prevRoutingTable = routingTable;
        routingTable = strategy.reroute(clusterState, "reroute").routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();
        assertThat(routingTable == prevRoutingTable, equalTo(true));

        logger.info("Start the shard on node 2");
        routingNodes = clusterState.getRoutingNodes();
        prevRoutingTable = routingTable;
        routingTable = strategy.applyStartedShards(clusterState, routingNodes.node("node2").shardsWithState(INITIALIZING)).routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();

        assertThat(routingTable != prevRoutingTable, equalTo(true));
        assertThat(routingTable.index("test").shards().size(), equalTo(1));
        assertThat(routingTable.index("test").shard(0).size(), equalTo(1));
        assertThat(routingTable.index("test").shard(0).shards().size(), equalTo(1));
        assertThat(routingTable.index("test").shard(0).shards().get(0).state(), equalTo(STARTED));
        assertThat(routingTable.index("test").shard(0).shards().get(0).currentNodeId(), equalTo("node2"));
    }

    public void testSingleIndexShardFailed() {
        AllocationService strategy = createAllocationService(Settings.builder().put("cluster.routing.allocation.node_concurrent_recoveries", 10).build());

        logger.info("Building initial routing table");

        MetaData metaData = MetaData.builder()
                .put(IndexMetaData.builder("test").settings(settings(Version.CURRENT)).numberOfShards(1).numberOfReplicas(0))
                .build();

        RoutingTable routingTable = RoutingTable.builder()
                .addAsNew(metaData.index("test"))
                .build();

        ClusterState clusterState = ClusterState.builder(org.elasticsearch.cluster.ClusterName.CLUSTER_NAME_SETTING.getDefault(Settings.EMPTY)).metaData(metaData).routingTable(routingTable).build();

        assertThat(routingTable.index("test").shards().size(), equalTo(1));
        assertThat(routingTable.index("test").shard(0).size(), equalTo(1));
        assertThat(routingTable.index("test").shard(0).shards().size(), equalTo(1));
        assertThat(routingTable.index("test").shard(0).shards().get(0).state(), equalTo(UNASSIGNED));
        assertThat(routingTable.index("test").shard(0).shards().get(0).currentNodeId(), nullValue());

        logger.info("Adding one node and rerouting");
        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder().add(newNode("node1"))).build();
        RoutingTable prevRoutingTable = routingTable;
        routingTable = strategy.reroute(clusterState, "reroute").routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();

        assertThat(prevRoutingTable != routingTable, equalTo(true));
        assertThat(routingTable.index("test").shards().size(), equalTo(1));
        assertThat(routingTable.index("test").shard(0).size(), equalTo(1));
        assertThat(routingTable.index("test").shard(0).shards().size(), equalTo(1));
        assertThat(routingTable.index("test").shard(0).shards().get(0).unassigned(), equalTo(false));
        assertThat(routingTable.index("test").shard(0).shards().get(0).state(), equalTo(INITIALIZING));
        assertThat(routingTable.index("test").shard(0).shards().get(0).currentNodeId(), equalTo("node1"));

        logger.info("Marking the shard as failed");
        RoutingNodes routingNodes = clusterState.getRoutingNodes();
        prevRoutingTable = routingTable;
        routingTable = strategy.applyFailedShard(clusterState, routingNodes.node("node1").shardsWithState(INITIALIZING).get(0)).routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();

        assertThat(prevRoutingTable != routingTable, equalTo(true));
        assertThat(routingTable.index("test").shards().size(), equalTo(1));
        assertThat(routingTable.index("test").shard(0).size(), equalTo(1));
        assertThat(routingTable.index("test").shard(0).shards().size(), equalTo(1));
        assertThat(routingTable.index("test").shard(0).shards().get(0).state(), equalTo(UNASSIGNED));
        assertThat(routingTable.index("test").shard(0).shards().get(0).currentNodeId(), nullValue());
    }

    public void testMultiIndexEvenDistribution() {
        AllocationService strategy = createAllocationService(Settings.builder()
                .put("cluster.routing.allocation.node_concurrent_recoveries", 10)
                .put(ClusterRebalanceAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ALLOW_REBALANCE_SETTING.getKey(), "always")
                .put("cluster.routing.allocation.cluster_concurrent_rebalance", -1)
                .build());

        final int numberOfIndices = 50;
        logger.info("Building initial routing table with " + numberOfIndices + " indices");

        MetaData.Builder metaDataBuilder = MetaData.builder();
        for (int i = 0; i < numberOfIndices; i++) {
            metaDataBuilder.put(IndexMetaData.builder("test" + i).settings(settings(Version.CURRENT)).numberOfShards(1).numberOfReplicas(0));
        }
        MetaData metaData = metaDataBuilder.build();

        RoutingTable.Builder routingTableBuilder = RoutingTable.builder();
        for (int i = 0; i < numberOfIndices; i++) {
            routingTableBuilder.addAsNew(metaData.index("test" + i));
        }
        RoutingTable routingTable = routingTableBuilder.build();
        ClusterState clusterState = ClusterState.builder(org.elasticsearch.cluster.ClusterName.CLUSTER_NAME_SETTING.getDefault(Settings.EMPTY)).metaData(metaData).routingTable(routingTable).build();

        assertThat(routingTable.indicesRouting().size(), equalTo(numberOfIndices));
        for (int i = 0; i < numberOfIndices; i++) {
            assertThat(routingTable.index("test" + i).shards().size(), equalTo(1));
            assertThat(routingTable.index("test" + i).shard(0).size(), equalTo(1));
            assertThat(routingTable.index("test" + i).shard(0).shards().size(), equalTo(1));
            assertThat(routingTable.index("test" + i).shard(0).shards().get(0).state(), equalTo(UNASSIGNED));
            assertThat(routingTable.index("test" + i).shard(0).shards().get(0).currentNodeId(), nullValue());
        }

        logger.info("Adding " + (numberOfIndices / 2) + " nodes");
        DiscoveryNodes.Builder nodesBuilder = DiscoveryNodes.builder();
        List<DiscoveryNode> nodes = new ArrayList<>();
        for (int i = 0; i < (numberOfIndices / 2); i++) {
            nodesBuilder.add(newNode("node" + i));
        }
        RoutingTable prevRoutingTable = routingTable;
        clusterState = ClusterState.builder(clusterState).nodes(nodesBuilder).build();
        routingTable = strategy.reroute(clusterState, "reroute").routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();

        assertThat(prevRoutingTable != routingTable, equalTo(true));
        for (int i = 0; i < numberOfIndices; i++) {
            assertThat(routingTable.index("test" + i).shards().size(), equalTo(1));
            assertThat(routingTable.index("test" + i).shard(0).size(), equalTo(1));
            assertThat(routingTable.index("test" + i).shard(0).shards().size(), equalTo(1));
            assertThat(routingTable.index("test" + i).shard(0).shards().get(0).unassigned(), equalTo(false));
            assertThat(routingTable.index("test" + i).shard(0).shards().get(0).state(), equalTo(INITIALIZING));
            assertThat(routingTable.index("test" + i).shard(0).shards().get(0).primary(), equalTo(true));
            // make sure we still have 2 shards initializing per node on the first 25 nodes
            String nodeId = routingTable.index("test" + i).shard(0).shards().get(0).currentNodeId();
            int nodeIndex = Integer.parseInt(nodeId.substring("node".length()));
            assertThat(nodeIndex, lessThan(25));
        }
        RoutingNodes routingNodes = clusterState.getRoutingNodes();
        Set<String> encounteredIndices = new HashSet<>();
        for (RoutingNode routingNode : routingNodes) {
            assertThat(routingNode.numberOfShardsWithState(STARTED), equalTo(0));
            assertThat(routingNode.size(), equalTo(2));
            // make sure we still have 2 shards initializing per node on the only 25 nodes
            int nodeIndex = Integer.parseInt(routingNode.nodeId().substring("node".length()));
            assertThat(nodeIndex, lessThan(25));
            // check that we don't have a shard associated with a node with the same index name (we have a single shard)
            for (ShardRouting shardRoutingEntry : routingNode) {
                assertThat(encounteredIndices, not(hasItem(shardRoutingEntry.getIndexName())));
                encounteredIndices.add(shardRoutingEntry.getIndexName());
            }
        }

        logger.info("Adding additional " + (numberOfIndices / 2) + " nodes, nothing should change");
        nodesBuilder = DiscoveryNodes.builder(clusterState.nodes());
        for (int i = (numberOfIndices / 2); i < numberOfIndices; i++) {
            nodesBuilder.add(newNode("node" + i));
        }
        prevRoutingTable = routingTable;
        clusterState = ClusterState.builder(clusterState).nodes(nodesBuilder).build();
        routingTable = strategy.reroute(clusterState, "reroute").routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();

        assertThat(prevRoutingTable != routingTable, equalTo(false));

        logger.info("Marking the shard as started");
        prevRoutingTable = routingTable;
        routingTable = strategy.applyStartedShards(clusterState, routingNodes.shardsWithState(INITIALIZING)).routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();

        assertThat(prevRoutingTable != routingTable, equalTo(true));
        int numberOfRelocatingShards = 0;
        int numberOfStartedShards = 0;
        for (int i = 0; i < numberOfIndices; i++) {
            assertThat(routingTable.index("test" + i).shards().size(), equalTo(1));
            assertThat(routingTable.index("test" + i).shard(0).size(), equalTo(1));
            assertThat(routingTable.index("test" + i).shard(0).shards().size(), equalTo(1));
            assertThat(routingTable.index("test" + i).shard(0).shards().get(0).unassigned(), equalTo(false));
            assertThat(routingTable.index("test" + i).shard(0).shards().get(0).state(), anyOf(equalTo(STARTED), equalTo(RELOCATING)));
            if (routingTable.index("test" + i).shard(0).shards().get(0).state() == STARTED) {
                numberOfStartedShards++;
            } else if (routingTable.index("test" + i).shard(0).shards().get(0).state() == RELOCATING) {
                numberOfRelocatingShards++;
            }
            assertThat(routingTable.index("test" + i).shard(0).shards().get(0).primary(), equalTo(true));
            // make sure we still have 2 shards either relocating or started on the first 25 nodes (still)
            String nodeId = routingTable.index("test" + i).shard(0).shards().get(0).currentNodeId();
            int nodeIndex = Integer.parseInt(nodeId.substring("node".length()));
            assertThat(nodeIndex, lessThan(25));
        }
        assertThat(numberOfRelocatingShards, equalTo(25));
        assertThat(numberOfStartedShards, equalTo(25));
    }

    public void testMultiIndexUnevenNodes() {
        AllocationService strategy = createAllocationService(Settings.builder()
                .put("cluster.routing.allocation.node_concurrent_recoveries", 10)
                .put(ClusterRebalanceAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ALLOW_REBALANCE_SETTING.getKey(), "always")
                .put("cluster.routing.allocation.cluster_concurrent_rebalance", -1)
                .build());

        final int numberOfIndices = 10;
        logger.info("Building initial routing table with " + numberOfIndices + " indices");

        MetaData.Builder metaDataBuilder = MetaData.builder();
        for (int i = 0; i < numberOfIndices; i++) {
            metaDataBuilder.put(IndexMetaData.builder("test" + i).settings(settings(Version.CURRENT)).numberOfShards(1).numberOfReplicas(0));
        }
        MetaData metaData = metaDataBuilder.build();

        RoutingTable.Builder routingTableBuilder = RoutingTable.builder();
        for (int i = 0; i < numberOfIndices; i++) {
            routingTableBuilder.addAsNew(metaData.index("test" + i));
        }
        RoutingTable routingTable = routingTableBuilder.build();

        ClusterState clusterState = ClusterState.builder(org.elasticsearch.cluster.ClusterName.CLUSTER_NAME_SETTING.getDefault(Settings.EMPTY)).metaData(metaData).routingTable(routingTable).build();

        assertThat(routingTable.indicesRouting().size(), equalTo(numberOfIndices));

        logger.info("Starting 3 nodes and rerouting");
        clusterState = ClusterState.builder(clusterState)
                .nodes(DiscoveryNodes.builder().add(newNode("node1")).add(newNode("node2")).add(newNode("node3")))
                .build();
        RoutingTable prevRoutingTable = routingTable;
        routingTable = strategy.reroute(clusterState, "reroute").routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();

        assertThat(prevRoutingTable != routingTable, equalTo(true));
        for (int i = 0; i < numberOfIndices; i++) {
            assertThat(routingTable.index("test" + i).shards().size(), equalTo(1));
            assertThat(routingTable.index("test" + i).shard(0).size(), equalTo(1));
            assertThat(routingTable.index("test" + i).shard(0).shards().size(), equalTo(1));
            assertThat(routingTable.index("test" + i).shard(0).shards().get(0).state(), equalTo(INITIALIZING));
        }
        RoutingNodes routingNodes = clusterState.getRoutingNodes();
        assertThat(numberOfShardsOfType(routingNodes, INITIALIZING), equalTo(numberOfIndices));
        assertThat(routingNodes.node("node1").numberOfShardsWithState(INITIALIZING), anyOf(equalTo(3), equalTo(4)));
        assertThat(routingNodes.node("node2").numberOfShardsWithState(INITIALIZING), anyOf(equalTo(3), equalTo(4)));
        assertThat(routingNodes.node("node2").numberOfShardsWithState(INITIALIZING), anyOf(equalTo(3), equalTo(4)));

        logger.info("Start two more nodes, things should remain the same");
        clusterState = ClusterState.builder(clusterState)
                .nodes(DiscoveryNodes.builder(clusterState.nodes()).add(newNode("node4")).add(newNode("node5")))
                .build();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();

        prevRoutingTable = routingTable;
        routingTable = strategy.reroute(clusterState, "reroute").routingTable();

        assertThat(prevRoutingTable == routingTable, equalTo(true));

        routingNodes = clusterState.getRoutingNodes();
        prevRoutingTable = routingTable;
        routingTable = strategy.applyStartedShards(clusterState, routingNodes.shardsWithState(INITIALIZING)).routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();

        assertThat(prevRoutingTable != routingTable, equalTo(true));
        for (int i = 0; i < numberOfIndices; i++) {
            assertThat(routingTable.index("test" + i).shards().size(), equalTo(1));
            assertThat(routingTable.index("test" + i).shard(0).size(), equalTo(1));
            assertThat(routingTable.index("test" + i).shard(0).shards().size(), equalTo(1));
            assertThat(routingTable.index("test" + i).shard(0).shards().get(0).state(), anyOf(equalTo(RELOCATING), equalTo(STARTED)));
        }
        routingNodes = clusterState.getRoutingNodes();
        assertThat("4 source shard routing are relocating", numberOfShardsOfType(routingNodes, RELOCATING), equalTo(4));
        assertThat("4 target shard routing are initializing", numberOfShardsOfType(routingNodes, INITIALIZING), equalTo(4));

        logger.info("Now, mark the relocated as started");
        prevRoutingTable = routingTable;
        routingTable = strategy.applyStartedShards(clusterState, routingNodes.shardsWithState(INITIALIZING)).routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();
//        routingTable = strategy.reroute(new RoutingStrategyInfo(metaData, routingTable), nodes);

        assertThat(prevRoutingTable != routingTable, equalTo(true));
        for (int i = 0; i < numberOfIndices; i++) {
            assertThat(routingTable.index("test" + i).shards().size(), equalTo(1));
            assertThat(routingTable.index("test" + i).shard(0).size(), equalTo(1));
            assertThat(routingTable.index("test" + i).shard(0).shards().size(), equalTo(1));
            assertThat(routingTable.index("test" + i).shard(0).shards().get(0).state(), anyOf(equalTo(RELOCATING), equalTo(STARTED)));
        }
        routingNodes = clusterState.getRoutingNodes();
        assertThat(numberOfShardsOfType(routingNodes, STARTED), equalTo(numberOfIndices));
        for (RoutingNode routingNode : routingNodes) {
            assertThat(routingNode.numberOfShardsWithState(STARTED), equalTo(2));
        }
    }
}
