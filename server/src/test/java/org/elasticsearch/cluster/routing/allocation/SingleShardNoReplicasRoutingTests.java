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
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.RoutingNodes;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.allocation.decider.ClusterRebalanceAllocationDecider;
import org.elasticsearch.common.settings.Settings;

import java.util.HashSet;
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

public class SingleShardNoReplicasRoutingTests extends ESAllocationTestCase {
    private final Logger logger = LogManager.getLogger(SingleShardNoReplicasRoutingTests.class);

    public void testSingleIndexStartedShard() {
        AllocationService strategy = createAllocationService(Settings.builder()
            .put("cluster.routing.allocation.node_concurrent_recoveries", 10).build());

        logger.info("Building initial routing table");

        Metadata metadata = Metadata.builder()
                .put(IndexMetadata.builder("test").settings(settings(Version.CURRENT)).numberOfShards(1).numberOfReplicas(0))
                .build();

        RoutingTable initialRoutingTable = RoutingTable.builder().addAsNew(metadata.index("test")).build();

        ClusterState clusterState = ClusterState.builder(org.elasticsearch.cluster.ClusterName.CLUSTER_NAME_SETTING
            .getDefault(Settings.EMPTY)).metadata(metadata).routingTable(initialRoutingTable).build();

        assertThat(clusterState.routingTable().index("test").shards().size(), equalTo(1));
        assertThat(clusterState.routingTable().index("test").shard(0).size(), equalTo(1));
        assertThat(clusterState.routingTable().index("test").shard(0).shards().size(), equalTo(1));
        assertThat(clusterState.routingTable().index("test").shard(0).shards().get(0).state(), equalTo(UNASSIGNED));
        assertThat(clusterState.routingTable().index("test").shard(0).shards().get(0).currentNodeId(), nullValue());

        logger.info("Adding one node and performing rerouting");
        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder().add(newNode("node1"))).build();
        clusterState = strategy.reroute(clusterState, "reroute");

        assertThat(clusterState.routingTable().index("test").shards().size(), equalTo(1));
        assertThat(clusterState.routingTable().index("test").shard(0).size(), equalTo(1));
        assertThat(clusterState.routingTable().index("test").shard(0).shards().size(), equalTo(1));
        assertThat(clusterState.routingTable().index("test").shard(0).shards().get(0).state(), equalTo(INITIALIZING));
        assertThat(clusterState.routingTable().index("test").shard(0).shards().get(0).currentNodeId(), equalTo("node1"));

        logger.info("Rerouting again, nothing should change");
        clusterState = ClusterState.builder(clusterState).build();
        ClusterState newState = strategy.reroute(clusterState, "reroute");
        assertThat(newState, equalTo(clusterState));
        clusterState = newState;

        logger.info("Marking the shard as started");
        RoutingNodes routingNodes = clusterState.getRoutingNodes();
        newState = startInitializingShardsAndReroute(strategy, clusterState, routingNodes.node("node1"));
        assertThat(newState, not(equalTo(clusterState)));
        clusterState = newState;

        assertThat(clusterState.routingTable().index("test").shards().size(), equalTo(1));
        assertThat(clusterState.routingTable().index("test").shard(0).size(), equalTo(1));
        assertThat(clusterState.routingTable().index("test").shard(0).shards().size(), equalTo(1));
        assertThat(clusterState.routingTable().index("test").shard(0).shards().get(0).state(), equalTo(STARTED));
        assertThat(clusterState.routingTable().index("test").shard(0).shards().get(0).currentNodeId(), equalTo("node1"));

        logger.info("Starting another node and making sure nothing changed");
        clusterState = ClusterState.builder(clusterState)
            .nodes(DiscoveryNodes.builder(clusterState.nodes()).add(newNode("node2"))).build();
        newState = strategy.reroute(clusterState, "reroute");
        assertThat(newState, equalTo(clusterState));
        clusterState = newState;

        assertThat(clusterState.routingTable().index("test").shards().size(), equalTo(1));
        assertThat(clusterState.routingTable().index("test").shard(0).size(), equalTo(1));
        assertThat(clusterState.routingTable().index("test").shard(0).shards().size(), equalTo(1));
        assertThat(clusterState.routingTable().index("test").shard(0).shards().get(0).state(), equalTo(STARTED));
        assertThat(clusterState.routingTable().index("test").shard(0).shards().get(0).currentNodeId(), equalTo("node1"));

        logger.info("Killing node1 where the shard is, checking the shard is unassigned");

        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder(clusterState.nodes()).remove("node1")).build();
        newState = strategy.disassociateDeadNodes(clusterState, true, "reroute");
        assertThat(newState, not(equalTo(clusterState)));
        clusterState = newState;

        assertThat(clusterState.routingTable().index("test").shards().size(), equalTo(1));
        assertThat(clusterState.routingTable().index("test").shard(0).size(), equalTo(1));
        assertThat(clusterState.routingTable().index("test").shard(0).shards().size(), equalTo(1));
        assertThat(clusterState.routingTable().index("test").shard(0).shards().get(0).state(), equalTo(UNASSIGNED));
        assertThat(clusterState.routingTable().index("test").shard(0).shards().get(0).currentNodeId(), nullValue());

        logger.info("Bring node1 back, and see it's assinged");

        clusterState = ClusterState.builder(clusterState)
            .nodes(DiscoveryNodes.builder(clusterState.nodes()).add(newNode("node1"))).build();
        newState = strategy.reroute(clusterState, "reroute");
        assertThat(newState, not(equalTo(clusterState)));
        clusterState = newState;

        assertThat(clusterState.routingTable().index("test").shards().size(), equalTo(1));
        assertThat(clusterState.routingTable().index("test").shard(0).size(), equalTo(1));
        assertThat(clusterState.routingTable().index("test").shard(0).shards().size(), equalTo(1));
        assertThat(clusterState.routingTable().index("test").shard(0).shards().get(0).state(), equalTo(INITIALIZING));
        assertThat(clusterState.routingTable().index("test").shard(0).shards().get(0).currentNodeId(), equalTo("node1"));


        logger.info("Start another node, make sure that things remain the same (shard is in node2 and initializing)");
        clusterState = ClusterState.builder(clusterState)
            .nodes(DiscoveryNodes.builder(clusterState.nodes()).add(newNode("node3"))).build();
        newState = strategy.reroute(clusterState, "reroute");
        assertThat(newState, equalTo(clusterState));

        logger.info("Start the shard on node 1");
        routingNodes = clusterState.getRoutingNodes();
        newState = startInitializingShardsAndReroute(strategy, clusterState, routingNodes.node("node1"));
        assertThat(newState, not(equalTo(clusterState)));
        clusterState = newState;

        assertThat(clusterState.routingTable().index("test").shards().size(), equalTo(1));
        assertThat(clusterState.routingTable().index("test").shard(0).size(), equalTo(1));
        assertThat(clusterState.routingTable().index("test").shard(0).shards().size(), equalTo(1));
        assertThat(clusterState.routingTable().index("test").shard(0).shards().get(0).state(), equalTo(STARTED));
        assertThat(clusterState.routingTable().index("test").shard(0).shards().get(0).currentNodeId(), equalTo("node1"));
    }

    public void testSingleIndexShardFailed() {
        AllocationService strategy = createAllocationService(Settings.builder()
            .put("cluster.routing.allocation.node_concurrent_recoveries", 10).build());

        logger.info("Building initial routing table");

        Metadata metadata = Metadata.builder()
                .put(IndexMetadata.builder("test").settings(settings(Version.CURRENT)).numberOfShards(1).numberOfReplicas(0))
                .build();

        RoutingTable.Builder routingTableBuilder = RoutingTable.builder()
                .addAsNew(metadata.index("test"));

        ClusterState clusterState = ClusterState.builder(org.elasticsearch.cluster.ClusterName.CLUSTER_NAME_SETTING
            .getDefault(Settings.EMPTY)).metadata(metadata).routingTable(routingTableBuilder.build()).build();

        assertThat(clusterState.routingTable().index("test").shards().size(), equalTo(1));
        assertThat(clusterState.routingTable().index("test").shard(0).size(), equalTo(1));
        assertThat(clusterState.routingTable().index("test").shard(0).shards().size(), equalTo(1));
        assertThat(clusterState.routingTable().index("test").shard(0).shards().get(0).state(), equalTo(UNASSIGNED));
        assertThat(clusterState.routingTable().index("test").shard(0).shards().get(0).currentNodeId(), nullValue());

        logger.info("Adding one node and rerouting");
        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder().add(newNode("node1"))).build();
        ClusterState newState = strategy.reroute(clusterState, "reroute");
        assertThat(newState, not(equalTo(clusterState)));
        clusterState = newState;

        assertThat(clusterState.routingTable().index("test").shards().size(), equalTo(1));
        assertThat(clusterState.routingTable().index("test").shard(0).size(), equalTo(1));
        assertThat(clusterState.routingTable().index("test").shard(0).shards().size(), equalTo(1));
        assertThat(clusterState.routingTable().index("test").shard(0).shards().get(0).unassigned(), equalTo(false));
        assertThat(clusterState.routingTable().index("test").shard(0).shards().get(0).state(), equalTo(INITIALIZING));
        assertThat(clusterState.routingTable().index("test").shard(0).shards().get(0).currentNodeId(), equalTo("node1"));

        logger.info("Marking the shard as failed");
        RoutingNodes routingNodes = clusterState.getRoutingNodes();
        newState = strategy.applyFailedShard(clusterState,
            routingNodes.node("node1").shardsWithState(INITIALIZING).get(0), randomBoolean());
        assertThat(newState, not(equalTo(clusterState)));
        clusterState = newState;

        assertThat(clusterState.routingTable().index("test").shards().size(), equalTo(1));
        assertThat(clusterState.routingTable().index("test").shard(0).size(), equalTo(1));
        assertThat(clusterState.routingTable().index("test").shard(0).shards().size(), equalTo(1));
        assertThat(clusterState.routingTable().index("test").shard(0).shards().get(0).state(), equalTo(UNASSIGNED));
        assertThat(clusterState.routingTable().index("test").shard(0).shards().get(0).currentNodeId(), nullValue());
    }

    public void testMultiIndexEvenDistribution() {
        AllocationService strategy = createAllocationService(Settings.builder()
                .put("cluster.routing.allocation.node_concurrent_recoveries", 10)
                .put(ClusterRebalanceAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ALLOW_REBALANCE_SETTING.getKey(), "always")
                .put("cluster.routing.allocation.cluster_concurrent_rebalance", -1)
                .build());

        final int numberOfIndices = 50;
        logger.info("Building initial routing table with " + numberOfIndices + " indices");

        Metadata.Builder metadataBuilder = Metadata.builder();
        for (int i = 0; i < numberOfIndices; i++) {
            metadataBuilder.put(IndexMetadata.builder("test" + i)
                .settings(settings(Version.CURRENT)).numberOfShards(1).numberOfReplicas(0));
        }
        Metadata metadata = metadataBuilder.build();

        RoutingTable.Builder routingTableBuilder = RoutingTable.builder();
        for (int i = 0; i < numberOfIndices; i++) {
            routingTableBuilder.addAsNew(metadata.index("test" + i));
        }
        ClusterState clusterState = ClusterState.builder(org.elasticsearch.cluster.ClusterName.CLUSTER_NAME_SETTING
            .getDefault(Settings.EMPTY)).metadata(metadata).routingTable(routingTableBuilder.build()).build();

        assertThat(clusterState.routingTable().indicesRouting().size(), equalTo(numberOfIndices));
        for (int i = 0; i < numberOfIndices; i++) {
            assertThat(clusterState.routingTable().index("test" + i).shards().size(), equalTo(1));
            assertThat(clusterState.routingTable().index("test" + i).shard(0).size(), equalTo(1));
            assertThat(clusterState.routingTable().index("test" + i).shard(0).shards().size(), equalTo(1));
            assertThat(clusterState.routingTable().index("test" + i).shard(0).shards().get(0).state(), equalTo(UNASSIGNED));
            assertThat(clusterState.routingTable().index("test" + i).shard(0).shards().get(0).currentNodeId(), nullValue());
        }

        logger.info("Adding " + (numberOfIndices / 2) + " nodes");
        DiscoveryNodes.Builder nodesBuilder = DiscoveryNodes.builder();
        for (int i = 0; i < (numberOfIndices / 2); i++) {
            nodesBuilder.add(newNode("node" + i));
        }
        clusterState = ClusterState.builder(clusterState).nodes(nodesBuilder).build();
        ClusterState newState = strategy.reroute(clusterState, "reroute");
        assertThat(newState, not(equalTo(clusterState)));
        clusterState = newState;

        for (int i = 0; i < numberOfIndices; i++) {
            assertThat(clusterState.routingTable().index("test" + i).shards().size(), equalTo(1));
            assertThat(clusterState.routingTable().index("test" + i).shard(0).size(), equalTo(1));
            assertThat(clusterState.routingTable().index("test" + i).shard(0).shards().size(), equalTo(1));
            assertThat(clusterState.routingTable().index("test" + i).shard(0).shards().get(0).unassigned(), equalTo(false));
            assertThat(clusterState.routingTable().index("test" + i).shard(0).shards().get(0).state(), equalTo(INITIALIZING));
            assertThat(clusterState.routingTable().index("test" + i).shard(0).shards().get(0).primary(), equalTo(true));
            // make sure we still have 2 shards initializing per node on the first 25 nodes
            String nodeId = clusterState.routingTable().index("test" + i).shard(0).shards().get(0).currentNodeId();
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
        clusterState = ClusterState.builder(clusterState).nodes(nodesBuilder).build();
        newState = strategy.reroute(clusterState, "reroute");
        assertThat(newState, equalTo(clusterState));

        logger.info("Marking the shard as started");
        newState = startShardsAndReroute(strategy, clusterState, routingNodes.shardsWithState(INITIALIZING));
        assertThat(newState, not(equalTo(clusterState)));

        clusterState = newState;
        int numberOfRelocatingShards = 0;
        int numberOfStartedShards = 0;
        for (int i = 0; i < numberOfIndices; i++) {
            assertThat(clusterState.routingTable().index("test" + i).shards().size(), equalTo(1));
            assertThat(clusterState.routingTable().index("test" + i).shard(0).size(), equalTo(1));
            assertThat(clusterState.routingTable().index("test" + i).shard(0).shards().size(), equalTo(1));
            assertThat(clusterState.routingTable().index("test" + i).shard(0).shards().get(0).unassigned(), equalTo(false));
            assertThat(clusterState.routingTable().index("test" + i).shard(0).shards().get(0).state(),
                anyOf(equalTo(STARTED), equalTo(RELOCATING)));
            if (clusterState.routingTable().index("test" + i).shard(0).shards().get(0).state() == STARTED) {
                numberOfStartedShards++;
            } else if (clusterState.routingTable().index("test" + i).shard(0).shards().get(0).state() == RELOCATING) {
                numberOfRelocatingShards++;
            }
            assertThat(clusterState.routingTable().index("test" + i).shard(0).shards().get(0).primary(), equalTo(true));
            // make sure we still have 2 shards either relocating or started on the first 25 nodes (still)
            String nodeId = clusterState.routingTable().index("test" + i).shard(0).shards().get(0).currentNodeId();
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

        Metadata.Builder metadataBuilder = Metadata.builder();
        for (int i = 0; i < numberOfIndices; i++) {
            metadataBuilder.put(IndexMetadata.builder("test" + i)
                .settings(settings(Version.CURRENT)).numberOfShards(1).numberOfReplicas(0));
        }
        Metadata metadata = metadataBuilder.build();

        RoutingTable.Builder routingTableBuilder = RoutingTable.builder();
        for (int i = 0; i < numberOfIndices; i++) {
            routingTableBuilder.addAsNew(metadata.index("test" + i));
        }

        ClusterState clusterState = ClusterState.builder(org.elasticsearch.cluster.ClusterName.CLUSTER_NAME_SETTING
            .getDefault(Settings.EMPTY)).metadata(metadata).routingTable(routingTableBuilder.build()).build();

        assertThat(clusterState.routingTable().indicesRouting().size(), equalTo(numberOfIndices));

        logger.info("Starting 3 nodes and rerouting");
        clusterState = ClusterState.builder(clusterState)
                .nodes(DiscoveryNodes.builder().add(newNode("node1")).add(newNode("node2")).add(newNode("node3")))
                .build();
        ClusterState newState = strategy.reroute(clusterState, "reroute");
        assertThat(newState, not(equalTo(clusterState)));
        clusterState = newState;

        for (int i = 0; i < numberOfIndices; i++) {
            assertThat(clusterState.routingTable().index("test" + i).shards().size(), equalTo(1));
            assertThat(clusterState.routingTable().index("test" + i).shard(0).size(), equalTo(1));
            assertThat(clusterState.routingTable().index("test" + i).shard(0).shards().size(), equalTo(1));
            assertThat(clusterState.routingTable().index("test" + i).shard(0).shards().get(0).state(), equalTo(INITIALIZING));
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
        newState = strategy.reroute(clusterState, "reroute");
        assertThat(newState, equalTo(clusterState));
        clusterState = newState;

        newState = startInitializingShardsAndReroute(strategy, clusterState);
        assertThat(newState, not(equalTo(clusterState)));
        clusterState = newState;

        for (int i = 0; i < numberOfIndices; i++) {
            assertThat(clusterState.routingTable().index("test" + i).shards().size(), equalTo(1));
            assertThat(clusterState.routingTable().index("test" + i).shard(0).size(), equalTo(1));
            assertThat(clusterState.routingTable().index("test" + i).shard(0).shards().size(), equalTo(1));
            assertThat(clusterState.routingTable().index("test" + i).shard(0).shards().get(0).state(),
                anyOf(equalTo(RELOCATING), equalTo(STARTED)));
        }
        routingNodes = clusterState.getRoutingNodes();
        assertThat("4 source shard routing are relocating", numberOfShardsOfType(routingNodes, RELOCATING), equalTo(4));
        assertThat("4 target shard routing are initializing", numberOfShardsOfType(routingNodes, INITIALIZING), equalTo(4));

        logger.info("Now, mark the relocated as started");
        newState = startInitializingShardsAndReroute(strategy, clusterState);
//        routingTable = strategy.reroute(new RoutingStrategyInfo(metadata, routingTable), nodes);
        assertThat(newState, not(equalTo(clusterState)));
        clusterState = newState;

        for (int i = 0; i < numberOfIndices; i++) {
            assertThat(clusterState.routingTable().index("test" + i).shards().size(), equalTo(1));
            assertThat(clusterState.routingTable().index("test" + i).shard(0).size(), equalTo(1));
            assertThat(clusterState.routingTable().index("test" + i).shard(0).shards().size(), equalTo(1));
            assertThat(clusterState.routingTable().index("test" + i).shard(0).shards().get(0).state(),
                anyOf(equalTo(RELOCATING), equalTo(STARTED)));
        }
        routingNodes = clusterState.getRoutingNodes();
        assertThat(numberOfShardsOfType(routingNodes, STARTED), equalTo(numberOfIndices));
        for (RoutingNode routingNode : routingNodes) {
            assertThat(routingNode.numberOfShardsWithState(STARTED), equalTo(2));
        }
    }
}
