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
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ESAllocationTestCase;
import org.elasticsearch.cluster.TestShardRoutingRoleStrategies;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.RoutingNodes;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexVersion;

import static org.elasticsearch.cluster.routing.RoutingNodesHelper.shardsWithState;
import static org.elasticsearch.cluster.routing.ShardRoutingState.INITIALIZING;
import static org.elasticsearch.cluster.routing.ShardRoutingState.STARTED;
import static org.elasticsearch.cluster.routing.ShardRoutingState.UNASSIGNED;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

public class PrimaryElectionRoutingTests extends ESAllocationTestCase {
    private final Logger logger = LogManager.getLogger(PrimaryElectionRoutingTests.class);

    public void testBackupElectionToPrimaryWhenPrimaryCanBeAllocatedToAnotherNode() {
        AllocationService strategy = createAllocationService(
            Settings.builder().put("cluster.routing.allocation.node_concurrent_recoveries", 10).build()
        );

        logger.info("Building initial routing table");

        Metadata metadata = Metadata.builder()
            .put(IndexMetadata.builder("test").settings(settings(IndexVersion.current())).numberOfShards(1).numberOfReplicas(1))
            .build();

        RoutingTable routingTable = RoutingTable.builder(TestShardRoutingRoleStrategies.DEFAULT_ROLE_ONLY)
            .addAsNew(metadata.index("test"))
            .build();

        ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT).metadata(metadata).routingTable(routingTable).build();

        logger.info("Adding two nodes and performing rerouting");
        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder().add(newNode("node1"))).build();
        clusterState = strategy.reroute(clusterState, "reroute", ActionListener.noop());

        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder(clusterState.nodes()).add(newNode("node2"))).build();
        clusterState = strategy.reroute(clusterState, "reroute", ActionListener.noop());

        logger.info("Start the primary shard (on node1)");
        RoutingNodes routingNodes = clusterState.getRoutingNodes();
        clusterState = startInitializingShardsAndReroute(strategy, clusterState, routingNodes.node("node1"));

        logger.info("Start the backup shard (on node2)");
        routingNodes = clusterState.getRoutingNodes();
        clusterState = startInitializingShardsAndReroute(strategy, clusterState, routingNodes.node("node2"));

        logger.info("Adding third node and reroute and kill first node");
        clusterState = ClusterState.builder(clusterState)
            .nodes(DiscoveryNodes.builder(clusterState.nodes()).add(newNode("node3")).remove("node1"))
            .build();
        RoutingTable prevRoutingTable = clusterState.routingTable();
        clusterState = strategy.disassociateDeadNodes(clusterState, true, "reroute");
        routingNodes = clusterState.getRoutingNodes();
        routingTable = clusterState.routingTable();

        assertThat(prevRoutingTable != routingTable, equalTo(true));
        assertThat(routingTable.index("test").size(), equalTo(1));
        assertThat(routingNodes.node("node1"), nullValue());
        assertThat(routingNodes.node("node2").numberOfShardsWithState(STARTED), equalTo(1));
        assertThat(routingNodes.node("node3").numberOfShardsWithState(INITIALIZING), equalTo(1));
        // verify where the primary is
        assertThat(routingTable.index("test").shard(0).primaryShard().currentNodeId(), equalTo("node2"));
        assertThat(clusterState.metadata().index("test").primaryTerm(0), equalTo(2L));
        assertThat(routingTable.index("test").shard(0).replicaShards().get(0).currentNodeId(), equalTo("node3"));
    }

    public void testRemovingInitializingReplicasIfPrimariesFails() {
        AllocationService allocation = createAllocationService(
            Settings.builder().put("cluster.routing.allocation.node_concurrent_recoveries", 10).build()
        );

        logger.info("Building initial routing table");

        Metadata metadata = Metadata.builder()
            .put(IndexMetadata.builder("test").settings(settings(IndexVersion.current())).numberOfShards(2).numberOfReplicas(1))
            .build();

        RoutingTable routingTable = RoutingTable.builder(TestShardRoutingRoleStrategies.DEFAULT_ROLE_ONLY)
            .addAsNew(metadata.index("test"))
            .build();

        ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT).metadata(metadata).routingTable(routingTable).build();

        logger.info("Adding two nodes and performing rerouting");
        clusterState = ClusterState.builder(clusterState)
            .nodes(DiscoveryNodes.builder().add(newNode("node1")).add(newNode("node2")))
            .build();
        clusterState = allocation.reroute(clusterState, "reroute", ActionListener.noop());

        logger.info("Start the primary shards");
        clusterState = startInitializingShardsAndReroute(allocation, clusterState);
        RoutingNodes routingNodes = clusterState.getRoutingNodes();

        assertThat(shardsWithState(routingNodes, STARTED).size(), equalTo(2));
        assertThat(shardsWithState(routingNodes, INITIALIZING).size(), equalTo(2));
        assertThat(clusterState.metadata().index("test").primaryTerm(0), equalTo(1L));
        assertThat(clusterState.metadata().index("test").primaryTerm(1), equalTo(1L));

        // now, fail one node, while the replica is initializing, and it also holds a primary
        logger.info("--> fail node with primary");
        String nodeIdToFail = clusterState.routingTable().index("test").shard(0).primaryShard().currentNodeId();
        String nodeIdRemaining = nodeIdToFail.equals("node1") ? "node2" : "node1";
        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder().add(newNode(nodeIdRemaining))).build();
        clusterState = allocation.disassociateDeadNodes(clusterState, true, "reroute");
        routingNodes = clusterState.getRoutingNodes();

        assertThat(shardsWithState(routingNodes, STARTED).size(), equalTo(1));
        assertThat(shardsWithState(routingNodes, INITIALIZING).size(), equalTo(0));
        assertThat(shardsWithState(routingNodes, UNASSIGNED).size(), equalTo(3)); // 2 replicas and one primary
        assertThat(routingNodes.node(nodeIdRemaining).shardsWithState(STARTED).findFirst().get().primary(), equalTo(true));
        assertThat(clusterState.metadata().index("test").primaryTerm(0), equalTo(2L));
    }
}
