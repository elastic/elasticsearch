/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.routing.allocation;

import org.apache.logging.log4j.Level;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ESAllocationTestCase;
import org.elasticsearch.cluster.TestShardRoutingRoleStrategies;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.allocation.command.AllocationCommands;
import org.elasticsearch.cluster.routing.allocation.command.MoveAllocationCommand;
import org.elasticsearch.cluster.routing.allocation.decider.ClusterRebalanceAllocationDecider;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.test.MockLog;

import static org.elasticsearch.cluster.routing.ShardRoutingState.INITIALIZING;
import static org.elasticsearch.cluster.routing.ShardRoutingState.RELOCATING;
import static org.elasticsearch.cluster.routing.ShardRoutingState.STARTED;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;

public class DeadNodesAllocationTests extends ESAllocationTestCase {

    public void testSimpleDeadNodeOnStartedPrimaryShard() {
        AllocationService allocation = createAllocationService(
            Settings.builder()
                .put("cluster.routing.allocation.node_concurrent_recoveries", 10)
                .put(ClusterRebalanceAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ALLOW_REBALANCE_SETTING.getKey(), "always")
                .build()
        );

        logger.info("--> building initial routing table");
        Metadata metadata = Metadata.builder()
            .put(IndexMetadata.builder("test").settings(settings(IndexVersion.current())).numberOfShards(1).numberOfReplicas(1))
            .build();
        RoutingTable routingTable = RoutingTable.builder(TestShardRoutingRoleStrategies.DEFAULT_ROLE_ONLY)
            .addAsNew(metadata.getProject().index("test"))
            .build();
        ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT).metadata(metadata).routingTable(routingTable).build();

        logger.info("--> adding 2 nodes on same rack and do rerouting");
        clusterState = ClusterState.builder(clusterState)
            .nodes(DiscoveryNodes.builder().add(newNode("node1")).add(newNode("node2")))
            .build();

        clusterState = allocation.reroute(clusterState, "reroute", ActionListener.noop());

        // starting primaries
        clusterState = startInitializingShardsAndReroute(allocation, clusterState);
        // starting replicas
        clusterState = startInitializingShardsAndReroute(allocation, clusterState);

        logger.info("--> verifying all is allocated");
        assertThat(clusterState.getRoutingNodes().node("node1").size(), equalTo(1));
        assertThat(clusterState.getRoutingNodes().node("node1").iterator().next().state(), equalTo(STARTED));
        assertThat(clusterState.getRoutingNodes().node("node2").size(), equalTo(1));
        assertThat(clusterState.getRoutingNodes().node("node2").iterator().next().state(), equalTo(STARTED));

        logger.info("--> fail node with primary");
        String nodeIdToFail = clusterState.routingTable().index("test").shard(0).primaryShard().currentNodeId();
        String nodeIdRemaining = nodeIdToFail.equals("node1") ? "node2" : "node1";
        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder().add(newNode(nodeIdRemaining))).build();

        clusterState = allocation.disassociateDeadNodes(clusterState, true, "reroute");

        assertThat(clusterState.getRoutingNodes().node(nodeIdRemaining).iterator().next().primary(), equalTo(true));
        assertThat(clusterState.getRoutingNodes().node(nodeIdRemaining).iterator().next().state(), equalTo(STARTED));
    }

    public void testLoggingOnNodeLeft() throws IllegalAccessException {
        final AllocationService allocationService = createAllocationService();
        final Metadata metadata = Metadata.builder()
            .put(IndexMetadata.builder("test").settings(settings(IndexVersion.current())).numberOfShards(1).numberOfReplicas(1))
            .build();
        final ClusterState initialState = applyStartedShardsUntilNoChange(
            ClusterState.builder(ClusterName.DEFAULT)
                .nodes(DiscoveryNodes.builder().add(newNode("node1")).add(newNode("node2")).build())
                .metadata(metadata)
                .routingTable(
                    RoutingTable.builder(TestShardRoutingRoleStrategies.DEFAULT_ROLE_ONLY)
                        .addAsNew(metadata.getProject().index("test"))
                        .build()
                )
                .build(),
            allocationService
        );

        assertTrue(initialState.toString(), initialState.getRoutingNodes().unassigned().isEmpty());

        try (var mockLog = MockLog.capture(AllocationService.class)) {
            final String dissociationReason = "node left " + randomAlphaOfLength(10);

            mockLog.addExpectation(
                new MockLog.SeenEventExpectation(
                    "health change log message",
                    AllocationService.class.getName(),
                    Level.INFO,
                    "Cluster health status changed from [GREEN] to [YELLOW] (reason: [" + dissociationReason + "])"
                )
            );

            allocationService.disassociateDeadNodes(
                ClusterState.builder(initialState)
                    .nodes(DiscoveryNodes.builder(initialState.nodes()).remove(initialState.nodes().resolveNode("node1")).build())
                    .build(),
                false,
                dissociationReason
            );

            mockLog.assertAllExpectationsMatched();
        }
    }

    public void testDeadNodeWhileRelocatingOnToNode() {
        AllocationService allocation = createAllocationService(
            Settings.builder()
                .put("cluster.routing.allocation.node_concurrent_recoveries", 10)
                .put(ClusterRebalanceAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ALLOW_REBALANCE_SETTING.getKey(), "always")
                .build()
        );

        logger.info("--> building initial routing table");
        Metadata metadata = Metadata.builder()
            .put(IndexMetadata.builder("test").settings(settings(IndexVersion.current())).numberOfShards(1).numberOfReplicas(1))
            .build();
        RoutingTable routingTable = RoutingTable.builder(TestShardRoutingRoleStrategies.DEFAULT_ROLE_ONLY)
            .addAsNew(metadata.getProject().index("test"))
            .build();
        ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT).metadata(metadata).routingTable(routingTable).build();

        logger.info("--> adding 2 nodes on same rack and do rerouting");
        clusterState = ClusterState.builder(clusterState)
            .nodes(DiscoveryNodes.builder().add(newNode("node1")).add(newNode("node2")))
            .build();

        clusterState = allocation.reroute(clusterState, "reroute", ActionListener.noop());

        // starting primaries
        clusterState = startInitializingShardsAndReroute(allocation, clusterState);
        // starting replicas
        clusterState = startInitializingShardsAndReroute(allocation, clusterState);

        logger.info("--> verifying all is allocated");
        assertThat(clusterState.getRoutingNodes().node("node1").size(), equalTo(1));
        assertThat(clusterState.getRoutingNodes().node("node1").iterator().next().state(), equalTo(STARTED));
        assertThat(clusterState.getRoutingNodes().node("node2").size(), equalTo(1));
        assertThat(clusterState.getRoutingNodes().node("node2").iterator().next().state(), equalTo(STARTED));

        logger.info("--> adding additional node");
        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder(clusterState.nodes()).add(newNode("node3"))).build();
        clusterState = allocation.reroute(clusterState, "reroute", ActionListener.noop());

        assertThat(clusterState.getRoutingNodes().node("node1").size(), equalTo(1));
        assertThat(clusterState.getRoutingNodes().node("node1").iterator().next().state(), equalTo(STARTED));
        assertThat(clusterState.getRoutingNodes().node("node2").size(), equalTo(1));
        assertThat(clusterState.getRoutingNodes().node("node2").iterator().next().state(), equalTo(STARTED));
        assertThat(clusterState.getRoutingNodes().node("node3").size(), equalTo(0));

        String origPrimaryNodeId = clusterState.routingTable().index("test").shard(0).primaryShard().currentNodeId();
        String origReplicaNodeId = clusterState.routingTable().index("test").shard(0).replicaShards().get(0).currentNodeId();

        logger.info("--> moving primary shard to node3");
        AllocationService.CommandsResult commandsResult = allocation.reroute(
            clusterState,
            new AllocationCommands(
                new MoveAllocationCommand(
                    "test",
                    0,
                    clusterState.routingTable().index("test").shard(0).primaryShard().currentNodeId(),
                    "node3"
                )
            ),
            false,
            false,
            false,
            ActionListener.noop()
        );
        assertThat(commandsResult.clusterState(), not(equalTo(clusterState)));
        clusterState = commandsResult.clusterState();
        assertThat(clusterState.getRoutingNodes().node(origPrimaryNodeId).iterator().next().state(), equalTo(RELOCATING));
        assertThat(clusterState.getRoutingNodes().node("node3").iterator().next().state(), equalTo(INITIALIZING));

        logger.info("--> fail primary shard recovering instance on node3 being initialized by killing node3");
        clusterState = ClusterState.builder(clusterState)
            .nodes(DiscoveryNodes.builder().add(newNode(origPrimaryNodeId)).add(newNode(origReplicaNodeId)))
            .build();
        clusterState = allocation.disassociateDeadNodes(clusterState, true, "reroute");

        assertThat(clusterState.getRoutingNodes().node(origPrimaryNodeId).iterator().next().state(), equalTo(STARTED));
        assertThat(clusterState.getRoutingNodes().node(origReplicaNodeId).iterator().next().state(), equalTo(STARTED));
    }

    public void testDeadNodeWhileRelocatingOnFromNode() {
        AllocationService allocation = createAllocationService(
            Settings.builder()
                .put("cluster.routing.allocation.node_concurrent_recoveries", 10)
                .put(ClusterRebalanceAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ALLOW_REBALANCE_SETTING.getKey(), "always")
                .build()
        );

        logger.info("--> building initial routing table");
        Metadata metadata = Metadata.builder()
            .put(IndexMetadata.builder("test").settings(settings(IndexVersion.current())).numberOfShards(1).numberOfReplicas(1))
            .build();
        RoutingTable routingTable = RoutingTable.builder(TestShardRoutingRoleStrategies.DEFAULT_ROLE_ONLY)
            .addAsNew(metadata.getProject().index("test"))
            .build();
        ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT).metadata(metadata).routingTable(routingTable).build();

        logger.info("--> adding 2 nodes on same rack and do rerouting");
        clusterState = ClusterState.builder(clusterState)
            .nodes(DiscoveryNodes.builder().add(newNode("node1")).add(newNode("node2")))
            .build();

        clusterState = allocation.reroute(clusterState, "reroute", ActionListener.noop());

        // starting primaries
        clusterState = startInitializingShardsAndReroute(allocation, clusterState);
        // starting replicas
        clusterState = startInitializingShardsAndReroute(allocation, clusterState);

        logger.info("--> verifying all is allocated");
        assertThat(clusterState.getRoutingNodes().node("node1").size(), equalTo(1));
        assertThat(clusterState.getRoutingNodes().node("node1").iterator().next().state(), equalTo(STARTED));
        assertThat(clusterState.getRoutingNodes().node("node2").size(), equalTo(1));
        assertThat(clusterState.getRoutingNodes().node("node2").iterator().next().state(), equalTo(STARTED));

        logger.info("--> adding additional node");
        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder(clusterState.nodes()).add(newNode("node3"))).build();
        clusterState = allocation.reroute(clusterState, "reroute", ActionListener.noop());

        assertThat(clusterState.getRoutingNodes().node("node1").size(), equalTo(1));
        assertThat(clusterState.getRoutingNodes().node("node1").iterator().next().state(), equalTo(STARTED));
        assertThat(clusterState.getRoutingNodes().node("node2").size(), equalTo(1));
        assertThat(clusterState.getRoutingNodes().node("node2").iterator().next().state(), equalTo(STARTED));
        assertThat(clusterState.getRoutingNodes().node("node3").size(), equalTo(0));

        String origPrimaryNodeId = clusterState.routingTable().index("test").shard(0).primaryShard().currentNodeId();
        String origReplicaNodeId = clusterState.routingTable().index("test").shard(0).replicaShards().get(0).currentNodeId();

        logger.info("--> moving primary shard to node3");
        AllocationService.CommandsResult commandsResult = allocation.reroute(
            clusterState,
            new AllocationCommands(
                new MoveAllocationCommand(
                    "test",
                    0,
                    clusterState.routingTable().index("test").shard(0).primaryShard().currentNodeId(),
                    "node3"
                )
            ),
            false,
            false,
            false,
            ActionListener.noop()
        );
        assertThat(commandsResult.clusterState(), not(equalTo(clusterState)));
        clusterState = commandsResult.clusterState();
        assertThat(clusterState.getRoutingNodes().node(origPrimaryNodeId).iterator().next().state(), equalTo(RELOCATING));
        assertThat(clusterState.getRoutingNodes().node("node3").iterator().next().state(), equalTo(INITIALIZING));

        logger.info("--> fail primary shard recovering instance on 'origPrimaryNodeId' being relocated");
        clusterState = ClusterState.builder(clusterState)
            .nodes(DiscoveryNodes.builder().add(newNode("node3")).add(newNode(origReplicaNodeId)))
            .build();
        clusterState = allocation.disassociateDeadNodes(clusterState, true, "reroute");

        assertThat(clusterState.getRoutingNodes().node(origReplicaNodeId).iterator().next().state(), equalTo(STARTED));
        assertThat(clusterState.getRoutingNodes().node("node3").iterator().next().state(), equalTo(INITIALIZING));
    }
}
