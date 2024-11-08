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
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.allocation.command.AllocationCommands;
import org.elasticsearch.cluster.routing.allocation.command.CancelAllocationCommand;
import org.elasticsearch.cluster.routing.allocation.command.MoveAllocationCommand;
import org.elasticsearch.cluster.routing.allocation.decider.AllocationDeciders;
import org.elasticsearch.cluster.routing.allocation.decider.AwarenessAllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.ClusterRebalanceAllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.Decision;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexVersion;

import java.util.HashMap;
import java.util.Map;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;

import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static org.elasticsearch.cluster.routing.RoutingNodesHelper.shardsWithState;
import static org.elasticsearch.cluster.routing.ShardRoutingState.INITIALIZING;
import static org.elasticsearch.cluster.routing.ShardRoutingState.RELOCATING;
import static org.elasticsearch.cluster.routing.ShardRoutingState.STARTED;
import static org.elasticsearch.cluster.routing.ShardRoutingState.UNASSIGNED;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.sameInstance;

public class AwarenessAllocationTests extends ESAllocationTestCase {

    public void testMoveShardOnceNewNodeWithAttributeAdded1() {
        AllocationService strategy = createAllocationService(
            Settings.builder()
                .put("cluster.routing.allocation.node_concurrent_recoveries", 10)
                .put(ClusterRebalanceAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ALLOW_REBALANCE_SETTING.getKey(), "always")
                .put("cluster.routing.allocation.awareness.attributes", "rack_id")
                .build()
        );

        logger.info("Building initial routing table for 'moveShardOnceNewNodeWithAttributeAdded1'");

        Metadata metadata = Metadata.builder()
            .put(IndexMetadata.builder("test").settings(settings(IndexVersion.current())).numberOfShards(1).numberOfReplicas(1))
            .build();

        RoutingTable initialRoutingTable = RoutingTable.builder(TestShardRoutingRoleStrategies.DEFAULT_ROLE_ONLY)
            .addAsNew(metadata.getProject().index("test"))
            .build();

        ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT).metadata(metadata).routingTable(initialRoutingTable).build();

        logger.info("--> adding two nodes on same rack and do rerouting");
        clusterState = ClusterState.builder(clusterState)
            .nodes(
                DiscoveryNodes.builder()
                    .add(newNode("node1", singletonMap("rack_id", "1")))
                    .add(newNode("node2", singletonMap("rack_id", "1")))
            )
            .build();
        clusterState = strategy.reroute(clusterState, "reroute", ActionListener.noop());
        assertThat(shardsWithState(clusterState.getRoutingNodes(), INITIALIZING).size(), equalTo(1));

        logger.info("--> start the shards (primaries)");
        clusterState = startInitializingShardsAndReroute(strategy, clusterState);

        logger.info("--> start the shards (replicas)");
        clusterState = startInitializingShardsAndReroute(strategy, clusterState);

        assertThat(shardsWithState(clusterState.getRoutingNodes(), ShardRoutingState.STARTED).size(), equalTo(2));

        logger.info("--> add a new node with a new rack and reroute");
        clusterState = ClusterState.builder(clusterState)
            .nodes(DiscoveryNodes.builder(clusterState.nodes()).add(newNode("node3", singletonMap("rack_id", "2"))))
            .build();
        clusterState = strategy.reroute(clusterState, "reroute", ActionListener.noop());

        assertThat(shardsWithState(clusterState.getRoutingNodes(), ShardRoutingState.STARTED).size(), equalTo(1));
        assertThat(shardsWithState(clusterState.getRoutingNodes(), ShardRoutingState.RELOCATING).size(), equalTo(1));
        assertThat(
            shardsWithState(clusterState.getRoutingNodes(), ShardRoutingState.RELOCATING).get(0).relocatingNodeId(),
            equalTo("node3")
        );

        logger.info("--> complete relocation");
        clusterState = startInitializingShardsAndReroute(strategy, clusterState);

        assertThat(shardsWithState(clusterState.getRoutingNodes(), ShardRoutingState.STARTED).size(), equalTo(2));

        logger.info("--> do another reroute, make sure nothing moves");
        assertThat(
            strategy.reroute(clusterState, "reroute", ActionListener.noop()).routingTable(),
            sameInstance(clusterState.routingTable())
        );

        logger.info("--> add another node with a new rack, make sure nothing moves");
        clusterState = ClusterState.builder(clusterState)
            .nodes(DiscoveryNodes.builder(clusterState.nodes()).add(newNode("node4", singletonMap("rack_id", "3"))))
            .build();
        ClusterState newState = strategy.reroute(clusterState, "reroute", ActionListener.noop());
        assertThat(newState, equalTo(clusterState));
        assertThat(shardsWithState(clusterState.getRoutingNodes(), STARTED).size(), equalTo(2));
    }

    public void testMoveShardOnceNewNodeWithAttributeAdded2() {
        AllocationService strategy = createAllocationService(
            Settings.builder()
                .put("cluster.routing.allocation.concurrent_recoveries", 10)
                .put(ClusterRebalanceAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ALLOW_REBALANCE_SETTING.getKey(), "always")
                .put("cluster.routing.allocation.awareness.attributes", "rack_id")
                .build()
        );

        logger.info("Building initial routing table for 'moveShardOnceNewNodeWithAttributeAdded2'");

        Metadata metadata = Metadata.builder()
            .put(IndexMetadata.builder("test").settings(settings(IndexVersion.current())).numberOfShards(1).numberOfReplicas(1))
            .build();

        RoutingTable initialRoutingTable = RoutingTable.builder(TestShardRoutingRoleStrategies.DEFAULT_ROLE_ONLY)
            .addAsNew(metadata.getProject().index("test"))
            .build();

        ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT).metadata(metadata).routingTable(initialRoutingTable).build();

        logger.info("--> adding two nodes on same rack and do rerouting");
        clusterState = ClusterState.builder(clusterState)
            .nodes(
                DiscoveryNodes.builder()
                    .add(newNode("node1", singletonMap("rack_id", "1")))
                    .add(newNode("node2", singletonMap("rack_id", "1")))
                    .add(newNode("node3", singletonMap("rack_id", "1")))
            )
            .build();
        clusterState = strategy.reroute(clusterState, "reroute", ActionListener.noop());
        assertThat(shardsWithState(clusterState.getRoutingNodes(), INITIALIZING).size(), equalTo(1));

        logger.info("--> start the shards (primaries)");
        clusterState = startInitializingShardsAndReroute(strategy, clusterState);

        logger.info("--> start the shards (replicas)");
        clusterState = startInitializingShardsAndReroute(strategy, clusterState);

        assertThat(shardsWithState(clusterState.getRoutingNodes(), ShardRoutingState.STARTED).size(), equalTo(2));

        logger.info("--> add a new node with a new rack and reroute");
        clusterState = ClusterState.builder(clusterState)
            .nodes(DiscoveryNodes.builder(clusterState.nodes()).add(newNode("node4", singletonMap("rack_id", "2"))))
            .build();
        clusterState = strategy.reroute(clusterState, "reroute", ActionListener.noop());

        assertThat(shardsWithState(clusterState.getRoutingNodes(), ShardRoutingState.STARTED).size(), equalTo(1));
        assertThat(shardsWithState(clusterState.getRoutingNodes(), ShardRoutingState.RELOCATING).size(), equalTo(1));
        assertThat(
            shardsWithState(clusterState.getRoutingNodes(), ShardRoutingState.RELOCATING).get(0).relocatingNodeId(),
            equalTo("node4")
        );

        logger.info("--> complete relocation");
        clusterState = startInitializingShardsAndReroute(strategy, clusterState);

        assertThat(shardsWithState(clusterState.getRoutingNodes(), ShardRoutingState.STARTED).size(), equalTo(2));

        logger.info("--> do another reroute, make sure nothing moves");
        assertThat(
            strategy.reroute(clusterState, "reroute", ActionListener.noop()).routingTable(),
            sameInstance(clusterState.routingTable())
        );

        logger.info("--> add another node with a new rack, make sure nothing moves");
        clusterState = ClusterState.builder(clusterState)
            .nodes(DiscoveryNodes.builder(clusterState.nodes()).add(newNode("node5", singletonMap("rack_id", "3"))))
            .build();
        ClusterState newState = strategy.reroute(clusterState, "reroute", ActionListener.noop());
        assertThat(newState, equalTo(clusterState));
        assertThat(shardsWithState(clusterState.getRoutingNodes(), STARTED).size(), equalTo(2));
    }

    public void testMoveShardOnceNewNodeWithAttributeAdded3() {
        AllocationService strategy = createAllocationService(
            Settings.builder()
                .put("cluster.routing.allocation.node_concurrent_recoveries", 10)
                .put("cluster.routing.allocation.node_initial_primaries_recoveries", 10)
                .put(ClusterRebalanceAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ALLOW_REBALANCE_SETTING.getKey(), "always")
                .put("cluster.routing.allocation.cluster_concurrent_rebalance", -1)
                .put("cluster.routing.allocation.awareness.attributes", "rack_id")
                .put("cluster.routing.allocation.balance.index", 0.0f)
                .put("cluster.routing.allocation.balance.replica", 1.0f)
                .put("cluster.routing.allocation.balance.primary", 0.0f)
                .build()
        );

        logger.info("Building initial routing table for 'moveShardOnceNewNodeWithAttributeAdded3'");

        Metadata metadata = Metadata.builder()
            .put(IndexMetadata.builder("test").settings(settings(IndexVersion.current())).numberOfShards(5).numberOfReplicas(1))
            .build();

        RoutingTable initialRoutingTable = RoutingTable.builder(TestShardRoutingRoleStrategies.DEFAULT_ROLE_ONLY)
            .addAsNew(metadata.getProject().index("test"))
            .build();

        ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT).metadata(metadata).routingTable(initialRoutingTable).build();

        logger.info("--> adding two nodes on same rack and do rerouting");
        clusterState = ClusterState.builder(clusterState)
            .nodes(
                DiscoveryNodes.builder()
                    .add(newNode("node1", singletonMap("rack_id", "1")))
                    .add(newNode("node2", singletonMap("rack_id", "1")))
            )
            .build();
        clusterState = strategy.reroute(clusterState, "reroute", ActionListener.noop());

        logger.info("Initializing shards: {}", shardsWithState(clusterState.getRoutingNodes(), INITIALIZING));
        logger.info("Started shards: {}", shardsWithState(clusterState.getRoutingNodes(), STARTED));
        logger.info("Relocating shards: {}", shardsWithState(clusterState.getRoutingNodes(), RELOCATING));
        logger.info("Unassigned shards: {}", shardsWithState(clusterState.getRoutingNodes(), UNASSIGNED));

        assertThat(shardsWithState(clusterState.getRoutingNodes(), INITIALIZING).size(), equalTo(5));

        logger.info("--> start the shards (primaries)");
        clusterState = startInitializingShardsAndReroute(strategy, clusterState);

        logger.info("--> start the shards (replicas)");
        clusterState = startInitializingShardsAndReroute(strategy, clusterState);

        assertThat(shardsWithState(clusterState.getRoutingNodes(), ShardRoutingState.STARTED).size(), equalTo(10));

        logger.info("--> add a new node with a new rack and reroute");
        clusterState = ClusterState.builder(clusterState)
            .nodes(DiscoveryNodes.builder(clusterState.nodes()).add(newNode("node3", singletonMap("rack_id", "2"))))
            .build();
        clusterState = strategy.reroute(clusterState, "reroute", ActionListener.noop());

        assertThat(shardsWithState(clusterState.getRoutingNodes(), ShardRoutingState.STARTED).size(), equalTo(5));
        assertThat(shardsWithState(clusterState.getRoutingNodes(), ShardRoutingState.RELOCATING).size(), equalTo(5));
        assertThat(shardsWithState(clusterState.getRoutingNodes(), ShardRoutingState.INITIALIZING).size(), equalTo(5));
        assertThat(
            shardsWithState(clusterState.getRoutingNodes(), ShardRoutingState.RELOCATING).get(0).relocatingNodeId(),
            equalTo("node3")
        );

        logger.info("--> complete initializing");
        clusterState = startInitializingShardsAndReroute(strategy, clusterState);

        logger.info("--> run it again, since we still might have relocation");
        clusterState = startInitializingShardsAndReroute(strategy, clusterState);

        assertThat(shardsWithState(clusterState.getRoutingNodes(), ShardRoutingState.STARTED).size(), equalTo(10));

        logger.info("--> do another reroute, make sure nothing moves");
        assertThat(
            strategy.reroute(clusterState, "reroute", ActionListener.noop()).routingTable(),
            sameInstance(clusterState.routingTable())
        );

        logger.info("--> add another node with a new rack, some more relocation should happen");
        clusterState = ClusterState.builder(clusterState)
            .nodes(DiscoveryNodes.builder(clusterState.nodes()).add(newNode("node4", singletonMap("rack_id", "3"))))
            .build();
        clusterState = strategy.reroute(clusterState, "reroute", ActionListener.noop());
        assertThat(shardsWithState(clusterState.getRoutingNodes(), RELOCATING).size(), greaterThan(0));

        logger.info("--> complete relocation");
        clusterState = startInitializingShardsAndReroute(strategy, clusterState);

        assertThat(shardsWithState(clusterState.getRoutingNodes(), ShardRoutingState.STARTED).size(), equalTo(10));

        logger.info("--> do another reroute, make sure nothing moves");
        assertThat(
            strategy.reroute(clusterState, "reroute", ActionListener.noop()).routingTable(),
            sameInstance(clusterState.routingTable())
        );
    }

    public void testMoveShardOnceNewNodeWithAttributeAdded4() {
        AllocationService strategy = createAllocationService(
            Settings.builder()
                .put("cluster.routing.allocation.node_concurrent_recoveries", 10)
                .put("cluster.routing.allocation.node_initial_primaries_recoveries", 10)
                .put(ClusterRebalanceAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ALLOW_REBALANCE_SETTING.getKey(), "always")
                .put("cluster.routing.allocation.cluster_concurrent_rebalance", -1)
                .put("cluster.routing.allocation.awareness.attributes", "rack_id")
                .build()
        );

        logger.info("Building initial routing table for 'moveShardOnceNewNodeWithAttributeAdded4'");

        Metadata metadata = Metadata.builder()
            .put(IndexMetadata.builder("test1").settings(settings(IndexVersion.current())).numberOfShards(5).numberOfReplicas(1))
            .put(IndexMetadata.builder("test2").settings(settings(IndexVersion.current())).numberOfShards(5).numberOfReplicas(1))
            .build();

        RoutingTable initialRoutingTable = RoutingTable.builder(TestShardRoutingRoleStrategies.DEFAULT_ROLE_ONLY)
            .addAsNew(metadata.getProject().index("test1"))
            .addAsNew(metadata.getProject().index("test2"))
            .build();

        ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT).metadata(metadata).routingTable(initialRoutingTable).build();

        logger.info("--> adding two nodes on same rack and do rerouting");
        clusterState = ClusterState.builder(clusterState)
            .nodes(
                DiscoveryNodes.builder()
                    .add(newNode("node1", singletonMap("rack_id", "1")))
                    .add(newNode("node2", singletonMap("rack_id", "1")))
            )
            .build();
        clusterState = strategy.reroute(clusterState, "reroute", ActionListener.noop());
        assertThat(shardsWithState(clusterState.getRoutingNodes(), INITIALIZING).size(), equalTo(10));

        logger.info("--> start the shards (primaries)");
        clusterState = startInitializingShardsAndReroute(strategy, clusterState);

        logger.info("--> start the shards (replicas)");
        clusterState = startInitializingShardsAndReroute(strategy, clusterState);

        assertThat(shardsWithState(clusterState.getRoutingNodes(), ShardRoutingState.STARTED).size(), equalTo(20));

        logger.info("--> add a new node with a new rack and reroute");
        clusterState = ClusterState.builder(clusterState)
            .nodes(DiscoveryNodes.builder(clusterState.nodes()).add(newNode("node3", singletonMap("rack_id", "2"))))
            .build();
        clusterState = strategy.reroute(clusterState, "reroute", ActionListener.noop());

        assertThat(shardsWithState(clusterState.getRoutingNodes(), ShardRoutingState.STARTED).size(), equalTo(10));
        assertThat(shardsWithState(clusterState.getRoutingNodes(), ShardRoutingState.RELOCATING).size(), equalTo(10));
        assertThat(shardsWithState(clusterState.getRoutingNodes(), ShardRoutingState.INITIALIZING).size(), equalTo(10));
        assertThat(
            shardsWithState(clusterState.getRoutingNodes(), ShardRoutingState.RELOCATING).get(0).relocatingNodeId(),
            equalTo("node3")
        );

        logger.info("--> complete initializing");
        for (int i = 0; i < 2; i++) {
            logger.info("--> complete initializing round: [{}]", i);
            clusterState = startInitializingShardsAndReroute(strategy, clusterState);
        }
        clusterState = startInitializingShardsAndReroute(strategy, clusterState);

        assertThat(shardsWithState(clusterState.getRoutingNodes(), ShardRoutingState.STARTED).size(), equalTo(20));
        assertThat(clusterState.getRoutingNodes().node("node3").size(), equalTo(10));
        assertThat(clusterState.getRoutingNodes().node("node2").size(), equalTo(5));
        assertThat(clusterState.getRoutingNodes().node("node1").size(), equalTo(5));

        logger.info("--> do another reroute, make sure nothing moves");
        assertThat(
            strategy.reroute(clusterState, "reroute", ActionListener.noop()).routingTable(),
            sameInstance(clusterState.routingTable())
        );

        logger.info("--> add another node with a new rack, some more relocation should happen");
        clusterState = ClusterState.builder(clusterState)
            .nodes(DiscoveryNodes.builder(clusterState.nodes()).add(newNode("node4", singletonMap("rack_id", "3"))))
            .build();
        clusterState = strategy.reroute(clusterState, "reroute", ActionListener.noop());
        assertThat(shardsWithState(clusterState.getRoutingNodes(), RELOCATING).size(), greaterThan(0));

        logger.info("--> complete relocation");
        for (int i = 0; i < 2; i++) {
            logger.info("--> complete initializing round: [{}]", i);
            clusterState = startInitializingShardsAndReroute(strategy, clusterState);
        }
        assertThat(shardsWithState(clusterState.getRoutingNodes(), ShardRoutingState.STARTED).size(), equalTo(20));
        assertThat(clusterState.getRoutingNodes().node("node3").size(), equalTo(5));
        assertThat(clusterState.getRoutingNodes().node("node4").size(), equalTo(5));
        assertThat(clusterState.getRoutingNodes().node("node2").size(), equalTo(5));
        assertThat(clusterState.getRoutingNodes().node("node1").size(), equalTo(5));

        logger.info("--> do another reroute, make sure nothing moves");
        assertThat(
            strategy.reroute(clusterState, "reroute", ActionListener.noop()).routingTable(),
            sameInstance(clusterState.routingTable())
        );
    }

    public void testMoveShardOnceNewNodeWithAttributeAdded5() {
        AllocationService strategy = createAllocationService(
            Settings.builder()
                .put("cluster.routing.allocation.node_concurrent_recoveries", 10)
                .put(ClusterRebalanceAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ALLOW_REBALANCE_SETTING.getKey(), "always")
                .put("cluster.routing.allocation.awareness.attributes", "rack_id")
                .build()
        );

        logger.info("Building initial routing table for 'moveShardOnceNewNodeWithAttributeAdded5'");

        Metadata metadata = Metadata.builder()
            .put(IndexMetadata.builder("test").settings(settings(IndexVersion.current())).numberOfShards(1).numberOfReplicas(2))
            .build();

        RoutingTable initialRoutingTable = RoutingTable.builder(TestShardRoutingRoleStrategies.DEFAULT_ROLE_ONLY)
            .addAsNew(metadata.getProject().index("test"))
            .build();

        ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT).metadata(metadata).routingTable(initialRoutingTable).build();

        logger.info("--> adding two nodes on same rack and do rerouting");
        clusterState = ClusterState.builder(clusterState)
            .nodes(
                DiscoveryNodes.builder()
                    .add(newNode("node1", singletonMap("rack_id", "1")))
                    .add(newNode("node2", singletonMap("rack_id", "1")))
            )
            .build();
        clusterState = strategy.reroute(clusterState, "reroute", ActionListener.noop());
        assertThat(shardsWithState(clusterState.getRoutingNodes(), INITIALIZING).size(), equalTo(1));

        logger.info("--> start the shards (primaries)");
        clusterState = startInitializingShardsAndReroute(strategy, clusterState);

        logger.info("--> start the shards (replicas)");
        clusterState = startInitializingShardsAndReroute(strategy, clusterState);

        assertThat(shardsWithState(clusterState.getRoutingNodes(), ShardRoutingState.STARTED).size(), equalTo(2));

        logger.info("--> add a new node with a new rack and reroute");
        clusterState = ClusterState.builder(clusterState)
            .nodes(DiscoveryNodes.builder(clusterState.nodes()).add(newNode("node3", singletonMap("rack_id", "2"))))
            .build();
        clusterState = strategy.reroute(clusterState, "reroute", ActionListener.noop());

        assertThat(shardsWithState(clusterState.getRoutingNodes(), ShardRoutingState.STARTED).size(), equalTo(2));
        assertThat(shardsWithState(clusterState.getRoutingNodes(), ShardRoutingState.INITIALIZING).size(), equalTo(1));
        assertThat(
            shardsWithState(clusterState.getRoutingNodes(), ShardRoutingState.INITIALIZING).get(0).currentNodeId(),
            equalTo("node3")
        );

        logger.info("--> complete relocation");
        clusterState = startInitializingShardsAndReroute(strategy, clusterState);

        assertThat(shardsWithState(clusterState.getRoutingNodes(), ShardRoutingState.STARTED).size(), equalTo(3));

        logger.info("--> do another reroute, make sure nothing moves");
        assertThat(
            strategy.reroute(clusterState, "reroute", ActionListener.noop()).routingTable(),
            sameInstance(clusterState.routingTable())
        );

        logger.info("--> add another node with a new rack, we will have another relocation");
        clusterState = ClusterState.builder(clusterState)
            .nodes(DiscoveryNodes.builder(clusterState.nodes()).add(newNode("node4", singletonMap("rack_id", "3"))))
            .build();
        clusterState = strategy.reroute(clusterState, "reroute", ActionListener.noop());
        assertThat(shardsWithState(clusterState.getRoutingNodes(), STARTED).size(), equalTo(2));
        assertThat(shardsWithState(clusterState.getRoutingNodes(), ShardRoutingState.RELOCATING).size(), equalTo(1));
        assertThat(
            shardsWithState(clusterState.getRoutingNodes(), ShardRoutingState.RELOCATING).get(0).relocatingNodeId(),
            equalTo("node4")
        );

        logger.info("--> complete relocation");
        clusterState = startInitializingShardsAndReroute(strategy, clusterState);

        assertThat(shardsWithState(clusterState.getRoutingNodes(), ShardRoutingState.STARTED).size(), equalTo(3));

        logger.info("--> make sure another reroute does not move things");
        assertThat(
            strategy.reroute(clusterState, "reroute", ActionListener.noop()).routingTable(),
            sameInstance(clusterState.routingTable())
        );
    }

    public void testMoveShardOnceNewNodeWithAttributeAdded6() {
        AllocationService strategy = createAllocationService(
            Settings.builder()
                .put("cluster.routing.allocation.node_concurrent_recoveries", 10)
                .put(ClusterRebalanceAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ALLOW_REBALANCE_SETTING.getKey(), "always")
                .put("cluster.routing.allocation.awareness.attributes", "rack_id")
                .build()
        );

        logger.info("Building initial routing table for 'moveShardOnceNewNodeWithAttributeAdded6'");

        Metadata metadata = Metadata.builder()
            .put(IndexMetadata.builder("test").settings(settings(IndexVersion.current())).numberOfShards(1).numberOfReplicas(3))
            .build();

        RoutingTable initialRoutingTable = RoutingTable.builder(TestShardRoutingRoleStrategies.DEFAULT_ROLE_ONLY)
            .addAsNew(metadata.getProject().index("test"))
            .build();

        ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT).metadata(metadata).routingTable(initialRoutingTable).build();

        logger.info("--> adding two nodes on same rack and do rerouting");
        clusterState = ClusterState.builder(clusterState)
            .nodes(
                DiscoveryNodes.builder()
                    .add(newNode("node1", singletonMap("rack_id", "1")))
                    .add(newNode("node2", singletonMap("rack_id", "1")))
                    .add(newNode("node3", singletonMap("rack_id", "1")))
                    .add(newNode("node4", singletonMap("rack_id", "1")))
            )
            .build();
        clusterState = strategy.reroute(clusterState, "reroute", ActionListener.noop());
        assertThat(shardsWithState(clusterState.getRoutingNodes(), INITIALIZING).size(), equalTo(1));

        logger.info("--> start the shards (primaries)");
        clusterState = startInitializingShardsAndReroute(strategy, clusterState);

        logger.info("--> start the shards (replicas)");
        clusterState = startInitializingShardsAndReroute(strategy, clusterState);

        assertThat(shardsWithState(clusterState.getRoutingNodes(), ShardRoutingState.STARTED).size(), equalTo(4));

        logger.info("--> add a new node with a new rack and reroute");
        clusterState = ClusterState.builder(clusterState)
            .nodes(DiscoveryNodes.builder(clusterState.nodes()).add(newNode("node5", singletonMap("rack_id", "2"))))
            .build();
        clusterState = strategy.reroute(clusterState, "reroute", ActionListener.noop());

        assertThat(shardsWithState(clusterState.getRoutingNodes(), ShardRoutingState.STARTED).size(), equalTo(3));
        assertThat(shardsWithState(clusterState.getRoutingNodes(), ShardRoutingState.RELOCATING).size(), equalTo(1));
        assertThat(
            shardsWithState(clusterState.getRoutingNodes(), ShardRoutingState.RELOCATING).get(0).relocatingNodeId(),
            equalTo("node5")
        );

        logger.info("--> complete relocation");
        clusterState = startInitializingShardsAndReroute(strategy, clusterState);

        assertThat(shardsWithState(clusterState.getRoutingNodes(), ShardRoutingState.STARTED).size(), equalTo(4));

        logger.info("--> do another reroute, make sure nothing moves");
        assertThat(
            strategy.reroute(clusterState, "reroute", ActionListener.noop()).routingTable(),
            sameInstance(clusterState.routingTable())
        );

        logger.info("--> add another node with a new rack, we will have another relocation");
        clusterState = ClusterState.builder(clusterState)
            .nodes(DiscoveryNodes.builder(clusterState.nodes()).add(newNode("node6", singletonMap("rack_id", "3"))))
            .build();
        clusterState = strategy.reroute(clusterState, "reroute", ActionListener.noop());
        assertThat(shardsWithState(clusterState.getRoutingNodes(), STARTED).size(), equalTo(3));
        assertThat(shardsWithState(clusterState.getRoutingNodes(), ShardRoutingState.RELOCATING).size(), equalTo(1));
        assertThat(
            shardsWithState(clusterState.getRoutingNodes(), ShardRoutingState.RELOCATING).get(0).relocatingNodeId(),
            equalTo("node6")
        );

        logger.info("--> complete relocation");
        clusterState = startInitializingShardsAndReroute(strategy, clusterState);

        assertThat(shardsWithState(clusterState.getRoutingNodes(), ShardRoutingState.STARTED).size(), equalTo(4));

        logger.info("--> make sure another reroute does not move things");
        assertThat(
            strategy.reroute(clusterState, "reroute", ActionListener.noop()).routingTable(),
            sameInstance(clusterState.routingTable())
        );
    }

    public void testFullAwareness1() {
        AllocationService strategy = createAllocationService(
            Settings.builder()
                .put("cluster.routing.allocation.node_concurrent_recoveries", 10)
                .put(ClusterRebalanceAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ALLOW_REBALANCE_SETTING.getKey(), "always")
                .put("cluster.routing.allocation.awareness.force.rack_id.values", "1,2")
                .put("cluster.routing.allocation.awareness.attributes", "rack_id")
                .build()
        );

        logger.info("Building initial routing table for 'fullAwareness1'");

        Metadata metadata = Metadata.builder()
            .put(IndexMetadata.builder("test").settings(settings(IndexVersion.current())).numberOfShards(1).numberOfReplicas(1))
            .build();

        RoutingTable initialRoutingTable = RoutingTable.builder(TestShardRoutingRoleStrategies.DEFAULT_ROLE_ONLY)
            .addAsNew(metadata.getProject().index("test"))
            .build();

        ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT).metadata(metadata).routingTable(initialRoutingTable).build();

        logger.info("--> adding two nodes on same rack and do rerouting");
        clusterState = ClusterState.builder(clusterState)
            .nodes(
                DiscoveryNodes.builder()
                    .add(newNode("node1", singletonMap("rack_id", "1")))
                    .add(newNode("node2", singletonMap("rack_id", "1")))
            )
            .build();
        clusterState = strategy.reroute(clusterState, "reroute", ActionListener.noop());
        assertThat(shardsWithState(clusterState.getRoutingNodes(), INITIALIZING).size(), equalTo(1));

        logger.info("--> start the shards (primaries)");
        clusterState = startInitializingShardsAndReroute(strategy, clusterState);

        logger.info("--> replica will not start because we have only one rack value");
        assertThat(shardsWithState(clusterState.getRoutingNodes(), STARTED).size(), equalTo(1));
        assertThat(shardsWithState(clusterState.getRoutingNodes(), INITIALIZING).size(), equalTo(0));

        logger.info("--> add a new node with a new rack and reroute");
        clusterState = ClusterState.builder(clusterState)
            .nodes(DiscoveryNodes.builder(clusterState.nodes()).add(newNode("node3", singletonMap("rack_id", "2"))))
            .build();
        clusterState = strategy.reroute(clusterState, "reroute", ActionListener.noop());

        assertThat(shardsWithState(clusterState.getRoutingNodes(), ShardRoutingState.STARTED).size(), equalTo(1));
        assertThat(shardsWithState(clusterState.getRoutingNodes(), ShardRoutingState.INITIALIZING).size(), equalTo(1));
        assertThat(
            shardsWithState(clusterState.getRoutingNodes(), ShardRoutingState.INITIALIZING).get(0).currentNodeId(),
            equalTo("node3")
        );

        logger.info("--> complete relocation");
        clusterState = startInitializingShardsAndReroute(strategy, clusterState);

        assertThat(shardsWithState(clusterState.getRoutingNodes(), ShardRoutingState.STARTED).size(), equalTo(2));

        logger.info("--> do another reroute, make sure nothing moves");
        assertThat(
            strategy.reroute(clusterState, "reroute", ActionListener.noop()).routingTable(),
            sameInstance(clusterState.routingTable())
        );

        logger.info("--> add another node with a new rack, make sure nothing moves");
        clusterState = ClusterState.builder(clusterState)
            .nodes(DiscoveryNodes.builder(clusterState.nodes()).add(newNode("node4", singletonMap("rack_id", "3"))))
            .build();
        ClusterState newState = strategy.reroute(clusterState, "reroute", ActionListener.noop());
        assertThat(newState, equalTo(clusterState));
        assertThat(shardsWithState(clusterState.getRoutingNodes(), STARTED).size(), equalTo(2));
    }

    public void testFullAwareness2() {
        AllocationService strategy = createAllocationService(
            Settings.builder()
                .put("cluster.routing.allocation.node_concurrent_recoveries", 10)
                .put(ClusterRebalanceAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ALLOW_REBALANCE_SETTING.getKey(), "always")
                .put("cluster.routing.allocation.awareness.force.rack_id.values", "1,2")
                .put("cluster.routing.allocation.awareness.attributes", "rack_id")
                .build()
        );

        logger.info("Building initial routing table for 'fullAwareness2'");

        Metadata metadata = Metadata.builder()
            .put(IndexMetadata.builder("test").settings(settings(IndexVersion.current())).numberOfShards(1).numberOfReplicas(1))
            .build();

        RoutingTable initialRoutingTable = RoutingTable.builder(TestShardRoutingRoleStrategies.DEFAULT_ROLE_ONLY)
            .addAsNew(metadata.getProject().index("test"))
            .build();

        ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT).metadata(metadata).routingTable(initialRoutingTable).build();

        logger.info("--> adding two nodes on same rack and do rerouting");
        clusterState = ClusterState.builder(clusterState)
            .nodes(
                DiscoveryNodes.builder()
                    .add(newNode("node1", singletonMap("rack_id", "1")))
                    .add(newNode("node2", singletonMap("rack_id", "1")))
                    .add(newNode("node3", singletonMap("rack_id", "1")))
            )
            .build();
        clusterState = strategy.reroute(clusterState, "reroute", ActionListener.noop());
        assertThat(shardsWithState(clusterState.getRoutingNodes(), INITIALIZING).size(), equalTo(1));

        logger.info("--> start the shards (primaries)");
        clusterState = startInitializingShardsAndReroute(strategy, clusterState);

        logger.info("--> replica will not start because we have only one rack value");
        assertThat(shardsWithState(clusterState.getRoutingNodes(), STARTED).size(), equalTo(1));
        assertThat(shardsWithState(clusterState.getRoutingNodes(), INITIALIZING).size(), equalTo(0));

        logger.info("--> add a new node with a new rack and reroute");
        clusterState = ClusterState.builder(clusterState)
            .nodes(DiscoveryNodes.builder(clusterState.nodes()).add(newNode("node4", singletonMap("rack_id", "2"))))
            .build();
        clusterState = strategy.reroute(clusterState, "reroute", ActionListener.noop());

        assertThat(shardsWithState(clusterState.getRoutingNodes(), ShardRoutingState.STARTED).size(), equalTo(1));
        assertThat(shardsWithState(clusterState.getRoutingNodes(), ShardRoutingState.INITIALIZING).size(), equalTo(1));
        assertThat(
            shardsWithState(clusterState.getRoutingNodes(), ShardRoutingState.INITIALIZING).get(0).currentNodeId(),
            equalTo("node4")
        );

        logger.info("--> complete relocation");
        clusterState = startInitializingShardsAndReroute(strategy, clusterState);

        assertThat(shardsWithState(clusterState.getRoutingNodes(), ShardRoutingState.STARTED).size(), equalTo(2));

        logger.info("--> do another reroute, make sure nothing moves");
        assertThat(
            strategy.reroute(clusterState, "reroute", ActionListener.noop()).routingTable(),
            sameInstance(clusterState.routingTable())
        );

        logger.info("--> add another node with a new rack, make sure nothing moves");
        clusterState = ClusterState.builder(clusterState)
            .nodes(DiscoveryNodes.builder(clusterState.nodes()).add(newNode("node5", singletonMap("rack_id", "3"))))
            .build();
        ClusterState newState = strategy.reroute(clusterState, "reroute", ActionListener.noop());
        assertThat(newState, equalTo(clusterState));
        assertThat(shardsWithState(clusterState.getRoutingNodes(), STARTED).size(), equalTo(2));
    }

    public void testFullAwareness3() {
        AllocationService strategy = createAllocationService(
            Settings.builder()
                .put("cluster.routing.allocation.node_concurrent_recoveries", 10)
                .put("cluster.routing.allocation.node_initial_primaries_recoveries", 10)
                .put(ClusterRebalanceAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ALLOW_REBALANCE_SETTING.getKey(), "always")
                .put("cluster.routing.allocation.cluster_concurrent_rebalance", -1)
                .put("cluster.routing.allocation.awareness.force.rack_id.values", "1,2")
                .put("cluster.routing.allocation.awareness.attributes", "rack_id")
                .put("cluster.routing.allocation.balance.index", 0.0f)
                .put("cluster.routing.allocation.balance.replica", 1.0f)
                .put("cluster.routing.allocation.balance.primary", 0.0f)
                .build()
        );

        logger.info("Building initial routing table for 'fullAwareness3'");

        Metadata metadata = Metadata.builder()
            .put(IndexMetadata.builder("test1").settings(settings(IndexVersion.current())).numberOfShards(5).numberOfReplicas(1))
            .put(IndexMetadata.builder("test2").settings(settings(IndexVersion.current())).numberOfShards(5).numberOfReplicas(1))
            .build();

        RoutingTable initialRoutingTable = RoutingTable.builder(TestShardRoutingRoleStrategies.DEFAULT_ROLE_ONLY)
            .addAsNew(metadata.getProject().index("test1"))
            .addAsNew(metadata.getProject().index("test2"))
            .build();

        ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT).metadata(metadata).routingTable(initialRoutingTable).build();

        logger.info("--> adding two nodes on same rack and do rerouting");
        clusterState = ClusterState.builder(clusterState)
            .nodes(
                DiscoveryNodes.builder()
                    .add(newNode("node1", singletonMap("rack_id", "1")))
                    .add(newNode("node2", singletonMap("rack_id", "1")))
            )
            .build();
        clusterState = strategy.reroute(clusterState, "reroute", ActionListener.noop());
        assertThat(shardsWithState(clusterState.getRoutingNodes(), INITIALIZING).size(), equalTo(10));

        logger.info("--> start the shards (primaries)");
        clusterState = startInitializingShardsAndReroute(strategy, clusterState);

        assertThat(shardsWithState(clusterState.getRoutingNodes(), ShardRoutingState.STARTED).size(), equalTo(10));

        logger.info("--> add a new node with a new rack and reroute");
        clusterState = ClusterState.builder(clusterState)
            .nodes(DiscoveryNodes.builder(clusterState.nodes()).add(newNode("node3", singletonMap("rack_id", "2"))))
            .build();
        clusterState = strategy.reroute(clusterState, "reroute", ActionListener.noop());

        assertThat(shardsWithState(clusterState.getRoutingNodes(), ShardRoutingState.STARTED).size(), equalTo(10));
        assertThat(shardsWithState(clusterState.getRoutingNodes(), ShardRoutingState.INITIALIZING).size(), equalTo(10));
        assertThat(
            shardsWithState(clusterState.getRoutingNodes(), ShardRoutingState.INITIALIZING).get(0).currentNodeId(),
            equalTo("node3")
        );

        logger.info("--> complete initializing");
        clusterState = startInitializingShardsAndReroute(strategy, clusterState);

        logger.info("--> run it again, since we still might have relocation");
        clusterState = startInitializingShardsAndReroute(strategy, clusterState);

        assertThat(shardsWithState(clusterState.getRoutingNodes(), ShardRoutingState.STARTED).size(), equalTo(20));

        logger.info("--> do another reroute, make sure nothing moves");
        assertThat(
            strategy.reroute(clusterState, "reroute", ActionListener.noop()).routingTable(),
            sameInstance(clusterState.routingTable())
        );

        logger.info("--> add another node with a new rack, some more relocation should happen");
        clusterState = ClusterState.builder(clusterState)
            .nodes(DiscoveryNodes.builder(clusterState.nodes()).add(newNode("node4", singletonMap("rack_id", "3"))))
            .build();
        clusterState = strategy.reroute(clusterState, "reroute", ActionListener.noop());
        assertThat(shardsWithState(clusterState.getRoutingNodes(), RELOCATING).size(), greaterThan(0));

        logger.info("--> complete relocation");
        clusterState = startInitializingShardsAndReroute(strategy, clusterState);

        assertThat(shardsWithState(clusterState.getRoutingNodes(), ShardRoutingState.STARTED).size(), equalTo(20));

        logger.info("--> do another reroute, make sure nothing moves");
        assertThat(
            strategy.reroute(clusterState, "reroute", ActionListener.noop()).routingTable(),
            sameInstance(clusterState.routingTable())
        );
    }

    public void testUnbalancedZones() {
        AllocationService strategy = createAllocationService(
            Settings.builder()
                .put("cluster.routing.allocation.awareness.force.zone.values", "a,b")
                .put("cluster.routing.allocation.awareness.attributes", "zone")
                .put("cluster.routing.allocation.node_concurrent_recoveries", 10)
                .put("cluster.routing.allocation.node_initial_primaries_recoveries", 10)
                .put(ClusterRebalanceAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ALLOW_REBALANCE_SETTING.getKey(), "always")
                .put("cluster.routing.allocation.cluster_concurrent_rebalance", -1)
                .build()
        );

        logger.info("Building initial routing table for 'testUnbalancedZones'");

        Metadata metadata = Metadata.builder()
            .put(IndexMetadata.builder("test").settings(settings(IndexVersion.current())).numberOfShards(5).numberOfReplicas(1))
            .build();

        RoutingTable initialRoutingTable = RoutingTable.builder(TestShardRoutingRoleStrategies.DEFAULT_ROLE_ONLY)
            .addAsNew(metadata.getProject().index("test"))
            .build();

        ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT).metadata(metadata).routingTable(initialRoutingTable).build();

        logger.info("--> adding two nodes in different zones and do rerouting");
        clusterState = ClusterState.builder(clusterState)
            .nodes(DiscoveryNodes.builder().add(newNode("A-0", singletonMap("zone", "a"))).add(newNode("B-0", singletonMap("zone", "b"))))
            .build();
        clusterState = strategy.reroute(clusterState, "reroute", ActionListener.noop());
        assertThat(shardsWithState(clusterState.getRoutingNodes(), STARTED).size(), equalTo(0));
        assertThat(shardsWithState(clusterState.getRoutingNodes(), INITIALIZING).size(), equalTo(5));

        logger.info("--> start the shards (primaries)");
        clusterState = startInitializingShardsAndReroute(strategy, clusterState);
        assertThat(shardsWithState(clusterState.getRoutingNodes(), STARTED).size(), equalTo(5));
        assertThat(shardsWithState(clusterState.getRoutingNodes(), INITIALIZING).size(), equalTo(5));

        clusterState = startInitializingShardsAndReroute(strategy, clusterState);
        logger.info("--> all replicas are allocated and started since we have on node in each zone");
        assertThat(shardsWithState(clusterState.getRoutingNodes(), STARTED).size(), equalTo(10));
        assertThat(shardsWithState(clusterState.getRoutingNodes(), INITIALIZING).size(), equalTo(0));

        logger.info("--> add a new node in zone 'a' and reroute");
        clusterState = ClusterState.builder(clusterState)
            .nodes(DiscoveryNodes.builder(clusterState.nodes()).add(newNode("A-1", singletonMap("zone", "a"))))
            .build();
        clusterState = strategy.reroute(clusterState, "reroute", ActionListener.noop());
        assertThat(shardsWithState(clusterState.getRoutingNodes(), ShardRoutingState.STARTED).size(), equalTo(8));
        assertThat(shardsWithState(clusterState.getRoutingNodes(), ShardRoutingState.INITIALIZING).size(), equalTo(2));
        assertThat(shardsWithState(clusterState.getRoutingNodes(), ShardRoutingState.INITIALIZING).get(0).currentNodeId(), equalTo("A-1"));
        logger.info("--> starting initializing shards on the new node");

        clusterState = startInitializingShardsAndReroute(strategy, clusterState);

        assertThat(shardsWithState(clusterState.getRoutingNodes(), ShardRoutingState.STARTED).size(), equalTo(10));
        assertThat(clusterState.getRoutingNodes().node("A-1").size(), equalTo(2));
        assertThat(clusterState.getRoutingNodes().node("A-0").size(), equalTo(3));
        assertThat(clusterState.getRoutingNodes().node("B-0").size(), equalTo(5));
    }

    public void testUnassignedShardsWithUnbalancedZones() {
        AllocationService strategy = createAllocationService(
            Settings.builder()
                .put("cluster.routing.allocation.node_concurrent_recoveries", 10)
                .put(ClusterRebalanceAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ALLOW_REBALANCE_SETTING.getKey(), "always")
                .put("cluster.routing.allocation.awareness.attributes", "zone")
                .build()
        );

        logger.info("Building initial routing table for 'testUnassignedShardsWithUnbalancedZones'");

        final Settings.Builder indexSettings = settings(IndexVersion.current());
        if (randomBoolean()) {
            indexSettings.put(IndexMetadata.SETTING_AUTO_EXPAND_REPLICAS, "0-4");
        }

        Metadata metadata = Metadata.builder()
            .put(IndexMetadata.builder("test").settings(indexSettings).numberOfShards(1).numberOfReplicas(4))
            .build();

        RoutingTable initialRoutingTable = RoutingTable.builder(TestShardRoutingRoleStrategies.DEFAULT_ROLE_ONLY)
            .addAsNew(metadata.getProject().index("test"))
            .build();

        ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT).metadata(metadata).routingTable(initialRoutingTable).build();

        logger.info("--> adding 5 nodes in different zones and do rerouting");
        clusterState = ClusterState.builder(clusterState)
            .nodes(
                DiscoveryNodes.builder()
                    .add(newNode("A-0", singletonMap("zone", "a")))
                    .add(newNode("A-1", singletonMap("zone", "a")))
                    .add(newNode("A-2", singletonMap("zone", "a")))
                    .add(newNode("A-3", singletonMap("zone", "a")))
                    .add(newNode("A-4", singletonMap("zone", "a")))
            )
            .build();
        clusterState = strategy.reroute(clusterState, "reroute", ActionListener.noop());
        assertThat(shardsWithState(clusterState.getRoutingNodes(), STARTED).size(), equalTo(0));
        assertThat(shardsWithState(clusterState.getRoutingNodes(), INITIALIZING).size(), equalTo(1));

        clusterState = ClusterState.builder(clusterState)
            .nodes(DiscoveryNodes.builder(clusterState.nodes()).add(newNode("B-0", singletonMap("zone", "b"))))
            .build();

        logger.info("--> start the shard (primary)");
        clusterState = startInitializingShardsAndReroute(strategy, clusterState);
        assertThat(shardsWithState(clusterState.getRoutingNodes(), STARTED).size(), equalTo(1));
        assertThat(shardsWithState(clusterState.getRoutingNodes(), INITIALIZING).size(), equalTo(3));
        assertThat(shardsWithState(clusterState.getRoutingNodes(), UNASSIGNED).size(), equalTo(1)); // Unassigned shard is expected.

        AllocationCommands commands = new AllocationCommands();
        final var unusedNodes = clusterState.nodes().stream().map(DiscoveryNode::getId).collect(Collectors.toSet());
        // Cancel all initializing shards
        for (ShardRouting routing : clusterState.routingTable().allShardsIterator()) {
            unusedNodes.remove(routing.currentNodeId());
            if (routing.initializing()) {
                commands.add(new CancelAllocationCommand(routing.shardId().getIndexName(), routing.id(), routing.currentNodeId(), false));
            }
        }
        // Move started primary to another node.
        for (ShardRouting routing : clusterState.routingTable().allShardsIterator()) {
            if (routing.primary()) {
                var currentNodeId = routing.currentNodeId();
                unusedNodes.remove(currentNodeId);
                var otherNodeId = randomFrom(unusedNodes);
                commands.add(new MoveAllocationCommand("test", 0, currentNodeId, otherNodeId));
                break;
            }
        }

        clusterState = strategy.reroute(clusterState, commands, false, false, false, ActionListener.noop()).clusterState();

        assertThat(shardsWithState(clusterState.getRoutingNodes(), STARTED).size(), equalTo(0));
        assertThat(shardsWithState(clusterState.getRoutingNodes(), RELOCATING).size(), equalTo(1));
        assertThat(shardsWithState(clusterState.getRoutingNodes(), INITIALIZING).size(), equalTo(4)); // +1 for relocating shard.
        assertThat(shardsWithState(clusterState.getRoutingNodes(), UNASSIGNED).size(), equalTo(1)); // Still 1 unassigned.
    }

    public void testMultipleAwarenessAttributes() {
        AllocationService strategy = createAllocationService(
            Settings.builder()
                .put(AwarenessAllocationDecider.CLUSTER_ROUTING_ALLOCATION_AWARENESS_ATTRIBUTE_SETTING.getKey(), "zone, rack")
                .put(AwarenessAllocationDecider.CLUSTER_ROUTING_ALLOCATION_AWARENESS_FORCE_GROUP_SETTING.getKey() + "zone.values", "a, b")
                .put(AwarenessAllocationDecider.CLUSTER_ROUTING_ALLOCATION_AWARENESS_FORCE_GROUP_SETTING.getKey() + "rack.values", "c, d")
                .put(ClusterRebalanceAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ALLOW_REBALANCE_SETTING.getKey(), "always")
                .build()
        );

        logger.info("Building initial routing table for 'testUnbalancedZones'");

        Metadata metadata = Metadata.builder()
            .put(IndexMetadata.builder("test").settings(settings(IndexVersion.current())).numberOfShards(1).numberOfReplicas(1))
            .build();

        RoutingTable initialRoutingTable = RoutingTable.builder(TestShardRoutingRoleStrategies.DEFAULT_ROLE_ONLY)
            .addAsNew(metadata.getProject().index("test"))
            .build();

        ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT).metadata(metadata).routingTable(initialRoutingTable).build();

        logger.info("--> adding two nodes in different zones and do rerouting");
        Map<String, String> nodeAAttributes = new HashMap<>();
        nodeAAttributes.put("zone", "a");
        nodeAAttributes.put("rack", "c");
        Map<String, String> nodeBAttributes = new HashMap<>();
        nodeBAttributes.put("zone", "b");
        nodeBAttributes.put("rack", "d");
        clusterState = ClusterState.builder(clusterState)
            .nodes(DiscoveryNodes.builder().add(newNode("A-0", nodeAAttributes)).add(newNode("B-0", nodeBAttributes)))
            .build();
        clusterState = strategy.reroute(clusterState, "reroute", ActionListener.noop());
        assertThat(shardsWithState(clusterState.getRoutingNodes(), STARTED).size(), equalTo(0));
        assertThat(shardsWithState(clusterState.getRoutingNodes(), INITIALIZING).size(), equalTo(1));

        logger.info("--> start the shards (primaries)");
        clusterState = startInitializingShardsAndReroute(strategy, clusterState);
        assertThat(shardsWithState(clusterState.getRoutingNodes(), STARTED).size(), equalTo(1));
        assertThat(shardsWithState(clusterState.getRoutingNodes(), INITIALIZING).size(), equalTo(1));

        clusterState = startInitializingShardsAndReroute(strategy, clusterState);
        logger.info("--> all replicas are allocated and started since we have one node in each zone and rack");
        assertThat(shardsWithState(clusterState.getRoutingNodes(), STARTED).size(), equalTo(2));
        assertThat(shardsWithState(clusterState.getRoutingNodes(), INITIALIZING).size(), equalTo(0));
    }

    public void testDisabledByAutoExpandReplicas() {
        final Settings settings = Settings.builder()
            .put(AwarenessAllocationDecider.CLUSTER_ROUTING_ALLOCATION_AWARENESS_ATTRIBUTE_SETTING.getKey(), "zone")
            .build();

        final AllocationService strategy = createAllocationService(settings);

        final Metadata metadata = Metadata.builder()
            .put(
                IndexMetadata.builder("test")
                    .settings(indexSettings(IndexVersion.current(), 1, 99).put(IndexMetadata.SETTING_AUTO_EXPAND_REPLICAS, "0-all"))
            )
            .build();

        final ClusterState clusterState = applyStartedShardsUntilNoChange(
            ClusterState.builder(ClusterName.DEFAULT)
                .metadata(metadata)
                .routingTable(
                    RoutingTable.builder(TestShardRoutingRoleStrategies.DEFAULT_ROLE_ONLY)
                        .addAsNew(metadata.getProject().index("test"))
                        .build()
                )
                .nodes(
                    DiscoveryNodes.builder()
                        .add(newNode("A-0", singletonMap("zone", "a")))
                        .add(newNode("A-1", singletonMap("zone", "a")))
                        .add(newNode("A-2", singletonMap("zone", "a")))
                        .add(newNode("A-3", singletonMap("zone", "a")))
                        .add(newNode("A-4", singletonMap("zone", "a")))
                        .add(newNode("B-0", singletonMap("zone", "b")))
                )
                .build(),
            strategy
        );

        assertThat(shardsWithState(clusterState.getRoutingNodes(), UNASSIGNED), empty());
    }

    public void testNodesWithoutAttributeAreIgnored() {
        final Settings settings = Settings.builder()
            .put(AwarenessAllocationDecider.CLUSTER_ROUTING_ALLOCATION_AWARENESS_ATTRIBUTE_SETTING.getKey(), "zone")
            .build();

        final AllocationService strategy = createAllocationService(settings);

        final Metadata metadata = Metadata.builder()
            .put(IndexMetadata.builder("test").settings(indexSettings(IndexVersion.current(), 1, 2)))
            .build();

        final ClusterState clusterState = applyStartedShardsUntilNoChange(
            ClusterState.builder(ClusterName.DEFAULT)
                .metadata(metadata)
                .routingTable(
                    RoutingTable.builder(TestShardRoutingRoleStrategies.DEFAULT_ROLE_ONLY)
                        .addAsNew(metadata.getProject().index("test"))
                        .build()
                )
                .nodes(
                    DiscoveryNodes.builder()
                        .add(newNode("A-0", singletonMap("zone", "a")))
                        .add(newNode("A-1", singletonMap("zone", "a")))
                        .add(newNode("B-0", singletonMap("zone", "b")))
                        .add(newNode("B-1", singletonMap("zone", "b")))
                        .add(newNode("X-0", emptyMap()))
                )
                .build(),
            strategy
        );

        assertThat(shardsWithState(clusterState.getRoutingNodes(), UNASSIGNED), empty());
        assertTrue(clusterState.getRoutingNodes().node("X-0").isEmpty());
    }

    public void testExplanation() {
        testExplanation(
            Settings.builder().put(AwarenessAllocationDecider.CLUSTER_ROUTING_ALLOCATION_AWARENESS_ATTRIBUTE_SETTING.getKey(), "zone"),
            UnaryOperator.identity(),
            "there are [5] copies of this shard and [2] values for attribute [zone] ([a, b] from nodes in the cluster and "
                + "no forced awareness) so there may be at most [3] copies of this shard allocated to nodes with each "
                + "value, but (including this copy) there would be [4] copies allocated to nodes with [node.attr.zone: a]"
        );
    }

    public void testExplanationWithMissingAttribute() {
        testExplanation(
            Settings.builder().put(AwarenessAllocationDecider.CLUSTER_ROUTING_ALLOCATION_AWARENESS_ATTRIBUTE_SETTING.getKey(), "zone"),
            n -> n.add(newNode("X-0", emptyMap())),
            "there are [5] copies of this shard and [2] values for attribute [zone] ([a, b] from nodes in the cluster and "
                + "no forced awareness) so there may be at most [3] copies of this shard allocated to nodes with each "
                + "value, but (including this copy) there would be [4] copies allocated to nodes with [node.attr.zone: a]"
        );
    }

    public void testExplanationWithForcedAttributes() {
        testExplanation(
            Settings.builder()
                .put(AwarenessAllocationDecider.CLUSTER_ROUTING_ALLOCATION_AWARENESS_ATTRIBUTE_SETTING.getKey(), "zone")
                .put(AwarenessAllocationDecider.CLUSTER_ROUTING_ALLOCATION_AWARENESS_FORCE_GROUP_SETTING.getKey() + "zone.values", "b,c"),
            UnaryOperator.identity(),
            "there are [5] copies of this shard and [3] values for attribute [zone] ([a, b] from nodes in the cluster and "
                + "[b, c] from forced awareness) so there may be at most [2] copies of this shard allocated to nodes with each "
                + "value, but (including this copy) there would be [3] copies allocated to nodes with [node.attr.zone: a]"
        );
    }

    private void testExplanation(
        Settings.Builder settingsBuilder,
        UnaryOperator<DiscoveryNodes.Builder> nodesOperator,
        String expectedMessage
    ) {
        final Settings settings = settingsBuilder.build();

        final AllocationService strategy = createAllocationService(settings);

        final Metadata metadata = Metadata.builder()
            .put(IndexMetadata.builder("test").settings(settings(IndexVersion.current())).numberOfShards(1).numberOfReplicas(4))
            .build();

        final ClusterState clusterState = applyStartedShardsUntilNoChange(
            ClusterState.builder(ClusterName.DEFAULT)
                .metadata(metadata)
                .routingTable(
                    RoutingTable.builder(TestShardRoutingRoleStrategies.DEFAULT_ROLE_ONLY)
                        .addAsNew(metadata.getProject().index("test"))
                        .build()
                )
                .nodes(
                    nodesOperator.apply(
                        DiscoveryNodes.builder()
                            .add(newNode("A-0", singletonMap("zone", "a")))
                            .add(newNode("A-1", singletonMap("zone", "a")))
                            .add(newNode("A-2", singletonMap("zone", "a")))
                            .add(newNode("A-3", singletonMap("zone", "a")))
                            .add(newNode("A-4", singletonMap("zone", "a")))
                            .add(newNode("B-0", singletonMap("zone", "b")))
                    )
                )
                .build(),
            strategy
        );

        final ShardRouting unassignedShard = shardsWithState(clusterState.getRoutingNodes(), UNASSIGNED).get(0);

        final AwarenessAllocationDecider decider = new AwarenessAllocationDecider(
            settings,
            new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS)
        );

        final RoutingNode emptyNode = clusterState.getRoutingNodes()
            .stream()
            .filter(RoutingNode::isEmpty)
            .findFirst()
            .orElseThrow(AssertionError::new);

        final RoutingAllocation routingAllocation = new RoutingAllocation(
            new AllocationDeciders(singletonList(decider)),
            clusterState,
            null,
            null,
            0L
        );
        routingAllocation.debugDecision(true);

        final Decision decision = decider.canAllocate(unassignedShard, emptyNode, routingAllocation);
        assertThat(decision.type(), equalTo(Decision.Type.NO));
        assertThat(decision.label(), equalTo("awareness"));
        assertThat(decision.getExplanation(), equalTo(expectedMessage));
    }

}
