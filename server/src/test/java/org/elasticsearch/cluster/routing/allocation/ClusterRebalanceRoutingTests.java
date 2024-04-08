/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
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
import org.elasticsearch.cluster.routing.RoutingNodes;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.cluster.routing.allocation.decider.ClusterRebalanceAllocationDecider;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.test.gateway.TestGatewayAllocator;

import java.util.concurrent.atomic.AtomicBoolean;

import static org.elasticsearch.cluster.routing.ShardRoutingState.INITIALIZING;
import static org.elasticsearch.cluster.routing.ShardRoutingState.RELOCATING;
import static org.elasticsearch.cluster.routing.ShardRoutingState.STARTED;
import static org.elasticsearch.cluster.routing.ShardRoutingState.UNASSIGNED;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;

public class ClusterRebalanceRoutingTests extends ESAllocationTestCase {

    public void testAlways() {
        AllocationService strategy = createAllocationService(
            Settings.builder()
                .put(
                    ClusterRebalanceAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ALLOW_REBALANCE_SETTING.getKey(),
                    ClusterRebalanceAllocationDecider.ClusterRebalanceType.ALWAYS.toString()
                )
                .build()
        );

        Metadata metadata = Metadata.builder()
            .put(IndexMetadata.builder("test1").settings(indexSettings(IndexVersion.current(), 1, 1)))
            .put(
                IndexMetadata.builder("test2")
                    .settings(indexSettings(IndexVersion.current(), 1, 1).put("index.routing.allocation.include._id", "node1,node2"))
            )
            .build();

        RoutingTable initialRoutingTable = RoutingTable.builder(TestShardRoutingRoleStrategies.DEFAULT_ROLE_ONLY)
            .addAsNew(metadata.index("test1"))
            .addAsNew(metadata.index("test2"))
            .build();

        ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT).metadata(metadata).routingTable(initialRoutingTable).build();

        logger.info("start two nodes");
        clusterState = ClusterState.builder(clusterState)
            .nodes(DiscoveryNodes.builder().add(newNode("node1")).add(newNode("node2")).localNodeId("node1").masterNodeId("node1"))
            .build();
        clusterState = strategy.reroute(clusterState, "reroute", ActionListener.noop());

        for (int i = 0; i < clusterState.routingTable().index("test1").size(); i++) {
            assertThat(clusterState.routingTable().index("test1").shard(i).size(), equalTo(2));
            assertThat(clusterState.routingTable().index("test1").shard(i).primaryShard().state(), equalTo(INITIALIZING));
            assertThat(clusterState.routingTable().index("test1").shard(i).replicaShards().get(0).state(), equalTo(UNASSIGNED));
        }

        for (int i = 0; i < clusterState.routingTable().index("test2").size(); i++) {
            assertThat(clusterState.routingTable().index("test2").shard(i).size(), equalTo(2));
            assertThat(clusterState.routingTable().index("test2").shard(i).primaryShard().state(), equalTo(INITIALIZING));
            assertThat(clusterState.routingTable().index("test2").shard(i).replicaShards().get(0).state(), equalTo(UNASSIGNED));
        }

        logger.info("start all the primary shards for test1, replicas will start initializing");
        clusterState = startInitializingShardsAndReroute(strategy, clusterState, "test1");

        for (int i = 0; i < clusterState.routingTable().index("test1").size(); i++) {
            assertThat(clusterState.routingTable().index("test1").shard(i).size(), equalTo(2));
            assertThat(clusterState.routingTable().index("test1").shard(i).primaryShard().state(), equalTo(STARTED));
            assertThat(clusterState.routingTable().index("test1").shard(i).replicaShards().get(0).state(), equalTo(INITIALIZING));
        }

        for (int i = 0; i < clusterState.routingTable().index("test2").size(); i++) {
            assertThat(clusterState.routingTable().index("test2").shard(i).size(), equalTo(2));
            assertThat(clusterState.routingTable().index("test2").shard(i).primaryShard().state(), equalTo(INITIALIZING));
            assertThat(clusterState.routingTable().index("test2").shard(i).replicaShards().get(0).state(), equalTo(UNASSIGNED));
        }

        logger.info("start the test1 replica shards");
        clusterState = startInitializingShardsAndReroute(strategy, clusterState, "test1");

        for (int i = 0; i < clusterState.routingTable().index("test1").size(); i++) {
            assertThat(clusterState.routingTable().index("test1").shard(i).size(), equalTo(2));
            assertThat(clusterState.routingTable().index("test1").shard(i).primaryShard().state(), equalTo(STARTED));
            assertThat(clusterState.routingTable().index("test1").shard(i).replicaShards().get(0).state(), equalTo(STARTED));
        }

        for (int i = 0; i < clusterState.routingTable().index("test2").size(); i++) {
            assertThat(clusterState.routingTable().index("test2").shard(i).size(), equalTo(2));
            assertThat(clusterState.routingTable().index("test2").shard(i).primaryShard().state(), equalTo(INITIALIZING));
            assertThat(clusterState.routingTable().index("test2").shard(i).replicaShards().get(0).state(), equalTo(UNASSIGNED));
        }

        logger.info("now, start 1 more nodes, check that rebalancing will happen (for test1) because we set it to always");
        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder(clusterState.nodes()).add(newNode("node3"))).build();
        clusterState = strategy.reroute(clusterState, "reroute", ActionListener.noop());

        final var newNodesIterator = clusterState.getRoutingNodes().node("node3").iterator();
        assertThat(newNodesIterator.next().shardId().getIndex().getName(), equalTo("test1"));
        assertFalse(newNodesIterator.hasNext());
    }

    public void testClusterPrimariesActive1() {
        AllocationService strategy = createAllocationService(
            Settings.builder()
                .put(
                    ClusterRebalanceAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ALLOW_REBALANCE_SETTING.getKey(),
                    ClusterRebalanceAllocationDecider.ClusterRebalanceType.INDICES_PRIMARIES_ACTIVE.toString()
                )
                .build()
        );

        Metadata metadata = Metadata.builder()
            .put(IndexMetadata.builder("test1").settings(settings(IndexVersion.current())).numberOfShards(1).numberOfReplicas(1))
            .put(
                IndexMetadata.builder("test2")
                    .settings(
                        settings(IndexVersion.current()).put(
                            IndexMetadata.INDEX_ROUTING_INCLUDE_GROUP_SETTING.getConcreteSettingForNamespace("_id").getKey(),
                            "node1,node2"
                        )
                    )
                    .numberOfShards(1)
                    .numberOfReplicas(1)
            )
            .build();

        RoutingTable initialRoutingTable = RoutingTable.builder(TestShardRoutingRoleStrategies.DEFAULT_ROLE_ONLY)
            .addAsNew(metadata.index("test1"))
            .addAsNew(metadata.index("test2"))
            .build();

        ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT).metadata(metadata).routingTable(initialRoutingTable).build();

        logger.info("start two nodes");
        clusterState = ClusterState.builder(clusterState)
            .nodes(DiscoveryNodes.builder().add(newNode("node1")).add(newNode("node2")))
            .build();
        clusterState = strategy.reroute(clusterState, "reroute", ActionListener.noop());
        for (int i = 0; i < clusterState.routingTable().index("test1").size(); i++) {
            assertThat(clusterState.routingTable().index("test1").shard(i).size(), equalTo(2));
            assertThat(clusterState.routingTable().index("test1").shard(i).primaryShard().state(), equalTo(INITIALIZING));
            assertThat(clusterState.routingTable().index("test1").shard(i).replicaShards().get(0).state(), equalTo(UNASSIGNED));
        }

        for (int i = 0; i < clusterState.routingTable().index("test2").size(); i++) {
            assertThat(clusterState.routingTable().index("test2").shard(i).size(), equalTo(2));
            assertThat(clusterState.routingTable().index("test2").shard(i).primaryShard().state(), equalTo(INITIALIZING));
            assertThat(clusterState.routingTable().index("test2").shard(i).replicaShards().get(0).state(), equalTo(UNASSIGNED));
        }

        logger.info("start all the primary shards for test1, replicas will start initializing");
        clusterState = startInitializingShardsAndReroute(strategy, clusterState, "test1");

        for (int i = 0; i < clusterState.routingTable().index("test1").size(); i++) {
            assertThat(clusterState.routingTable().index("test1").shard(i).size(), equalTo(2));
            assertThat(clusterState.routingTable().index("test1").shard(i).primaryShard().state(), equalTo(STARTED));
            assertThat(clusterState.routingTable().index("test1").shard(i).replicaShards().get(0).state(), equalTo(INITIALIZING));
        }

        for (int i = 0; i < clusterState.routingTable().index("test2").size(); i++) {
            assertThat(clusterState.routingTable().index("test2").shard(i).size(), equalTo(2));
            assertThat(clusterState.routingTable().index("test2").shard(i).primaryShard().state(), equalTo(INITIALIZING));
            assertThat(clusterState.routingTable().index("test2").shard(i).replicaShards().get(0).state(), equalTo(UNASSIGNED));
        }

        logger.info("start the test1 replica shards");
        clusterState = startInitializingShardsAndReroute(strategy, clusterState, "test1");

        for (int i = 0; i < clusterState.routingTable().index("test1").size(); i++) {
            assertThat(clusterState.routingTable().index("test1").shard(i).size(), equalTo(2));
            assertThat(clusterState.routingTable().index("test1").shard(i).primaryShard().state(), equalTo(STARTED));
            assertThat(clusterState.routingTable().index("test1").shard(i).replicaShards().get(0).state(), equalTo(STARTED));
        }

        for (int i = 0; i < clusterState.routingTable().index("test2").size(); i++) {
            assertThat(clusterState.routingTable().index("test2").shard(i).size(), equalTo(2));
            assertThat(clusterState.routingTable().index("test2").shard(i).primaryShard().state(), equalTo(INITIALIZING));
            assertThat(clusterState.routingTable().index("test2").shard(i).replicaShards().get(0).state(), equalTo(UNASSIGNED));
        }

        logger.info("start all the primary shards for test2, replicas will start initializing");
        clusterState = startInitializingShardsAndReroute(strategy, clusterState, "test2");

        for (int i = 0; i < clusterState.routingTable().index("test1").size(); i++) {
            assertThat(clusterState.routingTable().index("test1").shard(i).size(), equalTo(2));
            assertThat(clusterState.routingTable().index("test1").shard(i).primaryShard().state(), equalTo(STARTED));
            assertThat(clusterState.routingTable().index("test1").shard(i).replicaShards().get(0).state(), equalTo(STARTED));
        }

        for (int i = 0; i < clusterState.routingTable().index("test2").size(); i++) {
            assertThat(clusterState.routingTable().index("test2").shard(i).size(), equalTo(2));
            assertThat(clusterState.routingTable().index("test2").shard(i).primaryShard().state(), equalTo(STARTED));
            assertThat(clusterState.routingTable().index("test2").shard(i).replicaShards().get(0).state(), equalTo(INITIALIZING));
        }

        logger.info("now, start 1 more node, check that rebalancing happen (for test1) because we set it to primaries_active");
        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder(clusterState.nodes()).add(newNode("node3"))).build();
        clusterState = strategy.reroute(clusterState, "reroute", ActionListener.noop());

        RoutingNodes routingNodes = clusterState.getRoutingNodes();
        assertThat(routingNodes.node("node3").size(), equalTo(1));
        assertThat(routingNodes.node("node3").iterator().next().shardId().getIndex().getName(), equalTo("test1"));
    }

    public void testClusterPrimariesActive2() {
        AllocationService strategy = createAllocationService(
            Settings.builder()
                .put(
                    ClusterRebalanceAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ALLOW_REBALANCE_SETTING.getKey(),
                    ClusterRebalanceAllocationDecider.ClusterRebalanceType.INDICES_PRIMARIES_ACTIVE.toString()
                )
                .build()
        );

        Metadata metadata = Metadata.builder()
            .put(IndexMetadata.builder("test1").settings(settings(IndexVersion.current())).numberOfShards(1).numberOfReplicas(1))
            .put(IndexMetadata.builder("test2").settings(settings(IndexVersion.current())).numberOfShards(1).numberOfReplicas(1))

            .build();

        RoutingTable initialRoutingTable = RoutingTable.builder(TestShardRoutingRoleStrategies.DEFAULT_ROLE_ONLY)
            .addAsNew(metadata.index("test1"))
            .addAsNew(metadata.index("test2"))
            .build();

        ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT).metadata(metadata).routingTable(initialRoutingTable).build();

        logger.info("start two nodes");
        clusterState = ClusterState.builder(clusterState)
            .nodes(DiscoveryNodes.builder().add(newNode("node1")).add(newNode("node2")))
            .build();
        clusterState = strategy.reroute(clusterState, "reroute", ActionListener.noop());

        for (int i = 0; i < clusterState.routingTable().index("test1").size(); i++) {
            assertThat(clusterState.routingTable().index("test1").shard(i).size(), equalTo(2));
            assertThat(clusterState.routingTable().index("test1").shard(i).primaryShard().state(), equalTo(INITIALIZING));
            assertThat(clusterState.routingTable().index("test1").shard(i).replicaShards().get(0).state(), equalTo(UNASSIGNED));
        }

        for (int i = 0; i < clusterState.routingTable().index("test2").size(); i++) {
            assertThat(clusterState.routingTable().index("test2").shard(i).size(), equalTo(2));
            assertThat(clusterState.routingTable().index("test2").shard(i).primaryShard().state(), equalTo(INITIALIZING));
            assertThat(clusterState.routingTable().index("test2").shard(i).replicaShards().get(0).state(), equalTo(UNASSIGNED));
        }

        logger.info("start all the primary shards for test1, replicas will start initializing");
        clusterState = startInitializingShardsAndReroute(strategy, clusterState, "test1");

        for (int i = 0; i < clusterState.routingTable().index("test1").size(); i++) {
            assertThat(clusterState.routingTable().index("test1").shard(i).size(), equalTo(2));
            assertThat(clusterState.routingTable().index("test1").shard(i).primaryShard().state(), equalTo(STARTED));
            assertThat(clusterState.routingTable().index("test1").shard(i).replicaShards().get(0).state(), equalTo(INITIALIZING));
        }

        for (int i = 0; i < clusterState.routingTable().index("test2").size(); i++) {
            assertThat(clusterState.routingTable().index("test2").shard(i).size(), equalTo(2));
            assertThat(clusterState.routingTable().index("test2").shard(i).primaryShard().state(), equalTo(INITIALIZING));
            assertThat(clusterState.routingTable().index("test2").shard(i).replicaShards().get(0).state(), equalTo(UNASSIGNED));
        }

        logger.info("start the test1 replica shards");
        clusterState = startInitializingShardsAndReroute(strategy, clusterState, "test1");

        for (int i = 0; i < clusterState.routingTable().index("test1").size(); i++) {
            assertThat(clusterState.routingTable().index("test1").shard(i).size(), equalTo(2));
            assertThat(clusterState.routingTable().index("test1").shard(i).primaryShard().state(), equalTo(STARTED));
            assertThat(clusterState.routingTable().index("test1").shard(i).replicaShards().get(0).state(), equalTo(STARTED));
        }

        for (int i = 0; i < clusterState.routingTable().index("test2").size(); i++) {
            assertThat(clusterState.routingTable().index("test2").shard(i).size(), equalTo(2));
            assertThat(clusterState.routingTable().index("test2").shard(i).primaryShard().state(), equalTo(INITIALIZING));
            assertThat(clusterState.routingTable().index("test2").shard(i).replicaShards().get(0).state(), equalTo(UNASSIGNED));
        }

        logger.info("now, start 1 more node, check that rebalancing will not happen (for test1) because we set it to primaries_active");
        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder(clusterState.nodes()).add(newNode("node3"))).build();
        clusterState = strategy.reroute(clusterState, "reroute", ActionListener.noop());
        RoutingNodes routingNodes = clusterState.getRoutingNodes();
        assertThat(routingNodes.node("node3").isEmpty(), equalTo(true));
    }

    public void testClusterAllActive1() {
        AllocationService strategy = createAllocationService(
            Settings.builder()
                .put(
                    ClusterRebalanceAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ALLOW_REBALANCE_SETTING.getKey(),
                    ClusterRebalanceAllocationDecider.ClusterRebalanceType.INDICES_ALL_ACTIVE.toString()
                )
                .build()
        );

        Metadata metadata = Metadata.builder()
            .put(IndexMetadata.builder("test1").settings(settings(IndexVersion.current())).numberOfShards(1).numberOfReplicas(1))
            .put(IndexMetadata.builder("test2").settings(settings(IndexVersion.current())).numberOfShards(1).numberOfReplicas(1))
            .build();

        RoutingTable initialRoutingTable = RoutingTable.builder(TestShardRoutingRoleStrategies.DEFAULT_ROLE_ONLY)
            .addAsNew(metadata.index("test1"))
            .addAsNew(metadata.index("test2"))
            .build();

        ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT).metadata(metadata).routingTable(initialRoutingTable).build();

        logger.info("start two nodes");
        clusterState = ClusterState.builder(clusterState)
            .nodes(DiscoveryNodes.builder().add(newNode("node1")).add(newNode("node2")))
            .build();
        clusterState = strategy.reroute(clusterState, "reroute", ActionListener.noop());

        for (int i = 0; i < clusterState.routingTable().index("test1").size(); i++) {
            assertThat(clusterState.routingTable().index("test1").shard(i).size(), equalTo(2));
            assertThat(clusterState.routingTable().index("test1").shard(i).primaryShard().state(), equalTo(INITIALIZING));
            assertThat(clusterState.routingTable().index("test1").shard(i).replicaShards().get(0).state(), equalTo(UNASSIGNED));
        }

        for (int i = 0; i < clusterState.routingTable().index("test2").size(); i++) {
            assertThat(clusterState.routingTable().index("test2").shard(i).size(), equalTo(2));
            assertThat(clusterState.routingTable().index("test2").shard(i).primaryShard().state(), equalTo(INITIALIZING));
            assertThat(clusterState.routingTable().index("test2").shard(i).replicaShards().get(0).state(), equalTo(UNASSIGNED));
        }

        logger.info("start all the primary shards for test1, replicas will start initializing");
        clusterState = startInitializingShardsAndReroute(strategy, clusterState, "test1");

        for (int i = 0; i < clusterState.routingTable().index("test1").size(); i++) {
            assertThat(clusterState.routingTable().index("test1").shard(i).size(), equalTo(2));
            assertThat(clusterState.routingTable().index("test1").shard(i).primaryShard().state(), equalTo(STARTED));
            assertThat(clusterState.routingTable().index("test1").shard(i).replicaShards().get(0).state(), equalTo(INITIALIZING));
        }

        for (int i = 0; i < clusterState.routingTable().index("test2").size(); i++) {
            assertThat(clusterState.routingTable().index("test2").shard(i).size(), equalTo(2));
            assertThat(clusterState.routingTable().index("test2").shard(i).primaryShard().state(), equalTo(INITIALIZING));
            assertThat(clusterState.routingTable().index("test2").shard(i).replicaShards().get(0).state(), equalTo(UNASSIGNED));
        }

        logger.info("start the test1 replica shards");
        clusterState = startInitializingShardsAndReroute(strategy, clusterState, "test1");

        for (int i = 0; i < clusterState.routingTable().index("test1").size(); i++) {
            assertThat(clusterState.routingTable().index("test1").shard(i).size(), equalTo(2));
            assertThat(clusterState.routingTable().index("test1").shard(i).primaryShard().state(), equalTo(STARTED));
            assertThat(clusterState.routingTable().index("test1").shard(i).replicaShards().get(0).state(), equalTo(STARTED));
        }

        for (int i = 0; i < clusterState.routingTable().index("test2").size(); i++) {
            assertThat(clusterState.routingTable().index("test2").shard(i).size(), equalTo(2));
            assertThat(clusterState.routingTable().index("test2").shard(i).primaryShard().state(), equalTo(INITIALIZING));
            assertThat(clusterState.routingTable().index("test2").shard(i).replicaShards().get(0).state(), equalTo(UNASSIGNED));
        }

        logger.info("start all the primary shards for test2, replicas will start initializing");
        clusterState = startInitializingShardsAndReroute(strategy, clusterState, "test2");

        for (int i = 0; i < clusterState.routingTable().index("test1").size(); i++) {
            assertThat(clusterState.routingTable().index("test1").shard(i).size(), equalTo(2));
            assertThat(clusterState.routingTable().index("test1").shard(i).primaryShard().state(), equalTo(STARTED));
            assertThat(clusterState.routingTable().index("test1").shard(i).replicaShards().get(0).state(), equalTo(STARTED));
        }

        for (int i = 0; i < clusterState.routingTable().index("test2").size(); i++) {
            assertThat(clusterState.routingTable().index("test2").shard(i).size(), equalTo(2));
            assertThat(clusterState.routingTable().index("test2").shard(i).primaryShard().state(), equalTo(STARTED));
            assertThat(clusterState.routingTable().index("test2").shard(i).replicaShards().get(0).state(), equalTo(INITIALIZING));
        }

        logger.info("start the test2 replica shards");
        clusterState = startInitializingShardsAndReroute(strategy, clusterState, "test2");

        for (int i = 0; i < clusterState.routingTable().index("test1").size(); i++) {
            assertThat(clusterState.routingTable().index("test1").shard(i).size(), equalTo(2));
            assertThat(clusterState.routingTable().index("test1").shard(i).primaryShard().state(), equalTo(STARTED));
            assertThat(clusterState.routingTable().index("test1").shard(i).replicaShards().get(0).state(), equalTo(STARTED));
        }

        for (int i = 0; i < clusterState.routingTable().index("test2").size(); i++) {
            assertThat(clusterState.routingTable().index("test2").shard(i).size(), equalTo(2));
            assertThat(clusterState.routingTable().index("test2").shard(i).primaryShard().state(), equalTo(STARTED));
            assertThat(clusterState.routingTable().index("test2").shard(i).replicaShards().get(0).state(), equalTo(STARTED));
        }

        logger.info("now, start 1 more node, check that rebalancing happen (for test1) because we set it to all_active");
        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder(clusterState.nodes()).add(newNode("node3"))).build();
        clusterState = strategy.reroute(clusterState, "reroute", ActionListener.noop());
        RoutingNodes routingNodes = clusterState.getRoutingNodes();

        assertThat(routingNodes.node("node3").size(), equalTo(1));
        assertThat(routingNodes.node("node3").iterator().next().shardId().getIndex().getName(), anyOf(equalTo("test1"), equalTo("test2")));
    }

    public void testClusterAllActive2() {
        AllocationService strategy = createAllocationService(
            Settings.builder()
                .put(
                    ClusterRebalanceAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ALLOW_REBALANCE_SETTING.getKey(),
                    ClusterRebalanceAllocationDecider.ClusterRebalanceType.INDICES_ALL_ACTIVE.toString()
                )
                .build()
        );

        Metadata metadata = Metadata.builder()
            .put(IndexMetadata.builder("test1").settings(settings(IndexVersion.current())).numberOfShards(1).numberOfReplicas(1))
            .put(IndexMetadata.builder("test2").settings(settings(IndexVersion.current())).numberOfShards(1).numberOfReplicas(1))
            .build();

        RoutingTable initialRoutingTable = RoutingTable.builder(TestShardRoutingRoleStrategies.DEFAULT_ROLE_ONLY)
            .addAsNew(metadata.index("test1"))
            .addAsNew(metadata.index("test2"))
            .build();

        ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT).metadata(metadata).routingTable(initialRoutingTable).build();

        logger.info("start two nodes");
        clusterState = ClusterState.builder(clusterState)
            .nodes(DiscoveryNodes.builder().add(newNode("node1")).add(newNode("node2")))
            .build();
        clusterState = strategy.reroute(clusterState, "reroute", ActionListener.noop());

        for (int i = 0; i < clusterState.routingTable().index("test1").size(); i++) {
            assertThat(clusterState.routingTable().index("test1").shard(i).size(), equalTo(2));
            assertThat(clusterState.routingTable().index("test1").shard(i).primaryShard().state(), equalTo(INITIALIZING));
            assertThat(clusterState.routingTable().index("test1").shard(i).replicaShards().get(0).state(), equalTo(UNASSIGNED));
        }

        for (int i = 0; i < clusterState.routingTable().index("test2").size(); i++) {
            assertThat(clusterState.routingTable().index("test2").shard(i).size(), equalTo(2));
            assertThat(clusterState.routingTable().index("test2").shard(i).primaryShard().state(), equalTo(INITIALIZING));
            assertThat(clusterState.routingTable().index("test2").shard(i).replicaShards().get(0).state(), equalTo(UNASSIGNED));
        }

        logger.info("start all the primary shards for test1, replicas will start initializing");
        clusterState = startInitializingShardsAndReroute(strategy, clusterState, "test1");

        for (int i = 0; i < clusterState.routingTable().index("test1").size(); i++) {
            assertThat(clusterState.routingTable().index("test1").shard(i).size(), equalTo(2));
            assertThat(clusterState.routingTable().index("test1").shard(i).primaryShard().state(), equalTo(STARTED));
            assertThat(clusterState.routingTable().index("test1").shard(i).replicaShards().get(0).state(), equalTo(INITIALIZING));
        }

        for (int i = 0; i < clusterState.routingTable().index("test2").size(); i++) {
            assertThat(clusterState.routingTable().index("test2").shard(i).size(), equalTo(2));
            assertThat(clusterState.routingTable().index("test2").shard(i).primaryShard().state(), equalTo(INITIALIZING));
            assertThat(clusterState.routingTable().index("test2").shard(i).replicaShards().get(0).state(), equalTo(UNASSIGNED));
        }

        logger.info("start the test1 replica shards");
        clusterState = startInitializingShardsAndReroute(strategy, clusterState, "test1");

        for (int i = 0; i < clusterState.routingTable().index("test1").size(); i++) {
            assertThat(clusterState.routingTable().index("test1").shard(i).size(), equalTo(2));
            assertThat(clusterState.routingTable().index("test1").shard(i).primaryShard().state(), equalTo(STARTED));
            assertThat(clusterState.routingTable().index("test1").shard(i).replicaShards().get(0).state(), equalTo(STARTED));
        }

        for (int i = 0; i < clusterState.routingTable().index("test2").size(); i++) {
            assertThat(clusterState.routingTable().index("test2").shard(i).size(), equalTo(2));
            assertThat(clusterState.routingTable().index("test2").shard(i).primaryShard().state(), equalTo(INITIALIZING));
            assertThat(clusterState.routingTable().index("test2").shard(i).replicaShards().get(0).state(), equalTo(UNASSIGNED));
        }

        logger.info("now, start 1 more node, check that rebalancing will not happen (for test1) because we set it to all_active");
        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder(clusterState.nodes()).add(newNode("node3"))).build();
        clusterState = strategy.reroute(clusterState, "reroute", ActionListener.noop());
        RoutingNodes routingNodes = clusterState.getRoutingNodes();

        assertThat(routingNodes.node("node3").isEmpty(), equalTo(true));
    }

    public void testClusterAllActive3() {
        AllocationService strategy = createAllocationService(
            Settings.builder()
                .put(
                    ClusterRebalanceAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ALLOW_REBALANCE_SETTING.getKey(),
                    ClusterRebalanceAllocationDecider.ClusterRebalanceType.INDICES_ALL_ACTIVE.toString()
                )
                .build()
        );

        Metadata metadata = Metadata.builder()
            .put(IndexMetadata.builder("test1").settings(settings(IndexVersion.current())).numberOfShards(1).numberOfReplicas(1))
            .put(IndexMetadata.builder("test2").settings(settings(IndexVersion.current())).numberOfShards(1).numberOfReplicas(1))
            .build();

        RoutingTable initialRoutingTable = RoutingTable.builder(TestShardRoutingRoleStrategies.DEFAULT_ROLE_ONLY)
            .addAsNew(metadata.index("test1"))
            .addAsNew(metadata.index("test2"))
            .build();

        ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT).metadata(metadata).routingTable(initialRoutingTable).build();

        logger.info("start two nodes");
        clusterState = ClusterState.builder(clusterState)
            .nodes(DiscoveryNodes.builder().add(newNode("node1")).add(newNode("node2")))
            .build();
        clusterState = strategy.reroute(clusterState, "reroute", ActionListener.noop());

        for (int i = 0; i < clusterState.routingTable().index("test1").size(); i++) {
            assertThat(clusterState.routingTable().index("test1").shard(i).size(), equalTo(2));
            assertThat(clusterState.routingTable().index("test1").shard(i).primaryShard().state(), equalTo(INITIALIZING));
            assertThat(clusterState.routingTable().index("test1").shard(i).replicaShards().get(0).state(), equalTo(UNASSIGNED));
        }

        for (int i = 0; i < clusterState.routingTable().index("test2").size(); i++) {
            assertThat(clusterState.routingTable().index("test2").shard(i).size(), equalTo(2));
            assertThat(clusterState.routingTable().index("test2").shard(i).primaryShard().state(), equalTo(INITIALIZING));
            assertThat(clusterState.routingTable().index("test2").shard(i).replicaShards().get(0).state(), equalTo(UNASSIGNED));
        }

        logger.info("start all the primary shards for test1, replicas will start initializing");
        clusterState = startInitializingShardsAndReroute(strategy, clusterState, "test1");

        for (int i = 0; i < clusterState.routingTable().index("test1").size(); i++) {
            assertThat(clusterState.routingTable().index("test1").shard(i).size(), equalTo(2));
            assertThat(clusterState.routingTable().index("test1").shard(i).primaryShard().state(), equalTo(STARTED));
            assertThat(clusterState.routingTable().index("test1").shard(i).replicaShards().get(0).state(), equalTo(INITIALIZING));
        }

        for (int i = 0; i < clusterState.routingTable().index("test2").size(); i++) {
            assertThat(clusterState.routingTable().index("test2").shard(i).size(), equalTo(2));
            assertThat(clusterState.routingTable().index("test2").shard(i).primaryShard().state(), equalTo(INITIALIZING));
            assertThat(clusterState.routingTable().index("test2").shard(i).replicaShards().get(0).state(), equalTo(UNASSIGNED));
        }

        logger.info("start the test1 replica shards");
        clusterState = startInitializingShardsAndReroute(strategy, clusterState, "test1");

        for (int i = 0; i < clusterState.routingTable().index("test1").size(); i++) {
            assertThat(clusterState.routingTable().index("test1").shard(i).size(), equalTo(2));
            assertThat(clusterState.routingTable().index("test1").shard(i).primaryShard().state(), equalTo(STARTED));
            assertThat(clusterState.routingTable().index("test1").shard(i).replicaShards().get(0).state(), equalTo(STARTED));
        }

        for (int i = 0; i < clusterState.routingTable().index("test2").size(); i++) {
            assertThat(clusterState.routingTable().index("test2").shard(i).size(), equalTo(2));
            assertThat(clusterState.routingTable().index("test2").shard(i).primaryShard().state(), equalTo(INITIALIZING));
            assertThat(clusterState.routingTable().index("test2").shard(i).replicaShards().get(0).state(), equalTo(UNASSIGNED));
        }

        logger.info("start all the primary shards for test2, replicas will start initializing");
        clusterState = startInitializingShardsAndReroute(strategy, clusterState, "test2");

        for (int i = 0; i < clusterState.routingTable().index("test1").size(); i++) {
            assertThat(clusterState.routingTable().index("test1").shard(i).size(), equalTo(2));
            assertThat(clusterState.routingTable().index("test1").shard(i).primaryShard().state(), equalTo(STARTED));
            assertThat(clusterState.routingTable().index("test1").shard(i).replicaShards().get(0).state(), equalTo(STARTED));
        }

        for (int i = 0; i < clusterState.routingTable().index("test2").size(); i++) {
            assertThat(clusterState.routingTable().index("test2").shard(i).size(), equalTo(2));
            assertThat(clusterState.routingTable().index("test2").shard(i).primaryShard().state(), equalTo(STARTED));
            assertThat(clusterState.routingTable().index("test2").shard(i).replicaShards().get(0).state(), equalTo(INITIALIZING));
        }

        logger.info("now, start 1 more node, check that rebalancing will not happen (for test1) because we set it to all_active");
        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder(clusterState.nodes()).add(newNode("node3"))).build();
        clusterState = strategy.reroute(clusterState, "reroute", ActionListener.noop());
        RoutingNodes routingNodes = clusterState.getRoutingNodes();

        assertThat(routingNodes.node("node3").isEmpty(), equalTo(true));
    }

    public void testRebalanceWithIgnoredUnassignedShards() {
        final AtomicBoolean allocateTest1 = new AtomicBoolean(false);

        AllocationService strategy = createAllocationService(Settings.EMPTY, new TestGatewayAllocator() {
            @Override
            public void allocateUnassigned(
                ShardRouting shardRouting,
                RoutingAllocation allocation,
                UnassignedAllocationHandler unassignedAllocationHandler
            ) {
                if (allocateTest1.get() == false && "test1".equals(shardRouting.index().getName())) {
                    unassignedAllocationHandler.removeAndIgnore(UnassignedInfo.AllocationStatus.NO_ATTEMPT, allocation.changes());
                } else {
                    super.allocateUnassigned(shardRouting, allocation, unassignedAllocationHandler);
                }
            }
        });

        Metadata metadata = Metadata.builder()
            .put(IndexMetadata.builder("test").settings(settings(IndexVersion.current())).numberOfShards(2).numberOfReplicas(0))
            .put(IndexMetadata.builder("test1").settings(settings(IndexVersion.current())).numberOfShards(2).numberOfReplicas(0))
            .build();

        RoutingTable initialRoutingTable = RoutingTable.builder(TestShardRoutingRoleStrategies.DEFAULT_ROLE_ONLY)
            .addAsNew(metadata.index("test"))
            .addAsNew(metadata.index("test1"))
            .build();

        ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT).metadata(metadata).routingTable(initialRoutingTable).build();

        logger.info("start two nodes");
        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder().add(newNode("node1"))).build();
        clusterState = strategy.reroute(clusterState, "reroute", ActionListener.noop());

        for (int i = 0; i < clusterState.routingTable().index("test").size(); i++) {
            assertThat(clusterState.routingTable().index("test").shard(i).size(), equalTo(1));
            assertThat(clusterState.routingTable().index("test").shard(i).primaryShard().state(), equalTo(INITIALIZING));
        }

        logger.debug("start all the primary shards for test");
        clusterState = startInitializingShardsAndReroute(strategy, clusterState, "test");
        for (int i = 0; i < clusterState.routingTable().index("test").size(); i++) {
            assertThat(clusterState.routingTable().index("test").shard(i).size(), equalTo(1));
            assertThat(clusterState.routingTable().index("test").shard(i).primaryShard().state(), equalTo(STARTED));
        }

        logger.debug("now, start 1 more node, check that rebalancing will not happen since we unassigned shards");
        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder(clusterState.nodes()).add(newNode("node2"))).build();
        logger.debug("reroute and check that nothing has changed");
        ClusterState resultingState = strategy.reroute(clusterState, "reroute", ActionListener.noop());
        assertThat(resultingState, equalTo(clusterState));

        for (int i = 0; i < clusterState.routingTable().index("test").size(); i++) {
            assertThat(clusterState.routingTable().index("test").shard(i).size(), equalTo(1));
            assertThat(clusterState.routingTable().index("test").shard(i).primaryShard().state(), equalTo(STARTED));
        }
        for (int i = 0; i < clusterState.routingTable().index("test1").size(); i++) {
            assertThat(clusterState.routingTable().index("test1").shard(i).size(), equalTo(1));
            assertThat(clusterState.routingTable().index("test1").shard(i).primaryShard().state(), equalTo(UNASSIGNED));
        }
        logger.debug("now set allocateTest1 to true and reroute we should see the [test1] index initializing");
        allocateTest1.set(true);
        resultingState = strategy.reroute(clusterState, "reroute", ActionListener.noop());
        assertThat(resultingState, not(equalTo(clusterState)));
        clusterState = resultingState;
        for (int i = 0; i < clusterState.routingTable().index("test1").size(); i++) {
            assertThat(clusterState.routingTable().index("test1").shard(i).size(), equalTo(1));
            assertThat(clusterState.routingTable().index("test1").shard(i).primaryShard().state(), equalTo(INITIALIZING));
        }

        logger.debug(
            "now start initializing shards and expect exactly one rebalance" + " from node1 to node 2 since index [test] is all on node1"
        );

        clusterState = startInitializingShardsAndReroute(strategy, clusterState, "test1");

        for (int i = 0; i < clusterState.routingTable().index("test1").size(); i++) {
            assertThat(clusterState.routingTable().index("test1").shard(i).size(), equalTo(1));
            assertThat(clusterState.routingTable().index("test1").shard(i).primaryShard().state(), equalTo(STARTED));
        }
        int numStarted = 0;
        int numRelocating = 0;
        for (int i = 0; i < clusterState.routingTable().index("test").size(); i++) {
            assertThat(clusterState.routingTable().index("test").shard(i).size(), equalTo(1));
            if (clusterState.routingTable().index("test").shard(i).primaryShard().state() == STARTED) {
                numStarted++;
            } else if (clusterState.routingTable().index("test").shard(i).primaryShard().state() == RELOCATING) {
                numRelocating++;
            }
        }
        assertEquals(numStarted, 1);
        assertEquals(numRelocating, 1);

    }

    public void testRebalanceWhileShardFetching() {
        final AtomicBoolean hasFetches = new AtomicBoolean(true);
        AllocationService strategy = createAllocationService(
            Settings.builder()
                .put(
                    ClusterRebalanceAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ALLOW_REBALANCE_SETTING.getKey(),
                    ClusterRebalanceAllocationDecider.ClusterRebalanceType.ALWAYS.toString()
                )
                .put("cluster.routing.allocation.type", "balanced") // TODO fix for desired_balance
                .build(),
            new TestGatewayAllocator() {
                @Override
                public void beforeAllocation(RoutingAllocation allocation) {
                    if (hasFetches.get()) {
                        allocation.setHasPendingAsyncFetch();
                    }
                }
            }
        );
        assertCriticalWarnings(
            "[cluster.routing.allocation.type] setting was deprecated in Elasticsearch and will be removed in a future release."
        );

        Metadata metadata = Metadata.builder()
            .put(IndexMetadata.builder("test").settings(settings(IndexVersion.current())).numberOfShards(2).numberOfReplicas(0))
            .put(
                IndexMetadata.builder("test1")
                    .settings(
                        settings(IndexVersion.current()).put(
                            IndexMetadata.INDEX_ROUTING_EXCLUDE_GROUP_SETTING.getKey() + "_id",
                            "node1,node2"
                        )
                    )
                    .numberOfShards(2)
                    .numberOfReplicas(0)
            )
            .build();

        // we use a second index here (test1) that never gets assigned otherwise allocateUnassigned
        // is never called if we don't have unassigned shards.
        RoutingTable initialRoutingTable = RoutingTable.builder(TestShardRoutingRoleStrategies.DEFAULT_ROLE_ONLY)
            .addAsNew(metadata.index("test"))
            .addAsNew(metadata.index("test1"))
            .build();

        ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT).metadata(metadata).routingTable(initialRoutingTable).build();

        logger.info("start two nodes");
        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder().add(newNode("node1"))).build();
        clusterState = strategy.reroute(clusterState, "reroute", ActionListener.noop());

        for (int i = 0; i < clusterState.routingTable().index("test").size(); i++) {
            assertThat(clusterState.routingTable().index("test").shard(i).size(), equalTo(1));
            assertThat(clusterState.routingTable().index("test").shard(i).primaryShard().state(), equalTo(INITIALIZING));
        }

        logger.debug("start all the primary shards for test");
        clusterState = startInitializingShardsAndReroute(strategy, clusterState, "test");
        for (int i = 0; i < clusterState.routingTable().index("test").size(); i++) {
            assertThat(clusterState.routingTable().index("test").shard(i).size(), equalTo(1));
            assertThat(clusterState.routingTable().index("test").shard(i).primaryShard().state(), equalTo(STARTED));
        }

        logger.debug("now, start 1 more node, check that rebalancing will not happen since we have shard sync going on");
        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder(clusterState.nodes()).add(newNode("node2"))).build();
        logger.debug("reroute and check that nothing has changed");
        ClusterState resultState = strategy.reroute(clusterState, "reroute", ActionListener.noop());
        assertThat(resultState, equalTo(clusterState));

        for (int i = 0; i < clusterState.routingTable().index("test").size(); i++) {
            assertThat(clusterState.routingTable().index("test").shard(i).size(), equalTo(1));
            assertThat(clusterState.routingTable().index("test").shard(i).primaryShard().state(), equalTo(STARTED));
        }
        for (int i = 0; i < clusterState.routingTable().index("test1").size(); i++) {
            assertThat(clusterState.routingTable().index("test1").shard(i).size(), equalTo(1));
            assertThat(clusterState.routingTable().index("test1").shard(i).primaryShard().state(), equalTo(UNASSIGNED));
        }
        logger.debug("now set hasFetches to true and reroute we should now see exactly one relocating shard");
        hasFetches.set(false);
        resultState = strategy.reroute(clusterState, "reroute", ActionListener.noop());
        assertThat(resultState, not(equalTo(clusterState)));
        clusterState = resultState;
        int numStarted = 0;
        int numRelocating = 0;
        for (int i = 0; i < clusterState.routingTable().index("test").size(); i++) {

            assertThat(clusterState.routingTable().index("test").shard(i).size(), equalTo(1));
            if (clusterState.routingTable().index("test").shard(i).primaryShard().state() == STARTED) {
                numStarted++;
            } else if (clusterState.routingTable().index("test").shard(i).primaryShard().state() == RELOCATING) {
                numRelocating++;
            }
        }
        for (int i = 0; i < clusterState.routingTable().index("test1").size(); i++) {
            assertThat(clusterState.routingTable().index("test1").shard(i).size(), equalTo(1));
            assertThat(clusterState.routingTable().index("test1").shard(i).primaryShard().state(), equalTo(UNASSIGNED));
        }
        assertEquals(numStarted, 1);
        assertEquals(numRelocating, 1);
    }
}
