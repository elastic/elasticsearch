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
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ESAllocationTestCase;
import org.elasticsearch.cluster.TestShardRoutingRoleStrategies;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.allocation.decider.ShardsLimitAllocationDecider;
import org.elasticsearch.common.settings.Settings;

import static org.elasticsearch.cluster.routing.RoutingNodesHelper.numberOfShardsWithState;
import static org.elasticsearch.cluster.routing.ShardRoutingState.RELOCATING;
import static org.elasticsearch.cluster.routing.ShardRoutingState.STARTED;
import static org.hamcrest.Matchers.equalTo;

public class ShardsLimitAllocationTests extends ESAllocationTestCase {
    private final Logger logger = LogManager.getLogger(ShardsLimitAllocationTests.class);

    public void testIndexLevelShardsLimitAllocate() {
        AllocationService strategy = createAllocationService(
            Settings.builder().put("cluster.routing.allocation.node_concurrent_recoveries", 10).build()
        );

        logger.info("Building initial routing table");

        Metadata metadata = Metadata.builder()
            .put(
                IndexMetadata.builder("test")
                    .settings(
                        indexSettings(Version.CURRENT, 4, 1).put(
                            ShardsLimitAllocationDecider.INDEX_TOTAL_SHARDS_PER_NODE_SETTING.getKey(),
                            2
                        )
                    )
            )
            .build();

        RoutingTable routingTable = RoutingTable.builder(TestShardRoutingRoleStrategies.DEFAULT_ROLE_ONLY)
            .addAsNew(metadata.index("test"))
            .build();

        ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT).metadata(metadata).routingTable(routingTable).build();
        logger.info("Adding two nodes and performing rerouting");
        clusterState = ClusterState.builder(clusterState)
            .nodes(DiscoveryNodes.builder().add(newNode("node1")).add(newNode("node2")))
            .build();
        clusterState = strategy.reroute(clusterState, "reroute", ActionListener.noop());

        assertThat(clusterState.getRoutingNodes().node("node1").numberOfShardsWithState(ShardRoutingState.INITIALIZING), equalTo(2));
        assertThat(clusterState.getRoutingNodes().node("node2").numberOfShardsWithState(ShardRoutingState.INITIALIZING), equalTo(2));

        logger.info("Start the primary shards");
        clusterState = startInitializingShardsAndReroute(strategy, clusterState);

        assertThat(clusterState.getRoutingNodes().node("node1").numberOfShardsWithState(ShardRoutingState.STARTED), equalTo(2));
        assertThat(clusterState.getRoutingNodes().node("node1").numberOfShardsWithState(ShardRoutingState.INITIALIZING), equalTo(0));
        assertThat(clusterState.getRoutingNodes().node("node2").numberOfShardsWithState(ShardRoutingState.STARTED), equalTo(2));
        assertThat(clusterState.getRoutingNodes().node("node2").numberOfShardsWithState(ShardRoutingState.INITIALIZING), equalTo(0));
        assertThat(clusterState.getRoutingNodes().unassigned().size(), equalTo(4));

        logger.info("Do another reroute, make sure its still not allocated");
        startInitializingShardsAndReroute(strategy, clusterState);
    }

    public void testClusterLevelShardsLimitAllocate() {
        AllocationService strategy = createAllocationService(
            Settings.builder()
                .put("cluster.routing.allocation.node_concurrent_recoveries", 10)
                .put(ShardsLimitAllocationDecider.CLUSTER_TOTAL_SHARDS_PER_NODE_SETTING.getKey(), 1)
                .build()
        );

        logger.info("Building initial routing table");

        Metadata metadata = Metadata.builder().put(IndexMetadata.builder("test").settings(indexSettings(Version.CURRENT, 4, 0))).build();

        RoutingTable routingTable = RoutingTable.builder(TestShardRoutingRoleStrategies.DEFAULT_ROLE_ONLY)
            .addAsNew(metadata.index("test"))
            .build();

        ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT).metadata(metadata).routingTable(routingTable).build();
        logger.info("Adding two nodes and performing rerouting");
        clusterState = ClusterState.builder(clusterState)
            .nodes(DiscoveryNodes.builder().add(newNode("node1")).add(newNode("node2")))
            .build();
        clusterState = strategy.reroute(clusterState, "reroute", ActionListener.noop());

        assertThat(clusterState.getRoutingNodes().node("node1").numberOfShardsWithState(ShardRoutingState.INITIALIZING), equalTo(1));
        assertThat(clusterState.getRoutingNodes().node("node2").numberOfShardsWithState(ShardRoutingState.INITIALIZING), equalTo(1));

        logger.info("Start the primary shards");
        clusterState = startInitializingShardsAndReroute(strategy, clusterState);

        assertThat(clusterState.getRoutingNodes().node("node1").numberOfShardsWithState(ShardRoutingState.STARTED), equalTo(1));
        assertThat(clusterState.getRoutingNodes().node("node2").numberOfShardsWithState(ShardRoutingState.STARTED), equalTo(1));
        assertThat(clusterState.getRoutingNodes().unassigned().size(), equalTo(2));

        // Bump the cluster total shards to 2
        strategy = createAllocationService(
            Settings.builder()
                .put("cluster.routing.allocation.node_concurrent_recoveries", 10)
                .put(ShardsLimitAllocationDecider.CLUSTER_TOTAL_SHARDS_PER_NODE_SETTING.getKey(), 2)
                .build()
        );

        logger.info("Do another reroute, make sure shards are now allocated");
        clusterState = strategy.reroute(clusterState, "reroute", ActionListener.noop());

        assertThat(clusterState.getRoutingNodes().node("node1").numberOfShardsWithState(ShardRoutingState.INITIALIZING), equalTo(1));
        assertThat(clusterState.getRoutingNodes().node("node2").numberOfShardsWithState(ShardRoutingState.INITIALIZING), equalTo(1));

        clusterState = startInitializingShardsAndReroute(strategy, clusterState);

        assertThat(clusterState.getRoutingNodes().node("node1").numberOfShardsWithState(ShardRoutingState.STARTED), equalTo(2));
        assertThat(clusterState.getRoutingNodes().node("node2").numberOfShardsWithState(ShardRoutingState.STARTED), equalTo(2));
        assertThat(clusterState.getRoutingNodes().unassigned().size(), equalTo(0));
    }

    public void testIndexLevelShardsLimitRemain() {
        AllocationService strategy = createAllocationService(
            Settings.builder()
                .put("cluster.routing.allocation.node_concurrent_recoveries", 10)
                .put("cluster.routing.allocation.node_initial_primaries_recoveries", 10)
                .put("cluster.routing.allocation.cluster_concurrent_rebalance", -1)
                .put("cluster.routing.allocation.balance.index", 0.0f)
                .put("cluster.routing.allocation.balance.replica", 1.0f)
                .put("cluster.routing.allocation.balance.primary", 0.0f)
                .build()
        );

        logger.info("Building initial routing table");

        Metadata metadata = Metadata.builder().put(IndexMetadata.builder("test").settings(indexSettings(Version.CURRENT, 5, 0))).build();

        RoutingTable initialRoutingTable = RoutingTable.builder(TestShardRoutingRoleStrategies.DEFAULT_ROLE_ONLY)
            .addAsNew(metadata.index("test"))
            .build();

        ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT).metadata(metadata).routingTable(initialRoutingTable).build();
        logger.info("Adding one node and reroute");
        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder().add(newNode("node1"))).build();
        clusterState = strategy.reroute(clusterState, "reroute", ActionListener.noop());

        logger.info("Start the primary shards");
        clusterState = startInitializingShardsAndReroute(strategy, clusterState);

        assertThat(numberOfShardsWithState(clusterState.getRoutingNodes(), STARTED), equalTo(5));

        logger.info("add another index with 5 shards");
        metadata = Metadata.builder(clusterState.metadata())
            .put(IndexMetadata.builder("test1").settings(indexSettings(Version.CURRENT, 5, 0)))
            .build();
        RoutingTable updatedRoutingTable = RoutingTable.builder(
            TestShardRoutingRoleStrategies.DEFAULT_ROLE_ONLY,
            clusterState.routingTable()
        ).addAsNew(metadata.index("test1")).build();

        clusterState = ClusterState.builder(clusterState).metadata(metadata).routingTable(updatedRoutingTable).build();

        logger.info("Add another one node and reroute");
        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder(clusterState.nodes()).add(newNode("node2"))).build();
        clusterState = strategy.reroute(clusterState, "reroute", ActionListener.noop());

        clusterState = startInitializingShardsAndReroute(strategy, clusterState);

        assertThat(numberOfShardsWithState(clusterState.getRoutingNodes(), STARTED), equalTo(10));

        for (ShardRouting shardRouting : clusterState.getRoutingNodes().node("node1")) {
            assertThat(shardRouting.getIndexName(), equalTo("test"));
        }
        for (ShardRouting shardRouting : clusterState.getRoutingNodes().node("node2")) {
            assertThat(shardRouting.getIndexName(), equalTo("test1"));
        }

        logger.info("update {} for test, see that things move", ShardsLimitAllocationDecider.INDEX_TOTAL_SHARDS_PER_NODE_SETTING.getKey());
        metadata = Metadata.builder(clusterState.metadata())
            .put(
                IndexMetadata.builder(clusterState.metadata().index("test"))
                    .settings(
                        indexSettings(Version.CURRENT, 5, 0).put(
                            ShardsLimitAllocationDecider.INDEX_TOTAL_SHARDS_PER_NODE_SETTING.getKey(),
                            3
                        )
                    )
            )
            .build();

        clusterState = ClusterState.builder(clusterState).metadata(metadata).build();

        logger.info("reroute after setting");
        clusterState = strategy.reroute(clusterState, "reroute", ActionListener.noop());

        assertThat(clusterState.getRoutingNodes().node("node1").numberOfShardsWithState(STARTED), equalTo(3));
        assertThat(clusterState.getRoutingNodes().node("node1").numberOfShardsWithState(RELOCATING), equalTo(2));
        assertThat(clusterState.getRoutingNodes().node("node2").numberOfShardsWithState(RELOCATING), equalTo(2));
        assertThat(clusterState.getRoutingNodes().node("node2").numberOfShardsWithState(STARTED), equalTo(3));
        // the first move will destroy the balance and the balancer will move 2 shards from node2 to node one right after
        // moving the nodes to node2 since we consider INITIALIZING nodes during rebalance
        clusterState = startInitializingShardsAndReroute(strategy, clusterState);
        // now we are done compared to EvenShardCountAllocator since the Balancer is not soely based on the average
        assertThat(clusterState.getRoutingNodes().node("node1").numberOfShardsWithState(STARTED), equalTo(5));
        assertThat(clusterState.getRoutingNodes().node("node2").numberOfShardsWithState(STARTED), equalTo(5));
    }
}
