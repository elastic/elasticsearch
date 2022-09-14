/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.routing;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ESAllocationTestCase;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.cluster.routing.allocation.allocator.BalancedShardsAllocator;
import org.elasticsearch.cluster.routing.allocation.decider.ClusterRebalanceAllocationDecider;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;

import static org.elasticsearch.cluster.routing.ShardRoutingState.STARTED;
import static org.hamcrest.Matchers.equalTo;

public class IndexCreateWhenExpandNodeTests extends ESAllocationTestCase {
    private final Logger logger = LogManager.getLogger(IndexCreateWhenExpandNodeTests.class);

    public void testCreateIndexIgnoreNodeShardCounts() {
        AllocationService strategy = createAllocationService(
            Settings.builder()
                .put(
                    ClusterRebalanceAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ALLOW_REBALANCE_SETTING.getKey(),
                    ClusterRebalanceAllocationDecider.ClusterRebalanceType.ALWAYS.toString()
                )
                .put(BalancedShardsAllocator.NEW_INDEX_IGNORE_NODE_SHARDS.getKey(), true)
                .build()
        );
        Metadata metadata = Metadata.builder()
            .put(IndexMetadata.builder("test1").settings(settings(Version.CURRENT)).numberOfShards(8).numberOfReplicas(0))
            .put(IndexMetadata.builder("test2").settings(settings(Version.CURRENT)).numberOfShards(8).numberOfReplicas(0))
            .build();
        RoutingTable initialRoutingTable = RoutingTable.builder()
            .addAsNew(metadata.index("test1"))
            .addAsNew(metadata.index("test2"))
            .build();
        ClusterState clusterState = ClusterState.builder(
            org.elasticsearch.cluster.ClusterName.CLUSTER_NAME_SETTING.getDefault(Settings.EMPTY)
        ).metadata(metadata).routingTable(initialRoutingTable).build();
        logger.info("start 4 nodes");
        clusterState = ClusterState.builder(clusterState)
            .nodes(DiscoveryNodes.builder().add(newNode("node1")).add(newNode("node2")).add(newNode("node3")).add(newNode("node4")))
            .build();
        clusterState = strategy.reroute(clusterState, "reroute");
        logger.info("start all the primary shards, replicas will start initializing");
        clusterState = startInitializingShardsAndReroute(strategy, clusterState);
        RoutingNodes routingNodes = clusterState.getRoutingNodes();
        assertThat(routingNodes.node("node1").numberOfShardsWithState(STARTED), equalTo(4));
        assertThat(routingNodes.node("node2").numberOfShardsWithState(STARTED), equalTo(4));
        assertThat(routingNodes.node("node3").numberOfShardsWithState(STARTED), equalTo(4));
        assertThat(routingNodes.node("node4").numberOfShardsWithState(STARTED), equalTo(4));
        metadata = Metadata.builder(clusterState.metadata())
            .put(IndexMetadata.builder("test3").settings(settings(Version.CURRENT)).numberOfShards(8).numberOfReplicas(0))
            .build();
        clusterState = ClusterState.builder(clusterState)
            .nodes(
                DiscoveryNodes.builder(clusterState.nodes())
                    .add(newNode("node5"))
                    .add(newNode("node6"))
                    .add(newNode("node7"))
                    .add(newNode("node8"))
            )
            .metadata(metadata)
            .routingTable(RoutingTable.builder(clusterState.routingTable()).addAsNew(metadata.index("test3")).build())
            .build();
        clusterState = startInitializingShardsAndReroute(strategy, clusterState);
        routingNodes = clusterState.getRoutingNodes();
        Index index3 = clusterState.metadata().index("test3").getIndex();
        assertThat(routingNodes.node("node5").numberOfOwningShardsForIndex(index3), equalTo(1));
        assertThat(routingNodes.node("node6").numberOfOwningShardsForIndex(index3), equalTo(1));
        assertThat(routingNodes.node("node7").numberOfOwningShardsForIndex(index3), equalTo(1));
        assertThat(routingNodes.node("node8").numberOfOwningShardsForIndex(index3), equalTo(1));
        assertThat(routingNodes.node("node1").numberOfOwningShardsForIndex(index3), equalTo(1));
        assertThat(routingNodes.node("node2").numberOfOwningShardsForIndex(index3), equalTo(1));
        assertThat(routingNodes.node("node3").numberOfOwningShardsForIndex(index3), equalTo(1));
        assertThat(routingNodes.node("node4").numberOfOwningShardsForIndex(index3), equalTo(1));
        clusterState = startInitializingShardsAndReroute(strategy, clusterState);
        clusterState = startInitializingShardsAndReroute(strategy, clusterState);
        clusterState = startInitializingShardsAndReroute(strategy, clusterState);
        clusterState = startInitializingShardsAndReroute(strategy, clusterState);
        routingNodes = clusterState.getRoutingNodes();
        assertThat(routingNodes.node("node1").numberOfShardsWithState(STARTED), equalTo(3));
        assertThat(routingNodes.node("node2").numberOfShardsWithState(STARTED), equalTo(3));
        assertThat(routingNodes.node("node3").numberOfShardsWithState(STARTED), equalTo(3));
        assertThat(routingNodes.node("node4").numberOfShardsWithState(STARTED), equalTo(3));
        assertThat(routingNodes.node("node5").numberOfShardsWithState(STARTED), equalTo(3));
        assertThat(routingNodes.node("node6").numberOfShardsWithState(STARTED), equalTo(3));
        assertThat(routingNodes.node("node7").numberOfShardsWithState(STARTED), equalTo(3));
        assertThat(routingNodes.node("node8").numberOfShardsWithState(STARTED), equalTo(3));
        assertThat(routingNodes.node("node5").numberOfOwningShardsForIndex(index3), equalTo(1));
        assertThat(routingNodes.node("node6").numberOfOwningShardsForIndex(index3), equalTo(1));
        assertThat(routingNodes.node("node7").numberOfOwningShardsForIndex(index3), equalTo(1));
        assertThat(routingNodes.node("node8").numberOfOwningShardsForIndex(index3), equalTo(1));
        assertThat(routingNodes.node("node1").numberOfOwningShardsForIndex(index3), equalTo(1));
        assertThat(routingNodes.node("node2").numberOfOwningShardsForIndex(index3), equalTo(1));
        assertThat(routingNodes.node("node3").numberOfOwningShardsForIndex(index3), equalTo(1));
        assertThat(routingNodes.node("node4").numberOfOwningShardsForIndex(index3), equalTo(1));
    }

    public void testCreateIndexOrigin() {
        AllocationService strategy = createAllocationService(
            Settings.builder()
                .put(
                    ClusterRebalanceAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ALLOW_REBALANCE_SETTING.getKey(),
                    ClusterRebalanceAllocationDecider.ClusterRebalanceType.ALWAYS.toString()
                )
                .put(BalancedShardsAllocator.NEW_INDEX_IGNORE_NODE_SHARDS.getKey(), false)
                .build()
        );
        Metadata metadata = Metadata.builder()
            .put(IndexMetadata.builder("test1").settings(settings(Version.CURRENT)).numberOfShards(8).numberOfReplicas(0))
            .put(IndexMetadata.builder("test2").settings(settings(Version.CURRENT)).numberOfShards(8).numberOfReplicas(0))
            .build();
        RoutingTable initialRoutingTable = RoutingTable.builder()
            .addAsNew(metadata.index("test1"))
            .addAsNew(metadata.index("test2"))
            .build();
        ClusterState clusterState = ClusterState.builder(
            org.elasticsearch.cluster.ClusterName.CLUSTER_NAME_SETTING.getDefault(Settings.EMPTY)
        ).metadata(metadata).routingTable(initialRoutingTable).build();
        logger.info("start 4 nodes");
        clusterState = ClusterState.builder(clusterState)
            .nodes(DiscoveryNodes.builder().add(newNode("node1")).add(newNode("node2")).add(newNode("node3")).add(newNode("node4")))
            .build();
        clusterState = strategy.reroute(clusterState, "reroute");
        logger.info("start all the primary shards, replicas will start initializing");
        clusterState = startInitializingShardsAndReroute(strategy, clusterState);
        RoutingNodes routingNodes = clusterState.getRoutingNodes();
        assertThat(routingNodes.node("node1").numberOfShardsWithState(STARTED), equalTo(4));
        assertThat(routingNodes.node("node2").numberOfShardsWithState(STARTED), equalTo(4));
        assertThat(routingNodes.node("node3").numberOfShardsWithState(STARTED), equalTo(4));
        assertThat(routingNodes.node("node4").numberOfShardsWithState(STARTED), equalTo(4));
        metadata = Metadata.builder(clusterState.metadata())
            .put(IndexMetadata.builder("test3").settings(settings(Version.CURRENT)).numberOfShards(8).numberOfReplicas(0))
            .build();
        clusterState = ClusterState.builder(clusterState)
            .nodes(
                DiscoveryNodes.builder(clusterState.nodes())
                    .add(newNode("node5"))
                    .add(newNode("node6"))
                    .add(newNode("node7"))
                    .add(newNode("node8"))
            )
            .metadata(metadata)
            .routingTable(RoutingTable.builder(clusterState.routingTable()).addAsNew(metadata.index("test3")).build())
            .build();
        clusterState = startInitializingShardsAndReroute(strategy, clusterState);
        routingNodes = clusterState.getRoutingNodes();
        Index index3 = clusterState.metadata().index("test3").getIndex();
        assertThat(routingNodes.node("node5").numberOfOwningShardsForIndex(index3), equalTo(2));
        assertThat(routingNodes.node("node6").numberOfOwningShardsForIndex(index3), equalTo(2));
        assertThat(routingNodes.node("node7").numberOfOwningShardsForIndex(index3), equalTo(2));
        assertThat(routingNodes.node("node8").numberOfOwningShardsForIndex(index3), equalTo(2));
        assertThat(routingNodes.node("node1").numberOfOwningShardsForIndex(index3), equalTo(0));
        assertThat(routingNodes.node("node2").numberOfOwningShardsForIndex(index3), equalTo(0));
        assertThat(routingNodes.node("node3").numberOfOwningShardsForIndex(index3), equalTo(0));
        assertThat(routingNodes.node("node4").numberOfOwningShardsForIndex(index3), equalTo(0));
        clusterState = startInitializingShardsAndReroute(strategy, clusterState);
        clusterState = startInitializingShardsAndReroute(strategy, clusterState);
        clusterState = startInitializingShardsAndReroute(strategy, clusterState);
        clusterState = startInitializingShardsAndReroute(strategy, clusterState);
        clusterState = startInitializingShardsAndReroute(strategy, clusterState);
        clusterState = startInitializingShardsAndReroute(strategy, clusterState);
        clusterState = startInitializingShardsAndReroute(strategy, clusterState);
        routingNodes = clusterState.getRoutingNodes();
        assertThat(routingNodes.node("node1").numberOfShardsWithState(STARTED), equalTo(3));
        assertThat(routingNodes.node("node2").numberOfShardsWithState(STARTED), equalTo(3));
        assertThat(routingNodes.node("node3").numberOfShardsWithState(STARTED), equalTo(3));
        assertThat(routingNodes.node("node4").numberOfShardsWithState(STARTED), equalTo(3));
        assertThat(routingNodes.node("node5").numberOfShardsWithState(STARTED), equalTo(3));
        assertThat(routingNodes.node("node6").numberOfShardsWithState(STARTED), equalTo(3));
        assertThat(routingNodes.node("node7").numberOfShardsWithState(STARTED), equalTo(3));
        assertThat(routingNodes.node("node8").numberOfShardsWithState(STARTED), equalTo(3));
        assertThat(routingNodes.node("node5").numberOfOwningShardsForIndex(index3), equalTo(1));
        assertThat(routingNodes.node("node6").numberOfOwningShardsForIndex(index3), equalTo(1));
        assertThat(routingNodes.node("node7").numberOfOwningShardsForIndex(index3), equalTo(1));
        assertThat(routingNodes.node("node8").numberOfOwningShardsForIndex(index3), equalTo(1));
        assertThat(routingNodes.node("node1").numberOfOwningShardsForIndex(index3), equalTo(1));
        assertThat(routingNodes.node("node2").numberOfOwningShardsForIndex(index3), equalTo(1));
        assertThat(routingNodes.node("node3").numberOfOwningShardsForIndex(index3), equalTo(1));
        assertThat(routingNodes.node("node4").numberOfOwningShardsForIndex(index3), equalTo(1));
    }

}
