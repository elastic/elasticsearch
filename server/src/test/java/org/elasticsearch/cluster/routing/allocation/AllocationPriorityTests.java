/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.cluster.routing.allocation;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ESAllocationTestCase;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.allocation.decider.ThrottlingAllocationDecider;
import org.elasticsearch.common.settings.Settings;

import static org.elasticsearch.cluster.routing.RoutingNodesHelper.shardsWithState;
import static org.elasticsearch.cluster.routing.ShardRoutingState.INITIALIZING;

public class AllocationPriorityTests extends ESAllocationTestCase {

    /**
     * Tests that higher prioritized primaries and replicas are allocated first even on the balanced shard allocator
     * See https://github.com/elastic/elasticsearch/issues/13249 for details
     */
    public void testPrioritizedIndicesAllocatedFirst() {
        AllocationService allocation = createAllocationService(
            Settings.builder()
                .put(ThrottlingAllocationDecider.CLUSTER_ROUTING_ALLOCATION_NODE_CONCURRENT_RECOVERIES_SETTING.getKey(), 1)
                .put(ThrottlingAllocationDecider.CLUSTER_ROUTING_ALLOCATION_NODE_CONCURRENT_OUTGOING_RECOVERIES_SETTING.getKey(), 10)
                .put(ThrottlingAllocationDecider.CLUSTER_ROUTING_ALLOCATION_NODE_INITIAL_PRIMARIES_RECOVERIES_SETTING.getKey(), 1)
                .put(ThrottlingAllocationDecider.CLUSTER_ROUTING_ALLOCATION_NODE_CONCURRENT_INCOMING_RECOVERIES_SETTING.getKey(), 1)
                .build()
        );
        final String highPriorityName;
        final String lowPriorityName;
        final int priorityFirst;
        final int prioritySecond;
        if (randomBoolean()) {
            highPriorityName = "first";
            lowPriorityName = "second";
            prioritySecond = 1;
            priorityFirst = 100;
        } else {
            lowPriorityName = "first";
            highPriorityName = "second";
            prioritySecond = 100;
            priorityFirst = 1;
        }
        Metadata metadata = Metadata.builder()
            .put(
                IndexMetadata.builder("first")
                    .settings(settings(Version.CURRENT).put(IndexMetadata.SETTING_PRIORITY, priorityFirst))
                    .numberOfShards(2)
                    .numberOfReplicas(1)
            )
            .put(
                IndexMetadata.builder("second")
                    .settings(settings(Version.CURRENT).put(IndexMetadata.SETTING_PRIORITY, prioritySecond))
                    .numberOfShards(2)
                    .numberOfReplicas(1)
            )
            .build();
        RoutingTable initialRoutingTable = RoutingTable.builder()
            .addAsNew(metadata.index("first"))
            .addAsNew(metadata.index("second"))
            .build();
        ClusterState clusterState = ClusterState.builder(
            org.elasticsearch.cluster.ClusterName.CLUSTER_NAME_SETTING.getDefault(Settings.EMPTY)
        ).metadata(metadata).routingTable(initialRoutingTable).build();

        clusterState = ClusterState.builder(clusterState)
            .nodes(DiscoveryNodes.builder().add(newNode("node1")).add(newNode("node2")))
            .build();
        clusterState = allocation.reroute(clusterState, "reroute");

        clusterState = allocation.reroute(clusterState, "reroute");
        assertEquals(2, shardsWithState(clusterState.getRoutingNodes(), INITIALIZING).size());
        assertEquals(highPriorityName, shardsWithState(clusterState.getRoutingNodes(), INITIALIZING).get(0).getIndexName());
        assertEquals(highPriorityName, shardsWithState(clusterState.getRoutingNodes(), INITIALIZING).get(1).getIndexName());

        clusterState = startInitializingShardsAndReroute(allocation, clusterState);
        assertEquals(2, shardsWithState(clusterState.getRoutingNodes(), INITIALIZING).size());
        assertEquals(lowPriorityName, shardsWithState(clusterState.getRoutingNodes(), INITIALIZING).get(0).getIndexName());
        assertEquals(lowPriorityName, shardsWithState(clusterState.getRoutingNodes(), INITIALIZING).get(1).getIndexName());

        clusterState = startInitializingShardsAndReroute(allocation, clusterState);
        assertEquals(
            shardsWithState(clusterState.getRoutingNodes(), INITIALIZING).toString(),
            2,
            shardsWithState(clusterState.getRoutingNodes(), INITIALIZING).size()
        );
        assertEquals(highPriorityName, shardsWithState(clusterState.getRoutingNodes(), INITIALIZING).get(0).getIndexName());
        assertEquals(highPriorityName, shardsWithState(clusterState.getRoutingNodes(), INITIALIZING).get(1).getIndexName());

        clusterState = startInitializingShardsAndReroute(allocation, clusterState);
        assertEquals(2, shardsWithState(clusterState.getRoutingNodes(), INITIALIZING).size());
        assertEquals(lowPriorityName, shardsWithState(clusterState.getRoutingNodes(), INITIALIZING).get(0).getIndexName());
        assertEquals(lowPriorityName, shardsWithState(clusterState.getRoutingNodes(), INITIALIZING).get(1).getIndexName());

    }
}
