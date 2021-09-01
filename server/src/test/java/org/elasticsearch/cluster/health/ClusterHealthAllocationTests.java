/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.cluster.health;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ESAllocationTestCase;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.allocation.AllocationService;

import java.util.Collections;

public class ClusterHealthAllocationTests extends ESAllocationTestCase {

    public void testClusterHealth() {
        ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT).build();
        if (randomBoolean()) {
            clusterState = addNode(clusterState, "node_m", true);
        }
        assertEquals(ClusterHealthStatus.GREEN, getClusterHealthStatus(clusterState));

        Metadata metadata = Metadata.builder()
            .put(IndexMetadata.builder("test")
                .settings(settings(Version.CURRENT))
                .numberOfShards(2)
                .numberOfReplicas(1))
            .build();
        RoutingTable routingTable = RoutingTable.builder().addAsNew(metadata.index("test")).build();
        clusterState = ClusterState.builder(clusterState).metadata(metadata).routingTable(routingTable).build();
        MockAllocationService allocation = createAllocationService();
        clusterState = applyStartedShardsUntilNoChange(clusterState, allocation);
        assertEquals(0, clusterState.nodes().getDataNodes().size());
        assertEquals(ClusterHealthStatus.RED, getClusterHealthStatus(clusterState));

        clusterState = addNode(clusterState, "node_d1", false);
        assertEquals(1, clusterState.nodes().getDataNodes().size());
        clusterState = applyStartedShardsUntilNoChange(clusterState, allocation);
        assertEquals(ClusterHealthStatus.YELLOW, getClusterHealthStatus(clusterState));

        clusterState = addNode(clusterState, "node_d2", false);
        clusterState = applyStartedShardsUntilNoChange(clusterState, allocation);
        assertEquals(ClusterHealthStatus.GREEN, getClusterHealthStatus(clusterState));

        clusterState = removeNode(clusterState, "node_d1", allocation);
        assertEquals(ClusterHealthStatus.YELLOW, getClusterHealthStatus(clusterState));

        clusterState = removeNode(clusterState, "node_d2", allocation);
        assertEquals(ClusterHealthStatus.RED, getClusterHealthStatus(clusterState));

        routingTable = RoutingTable.builder(routingTable).remove("test").build();
        metadata = Metadata.builder(clusterState.metadata()).remove("test").build();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).metadata(metadata).build();
        assertEquals(0, clusterState.nodes().getDataNodes().size());
        assertEquals(ClusterHealthStatus.GREEN, getClusterHealthStatus(clusterState));
    }

    private ClusterState addNode(ClusterState clusterState, String nodeName, boolean isMaster) {
        DiscoveryNodes.Builder nodeBuilder = DiscoveryNodes.builder(clusterState.getNodes());
        nodeBuilder.add(newNode(nodeName, Collections.singleton(isMaster ? DiscoveryNodeRole.MASTER_ROLE : DiscoveryNodeRole.DATA_ROLE)));
        return ClusterState.builder(clusterState).nodes(nodeBuilder).build();
    }

    private ClusterState removeNode(ClusterState clusterState, String nodeName, AllocationService allocationService) {
        return allocationService.disassociateDeadNodes(ClusterState.builder(clusterState)
            .nodes(DiscoveryNodes.builder(clusterState.getNodes()).remove(nodeName)).build(), true, "reroute");
    }

    private ClusterHealthStatus getClusterHealthStatus(ClusterState clusterState) {
        return new ClusterStateHealth(clusterState).getStatus();
    }

}
