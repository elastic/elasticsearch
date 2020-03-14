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
package org.elasticsearch.cluster.health;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ESAllocationTestCase;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
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

        MetaData metaData = MetaData.builder()
            .put(IndexMetaData.builder("test")
                .settings(settings(Version.CURRENT))
                .numberOfShards(2)
                .numberOfReplicas(1))
            .build();
        RoutingTable routingTable = RoutingTable.builder().addAsNew(metaData.index("test")).build();
        clusterState = ClusterState.builder(clusterState).metaData(metaData).routingTable(routingTable).build();
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
        metaData = MetaData.builder(clusterState.metaData()).remove("test").build();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).metaData(metaData).build();
        assertEquals(0, clusterState.nodes().getDataNodes().size());
        assertEquals(ClusterHealthStatus.GREEN, getClusterHealthStatus(clusterState));
    }

    public void testClusterHealthWithMultipleNodes() {
        ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT).build();
        if (randomBoolean()) {
            clusterState = addNode(clusterState, "node_m", true);
        }
        assertEquals(ClusterHealthStatus.GREEN, getClusterHealthStatus(clusterState));

        MetaData metaData = MetaData.builder()
            .put(IndexMetaData.builder("test")
                .settings(settings(Version.CURRENT))
                .numberOfShards(5)
                .numberOfReplicas(2))
            .build();
        RoutingTable routingTable = RoutingTable.builder().addAsNew(metaData.index("test")).build();
        clusterState = ClusterState.builder(clusterState).metaData(metaData).routingTable(routingTable).build();
        MockAllocationService allocation = createAllocationService();
        clusterState = applyStartedShardsUntilNoChange(clusterState, allocation);
        assertEquals(0, clusterState.nodes().getDataNodes().size());
        assertEquals(ClusterHealthStatus.RED, getClusterHealthStatus(clusterState));

        // Add 2 Nodes. (Red -> Yellow)
        clusterState = addNodes(clusterState, 1, 2);
        assertEquals(2, clusterState.nodes().getDataNodes().size());
        clusterState = applyStartedShardsUntilNoChange(clusterState, allocation);
        assertEquals(ClusterHealthStatus.YELLOW, getClusterHealthStatus(clusterState));
        // Go back to the Red state.
        clusterState = removeNodes(clusterState, 1, 2, allocation);
        clusterState = applyStartedShardsUntilNoChange(clusterState, allocation);
        // Add 3 Nodes. (Red -> Green)
        clusterState = addNodes(clusterState, 1, 3);
        assertEquals(3, clusterState.nodes().getDataNodes().size());
        clusterState = applyStartedShardsUntilNoChange(clusterState, allocation);
        assertEquals(ClusterHealthStatus.GREEN, getClusterHealthStatus(clusterState));

        // remove 2 Nodes. (Green -> Yellow)
        clusterState = removeNodes(clusterState, 1, 2, allocation) ;
        assertEquals(1, clusterState.nodes().getDataNodes().size());
        clusterState = applyStartedShardsUntilNoChange(clusterState, allocation);
        assertEquals(ClusterHealthStatus.YELLOW, getClusterHealthStatus(clusterState));
        // Go back to the Green state.
        clusterState = addNodes(clusterState, 1, 2);
        clusterState = applyStartedShardsUntilNoChange(clusterState, allocation);
        // remove 3 Nodes. (Green -> Red)
        clusterState = removeNodes(clusterState, 1, 3, allocation) ;
        assertEquals(0, clusterState.nodes().getDataNodes().size());
        clusterState = applyStartedShardsUntilNoChange(clusterState, allocation);
        assertEquals(ClusterHealthStatus.RED, getClusterHealthStatus(clusterState));
    }

    private ClusterState addNodes(ClusterState clusterState, int startNode, int numberOfNodes) {
        DiscoveryNodes.Builder nodeBuilder = DiscoveryNodes.builder(clusterState.getNodes());
        for(int nodeIndex = startNode ; nodeIndex <= numberOfNodes ; nodeIndex++) {
            String nodeName = "node_d" + nodeIndex;
            nodeBuilder.add(newNode(nodeName, Collections.singleton(DiscoveryNodeRole.DATA_ROLE)));
        }
        return ClusterState.builder(clusterState).nodes(nodeBuilder).build();
    }

    private ClusterState removeNodes(ClusterState clusterState, int startNode, int numberOfNodes, MockAllocationService allocation) {
        for(int nodeIndex = startNode ; nodeIndex <= numberOfNodes ; nodeIndex++) {
            String nodeName = "node_d" + nodeIndex;
            clusterState = removeNode(clusterState, nodeName, allocation);
        }
        return clusterState ;
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
