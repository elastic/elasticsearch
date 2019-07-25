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
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.RoutingTable;

import java.util.Collections;

import static org.elasticsearch.cluster.routing.ShardRoutingState.*;

public class ClusterHealthAllocationTests extends ESAllocationTestCase {
    public void testClusterHealth() {
        ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT).build();
        clusterState = addNode(clusterState, "node_m", true);
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
        clusterState = allocation.reroute(clusterState, "reroute");
        assertTrue(clusterState.nodes().getDataNodes().size() == 0);
        assertTrue(clusterState.nodes().getMasterNodes().size() == 1);
        assertEquals(ClusterHealthStatus.RED, getClusterHealthStatus(clusterState));
        clusterState = addNode(clusterState, "node_d1", false);
        assertTrue(clusterState.nodes().getDataNodes().size() == 1);
        clusterState = allocation.reroute(clusterState, "reroute");
        // starting primaries
        clusterState = allocation.applyStartedShards(clusterState, clusterState.getRoutingNodes().shardsWithState(INITIALIZING));
        assertEquals(ClusterHealthStatus.YELLOW, getClusterHealthStatus(clusterState));
        clusterState = addNode(clusterState, "node_d2", false);
        clusterState = allocation.reroute(clusterState, "reroute");
        // starting replicas
        clusterState = allocation.applyStartedShards(clusterState, clusterState.getRoutingNodes().shardsWithState(INITIALIZING));
        assertEquals(ClusterHealthStatus.GREEN, getClusterHealthStatus(clusterState));
        clusterState = removeNode(clusterState, "node_d1");
        assertEquals(ClusterHealthStatus.YELLOW, getClusterHealthStatus(clusterState));
        clusterState = removeNode(clusterState, "node_d2");
        assertEquals(ClusterHealthStatus.RED, getClusterHealthStatus(clusterState));
        routingTable = RoutingTable.builder(routingTable).remove("test").build();
        metaData = MetaData.builder(clusterState.metaData()).remove("test").build();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).metaData(metaData).build();
        assertTrue(clusterState.nodes().getDataNodes().size() == 0);
        assertTrue(clusterState.nodes().getMasterNodes().size() == 1);
        assertEquals(ClusterHealthStatus.GREEN, getClusterHealthStatus(clusterState));
    }

    private ClusterState addNode(ClusterState clusterState, String nodeName, boolean isMaster) {

        DiscoveryNodes.Builder nodeBuilder = DiscoveryNodes.builder(clusterState.getNodes());
        if (isMaster) {
            nodeBuilder = nodeBuilder.add(newNode(nodeName, Collections.singleton(DiscoveryNode.Role.MASTER)));
        } else {
            nodeBuilder = nodeBuilder.add(newNode(nodeName, Collections.singleton(DiscoveryNode.Role.DATA)));
        }
        clusterState = ClusterState.builder(clusterState)
            .nodes(nodeBuilder.build())
            .build();
        return clusterState;
    }

    private ClusterState removeNode(ClusterState clusterState, String nodeName) {
        clusterState = ClusterState.builder(clusterState)
            .nodes(DiscoveryNodes.builder(clusterState.getNodes())
                .remove(nodeName).build())
            .build();
        clusterState = createAllocationService().deassociateDeadNodes(clusterState, true, "reroute");
        return clusterState;
    }

    private ClusterHealthStatus getClusterHealthStatus(ClusterState clusterState) {
        return new ClusterStateHealth(clusterState).getStatus();
    }
}
