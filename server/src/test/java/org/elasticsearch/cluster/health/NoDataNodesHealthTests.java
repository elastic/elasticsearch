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
import org.elasticsearch.cluster.routing.RoutingNodes;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.common.util.set.Sets;

import java.util.Collections;

import static org.elasticsearch.cluster.routing.ShardRoutingState.INITIALIZING;
import static org.elasticsearch.cluster.routing.ShardRoutingState.STARTED;

/**
 * The test case checks for the scenario when there is no data node in the cluster and only
 * master is active. At this moment when index creation is tried, the cluster health status should
 * change to RED
 */
public class NoDataNodesHealthTests extends ESAllocationTestCase {
    /**
     * This method specifically creates a cluster with no data nodes
     * and a single master node
     */
    private ClusterState setUpClusterWithNoDataNodes() {

        DiscoveryNodes node = DiscoveryNodes.builder().add(newNode("node_m", Collections.singleton(DiscoveryNode.Role.MASTER))).build();
        MetaData metaData = MetaData.builder()
                .put(IndexMetaData.builder("TestIndex")
                        .settings(settings(Version.CURRENT))
                        .numberOfShards(randomIntBetween(1, 3))
                        .numberOfReplicas(randomIntBetween(0, 2)))
                .build();
        RoutingTable routingTable = RoutingTable.builder().addAsNew(metaData.index("TestIndex")).build();
        ClusterState state = ClusterState.builder(new ClusterName("test_cluster"))
                .nodes(node)
                .metaData(metaData)
                .routingTable(routingTable)
                .build();
        MockAllocationService service = createAllocationService();
        state = service.reroute(state, "reroute");
        return state;
    }

    public void testClusterHealthWithNoDataNodes() {
        ClusterState state = setUpClusterWithNoDataNodes();
        int dataNodes = state.nodes().getDataNodes().size();
        int masterNodes = state.nodes().getMasterNodes().size();
        assertTrue(dataNodes == 0);
        assertTrue(masterNodes > 0);
        ClusterHealthStatus clusterHealthStatus = new ClusterStateHealth(state).getStatus();
        assertEquals(ClusterHealthStatus.RED, clusterHealthStatus);
    }

    /**
     * The method test for scenario where we have a cluster with indices and data nodes
     * and then all data nodes gets terminated now new index is created then
     * all indices and cluster health should be red, but last allocation attempts of new index shards v/s
     * old index shards should be different.
     */
    public void testAllocationStatusForTerminatedNodes() {
        //creates one master and two data nodes
        DiscoveryNodes.Builder nodeBuilder = DiscoveryNodes.builder().add(newNode("node_m", Collections.singleton(DiscoveryNode.Role.MASTER)))
                .add(newNode("node_d1", Collections.singleton(DiscoveryNode.Role.DATA)))
                .add(newNode("node_d2", Collections.singleton(DiscoveryNode.Role.DATA)));
        MetaData metaData = MetaData.builder()
                .put(IndexMetaData.builder("TestIndex")
                        .settings(settings(Version.CURRENT))
                        .numberOfShards(2)
                        .numberOfReplicas(0))
                .build();
        RoutingTable routingTable = RoutingTable.builder().addAsNew(metaData.index("TestIndex")).build();
        ClusterState state = ClusterState.builder(new ClusterName("test_cluster"))
                .nodes(nodeBuilder.build())
                .metaData(metaData)
                .routingTable(routingTable)
                .build();
        MockAllocationService allocationService = createAllocationService();
        //perform allocation of TestIndex
        state = allocationService.reroute(state, "Test_allocation");
        state = allocationService.applyStartedShards(state, state.getRoutingNodes().shardsWithState(INITIALIZING));
        IndexMetaData.Builder idxMetaBuilder = IndexMetaData.builder(state.metaData().index("TestIndex"));
        for (final ShardRouting shards : state.getRoutingTable().index("TestIndex").shardsWithState(STARTED)) {
            idxMetaBuilder.putInSyncAllocationIds(shards.getId(), Sets.newHashSet(shards.allocationId().getId()));
        }
        state = ClusterState.builder(state).metaData(MetaData.builder(state.metaData()).put(idxMetaBuilder)).build();
        //asserting the cluster is in green after TestIndex Creation
        assertEquals(ClusterHealthStatus.GREEN, new ClusterStateHealth(state).getStatus());
        //Terminating data nodes
        state = ClusterState.builder(state)
                .nodes(DiscoveryNodes.builder(state.getNodes())
                        .remove("node_d1").remove("node_d2").build())
                .build();
        //Removing dead nodes from cluster with a cluster reroute
        state = allocationService.deassociateDeadNodes(state, true, "Test_allocation");
        //asserting that a cluster state goes Red after data nodes goes terminated
        assertEquals(ClusterHealthStatus.RED, new ClusterStateHealth(state).getStatus());
        //Creating NewTestIndex meta deta
        metaData = MetaData.builder(state.metaData())
                .put(IndexMetaData.builder("NewTestIndex")
                        .settings(settings(Version.CURRENT))
                        .numberOfShards(2)
                        .numberOfReplicas(0))
                .build();
        //changed cluster state
        state = ClusterState.builder(state)
                .metaData(metaData)
                .routingTable(RoutingTable.builder(state.getRoutingTable()).addAsNew(metaData.index("NewTestIndex")).build()).build();
        //allocation after newly created index
        state = allocationService.reroute(state, "no data nodes");
        assertEquals(ClusterHealthStatus.RED, new ClusterStateHealth(state).getStatus());
        RoutingNodes routingNodes = state.getRoutingNodes();
        RoutingNodes.UnassignedShards unassignedShards = routingNodes.unassigned();
        assertFalse(unassignedShards.isEmpty());
        RoutingNodes.UnassignedShards.UnassignedIterator unassignedIterator = unassignedShards.iterator();
        while (unassignedIterator.hasNext()) {
            ShardRouting shard = unassignedIterator.next();
            UnassignedInfo shardInfo = shard.unassignedInfo();
            /* asserting that the TestIndex shards have different AllocationStatus than DECIDERS_NO
               and NewTestIndex status is DECIDERS_NO */
            if (shard.getIndexName().equals("TestIndex")) {
                assertNotEquals(shardInfo.getLastAllocationStatus(), UnassignedInfo.AllocationStatus.DECIDERS_NO);
            } else if (shard.getIndexName().equals("NewTestIndex")) {
                assertEquals(shardInfo.getLastAllocationStatus(), UnassignedInfo.AllocationStatus.DECIDERS_NO);
            }
        }
    }
}
