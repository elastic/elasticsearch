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
package org.elasticsearch.cluster.routing.allocation;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ESAllocationTestCase;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.allocation.decider.ThrottlingAllocationDecider;
import org.elasticsearch.common.settings.Settings;

import static org.elasticsearch.cluster.routing.ShardRoutingState.INITIALIZING;

public class AllocationPriorityTests extends ESAllocationTestCase {

    /**
     * Tests that higher prioritized primaries and replicas are allocated first even on the balanced shard allocator
     * See https://github.com/elastic/elasticsearch/issues/13249 for details
     */
    public void testPrioritizedIndicesAllocatedFirst() {
        AllocationService allocation = createAllocationService(Settings.builder().
                put(ThrottlingAllocationDecider.CLUSTER_ROUTING_ALLOCATION_NODE_CONCURRENT_RECOVERIES_SETTING.getKey(), 1)
                .put(ThrottlingAllocationDecider.CLUSTER_ROUTING_ALLOCATION_NODE_CONCURRENT_OUTGOING_RECOVERIES_SETTING.getKey(), 10)
                .put(ThrottlingAllocationDecider.CLUSTER_ROUTING_ALLOCATION_NODE_INITIAL_PRIMARIES_RECOVERIES_SETTING.getKey(), 1)
                .put(ThrottlingAllocationDecider.CLUSTER_ROUTING_ALLOCATION_NODE_CONCURRENT_INCOMING_RECOVERIES_SETTING.getKey(), 1)
            .build());
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
        MetaData metaData = MetaData.builder()
                .put(IndexMetaData.builder("first").settings(settings(Version.CURRENT)
                    .put(IndexMetaData.SETTING_PRIORITY, priorityFirst)).numberOfShards(2).numberOfReplicas(1))
                .put(IndexMetaData.builder("second").settings(settings(Version.CURRENT)
                    .put(IndexMetaData.SETTING_PRIORITY, prioritySecond)).numberOfShards(2).numberOfReplicas(1))
                .build();
        RoutingTable initialRoutingTable = RoutingTable.builder()
                .addAsNew(metaData.index("first"))
                .addAsNew(metaData.index("second"))
                .build();
        ClusterState clusterState = ClusterState.builder(org.elasticsearch.cluster.ClusterName.CLUSTER_NAME_SETTING
            .getDefault(Settings.EMPTY)).metaData(metaData).routingTable(initialRoutingTable).build();

        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder().add(newNode("node1"))
            .add(newNode("node2"))).build();
        clusterState = allocation.reroute(clusterState, "reroute");

        clusterState = allocation.reroute(clusterState, "reroute");
        assertEquals(2, clusterState.getRoutingNodes().shardsWithState(INITIALIZING).size());
        assertEquals(highPriorityName, clusterState.getRoutingNodes().shardsWithState(INITIALIZING).get(0).getIndexName());
        assertEquals(highPriorityName, clusterState.getRoutingNodes().shardsWithState(INITIALIZING).get(1).getIndexName());

        clusterState = startInitializingShardsAndReroute(allocation, clusterState);
        assertEquals(2, clusterState.getRoutingNodes().shardsWithState(INITIALIZING).size());
        assertEquals(lowPriorityName, clusterState.getRoutingNodes().shardsWithState(INITIALIZING).get(0).getIndexName());
        assertEquals(lowPriorityName, clusterState.getRoutingNodes().shardsWithState(INITIALIZING).get(1).getIndexName());

        clusterState = startInitializingShardsAndReroute(allocation, clusterState);
        assertEquals(clusterState.getRoutingNodes().shardsWithState(INITIALIZING).toString(),2,
            clusterState.getRoutingNodes().shardsWithState(INITIALIZING).size());
        assertEquals(highPriorityName, clusterState.getRoutingNodes().shardsWithState(INITIALIZING).get(0).getIndexName());
        assertEquals(highPriorityName, clusterState.getRoutingNodes().shardsWithState(INITIALIZING).get(1).getIndexName());

        clusterState = startInitializingShardsAndReroute(allocation, clusterState);
        assertEquals(2, clusterState.getRoutingNodes().shardsWithState(INITIALIZING).size());
        assertEquals(lowPriorityName, clusterState.getRoutingNodes().shardsWithState(INITIALIZING).get(0).getIndexName());
        assertEquals(lowPriorityName, clusterState.getRoutingNodes().shardsWithState(INITIALIZING).get(1).getIndexName());

    }
}
