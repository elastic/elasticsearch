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

package org.elasticsearch.cluster.routing.allocation.decider;

import com.carrotsearch.hppc.ObjectIntHashMap;
import com.carrotsearch.hppc.cursors.ObjectCursor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ESAllocationTestCase;
import org.elasticsearch.cluster.EmptyClusterInfoService;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.cluster.routing.allocation.allocator.BalancedShardsAllocator;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.gateway.TestGatewayAllocator;

import java.util.Collections;

import static org.elasticsearch.cluster.routing.ShardRoutingState.INITIALIZING;
import static org.elasticsearch.cluster.routing.ShardRoutingState.STARTED;
import static org.elasticsearch.cluster.routing.allocation.decider.AwarenessAllocationDecider.CLUSTER_ROUTING_ALLOCATION_AWARENESS_ATTRIBUTE_SETTING;

public class AwarenessAllocationTests extends ESAllocationTestCase {

    private final Logger logger = LogManager.getLogger(AwarenessAllocationTests.class);

    public void testWithDifferentReplicas() {
        for (int i = 0; i <= 100; i++) {
            testAwarenessAllocation(i);
        }
    }

    private void testAwarenessAllocation(int replicas) {
        Settings settings = Settings.builder()
            .put(CLUSTER_ROUTING_ALLOCATION_AWARENESS_ATTRIBUTE_SETTING.getKey(), "group")
            .build();
        ClusterSettings clusterSettings = new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        AllocationDeciders allocationDeciders = new AllocationDeciders(
            Collections.singletonList(new AwarenessAllocationDecider(settings, clusterSettings)));
        AllocationService service = new AllocationService(allocationDeciders,
            new TestGatewayAllocator(), new BalancedShardsAllocator(settings), EmptyClusterInfoService.INSTANCE);

        logger.info("Building initial routing table");

        MetaData metaData = MetaData.builder()
            .put(IndexMetaData.builder("test").settings(settings(Version.CURRENT)).numberOfShards(1).numberOfReplicas(replicas))
            .build();

        RoutingTable routingTable = RoutingTable.builder()
            .addAsNew(metaData.index("test"))
            .build();

        ClusterState clusterState = ClusterState.builder(org.elasticsearch.cluster.ClusterName.CLUSTER_NAME_SETTING
            .getDefault(settings)).metaData(metaData).routingTable(routingTable).build();

        logger.info("--> adding three nodes and do rerouting");
        clusterState = ClusterState.builder(clusterState).nodes(createBuilder(replicas + 1)).build();

        clusterState = service.reroute(clusterState, "reroute");
        routingTable = clusterState.routingTable();
        assertEquals(routingTable.index("test").shard(0).primaryShard().state(), INITIALIZING);
        if (replicas > 0) {
            assertEquals(routingTable.index("test").shard(0).replicaShards().get(0).state(), INITIALIZING);
        }

        clusterState = startShardsAndReroute(service, clusterState, routingTable.index("test").shard(0).shardsWithState(INITIALIZING));
        routingTable = clusterState.routingTable();
        assertEquals(routingTable.index("test").shard(0).primaryShard().state(), STARTED);
        if (replicas > 0) {
            assertEquals(routingTable.index("test").shard(0).replicaShards().get(0).state(), STARTED);
        }

        ObjectIntHashMap<String> nodesPerAttribute = clusterState.getRoutingNodes().nodesPerAttributesCounts("group");
        ObjectIntHashMap<String> shardPerAttribute = new ObjectIntHashMap<>();
        for (ShardRouting assignedShard : clusterState.getRoutingNodes().assignedShards(routingTable.index("test").shard(0).shardId())) {
            if (assignedShard.started() || assignedShard.initializing()) {
                RoutingNode routingNode = clusterState.getRoutingNodes().node(assignedShard.currentNodeId());
                shardPerAttribute.addTo(routingNode.node().getAttributes().get("group"), 1);
            }
        }

        int max = (replicas + 1) / nodesPerAttribute.size();
        int min = (replicas + 1) / nodesPerAttribute.size();
        for (ObjectCursor<String> cursor : nodesPerAttribute.keys()) {
            int value = shardPerAttribute.get(cursor.value);
            if (value > max) {
                max = value;
            } else if (value < min) {
                min = value;
            }
        }

        assertTrue(max - min <= 1);
    }

    private DiscoveryNodes.Builder createBuilder(int shards) {
        DiscoveryNodes.Builder builder = DiscoveryNodes.builder();
        int num = 0;
        for (int i = 0; i < shards; i++) {
            if (num++ > 4) {
                num = 1;
            }
            builder.add(newNode("node" + i, Collections.singletonMap("group", "group" + num)));
        }
        return builder;
    }
}
