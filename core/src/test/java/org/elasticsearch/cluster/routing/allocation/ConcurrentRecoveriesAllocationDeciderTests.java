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

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;

import java.util.HashMap;
import java.util.Map;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ESAllocationTestCase;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.common.settings.Settings;

public class ConcurrentRecoveriesAllocationDeciderTests extends ESAllocationTestCase {

    public void testClusterConcurrentRecoveries() {
        int primaryShards = 5, replicaShards = 1, numberIndices = 100;
        int clusterConcurrentRecoveries = 15;
        AllocationService initialStrategy = createAllocationService(
        Settings.builder().put("cluster.routing.allocation.awareness.attributes", "zone")
        .put("cluster.routing.allocation.node_initial_primaries_recoveries", "8")
        .put("cluster.routing.allocation.node_concurrent_recoveries", "4").put("cluster.routing.allocation.exclude.tag", "tag_0").build());

        AllocationService excludeStrategy = createAllocationService(Settings.builder()
        .put("cluster.routing.allocation.awareness.attributes", "zone").put("cluster.routing.allocation.node_concurrent_recoveries", "4")
        .put("cluster.routing.allocation.cluster_concurrent_recoveries", String.valueOf(clusterConcurrentRecoveries))
        .put("cluster.routing.allocation.exclude.tag", "tag_1").build());

        logger.info("Building initial routing table");

        MetaData.Builder metaDataBuilder = MetaData.builder();
        for (int i = 0; i < numberIndices; i++) {
            metaDataBuilder.put(IndexMetaData.builder("test_" + i).settings(settings(Version.CURRENT)).numberOfShards(primaryShards)
            .numberOfReplicas(replicaShards));
        }
        RoutingTable.Builder initialRoutingTableBuilder = RoutingTable.builder();
        MetaData metaData = metaDataBuilder.build();
        for (int i = 0; i < numberIndices; i++) {
            initialRoutingTableBuilder.addAsNew(metaData.index("test_" + i));
        }
        RoutingTable routingTable = initialRoutingTableBuilder.build();

        logger.info("--> adding nodes and starting shards");

        ClusterState clusterState = ClusterState.builder(ClusterName.CLUSTER_NAME_SETTING.getDefault(Settings.EMPTY)).metaData(metaData)
        .routingTable(routingTable).nodes(setUpClusterNodes(randomIntBetween(5, 10), randomIntBetween(5, 10))).build();

        clusterState = initialStrategy.reroute(clusterState, "reroute");

        // Initialize shards
        while (clusterState.getRoutingNodes().hasUnassignedShards()) {
            clusterState = initialStrategy.applyStartedShards(clusterState,
            clusterState.getRoutingNodes().shardsWithState(ShardRoutingState.INITIALIZING));
        }

        // Ensure all shards are started
        while (clusterState.getRoutingNodes().shardsWithState(ShardRoutingState.INITIALIZING).size() > 0) {
            clusterState = initialStrategy.applyStartedShards(clusterState,
            clusterState.getRoutingNodes().shardsWithState(ShardRoutingState.INITIALIZING));
        }

        assertThat(clusterState.getRoutingNodes().shardsWithState(ShardRoutingState.STARTED).size(),
        equalTo((replicaShards + 1) * primaryShards * numberIndices));
        assertThat(clusterState.getRoutingNodes().shardsWithState(ShardRoutingState.INITIALIZING).size(), equalTo(0));
        assertThat(clusterState.getRoutingNodes().shardsWithState(ShardRoutingState.RELOCATING).size(), equalTo(0));

        logger.info("--> Performing a reroute ");

        clusterState = excludeStrategy.reroute(clusterState, "reroute");
        assertThat(clusterState.getRoutingNodes().shardsWithState(ShardRoutingState.RELOCATING).size(),
        equalTo(clusterConcurrentRecoveries));
        for (ShardRouting startedShard : clusterState.getRoutingNodes().shardsWithState(ShardRoutingState.STARTED)) {
            assertThat(clusterState.getRoutingNodes().node(startedShard.currentNodeId()).node().getAttributes().get("tag"),
            equalTo("tag_1"));
        }

        logger.info("--> Disabling cluster_concurrent_recoveries and re-routing ");

        excludeStrategy = createAllocationService(Settings.builder().put("cluster.routing.allocation.awareness.attributes", "zone")
        .put("cluster.routing.allocation.node_concurrent_recoveries", "4")
        .put("cluster.routing.allocation.cluster_concurrent_recoveries", "-1").put("cluster.routing.allocation.exclude.tag", "tag_1")
        .build());

        // verify if the recoveries are atleast min node
        // count*node_concurrent_recoveries
        clusterState = excludeStrategy.reroute(clusterState, "reroute");
        assertThat(clusterState.getRoutingNodes().shardsWithState(ShardRoutingState.RELOCATING).size(), greaterThanOrEqualTo(20));
    }

    private DiscoveryNodes.Builder setUpClusterNodes(int sourceNodes, int targetNodes) {
        DiscoveryNodes.Builder nb = DiscoveryNodes.builder();
        for (int i = 1; i <= sourceNodes; i++) {
            Map<String, String> attributes = new HashMap<>();
            attributes.put("tag", "tag_" + 1);
            attributes.put("zone", "zone_" + (i % 2));
            nb.add(newNode("node_s_" + i, attributes));
        }
        for (int j = 1; j <= targetNodes; j++) {
            Map<String, String> attributes = new HashMap<>();
            attributes.put("tag", "tag_" + 0);
            attributes.put("zone", "zone_" + (j % 2));
            nb.add(newNode("node_t_" + j, attributes));
        }
        return nb;
    }
}
