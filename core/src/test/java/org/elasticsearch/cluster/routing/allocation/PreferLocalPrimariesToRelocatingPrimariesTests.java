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
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESAllocationTestCase;

import static java.util.Collections.singletonMap;
import static org.elasticsearch.cluster.routing.ShardRoutingState.INITIALIZING;
import static org.elasticsearch.cluster.routing.ShardRoutingState.RELOCATING;
import static org.elasticsearch.cluster.routing.ShardRoutingState.STARTED;
import static org.elasticsearch.cluster.routing.ShardRoutingState.UNASSIGNED;
import static org.hamcrest.Matchers.equalTo;

/**
 */
public class PreferLocalPrimariesToRelocatingPrimariesTests extends ESAllocationTestCase {
    public void testPreferLocalPrimaryAllocationOverFiltered() {
        int concurrentRecoveries = randomIntBetween(1, 10);
        int primaryRecoveries = randomIntBetween(1, 10);
        int numberOfShards = randomIntBetween(5, 20);
        int totalNumberOfShards = numberOfShards * 2;

        logger.info("create an allocation with [{}] initial primary recoveries and [{}] concurrent recoveries", primaryRecoveries, concurrentRecoveries);
        AllocationService strategy = createAllocationService(Settings.builder()
                .put("cluster.routing.allocation.node_concurrent_recoveries", concurrentRecoveries)
                .put("cluster.routing.allocation.node_initial_primaries_recoveries", primaryRecoveries)
                .build());

        logger.info("create 2 indices with [{}] no replicas, and wait till all are allocated", numberOfShards);

        MetaData metaData = MetaData.builder()
                .put(IndexMetaData.builder("test1").settings(settings(Version.CURRENT)).numberOfShards(numberOfShards).numberOfReplicas(0))
                .put(IndexMetaData.builder("test2").settings(settings(Version.CURRENT)).numberOfShards(numberOfShards).numberOfReplicas(0))
                .build();

        RoutingTable routingTable = RoutingTable.builder()
                .addAsNew(metaData.index("test1"))
                .addAsNew(metaData.index("test2"))
                .build();

        ClusterState clusterState = ClusterState.builder(org.elasticsearch.cluster.ClusterName.CLUSTER_NAME_SETTING.getDefault(Settings.EMPTY)).metaData(metaData).routingTable(routingTable).build();

        logger.info("adding two nodes and performing rerouting till all are allocated");
        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder()
                .put(newNode("node1", singletonMap("tag1", "value1")))
                .put(newNode("node2", singletonMap("tag1", "value2")))).build();

        routingTable = strategy.reroute(clusterState, "reroute").routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();

        while (!clusterState.getRoutingNodes().shardsWithState(INITIALIZING).isEmpty()) {
            routingTable = strategy.applyStartedShards(clusterState, clusterState.getRoutingNodes().shardsWithState(INITIALIZING)).routingTable();
            clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();
        }

        logger.info("remove one of the nodes and apply filter to move everything from another node");

        metaData = MetaData.builder()
                .put(IndexMetaData.builder("test1").settings(settings(Version.CURRENT)
                        .put("index.number_of_shards", numberOfShards)
                        .put("index.number_of_replicas", 0)
                        .put("index.routing.allocation.exclude.tag1", "value2")
                        .build()))
                .put(IndexMetaData.builder("test2").settings(settings(Version.CURRENT)
                        .put("index.number_of_shards", numberOfShards)
                        .put("index.number_of_replicas", 0)
                        .put("index.routing.allocation.exclude.tag1", "value2")
                        .build()))
                .build();
        clusterState = ClusterState.builder(clusterState).metaData(metaData).nodes(DiscoveryNodes.builder(clusterState.nodes()).remove("node1")).build();
        routingTable = strategy.reroute(clusterState, "reroute").routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();

        logger.info("[{}] primaries should be still started but [{}] other primaries should be unassigned", numberOfShards, numberOfShards);
        assertThat(clusterState.getRoutingNodes().shardsWithState(STARTED).size(), equalTo(numberOfShards));
        assertThat(clusterState.getRoutingNodes().shardsWithState(INITIALIZING).size(), equalTo(0));
        assertThat(clusterState.routingTable().shardsWithState(UNASSIGNED).size(), equalTo(numberOfShards));

        logger.info("start node back up");
        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder(clusterState.nodes())
                .put(newNode("node1", singletonMap("tag1", "value1")))).build();
        routingTable = strategy.reroute(clusterState, "reroute").routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();

        while (clusterState.getRoutingNodes().shardsWithState(STARTED).size() < totalNumberOfShards) {
            int localInitializations = 0;
            int relocatingInitializations = 0;
            for (ShardRouting routing : clusterState.getRoutingNodes().shardsWithState(INITIALIZING)) {
                if (routing.relocatingNodeId() == null) {
                    localInitializations++;
                } else {
                    relocatingInitializations++;
                }
            }
            int needToInitialize = totalNumberOfShards - clusterState.getRoutingNodes().shardsWithState(STARTED).size() - clusterState.getRoutingNodes().shardsWithState(RELOCATING).size();
            logger.info("local initializations: [{}], relocating: [{}], need to initialize: {}", localInitializations, relocatingInitializations, needToInitialize);
            assertThat(localInitializations, equalTo(Math.min(primaryRecoveries, needToInitialize)));
            clusterState = startRandomInitializingShard(clusterState, strategy);
        }
    }
}
