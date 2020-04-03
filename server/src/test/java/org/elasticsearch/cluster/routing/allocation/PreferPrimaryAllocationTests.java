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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ESAllocationTestCase;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.allocation.decider.ThrottlingAllocationDecider;
import org.elasticsearch.common.settings.Settings;

import static org.elasticsearch.cluster.routing.ShardRoutingState.INITIALIZING;
import static org.hamcrest.Matchers.equalTo;

public class PreferPrimaryAllocationTests extends ESAllocationTestCase {
    private final Logger logger = LogManager.getLogger(PreferPrimaryAllocationTests.class);

    public void testPreferPrimaryAllocationOverReplicas() {
        logger.info("create an allocation with 1 initial recoveries");
        AllocationService strategy = createAllocationService(Settings.builder()
                .put("cluster.routing.allocation.node_concurrent_recoveries", 1)
                .put(ThrottlingAllocationDecider.CLUSTER_ROUTING_ALLOCATION_NODE_CONCURRENT_OUTGOING_RECOVERIES_SETTING.getKey(), 10)
                .put("cluster.routing.allocation.node_initial_primaries_recoveries", 1)
                .build());

        logger.info("create several indices with no replicas, and wait till all are allocated");

        Metadata metadata = Metadata.builder()
                .put(IndexMetadata.builder("test1").settings(settings(Version.CURRENT)).numberOfShards(10).numberOfReplicas(0))
                .put(IndexMetadata.builder("test2").settings(settings(Version.CURRENT)).numberOfShards(10).numberOfReplicas(0))
                .build();

        RoutingTable initialRoutingTable = RoutingTable.builder()
                .addAsNew(metadata.index("test1"))
                .addAsNew(metadata.index("test2"))
                .build();

        ClusterState clusterState = ClusterState.builder(org.elasticsearch.cluster.ClusterName.CLUSTER_NAME_SETTING
            .getDefault(Settings.EMPTY)).metadata(metadata).routingTable(initialRoutingTable).build();

        logger.info("adding two nodes and performing rerouting till all are allocated");
        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder()
            .add(newNode("node1")).add(newNode("node2"))).build();
        clusterState = strategy.reroute(clusterState, "reroute");

        while (clusterState.getRoutingNodes().shardsWithState(INITIALIZING).isEmpty() == false) {
            clusterState = startInitializingShardsAndReroute(strategy, clusterState);
        }

        logger.info("increasing the number of replicas to 1, and perform a reroute (to get the replicas allocation going)");
        final String[] indices = {"test1", "test2"};
        RoutingTable updatedRoutingTable =
                RoutingTable.builder(clusterState.routingTable()).updateNumberOfReplicas(1, indices).build();
        metadata = Metadata.builder(clusterState.metadata()).updateNumberOfReplicas(1, indices).build();
        clusterState = ClusterState.builder(clusterState).routingTable(updatedRoutingTable).metadata(metadata).build();

        clusterState = strategy.reroute(clusterState, "reroute");

        logger.info("2 replicas should be initializing now for the existing indices (we throttle to 1)");
        assertThat(clusterState.getRoutingNodes().shardsWithState(INITIALIZING).size(), equalTo(2));

        logger.info("create a new index");
        metadata = Metadata.builder(clusterState.metadata())
                .put(IndexMetadata.builder("new_index").settings(settings(Version.CURRENT)).numberOfShards(4).numberOfReplicas(0))
                .build();

        updatedRoutingTable = RoutingTable.builder(clusterState.routingTable())
                .addAsNew(metadata.index("new_index"))
                .build();

        clusterState = ClusterState.builder(clusterState).metadata(metadata).routingTable(updatedRoutingTable).build();

        logger.info("reroute, verify that primaries for the new index primary shards are allocated");
        clusterState = strategy.reroute(clusterState, "reroute");

        assertThat(clusterState.routingTable().index("new_index").shardsWithState(INITIALIZING).size(), equalTo(2));
    }
}
