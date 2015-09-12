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
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.test.ESAllocationTestCase;
import org.junit.Test;

import static org.elasticsearch.cluster.routing.ShardRoutingState.INITIALIZING;
import static org.elasticsearch.common.settings.Settings.settingsBuilder;
import static org.hamcrest.Matchers.equalTo;

/**
 */
public class PreferPrimaryAllocationTests extends ESAllocationTestCase {

    private final ESLogger logger = Loggers.getLogger(PreferPrimaryAllocationTests.class);

    @Test
    public void testPreferPrimaryAllocationOverReplicas() {
        logger.info("create an allocation with 1 initial recoveries");
        AllocationService strategy = createAllocationService(settingsBuilder()
                .put("cluster.routing.allocation.node_concurrent_recoveries", 1)
                .put("cluster.routing.allocation.node_initial_primaries_recoveries", 1)
                .build());

        logger.info("create several indices with no replicas, and wait till all are allocated");

        MetaData metaData = MetaData.builder()
                .put(IndexMetaData.builder("test1").settings(settings(Version.CURRENT)).numberOfShards(10).numberOfReplicas(0))
                .put(IndexMetaData.builder("test2").settings(settings(Version.CURRENT)).numberOfShards(10).numberOfReplicas(0))
                .build();

        RoutingTable routingTable = RoutingTable.builder()
                .addAsNew(metaData.index("test1"))
                .addAsNew(metaData.index("test2"))
                .build();

        ClusterState clusterState = ClusterState.builder(org.elasticsearch.cluster.ClusterName.DEFAULT).metaData(metaData).routingTable(routingTable).build();

        logger.info("adding two nodes and performing rerouting till all are allocated");
        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder().put(newNode("node1")).put(newNode("node2"))).build();
        routingTable = strategy.reroute(clusterState).routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();

        while (!clusterState.getRoutingNodes().shardsWithState(INITIALIZING).isEmpty()) {
            routingTable = strategy.applyStartedShards(clusterState, clusterState.getRoutingNodes().shardsWithState(INITIALIZING)).routingTable();
            clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();
        }

        logger.info("increasing the number of replicas to 1, and perform a reroute (to get the replicas allocation going)");
        routingTable = RoutingTable.builder(routingTable).updateNumberOfReplicas(1).build();
        metaData = MetaData.builder(clusterState.metaData()).updateNumberOfReplicas(1).build();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).metaData(metaData).build();

        routingTable = strategy.reroute(clusterState).routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();

        logger.info("2 replicas should be initializing now for the existing indices (we throttle to 1)");
        assertThat(clusterState.getRoutingNodes().shardsWithState(INITIALIZING).size(), equalTo(2));

        logger.info("create a new index");
        metaData = MetaData.builder(clusterState.metaData())
                .put(IndexMetaData.builder("new_index").settings(settings(Version.CURRENT)).numberOfShards(4).numberOfReplicas(0))
                .build();

        routingTable = RoutingTable.builder(clusterState.routingTable())
                .addAsNew(metaData.index("new_index"))
                .build();

        clusterState = ClusterState.builder(clusterState).metaData(metaData).routingTable(routingTable).build();

        logger.info("reroute, verify that primaries for the new index primary shards are allocated");
        routingTable = strategy.reroute(clusterState).routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();

        assertThat(clusterState.routingTable().index("new_index").shardsWithState(INITIALIZING).size(), equalTo(2));
    }
}
