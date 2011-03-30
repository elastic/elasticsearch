/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.testng.annotations.Test;

import static org.elasticsearch.cluster.ClusterState.*;
import static org.elasticsearch.cluster.metadata.IndexMetaData.*;
import static org.elasticsearch.cluster.metadata.MetaData.*;
import static org.elasticsearch.cluster.node.DiscoveryNodes.*;
import static org.elasticsearch.cluster.routing.RoutingBuilders.*;
import static org.elasticsearch.cluster.routing.ShardRoutingState.*;
import static org.elasticsearch.cluster.routing.allocation.RoutingAllocationTests.*;
import static org.elasticsearch.common.settings.ImmutableSettings.*;
import static org.hamcrest.MatcherAssert.*;
import static org.hamcrest.Matchers.*;

/**
 * @author kimchy (shay.banon)
 */
public class ThrottlingAllocationTests {

    private final ESLogger logger = Loggers.getLogger(ThrottlingAllocationTests.class);

    @Test public void testPrimaryRecoveryThrottling() {
        ShardsAllocation strategy = new ShardsAllocation(settingsBuilder()
                .put("cluster.routing.allocation.node_concurrent_recoveries", 3)
                .put("cluster.routing.allocation.node_initial_primaries_recoveries", 3)
                .build());

        logger.info("Building initial routing table");

        MetaData metaData = newMetaDataBuilder()
                .put(newIndexMetaDataBuilder("test").numberOfShards(10).numberOfReplicas(1))
                .build();

        RoutingTable routingTable = routingTable()
                .add(indexRoutingTable("test").initializeEmpty(metaData.index("test")))
                .build();

        ClusterState clusterState = newClusterStateBuilder().metaData(metaData).routingTable(routingTable).build();

        logger.info("start one node, do reroute, only 3 should initialize");
        clusterState = newClusterStateBuilder().state(clusterState).nodes(newNodesBuilder().put(newNode("node1"))).build();
        routingTable = strategy.reroute(clusterState).routingTable();
        clusterState = newClusterStateBuilder().state(clusterState).routingTable(routingTable).build();

        assertThat(routingTable.shardsWithState(STARTED).size(), equalTo(0));
        assertThat(routingTable.shardsWithState(INITIALIZING).size(), equalTo(3));
        assertThat(routingTable.shardsWithState(UNASSIGNED).size(), equalTo(17));

        logger.info("start initializing, another 3 should initialize");
        routingTable = strategy.applyStartedShards(clusterState, routingTable.shardsWithState(INITIALIZING)).routingTable();
        clusterState = newClusterStateBuilder().state(clusterState).routingTable(routingTable).build();

        assertThat(routingTable.shardsWithState(STARTED).size(), equalTo(3));
        assertThat(routingTable.shardsWithState(INITIALIZING).size(), equalTo(3));
        assertThat(routingTable.shardsWithState(UNASSIGNED).size(), equalTo(14));

        logger.info("start initializing, another 3 should initialize");
        routingTable = strategy.applyStartedShards(clusterState, routingTable.shardsWithState(INITIALIZING)).routingTable();
        clusterState = newClusterStateBuilder().state(clusterState).routingTable(routingTable).build();

        assertThat(routingTable.shardsWithState(STARTED).size(), equalTo(6));
        assertThat(routingTable.shardsWithState(INITIALIZING).size(), equalTo(3));
        assertThat(routingTable.shardsWithState(UNASSIGNED).size(), equalTo(11));

        logger.info("start initializing, another 1 should initialize");
        routingTable = strategy.applyStartedShards(clusterState, routingTable.shardsWithState(INITIALIZING)).routingTable();
        clusterState = newClusterStateBuilder().state(clusterState).routingTable(routingTable).build();

        assertThat(routingTable.shardsWithState(STARTED).size(), equalTo(9));
        assertThat(routingTable.shardsWithState(INITIALIZING).size(), equalTo(1));
        assertThat(routingTable.shardsWithState(UNASSIGNED).size(), equalTo(10));

        logger.info("start initializing, all primaries should be started");
        routingTable = strategy.applyStartedShards(clusterState, routingTable.shardsWithState(INITIALIZING)).routingTable();
        clusterState = newClusterStateBuilder().state(clusterState).routingTable(routingTable).build();

        assertThat(routingTable.shardsWithState(STARTED).size(), equalTo(10));
        assertThat(routingTable.shardsWithState(INITIALIZING).size(), equalTo(0));
        assertThat(routingTable.shardsWithState(UNASSIGNED).size(), equalTo(10));
    }

    @Test public void testReplicaAndPrimaryRecoveryThrottling() {
        ShardsAllocation strategy = new ShardsAllocation(settingsBuilder()
                .put("cluster.routing.allocation.concurrent_recoveries", 3)
                .put("cluster.routing.allocation.node_initial_primaries_recoveries", 3)
                .build());

        logger.info("Building initial routing table");

        MetaData metaData = newMetaDataBuilder()
                .put(newIndexMetaDataBuilder("test").numberOfShards(5).numberOfReplicas(1))
                .build();

        RoutingTable routingTable = routingTable()
                .add(indexRoutingTable("test").initializeEmpty(metaData.index("test")))
                .build();

        ClusterState clusterState = newClusterStateBuilder().metaData(metaData).routingTable(routingTable).build();

        logger.info("start one node, do reroute, only 3 should initialize");
        clusterState = newClusterStateBuilder().state(clusterState).nodes(newNodesBuilder().put(newNode("node1"))).build();
        routingTable = strategy.reroute(clusterState).routingTable();
        clusterState = newClusterStateBuilder().state(clusterState).routingTable(routingTable).build();

        assertThat(routingTable.shardsWithState(STARTED).size(), equalTo(0));
        assertThat(routingTable.shardsWithState(INITIALIZING).size(), equalTo(3));
        assertThat(routingTable.shardsWithState(UNASSIGNED).size(), equalTo(7));

        logger.info("start initializing, another 2 should initialize");
        routingTable = strategy.applyStartedShards(clusterState, routingTable.shardsWithState(INITIALIZING)).routingTable();
        clusterState = newClusterStateBuilder().state(clusterState).routingTable(routingTable).build();

        assertThat(routingTable.shardsWithState(STARTED).size(), equalTo(3));
        assertThat(routingTable.shardsWithState(INITIALIZING).size(), equalTo(2));
        assertThat(routingTable.shardsWithState(UNASSIGNED).size(), equalTo(5));

        logger.info("start initializing, all primaries should be started");
        routingTable = strategy.applyStartedShards(clusterState, routingTable.shardsWithState(INITIALIZING)).routingTable();
        clusterState = newClusterStateBuilder().state(clusterState).routingTable(routingTable).build();

        assertThat(routingTable.shardsWithState(STARTED).size(), equalTo(5));
        assertThat(routingTable.shardsWithState(INITIALIZING).size(), equalTo(0));
        assertThat(routingTable.shardsWithState(UNASSIGNED).size(), equalTo(5));

        logger.info("start another node, replicas should start being allocated");
        clusterState = newClusterStateBuilder().state(clusterState).nodes(newNodesBuilder().putAll(clusterState.nodes()).put(newNode("node2"))).build();
        routingTable = strategy.reroute(clusterState).routingTable();
        clusterState = newClusterStateBuilder().state(clusterState).routingTable(routingTable).build();

        assertThat(routingTable.shardsWithState(STARTED).size(), equalTo(5));
        assertThat(routingTable.shardsWithState(INITIALIZING).size(), equalTo(3));
        assertThat(routingTable.shardsWithState(UNASSIGNED).size(), equalTo(2));

        logger.info("start initializing replicas");
        routingTable = strategy.applyStartedShards(clusterState, routingTable.shardsWithState(INITIALIZING)).routingTable();
        clusterState = newClusterStateBuilder().state(clusterState).routingTable(routingTable).build();

        assertThat(routingTable.shardsWithState(STARTED).size(), equalTo(8));
        assertThat(routingTable.shardsWithState(INITIALIZING).size(), equalTo(2));
        assertThat(routingTable.shardsWithState(UNASSIGNED).size(), equalTo(0));

        logger.info("start initializing replicas, all should be started");
        routingTable = strategy.applyStartedShards(clusterState, routingTable.shardsWithState(INITIALIZING)).routingTable();
        clusterState = newClusterStateBuilder().state(clusterState).routingTable(routingTable).build();

        assertThat(routingTable.shardsWithState(STARTED).size(), equalTo(10));
        assertThat(routingTable.shardsWithState(INITIALIZING).size(), equalTo(0));
        assertThat(routingTable.shardsWithState(UNASSIGNED).size(), equalTo(0));
    }
}
