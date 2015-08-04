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

import static org.elasticsearch.cluster.routing.ShardRoutingState.*;
import static org.elasticsearch.common.settings.Settings.settingsBuilder;
import static org.hamcrest.Matchers.equalTo;

/**
 *
 */
public class ThrottlingAllocationTests extends ESAllocationTestCase {

    private final ESLogger logger = Loggers.getLogger(ThrottlingAllocationTests.class);

    @Test
    public void testPrimaryRecoveryThrottling() {
        AllocationService strategy = createAllocationService(settingsBuilder()
                .put("cluster.routing.allocation.node_concurrent_recoveries", 3)
                .put("cluster.routing.allocation.node_initial_primaries_recoveries", 3)
                .build());

        logger.info("Building initial routing table");

        MetaData metaData = MetaData.builder()
                .put(IndexMetaData.builder("test").settings(settings(Version.CURRENT)).numberOfShards(10).numberOfReplicas(1))
                .build();

        RoutingTable routingTable = RoutingTable.builder()
                .addAsNew(metaData.index("test"))
                .build();

        ClusterState clusterState = ClusterState.builder(org.elasticsearch.cluster.ClusterName.DEFAULT).metaData(metaData).routingTable(routingTable).build();

        logger.info("start one node, do reroute, only 3 should initialize");
        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder().put(newNode("node1"))).build();
        routingTable = strategy.reroute(clusterState).routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();

        assertThat(routingTable.shardsWithState(STARTED).size(), equalTo(0));
        assertThat(routingTable.shardsWithState(INITIALIZING).size(), equalTo(3));
        assertThat(routingTable.shardsWithState(UNASSIGNED).size(), equalTo(17));

        logger.info("start initializing, another 3 should initialize");
        routingTable = strategy.applyStartedShards(clusterState, routingTable.shardsWithState(INITIALIZING)).routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();

        assertThat(routingTable.shardsWithState(STARTED).size(), equalTo(3));
        assertThat(routingTable.shardsWithState(INITIALIZING).size(), equalTo(3));
        assertThat(routingTable.shardsWithState(UNASSIGNED).size(), equalTo(14));

        logger.info("start initializing, another 3 should initialize");
        routingTable = strategy.applyStartedShards(clusterState, routingTable.shardsWithState(INITIALIZING)).routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();

        assertThat(routingTable.shardsWithState(STARTED).size(), equalTo(6));
        assertThat(routingTable.shardsWithState(INITIALIZING).size(), equalTo(3));
        assertThat(routingTable.shardsWithState(UNASSIGNED).size(), equalTo(11));

        logger.info("start initializing, another 1 should initialize");
        routingTable = strategy.applyStartedShards(clusterState, routingTable.shardsWithState(INITIALIZING)).routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();

        assertThat(routingTable.shardsWithState(STARTED).size(), equalTo(9));
        assertThat(routingTable.shardsWithState(INITIALIZING).size(), equalTo(1));
        assertThat(routingTable.shardsWithState(UNASSIGNED).size(), equalTo(10));

        logger.info("start initializing, all primaries should be started");
        routingTable = strategy.applyStartedShards(clusterState, routingTable.shardsWithState(INITIALIZING)).routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();

        assertThat(routingTable.shardsWithState(STARTED).size(), equalTo(10));
        assertThat(routingTable.shardsWithState(INITIALIZING).size(), equalTo(0));
        assertThat(routingTable.shardsWithState(UNASSIGNED).size(), equalTo(10));
    }

    @Test
    public void testReplicaAndPrimaryRecoveryThrottling() {
        AllocationService strategy = createAllocationService(settingsBuilder()
                .put("cluster.routing.allocation.concurrent_recoveries", 3)
                .put("cluster.routing.allocation.node_initial_primaries_recoveries", 3)
                .build());

        logger.info("Building initial routing table");

        MetaData metaData = MetaData.builder()
                .put(IndexMetaData.builder("test").settings(settings(Version.CURRENT)).numberOfShards(5).numberOfReplicas(1))
                .build();

        RoutingTable routingTable = RoutingTable.builder()
                .addAsNew(metaData.index("test"))
                .build();

        ClusterState clusterState = ClusterState.builder(org.elasticsearch.cluster.ClusterName.DEFAULT).metaData(metaData).routingTable(routingTable).build();

        logger.info("start one node, do reroute, only 3 should initialize");
        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder().put(newNode("node1"))).build();
        routingTable = strategy.reroute(clusterState).routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();

        assertThat(routingTable.shardsWithState(STARTED).size(), equalTo(0));
        assertThat(routingTable.shardsWithState(INITIALIZING).size(), equalTo(3));
        assertThat(routingTable.shardsWithState(UNASSIGNED).size(), equalTo(7));

        logger.info("start initializing, another 2 should initialize");
        routingTable = strategy.applyStartedShards(clusterState, routingTable.shardsWithState(INITIALIZING)).routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();

        assertThat(routingTable.shardsWithState(STARTED).size(), equalTo(3));
        assertThat(routingTable.shardsWithState(INITIALIZING).size(), equalTo(2));
        assertThat(routingTable.shardsWithState(UNASSIGNED).size(), equalTo(5));

        logger.info("start initializing, all primaries should be started");
        routingTable = strategy.applyStartedShards(clusterState, routingTable.shardsWithState(INITIALIZING)).routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();

        assertThat(routingTable.shardsWithState(STARTED).size(), equalTo(5));
        assertThat(routingTable.shardsWithState(INITIALIZING).size(), equalTo(0));
        assertThat(routingTable.shardsWithState(UNASSIGNED).size(), equalTo(5));

        logger.info("start another node, replicas should start being allocated");
        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder(clusterState.nodes()).put(newNode("node2"))).build();
        routingTable = strategy.reroute(clusterState).routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();

        assertThat(routingTable.shardsWithState(STARTED).size(), equalTo(5));
        assertThat(routingTable.shardsWithState(INITIALIZING).size(), equalTo(3));
        assertThat(routingTable.shardsWithState(UNASSIGNED).size(), equalTo(2));

        logger.info("start initializing replicas");
        routingTable = strategy.applyStartedShards(clusterState, routingTable.shardsWithState(INITIALIZING)).routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();

        assertThat(routingTable.shardsWithState(STARTED).size(), equalTo(8));
        assertThat(routingTable.shardsWithState(INITIALIZING).size(), equalTo(2));
        assertThat(routingTable.shardsWithState(UNASSIGNED).size(), equalTo(0));

        logger.info("start initializing replicas, all should be started");
        routingTable = strategy.applyStartedShards(clusterState, routingTable.shardsWithState(INITIALIZING)).routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();

        assertThat(routingTable.shardsWithState(STARTED).size(), equalTo(10));
        assertThat(routingTable.shardsWithState(INITIALIZING).size(), equalTo(0));
        assertThat(routingTable.shardsWithState(UNASSIGNED).size(), equalTo(0));
    }
}
