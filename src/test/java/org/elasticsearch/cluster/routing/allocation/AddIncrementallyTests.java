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

import com.carrotsearch.hppc.cursors.ObjectCursor;
import com.google.common.collect.Lists;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.RoutingNodes;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.allocation.decider.ClusterRebalanceAllocationDecider;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.test.ElasticsearchAllocationTestCase;
import org.hamcrest.Matcher;
import org.hamcrest.Matchers;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;

import static org.elasticsearch.cluster.routing.ShardRoutingState.INITIALIZING;
import static org.elasticsearch.cluster.routing.ShardRoutingState.STARTED;
import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;

public class AddIncrementallyTests extends ElasticsearchAllocationTestCase {
    private final ESLogger logger = Loggers.getLogger(AddIncrementallyTests.class);

    @Test
    public void testAddNodesAndIndices() {
        ImmutableSettings.Builder settings = settingsBuilder();
        settings.put(ClusterRebalanceAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ALLOW_REBALANCE, ClusterRebalanceAllocationDecider.ClusterRebalanceType.ALWAYS.toString());
        AllocationService service = createAllocationService(settings.build());

        ClusterState clusterState = initCluster(service, 1, 3, 3, 1);
        assertThat(clusterState.routingNodes().node("node0").shardsWithState(STARTED).size(), Matchers.equalTo(9));
        assertThat(clusterState.routingNodes().unassigned().size(), Matchers.equalTo(9));
        int nodeOffset = 1;
        clusterState = addNodes(clusterState, service, 1, nodeOffset++);
        assertThat(clusterState.routingNodes().node("node0").shardsWithState(STARTED).size(), Matchers.equalTo(9));
        assertThat(clusterState.routingNodes().node("node1").shardsWithState(STARTED).size(), Matchers.equalTo(9));
        assertThat(clusterState.routingNodes().unassigned().size(), Matchers.equalTo(0));
        assertNumIndexShardsPerNode(clusterState, Matchers.equalTo(3));
        clusterState = addNodes(clusterState, service, 1, nodeOffset++);
        assertNumIndexShardsPerNode(clusterState, Matchers.equalTo(2));
        clusterState = addNodes(clusterState, service, 1, nodeOffset++);
        assertNumIndexShardsPerNode(clusterState, Matchers.lessThanOrEqualTo(2));
        assertAtLeastOneIndexShardPerNode(clusterState);
        clusterState = removeNodes(clusterState, service, 1);
        assertNumIndexShardsPerNode(clusterState, Matchers.equalTo(2));

        clusterState = addIndex(clusterState, service, 3, 2, 3);
        assertThat(clusterState.routingNodes().unassigned().size(), Matchers.equalTo(2));
        assertNumIndexShardsPerNode(clusterState, "test3", Matchers.equalTo(2));
        assertNumIndexShardsPerNode(clusterState, Matchers.lessThanOrEqualTo(2));

        clusterState = addIndex(clusterState, service, 4, 2, 3);
        assertThat(clusterState.routingNodes().unassigned().size(), Matchers.equalTo(4));
        assertNumIndexShardsPerNode(clusterState, "test4", Matchers.equalTo(2));
        assertNumIndexShardsPerNode(clusterState, Matchers.lessThanOrEqualTo(2));
        clusterState = addNodes(clusterState, service, 1, nodeOffset++);
        assertNumIndexShardsPerNode(clusterState, Matchers.lessThanOrEqualTo(2));
        assertThat(clusterState.routingNodes().unassigned().size(), Matchers.equalTo(0));
        clusterState = removeNodes(clusterState, service, 1);
        assertThat(clusterState.routingNodes().unassigned().size(), Matchers.equalTo(4));
        assertNumIndexShardsPerNode(clusterState, Matchers.lessThanOrEqualTo(2));
        clusterState = addNodes(clusterState, service, 1, nodeOffset++);
        assertNumIndexShardsPerNode(clusterState, Matchers.lessThanOrEqualTo(2));
        assertThat(clusterState.routingNodes().unassigned().size(), Matchers.equalTo(0));
        logger.debug("ClusterState: {}", clusterState.getRoutingNodes().prettyPrint());
    }

    @Test
    public void testMinimalRelocations() {
        ImmutableSettings.Builder settings = settingsBuilder();
        settings.put(ClusterRebalanceAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ALLOW_REBALANCE, ClusterRebalanceAllocationDecider.ClusterRebalanceType.ALWAYS.toString())
                .put("cluster.routing.allocation.node_concurrent_recoveries", 2);
        AllocationService service = createAllocationService(settings.build());

        ClusterState clusterState = initCluster(service, 1, 3, 3, 1);
        assertThat(clusterState.routingNodes().node("node0").shardsWithState(STARTED).size(), Matchers.equalTo(9));
        assertThat(clusterState.routingNodes().unassigned().size(), Matchers.equalTo(9));
        int nodeOffset = 1;
        clusterState = addNodes(clusterState, service, 1, nodeOffset++);
        assertThat(clusterState.routingNodes().node("node0").shardsWithState(STARTED).size(), Matchers.equalTo(9));
        assertThat(clusterState.routingNodes().node("node1").shardsWithState(STARTED).size(), Matchers.equalTo(9));
        assertThat(clusterState.routingNodes().unassigned().size(), Matchers.equalTo(0));
        assertNumIndexShardsPerNode(clusterState, Matchers.equalTo(3));

        logger.info("now, start one more node, check that rebalancing will happen because we set it to always");
        DiscoveryNodes.Builder nodes = DiscoveryNodes.builder(clusterState.nodes());
        nodes.put(newNode("node2"));
        clusterState = ClusterState.builder(clusterState).nodes(nodes.build()).build();

        RoutingTable routingTable = service.reroute(clusterState).routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();
        RoutingNodes routingNodes = clusterState.routingNodes();

        assertThat(clusterState.routingNodes().node("node2").shardsWithState(INITIALIZING).size(), Matchers.equalTo(2));
        assertThat(clusterState.routingNodes().node("node0").shardsWithState(INITIALIZING).size(), Matchers.equalTo(0));
        assertThat(clusterState.routingNodes().node("node1").shardsWithState(INITIALIZING).size(), Matchers.equalTo(0));

        RoutingTable prev = routingTable;
        routingTable = service.applyStartedShards(clusterState, routingNodes.shardsWithState(INITIALIZING)).routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();
        routingNodes = clusterState.routingNodes();
        assertThat(prev, Matchers.not(Matchers.sameInstance(routingTable)));
        assertThat(clusterState.routingNodes().node("node2").shardsWithState(STARTED).size(), Matchers.equalTo(2));
        assertThat(clusterState.routingNodes().node("node2").shardsWithState(INITIALIZING).size(), Matchers.equalTo(2));
        assertThat(clusterState.routingNodes().node("node0").shardsWithState(INITIALIZING).size(), Matchers.equalTo(0));
        assertThat(clusterState.routingNodes().node("node1").shardsWithState(INITIALIZING).size(), Matchers.equalTo(0));
        assertThat(prev, Matchers.not(Matchers.sameInstance(routingTable)));

        prev = routingTable;
        routingTable = service.applyStartedShards(clusterState, routingNodes.shardsWithState(INITIALIZING)).routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();
        routingNodes = clusterState.routingNodes();
        assertThat(clusterState.routingNodes().node("node2").shardsWithState(STARTED).size(), Matchers.equalTo(4));
        assertThat(clusterState.routingNodes().node("node2").shardsWithState(INITIALIZING).size(), Matchers.equalTo(2));
        assertThat(clusterState.routingNodes().node("node0").shardsWithState(INITIALIZING).size(), Matchers.equalTo(0));
        assertThat(clusterState.routingNodes().node("node1").shardsWithState(INITIALIZING).size(), Matchers.equalTo(0));
        assertThat(prev, Matchers.not(Matchers.sameInstance(routingTable)));

        prev = routingTable;
        routingTable = service.applyStartedShards(clusterState, routingNodes.shardsWithState(INITIALIZING)).routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();
        routingNodes = clusterState.routingNodes();
        assertThat(clusterState.routingNodes().node("node2").shardsWithState(STARTED).size(), Matchers.equalTo(6));
        assertThat(clusterState.routingNodes().node("node2").shardsWithState(INITIALIZING).size(), Matchers.equalTo(0));
        assertThat(clusterState.routingNodes().node("node0").shardsWithState(INITIALIZING).size(), Matchers.equalTo(0));
        assertThat(clusterState.routingNodes().node("node1").shardsWithState(INITIALIZING).size(), Matchers.equalTo(0));
        assertThat(prev, Matchers.not(Matchers.sameInstance(routingTable)));

        prev = routingTable;
        routingTable = service.applyStartedShards(clusterState, routingNodes.shardsWithState(INITIALIZING)).routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();
        routingNodes = clusterState.routingNodes();
        assertThat(prev, Matchers.sameInstance(routingTable));
        assertNumIndexShardsPerNode(clusterState, Matchers.equalTo(2));
        logger.debug("ClusterState: {}", clusterState.getRoutingNodes().prettyPrint());
    }

    @Test
    public void testMinimalRelocationsNoLimit() {
        ImmutableSettings.Builder settings = settingsBuilder();
        settings.put(ClusterRebalanceAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ALLOW_REBALANCE, ClusterRebalanceAllocationDecider.ClusterRebalanceType.ALWAYS.toString())
                .put("cluster.routing.allocation.node_concurrent_recoveries", 100)
                .put("cluster.routing.allocation.node_initial_primaries_recoveries", 100);
        AllocationService service = createAllocationService(settings.build());

        ClusterState clusterState = initCluster(service, 1, 3, 3, 1);
        assertThat(clusterState.routingNodes().node("node0").shardsWithState(STARTED).size(), Matchers.equalTo(9));
        assertThat(clusterState.routingNodes().unassigned().size(), Matchers.equalTo(9));
        int nodeOffset = 1;
        clusterState = addNodes(clusterState, service, 1, nodeOffset++);
        assertThat(clusterState.routingNodes().node("node0").shardsWithState(STARTED).size(), Matchers.equalTo(9));
        assertThat(clusterState.routingNodes().node("node1").shardsWithState(STARTED).size(), Matchers.equalTo(9));
        assertThat(clusterState.routingNodes().unassigned().size(), Matchers.equalTo(0));
        assertNumIndexShardsPerNode(clusterState, Matchers.equalTo(3));

        logger.info("now, start one more node, check that rebalancing will happen because we set it to always");
        DiscoveryNodes.Builder nodes = DiscoveryNodes.builder(clusterState.nodes());
        nodes.put(newNode("node2"));
        clusterState = ClusterState.builder(clusterState).nodes(nodes.build()).build();

        RoutingTable routingTable = service.reroute(clusterState).routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();
        RoutingNodes routingNodes = clusterState.routingNodes();

        assertThat(clusterState.routingNodes().node("node2").shardsWithState(INITIALIZING).size(), Matchers.equalTo(2));
        assertThat(clusterState.routingNodes().node("node0").shardsWithState(INITIALIZING).size(), Matchers.equalTo(0));
        assertThat(clusterState.routingNodes().node("node1").shardsWithState(INITIALIZING).size(), Matchers.equalTo(0));

        RoutingTable prev = routingTable;
        routingTable = service.applyStartedShards(clusterState, routingNodes.shardsWithState(INITIALIZING)).routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();
        routingNodes = clusterState.routingNodes();
        assertThat(prev, Matchers.not(Matchers.sameInstance(routingTable)));
        assertThat(clusterState.routingNodes().node("node2").shardsWithState(STARTED).size(), Matchers.equalTo(2));
        assertThat(clusterState.routingNodes().node("node2").shardsWithState(INITIALIZING).size(), Matchers.equalTo(2));
        assertThat(clusterState.routingNodes().node("node0").shardsWithState(INITIALIZING).size(), Matchers.equalTo(0));
        assertThat(clusterState.routingNodes().node("node1").shardsWithState(INITIALIZING).size(), Matchers.equalTo(0));
        assertThat(prev, Matchers.not(Matchers.sameInstance(routingTable)));

        prev = routingTable;
        routingTable = service.applyStartedShards(clusterState, routingNodes.shardsWithState(INITIALIZING)).routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();
        routingNodes = clusterState.routingNodes();
        assertThat(clusterState.routingNodes().node("node2").shardsWithState(STARTED).size(), Matchers.equalTo(4));
        assertThat(clusterState.routingNodes().node("node2").shardsWithState(INITIALIZING).size(), Matchers.equalTo(2));
        assertThat(clusterState.routingNodes().node("node0").shardsWithState(INITIALIZING).size(), Matchers.equalTo(0));
        assertThat(clusterState.routingNodes().node("node1").shardsWithState(INITIALIZING).size(), Matchers.equalTo(0));
        assertThat(prev, Matchers.not(Matchers.sameInstance(routingTable)));

        prev = routingTable;
        routingTable = service.applyStartedShards(clusterState, routingNodes.shardsWithState(INITIALIZING)).routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();
        routingNodes = clusterState.routingNodes();
        assertThat(clusterState.routingNodes().node("node2").shardsWithState(STARTED).size(), Matchers.equalTo(6));
        assertThat(clusterState.routingNodes().node("node2").shardsWithState(INITIALIZING).size(), Matchers.equalTo(0));
        assertThat(clusterState.routingNodes().node("node0").shardsWithState(INITIALIZING).size(), Matchers.equalTo(0));
        assertThat(clusterState.routingNodes().node("node1").shardsWithState(INITIALIZING).size(), Matchers.equalTo(0));
        assertThat(prev, Matchers.not(Matchers.sameInstance(routingTable)));

        prev = routingTable;
        routingTable = service.applyStartedShards(clusterState, routingNodes.shardsWithState(INITIALIZING)).routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();
        routingNodes = clusterState.routingNodes();
        assertThat(prev, Matchers.sameInstance(routingTable));
        assertNumIndexShardsPerNode(clusterState, Matchers.equalTo(2));
        logger.debug("ClusterState: {}", clusterState.getRoutingNodes().prettyPrint());
    }


    private void assertNumIndexShardsPerNode(ClusterState state, Matcher<Integer> matcher) {
        for (String index : state.routingTable().indicesRouting().keySet()) {
            assertNumIndexShardsPerNode(state, index, matcher);
        }
    }

    private void assertNumIndexShardsPerNode(ClusterState state, String index, Matcher<Integer> matcher) {
        for (RoutingNode node : state.routingNodes()) {
            assertThat(node.shardsWithState(index, STARTED).size(), matcher);
        }
    }


    private void assertAtLeastOneIndexShardPerNode(ClusterState state) {
        for (String index : state.routingTable().indicesRouting().keySet()) {

            for (RoutingNode node : state.routingNodes()) {
                assertThat(node.shardsWithState(index, STARTED).size(), Matchers.greaterThanOrEqualTo(1));
            }
        }

    }

    private ClusterState addNodes(ClusterState clusterState, AllocationService service, int numNodes, int nodeOffset) {
        logger.info("now, start [{}] more node, check that rebalancing will happen because we set it to always", numNodes);
        DiscoveryNodes.Builder nodes = DiscoveryNodes.builder(clusterState.nodes());
        for (int i = 0; i < numNodes; i++) {
            nodes.put(newNode("node" + (i + nodeOffset)));
        }

        clusterState = ClusterState.builder(clusterState).nodes(nodes.build()).build();

        RoutingTable routingTable = service.reroute(clusterState).routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();
        RoutingNodes routingNodes = clusterState.routingNodes();

        // move initializing to started

        RoutingTable prev = routingTable;
        while (true) {
            logger.debug("ClusterState: {}", clusterState.getRoutingNodes().prettyPrint());
            routingTable = service.applyStartedShards(clusterState, routingNodes.shardsWithState(INITIALIZING)).routingTable();
            clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();
            routingNodes = clusterState.routingNodes();
            if (routingTable == prev)
                break;
            prev = routingTable;
        }

        return clusterState;
    }

    private ClusterState initCluster(AllocationService service, int numberOfNodes, int numberOfIndices, int numberOfShards,
                                     int numberOfReplicas) {
        MetaData.Builder metaDataBuilder = MetaData.builder();
        RoutingTable.Builder routingTableBuilder = RoutingTable.builder();

        for (int i = 0; i < numberOfIndices; i++) {
            IndexMetaData.Builder index = IndexMetaData.builder("test" + i).settings(settings(Version.CURRENT)).numberOfShards(numberOfShards).numberOfReplicas(
                    numberOfReplicas);
            metaDataBuilder = metaDataBuilder.put(index);
        }

        MetaData metaData = metaDataBuilder.build();

        for (ObjectCursor<IndexMetaData> cursor : metaData.indices().values()) {
            routingTableBuilder.addAsNew(cursor.value);
        }

        RoutingTable routingTable = routingTableBuilder.build();

        logger.info("start " + numberOfNodes + " nodes");
        DiscoveryNodes.Builder nodes = DiscoveryNodes.builder();
        for (int i = 0; i < numberOfNodes; i++) {
            nodes.put(newNode("node" + i));
        }
        ClusterState clusterState = ClusterState.builder(org.elasticsearch.cluster.ClusterName.DEFAULT).nodes(nodes).metaData(metaData).routingTable(routingTable).build();
        routingTable = service.reroute(clusterState).routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();
        RoutingNodes routingNodes = clusterState.routingNodes();

        logger.info("restart all the primary shards, replicas will start initializing");
        routingNodes = clusterState.routingNodes();
        routingTable = service.applyStartedShards(clusterState, routingNodes.shardsWithState(INITIALIZING)).routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();
        routingNodes = clusterState.routingNodes();

        logger.info("start the replica shards");
        routingNodes = clusterState.routingNodes();
        routingTable = service.applyStartedShards(clusterState, routingNodes.shardsWithState(INITIALIZING)).routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();
        routingNodes = clusterState.routingNodes();

        logger.info("complete rebalancing");
        RoutingTable prev = routingTable;
        while (true) {
            logger.debug("ClusterState: {}", clusterState.getRoutingNodes().prettyPrint());
            routingTable = service.applyStartedShards(clusterState, routingNodes.shardsWithState(INITIALIZING)).routingTable();
            clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();
            routingNodes = clusterState.routingNodes();
            if (routingTable == prev)
                break;
            prev = routingTable;
        }

        return clusterState;
    }

    private ClusterState addIndex(ClusterState clusterState, AllocationService service, int indexOrdinal, int numberOfShards,
                                  int numberOfReplicas) {
        MetaData.Builder metaDataBuilder = MetaData.builder(clusterState.getMetaData());
        RoutingTable.Builder routingTableBuilder = RoutingTable.builder(clusterState.routingTable());

        IndexMetaData.Builder index = IndexMetaData.builder("test" + indexOrdinal).settings(settings(Version.CURRENT)).numberOfShards(numberOfShards).numberOfReplicas(
                numberOfReplicas);
        IndexMetaData imd = index.build();
        metaDataBuilder = metaDataBuilder.put(imd, true);
        routingTableBuilder.addAsNew(imd);

        MetaData metaData = metaDataBuilder.build();
        RoutingTable routingTable = routingTableBuilder.build();
        clusterState = ClusterState.builder(clusterState).metaData(metaData).routingTable(routingTable).build();
        routingTable = service.reroute(clusterState).routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();
        RoutingNodes routingNodes = clusterState.routingNodes();

        logger.info("restart all the primary shards, replicas will start initializing");
        routingNodes = clusterState.routingNodes();
        routingTable = service.applyStartedShards(clusterState, routingNodes.shardsWithState(INITIALIZING)).routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();
        routingNodes = clusterState.routingNodes();

        logger.info("start the replica shards");
        routingNodes = clusterState.routingNodes();
        routingTable = service.applyStartedShards(clusterState, routingNodes.shardsWithState(INITIALIZING)).routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();
        routingNodes = clusterState.routingNodes();

        logger.info("complete rebalancing");
        RoutingTable prev = routingTable;
        while (true) {
            logger.debug("ClusterState: {}", clusterState.getRoutingNodes().prettyPrint());
            routingTable = service.applyStartedShards(clusterState, routingNodes.shardsWithState(INITIALIZING)).routingTable();
            clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();
            routingNodes = clusterState.routingNodes();
            if (routingTable == prev)
                break;
            prev = routingTable;
        }

        return clusterState;
    }

    private ClusterState removeNodes(ClusterState clusterState, AllocationService service, int numNodes) {
        logger.info("Removing [{}] nodes", numNodes);
        DiscoveryNodes.Builder nodes = DiscoveryNodes.builder(clusterState.nodes());
        ArrayList<DiscoveryNode> discoveryNodes = Lists.newArrayList(clusterState.nodes());
        Collections.shuffle(discoveryNodes, getRandom());
        for (DiscoveryNode node : discoveryNodes) {
            nodes.remove(node.id());
            numNodes--;
            if (numNodes <= 0) {
                break;
            }
        }

        clusterState = ClusterState.builder(clusterState).nodes(nodes.build()).build();
        RoutingNodes routingNodes = clusterState.routingNodes();

        logger.info("start all the primary shards, replicas will start initializing");
        RoutingTable routingTable = service.applyStartedShards(clusterState, routingNodes.shardsWithState(INITIALIZING)).routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();
        routingNodes = clusterState.routingNodes();

        logger.info("start the replica shards");
        routingTable = service.applyStartedShards(clusterState, routingNodes.shardsWithState(INITIALIZING)).routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();
        routingNodes = clusterState.routingNodes();

        logger.info("rebalancing");
        routingTable = service.reroute(clusterState).routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();
        routingNodes = clusterState.routingNodes();

        logger.info("complete rebalancing");
        RoutingTable prev = routingTable;
        while (true) {
            logger.debug("ClusterState: {}", clusterState.getRoutingNodes().prettyPrint());
            routingTable = service.applyStartedShards(clusterState, routingNodes.shardsWithState(INITIALIZING)).routingTable();
            clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();
            routingNodes = clusterState.routingNodes();
            if (routingTable == prev)
                break;
            prev = routingTable;
        }

        return clusterState;
    }
}
