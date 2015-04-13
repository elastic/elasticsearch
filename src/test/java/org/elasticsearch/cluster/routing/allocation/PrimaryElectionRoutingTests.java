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
import org.elasticsearch.cluster.routing.RoutingNodes;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.test.ElasticsearchAllocationTestCase;
import org.junit.Test;

import static org.elasticsearch.cluster.routing.ShardRoutingState.INITIALIZING;
import static org.elasticsearch.cluster.routing.ShardRoutingState.STARTED;
import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

/**
 *
 */
public class PrimaryElectionRoutingTests extends ElasticsearchAllocationTestCase {

    private final ESLogger logger = Loggers.getLogger(PrimaryElectionRoutingTests.class);

    @Test
    public void testBackupElectionToPrimaryWhenPrimaryCanBeAllocatedToAnotherNode() {
        AllocationService strategy = createAllocationService(settingsBuilder().put("cluster.routing.allocation.concurrent_recoveries", 10).build());

        logger.info("Building initial routing table");

        MetaData metaData = MetaData.builder()
                .put(IndexMetaData.builder("test").settings(settings(Version.CURRENT)).numberOfShards(1).numberOfReplicas(1))
                .build();

        RoutingTable routingTable = RoutingTable.builder()
                .addAsNew(metaData.index("test"))
                .build();

        ClusterState clusterState = ClusterState.builder(org.elasticsearch.cluster.ClusterName.DEFAULT).metaData(metaData).routingTable(routingTable).build();

        logger.info("Adding two nodes and performing rerouting");
        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder().put(newNode("node1")).put(newNode("node2"))).build();
        RoutingTable prevRoutingTable = routingTable;
        routingTable = strategy.reroute(clusterState).routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();

        logger.info("Start the primary shard (on node1)");
        RoutingNodes routingNodes = clusterState.routingNodes();
        prevRoutingTable = routingTable;
        routingTable = strategy.applyStartedShards(clusterState, routingNodes.node("node1").shardsWithState(INITIALIZING)).routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();

        logger.info("Start the backup shard (on node2)");
        routingNodes = clusterState.routingNodes();
        prevRoutingTable = routingTable;
        routingTable = strategy.applyStartedShards(clusterState, routingNodes.node("node2").shardsWithState(INITIALIZING)).routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();

        logger.info("Adding third node and reroute and kill first node");
        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder(clusterState.nodes()).put(newNode("node3")).remove("node1")).build();
        prevRoutingTable = routingTable;
        routingTable = strategy.reroute(clusterState).routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();
        routingNodes = clusterState.routingNodes();

        assertThat(prevRoutingTable != routingTable, equalTo(true));
        assertThat(routingTable.index("test").shards().size(), equalTo(1));
        assertThat(routingNodes.node("node1"), nullValue());
        assertThat(routingNodes.node("node2").numberOfShardsWithState(STARTED), equalTo(1));
        assertThat(routingNodes.node("node3").numberOfShardsWithState(INITIALIZING), equalTo(1));
        // verify where the primary is
        assertThat(routingTable.index("test").shard(0).primaryShard().currentNodeId(), equalTo("node2"));
        assertThat(routingTable.index("test").shard(0).replicaShards().get(0).currentNodeId(), equalTo("node3"));
    }

    @Test
    public void testRemovingInitializingReplicasIfPrimariesFails() {
        AllocationService allocation = createAllocationService(settingsBuilder().put("cluster.routing.allocation.concurrent_recoveries", 10).build());

        logger.info("Building initial routing table");

        MetaData metaData = MetaData.builder()
                .put(IndexMetaData.builder("test").settings(settings(Version.CURRENT)).numberOfShards(2).numberOfReplicas(1))
                .build();

        RoutingTable routingTable = RoutingTable.builder()
                .addAsNew(metaData.index("test"))
                .build();

        ClusterState clusterState = ClusterState.builder(org.elasticsearch.cluster.ClusterName.DEFAULT).metaData(metaData).routingTable(routingTable).build();

        logger.info("Adding two nodes and performing rerouting");
        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder().put(newNode("node1")).put(newNode("node2"))).build();
        RoutingAllocation.Result rerouteResult = allocation.reroute(clusterState);
        clusterState = ClusterState.builder(clusterState).routingTable(rerouteResult.routingTable()).build();

        logger.info("Start the primary shards");
        RoutingNodes routingNodes = clusterState.routingNodes();
        rerouteResult = allocation.applyStartedShards(clusterState, routingNodes.shardsWithState(INITIALIZING));
        clusterState = ClusterState.builder(clusterState).routingTable(rerouteResult.routingTable()).build();
        routingNodes = clusterState.routingNodes();

        assertThat(routingNodes.shardsWithState(STARTED).size(), equalTo(2));
        assertThat(routingNodes.shardsWithState(INITIALIZING).size(), equalTo(2));

        // now, fail one node, while the replica is initializing, and it also holds a primary
        logger.info("--> fail node with primary");
        String nodeIdToFail = clusterState.routingTable().index("test").shard(0).primaryShard().currentNodeId();
        String nodeIdRemaining = nodeIdToFail.equals("node1") ? "node2" : "node1";
        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder()
                .put(newNode(nodeIdRemaining))
        ).build();
        rerouteResult = allocation.reroute(clusterState);
        clusterState = ClusterState.builder(clusterState).routingTable(rerouteResult.routingTable()).build();
        routingNodes = clusterState.routingNodes();

        assertThat(routingNodes.shardsWithState(STARTED).size(), equalTo(1));
        assertThat(routingNodes.shardsWithState(INITIALIZING).size(), equalTo(1));
        assertThat(routingNodes.node(nodeIdRemaining).shardsWithState(INITIALIZING).get(0).primary(), equalTo(true));

    }
}
