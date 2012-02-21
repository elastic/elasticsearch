/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
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

package org.elasticsearch.test.unit.cluster.routing.allocation;

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.RoutingNodes;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.testng.annotations.Test;

import static org.elasticsearch.cluster.ClusterState.newClusterStateBuilder;
import static org.elasticsearch.cluster.metadata.IndexMetaData.newIndexMetaDataBuilder;
import static org.elasticsearch.cluster.metadata.MetaData.newMetaDataBuilder;
import static org.elasticsearch.cluster.node.DiscoveryNodes.newNodesBuilder;
import static org.elasticsearch.cluster.routing.RoutingBuilders.indexRoutingTable;
import static org.elasticsearch.cluster.routing.RoutingBuilders.routingTable;
import static org.elasticsearch.cluster.routing.ShardRoutingState.INITIALIZING;
import static org.elasticsearch.cluster.routing.ShardRoutingState.STARTED;
import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;
import static org.elasticsearch.test.unit.cluster.routing.allocation.RoutingAllocationTests.newNode;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

/**
 *
 */
public class ElectReplicaAsPrimaryDuringRelocationTests {

    private final ESLogger logger = Loggers.getLogger(ElectReplicaAsPrimaryDuringRelocationTests.class);

    @Test
    public void testElectReplicaAsPrimaryDuringRelocation() {
        AllocationService strategy = new AllocationService(settingsBuilder().put("cluster.routing.allocation.concurrent_recoveries", 10).build());

        logger.info("Building initial routing table");

        MetaData metaData = newMetaDataBuilder()
                .put(newIndexMetaDataBuilder("test").numberOfShards(2).numberOfReplicas(1))
                .build();

        RoutingTable routingTable = routingTable()
                .add(indexRoutingTable("test").initializeEmpty(metaData.index("test")))
                .build();

        ClusterState clusterState = newClusterStateBuilder().metaData(metaData).routingTable(routingTable).build();

        logger.info("Adding two nodes and performing rerouting");
        clusterState = newClusterStateBuilder().state(clusterState).nodes(newNodesBuilder().put(newNode("node1")).put(newNode("node2"))).build();
        RoutingTable prevRoutingTable = routingTable;
        routingTable = strategy.reroute(clusterState).routingTable();
        clusterState = newClusterStateBuilder().state(clusterState).routingTable(routingTable).build();

        logger.info("Start the primary shards");
        RoutingNodes routingNodes = clusterState.routingNodes();
        prevRoutingTable = routingTable;
        routingTable = strategy.applyStartedShards(clusterState, routingNodes.shardsWithState(INITIALIZING)).routingTable();
        clusterState = newClusterStateBuilder().state(clusterState).routingTable(routingTable).build();

        logger.info("Start the replica shards");
        routingNodes = clusterState.routingNodes();
        prevRoutingTable = routingTable;
        routingTable = strategy.applyStartedShards(clusterState, routingNodes.shardsWithState(INITIALIZING)).routingTable();
        clusterState = newClusterStateBuilder().state(clusterState).routingTable(routingTable).build();
        routingNodes = clusterState.routingNodes();

        assertThat(prevRoutingTable != routingTable, equalTo(true));
        assertThat(routingTable.index("test").shards().size(), equalTo(2));
        assertThat(routingNodes.node("node1").numberOfShardsWithState(STARTED), equalTo(2));
        assertThat(routingNodes.node("node2").numberOfShardsWithState(STARTED), equalTo(2));

        logger.info("Start another node and perform rerouting");
        clusterState = newClusterStateBuilder().state(clusterState).nodes(newNodesBuilder().putAll(clusterState.nodes()).put(newNode("node3"))).build();
        prevRoutingTable = routingTable;
        routingTable = strategy.reroute(clusterState).routingTable();
        clusterState = newClusterStateBuilder().state(clusterState).routingTable(routingTable).build();

        logger.info("find the replica shard that gets relocated");
        IndexShardRoutingTable indexShardRoutingTable = null;
        if (routingTable.index("test").shard(0).replicaShards().get(0).relocating()) {
            indexShardRoutingTable = routingTable.index("test").shard(0);
        } else if (routingTable.index("test").shard(1).replicaShards().get(0).relocating()) {
            indexShardRoutingTable = routingTable.index("test").shard(1);
        }

        // we might have primary relocating, and the test is only for replicas, so only test in the case of replica allocation
        if (indexShardRoutingTable != null) {
            logger.info("kill the node [{}] of the primary shard for the relocating replica", indexShardRoutingTable.primaryShard().currentNodeId());
            clusterState = newClusterStateBuilder().state(clusterState).nodes(newNodesBuilder().putAll(clusterState.nodes()).remove(indexShardRoutingTable.primaryShard().currentNodeId())).build();
            prevRoutingTable = routingTable;
            routingTable = strategy.reroute(clusterState).routingTable();
            clusterState = newClusterStateBuilder().state(clusterState).routingTable(routingTable).build();

            logger.info("make sure all the primary shards are active");
            assertThat(routingTable.index("test").shard(0).primaryShard().active(), equalTo(true));
            assertThat(routingTable.index("test").shard(1).primaryShard().active(), equalTo(true));
        }
    }
}
