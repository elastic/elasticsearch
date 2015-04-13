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
import org.elasticsearch.cluster.routing.MutableShardRouting;
import org.elasticsearch.cluster.routing.RoutingNodes;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.allocation.decider.ShardsLimitAllocationDecider;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.test.ElasticsearchAllocationTestCase;
import org.junit.Test;

import static org.elasticsearch.cluster.routing.ShardRoutingState.*;
import static org.elasticsearch.cluster.routing.allocation.RoutingNodesUtils.numberOfShardsOfType;
import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;
import static org.hamcrest.Matchers.equalTo;

/**
 */
public class ShardsLimitAllocationTests extends ElasticsearchAllocationTestCase {

    private final ESLogger logger = Loggers.getLogger(ShardsLimitAllocationTests.class);

    @Test
    public void indexLevelShardsLimitAllocate() {
        AllocationService strategy = createAllocationService(settingsBuilder().put("cluster.routing.allocation.concurrent_recoveries", 10).build());

        logger.info("Building initial routing table");

        MetaData metaData = MetaData.builder()
                .put(IndexMetaData.builder("test").settings(settings(Version.CURRENT)
                        .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 4)
                        .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 1)
                        .put(ShardsLimitAllocationDecider.INDEX_TOTAL_SHARDS_PER_NODE, 2)))
                .build();

        RoutingTable routingTable = RoutingTable.builder()
                .addAsNew(metaData.index("test"))
                .build();

        ClusterState clusterState = ClusterState.builder(org.elasticsearch.cluster.ClusterName.DEFAULT).metaData(metaData).routingTable(routingTable).build();
        logger.info("Adding two nodes and performing rerouting");
        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder().put(newNode("node1")).put(newNode("node2"))).build();
        routingTable = strategy.reroute(clusterState).routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();

        assertThat(clusterState.readOnlyRoutingNodes().node("node1").numberOfShardsWithState(ShardRoutingState.INITIALIZING), equalTo(2));
        assertThat(clusterState.readOnlyRoutingNodes().node("node2").numberOfShardsWithState(ShardRoutingState.INITIALIZING), equalTo(2));

        logger.info("Start the primary shards");
        RoutingNodes routingNodes = clusterState.routingNodes();
        routingTable = strategy.applyStartedShards(clusterState, routingNodes.shardsWithState(INITIALIZING)).routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();

        assertThat(clusterState.readOnlyRoutingNodes().node("node1").numberOfShardsWithState(ShardRoutingState.STARTED), equalTo(2));
        assertThat(clusterState.readOnlyRoutingNodes().node("node1").numberOfShardsWithState(ShardRoutingState.INITIALIZING), equalTo(0));
        assertThat(clusterState.readOnlyRoutingNodes().node("node2").numberOfShardsWithState(ShardRoutingState.STARTED), equalTo(2));
        assertThat(clusterState.readOnlyRoutingNodes().node("node2").numberOfShardsWithState(ShardRoutingState.INITIALIZING), equalTo(0));
        assertThat(clusterState.readOnlyRoutingNodes().unassigned().size(), equalTo(4));

        logger.info("Do another reroute, make sure its still not allocated");
        routingNodes = clusterState.routingNodes();
        routingTable = strategy.applyStartedShards(clusterState, routingNodes.shardsWithState(INITIALIZING)).routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();
    }

    @Test
    public void indexLevelShardsLimitRemain() {
        AllocationService strategy = createAllocationService(settingsBuilder()
                .put("cluster.routing.allocation.concurrent_recoveries", 10)
                .put("cluster.routing.allocation.node_initial_primaries_recoveries", 10)
                .put("cluster.routing.allocation.cluster_concurrent_rebalance", -1)
                .put("cluster.routing.allocation.balance.index", 0.0f)
                .put("cluster.routing.allocation.balance.replica", 1.0f)
                .put("cluster.routing.allocation.balance.primary", 0.0f)
                .build());

        logger.info("Building initial routing table");

        MetaData metaData = MetaData.builder()
                .put(IndexMetaData.builder("test").settings(settings(Version.CURRENT)
                        .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 5)
                        .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 0)
                ))
                .build();

        RoutingTable routingTable = RoutingTable.builder()
                .addAsNew(metaData.index("test"))
                .build();

        ClusterState clusterState = ClusterState.builder(org.elasticsearch.cluster.ClusterName.DEFAULT).metaData(metaData).routingTable(routingTable).build();
        logger.info("Adding one node and reroute");
        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder().put(newNode("node1"))).build();
        routingTable = strategy.reroute(clusterState).routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();

        logger.info("Start the primary shards");
        RoutingNodes routingNodes = clusterState.routingNodes();
        routingTable = strategy.applyStartedShards(clusterState, routingNodes.shardsWithState(INITIALIZING)).routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();

        assertThat(numberOfShardsOfType(clusterState.readOnlyRoutingNodes(), STARTED), equalTo(5));

        logger.info("add another index with 5 shards");
        metaData = MetaData.builder(metaData)
                .put(IndexMetaData.builder("test1").settings(settings(Version.CURRENT)
                        .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 5)
                        .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 0)
                ))
                .build();
        routingTable = RoutingTable.builder(routingTable)
                .addAsNew(metaData.index("test1"))
                .build();

        clusterState = ClusterState.builder(clusterState).metaData(metaData).routingTable(routingTable).build();

        logger.info("Add another one node and reroute");
        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder(clusterState.nodes()).put(newNode("node2"))).build();
        routingTable = strategy.reroute(clusterState).routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();

        routingNodes = clusterState.routingNodes();
        routingTable = strategy.applyStartedShards(clusterState, routingNodes.shardsWithState(INITIALIZING)).routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();

        assertThat(numberOfShardsOfType(clusterState.readOnlyRoutingNodes(), STARTED), equalTo(10));

        for (MutableShardRouting shardRouting : clusterState.readOnlyRoutingNodes().node("node1")) {
            assertThat(shardRouting.index(), equalTo("test"));
        }
        for (MutableShardRouting shardRouting : clusterState.readOnlyRoutingNodes().node("node2")) {
            assertThat(shardRouting.index(), equalTo("test1"));
        }

        logger.info("update " + ShardsLimitAllocationDecider.INDEX_TOTAL_SHARDS_PER_NODE + " for test, see that things move");
        metaData = MetaData.builder(metaData)
                .put(IndexMetaData.builder("test").settings(settings(Version.CURRENT)
                        .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 5)
                        .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 0)
                        .put(ShardsLimitAllocationDecider.INDEX_TOTAL_SHARDS_PER_NODE, 3)
                ))
                .build();


        clusterState = ClusterState.builder(clusterState).metaData(metaData).build();

        logger.info("reroute after setting");
        routingTable = strategy.reroute(clusterState).routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();

        assertThat(clusterState.readOnlyRoutingNodes().node("node1").numberOfShardsWithState(STARTED), equalTo(3));
        assertThat(clusterState.readOnlyRoutingNodes().node("node1").numberOfShardsWithState(RELOCATING), equalTo(2));
        assertThat(clusterState.readOnlyRoutingNodes().node("node2").numberOfShardsWithState(RELOCATING), equalTo(2));
        assertThat(clusterState.readOnlyRoutingNodes().node("node2").numberOfShardsWithState(STARTED), equalTo(3));
        // the first move will destroy the balance and the balancer will move 2 shards from node2 to node one right after
        // moving the nodes to node2 since we consider INITIALIZING nodes during rebalance
        routingNodes = clusterState.routingNodes();
        routingTable = strategy.applyStartedShards(clusterState, routingNodes.shardsWithState(INITIALIZING)).routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();
        // now we are done compared to EvenShardCountAllocator since the Balancer is not soely based on the average 
        assertThat(clusterState.readOnlyRoutingNodes().node("node1").numberOfShardsWithState(STARTED), equalTo(5));
        assertThat(clusterState.readOnlyRoutingNodes().node("node2").numberOfShardsWithState(STARTED), equalTo(5));
    }
}
