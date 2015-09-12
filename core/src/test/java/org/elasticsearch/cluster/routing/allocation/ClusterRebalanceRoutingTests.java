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
import org.elasticsearch.cluster.routing.allocation.decider.ClusterRebalanceAllocationDecider;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.test.ESAllocationTestCase;
import org.junit.Test;

import static org.elasticsearch.cluster.routing.ShardRoutingState.*;
import static org.elasticsearch.common.settings.Settings.settingsBuilder;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.equalTo;

public class ClusterRebalanceRoutingTests extends ESAllocationTestCase {

    private final ESLogger logger = Loggers.getLogger(ClusterRebalanceRoutingTests.class);

    @Test
    public void testAlways() {
        AllocationService strategy = createAllocationService(settingsBuilder().put(ClusterRebalanceAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ALLOW_REBALANCE,
                ClusterRebalanceAllocationDecider.ClusterRebalanceType.ALWAYS.toString()).build());

        MetaData metaData = MetaData.builder()
                .put(IndexMetaData.builder("test1").settings(settings(Version.CURRENT)).numberOfShards(1).numberOfReplicas(1))
                .put(IndexMetaData.builder("test2").settings(settings(Version.CURRENT)).numberOfShards(1).numberOfReplicas(1))
                .build();

        RoutingTable routingTable = RoutingTable.builder()
                .addAsNew(metaData.index("test1"))
                .addAsNew(metaData.index("test2"))
                .build();

        ClusterState clusterState = ClusterState.builder(org.elasticsearch.cluster.ClusterName.DEFAULT).metaData(metaData).routingTable(routingTable).build();

        logger.info("start two nodes");
        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder().put(newNode("node1")).put(newNode("node2"))).build();
        RoutingTable prevRoutingTable = routingTable;
        routingTable = strategy.reroute(clusterState).routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();

        for (int i = 0; i < routingTable.index("test1").shards().size(); i++) {
            assertThat(routingTable.index("test1").shard(i).shards().size(), equalTo(2));
            assertThat(routingTable.index("test1").shard(i).primaryShard().state(), equalTo(INITIALIZING));
            assertThat(routingTable.index("test1").shard(i).replicaShards().get(0).state(), equalTo(UNASSIGNED));
        }

        for (int i = 0; i < routingTable.index("test2").shards().size(); i++) {
            assertThat(routingTable.index("test2").shard(i).shards().size(), equalTo(2));
            assertThat(routingTable.index("test2").shard(i).primaryShard().state(), equalTo(INITIALIZING));
            assertThat(routingTable.index("test2").shard(i).replicaShards().get(0).state(), equalTo(UNASSIGNED));
        }

        logger.info("start all the primary shards for test1, replicas will start initializing");
        RoutingNodes routingNodes = clusterState.getRoutingNodes();
        prevRoutingTable = routingTable;
        routingTable = strategy.applyStartedShards(clusterState, routingNodes.shardsWithState("test1", INITIALIZING)).routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();
        routingNodes = clusterState.getRoutingNodes();

        for (int i = 0; i < routingTable.index("test1").shards().size(); i++) {
            assertThat(routingTable.index("test1").shard(i).shards().size(), equalTo(2));
//            assertThat(routingTable.index("test1").shard(i).primaryShard().state(), equalTo(STARTED));
            assertThat(routingTable.index("test1").shard(i).replicaShards().get(0).state(), equalTo(INITIALIZING));
        }

        for (int i = 0; i < routingTable.index("test2").shards().size(); i++) {
            assertThat(routingTable.index("test2").shard(i).shards().size(), equalTo(2));
            assertThat(routingTable.index("test2").shard(i).primaryShard().state(), equalTo(INITIALIZING));
            assertThat(routingTable.index("test2").shard(i).replicaShards().get(0).state(), equalTo(UNASSIGNED));
        }

        logger.info("start the test1 replica shards");
        routingNodes = clusterState.getRoutingNodes();
        prevRoutingTable = routingTable;
        routingTable = strategy.applyStartedShards(clusterState, routingNodes.shardsWithState("test1", INITIALIZING)).routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();
        routingNodes = clusterState.getRoutingNodes();

        for (int i = 0; i < routingTable.index("test1").shards().size(); i++) {
            assertThat(routingTable.index("test1").shard(i).shards().size(), equalTo(2));
            assertThat(routingTable.index("test1").shard(i).primaryShard().state(), equalTo(STARTED));
            assertThat(routingTable.index("test1").shard(i).replicaShards().get(0).state(), equalTo(STARTED));
        }

        for (int i = 0; i < routingTable.index("test2").shards().size(); i++) {
            assertThat(routingTable.index("test2").shard(i).shards().size(), equalTo(2));
            assertThat(routingTable.index("test2").shard(i).primaryShard().state(), equalTo(INITIALIZING));
            assertThat(routingTable.index("test2").shard(i).replicaShards().get(0).state(), equalTo(UNASSIGNED));
        }

        logger.info("now, start 1 more node, check that rebalancing will happen (for test1) because we set it to always");
        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder(clusterState.nodes())
                .put(newNode("node3")))
                .build();
        prevRoutingTable = routingTable;
        routingTable = strategy.reroute(clusterState).routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();
        routingNodes = clusterState.getRoutingNodes();

        assertThat(routingNodes.node("node3").size(), equalTo(1));
        assertThat(routingNodes.node("node3").get(0).shardId().index().name(), equalTo("test1"));
    }


    @Test
    public void testClusterPrimariesActive1() {
        AllocationService strategy = createAllocationService(settingsBuilder().put(ClusterRebalanceAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ALLOW_REBALANCE,
                ClusterRebalanceAllocationDecider.ClusterRebalanceType.INDICES_PRIMARIES_ACTIVE.toString()).build());

        MetaData metaData = MetaData.builder()
                .put(IndexMetaData.builder("test1").settings(settings(Version.CURRENT)).numberOfShards(1).numberOfReplicas(1))
                .put(IndexMetaData.builder("test2").settings(settings(Version.CURRENT)).numberOfShards(1).numberOfReplicas(1))
                .build();

        RoutingTable routingTable = RoutingTable.builder()
                .addAsNew(metaData.index("test1"))
                .addAsNew(metaData.index("test2"))
                .build();

        ClusterState clusterState = ClusterState.builder(org.elasticsearch.cluster.ClusterName.DEFAULT).metaData(metaData).routingTable(routingTable).build();

        logger.info("start two nodes");
        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder().put(newNode("node1")).put(newNode("node2"))).build();
        RoutingTable prevRoutingTable = routingTable;
        routingTable = strategy.reroute(clusterState).routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();

        for (int i = 0; i < routingTable.index("test1").shards().size(); i++) {
            assertThat(routingTable.index("test1").shard(i).size(), equalTo(2));
            assertThat(routingTable.index("test1").shard(i).primaryShard().state(), equalTo(INITIALIZING));
            assertThat(routingTable.index("test1").shard(i).replicaShards().get(0).state(), equalTo(UNASSIGNED));
        }

        for (int i = 0; i < routingTable.index("test2").shards().size(); i++) {
            assertThat(routingTable.index("test2").shard(i).size(), equalTo(2));
            assertThat(routingTable.index("test2").shard(i).primaryShard().state(), equalTo(INITIALIZING));
            assertThat(routingTable.index("test2").shard(i).replicaShards().get(0).state(), equalTo(UNASSIGNED));
        }

        logger.info("start all the primary shards for test1, replicas will start initializing");
        RoutingNodes routingNodes = clusterState.getRoutingNodes();
        prevRoutingTable = routingTable;
        routingTable = strategy.applyStartedShards(clusterState, routingNodes.shardsWithState("test1", INITIALIZING)).routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();
        routingNodes = clusterState.getRoutingNodes();

        for (int i = 0; i < routingTable.index("test1").shards().size(); i++) {
            assertThat(routingTable.index("test1").shard(i).size(), equalTo(2));
            assertThat(routingTable.index("test1").shard(i).primaryShard().state(), equalTo(STARTED));
            assertThat(routingTable.index("test1").shard(i).replicaShards().get(0).state(), equalTo(INITIALIZING));
        }

        for (int i = 0; i < routingTable.index("test2").shards().size(); i++) {
            assertThat(routingTable.index("test2").shard(i).size(), equalTo(2));
            assertThat(routingTable.index("test2").shard(i).primaryShard().state(), equalTo(INITIALIZING));
            assertThat(routingTable.index("test2").shard(i).replicaShards().get(0).state(), equalTo(UNASSIGNED));
        }

        logger.info("start the test1 replica shards");
        routingNodes = clusterState.getRoutingNodes();
        prevRoutingTable = routingTable;
        routingTable = strategy.applyStartedShards(clusterState, routingNodes.shardsWithState("test1", INITIALIZING)).routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();
        routingNodes = clusterState.getRoutingNodes();

        for (int i = 0; i < routingTable.index("test1").shards().size(); i++) {
            assertThat(routingTable.index("test1").shard(i).size(), equalTo(2));
            assertThat(routingTable.index("test1").shard(i).primaryShard().state(), equalTo(STARTED));
            assertThat(routingTable.index("test1").shard(i).replicaShards().get(0).state(), equalTo(STARTED));
        }

        for (int i = 0; i < routingTable.index("test2").shards().size(); i++) {
            assertThat(routingTable.index("test2").shard(i).size(), equalTo(2));
            assertThat(routingTable.index("test2").shard(i).primaryShard().state(), equalTo(INITIALIZING));
            assertThat(routingTable.index("test2").shard(i).replicaShards().get(0).state(), equalTo(UNASSIGNED));
        }

        logger.info("start all the primary shards for test2, replicas will start initializing");
        routingNodes = clusterState.getRoutingNodes();
        prevRoutingTable = routingTable;
        routingTable = strategy.applyStartedShards(clusterState, routingNodes.shardsWithState("test2", INITIALIZING)).routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();
        routingNodes = clusterState.getRoutingNodes();

        for (int i = 0; i < routingTable.index("test1").shards().size(); i++) {
            assertThat(routingTable.index("test1").shard(i).size(), equalTo(2));
            assertThat(routingTable.index("test1").shard(i).primaryShard().state(), equalTo(STARTED));
            assertThat(routingTable.index("test1").shard(i).replicaShards().get(0).state(), equalTo(STARTED));
        }

        for (int i = 0; i < routingTable.index("test2").shards().size(); i++) {
            assertThat(routingTable.index("test2").shard(i).size(), equalTo(2));
            assertThat(routingTable.index("test2").shard(i).primaryShard().state(), equalTo(STARTED));
            assertThat(routingTable.index("test2").shard(i).replicaShards().get(0).state(), equalTo(INITIALIZING));
        }

        logger.info("now, start 1 more node, check that rebalancing happen (for test1) because we set it to primaries_active");
        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder(clusterState.nodes())
                .put(newNode("node3")))
                .build();
        prevRoutingTable = routingTable;
        routingTable = strategy.reroute(clusterState).routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();
        routingNodes = clusterState.getRoutingNodes();

        assertThat(routingNodes.node("node3").size(), equalTo(1));
        assertThat(routingNodes.node("node3").get(0).shardId().index().name(), equalTo("test1"));
    }

    @Test
    public void testClusterPrimariesActive2() {
        AllocationService strategy = createAllocationService(settingsBuilder().put(ClusterRebalanceAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ALLOW_REBALANCE,
                ClusterRebalanceAllocationDecider.ClusterRebalanceType.INDICES_PRIMARIES_ACTIVE.toString()).build());

        MetaData metaData = MetaData.builder()
                .put(IndexMetaData.builder("test1").settings(settings(Version.CURRENT)).numberOfShards(1).numberOfReplicas(1))
                .put(IndexMetaData.builder("test2").settings(settings(Version.CURRENT)).numberOfShards(1).numberOfReplicas(1))
                .build();

        RoutingTable routingTable = RoutingTable.builder()
                .addAsNew(metaData.index("test1"))
                .addAsNew(metaData.index("test2"))
                .build();

        ClusterState clusterState = ClusterState.builder(org.elasticsearch.cluster.ClusterName.DEFAULT).metaData(metaData).routingTable(routingTable).build();

        logger.info("start two nodes");
        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder().put(newNode("node1")).put(newNode("node2"))).build();
        RoutingTable prevRoutingTable = routingTable;
        routingTable = strategy.reroute(clusterState).routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();

        for (int i = 0; i < routingTable.index("test1").shards().size(); i++) {
            assertThat(routingTable.index("test1").shard(i).shards().size(), equalTo(2));
            assertThat(routingTable.index("test1").shard(i).primaryShard().state(), equalTo(INITIALIZING));
            assertThat(routingTable.index("test1").shard(i).replicaShards().get(0).state(), equalTo(UNASSIGNED));
        }

        for (int i = 0; i < routingTable.index("test2").shards().size(); i++) {
            assertThat(routingTable.index("test2").shard(i).shards().size(), equalTo(2));
            assertThat(routingTable.index("test2").shard(i).primaryShard().state(), equalTo(INITIALIZING));
            assertThat(routingTable.index("test2").shard(i).replicaShards().get(0).state(), equalTo(UNASSIGNED));
        }

        logger.info("start all the primary shards for test1, replicas will start initializing");
        RoutingNodes routingNodes = clusterState.getRoutingNodes();
        prevRoutingTable = routingTable;
        routingTable = strategy.applyStartedShards(clusterState, routingNodes.shardsWithState("test1", INITIALIZING)).routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();
        routingNodes = clusterState.getRoutingNodes();

        for (int i = 0; i < routingTable.index("test1").shards().size(); i++) {
            assertThat(routingTable.index("test1").shard(i).shards().size(), equalTo(2));
            assertThat(routingTable.index("test1").shard(i).primaryShard().state(), equalTo(STARTED));
            assertThat(routingTable.index("test1").shard(i).replicaShards().get(0).state(), equalTo(INITIALIZING));
        }

        for (int i = 0; i < routingTable.index("test2").shards().size(); i++) {
            assertThat(routingTable.index("test2").shard(i).shards().size(), equalTo(2));
            assertThat(routingTable.index("test2").shard(i).primaryShard().state(), equalTo(INITIALIZING));
            assertThat(routingTable.index("test2").shard(i).replicaShards().get(0).state(), equalTo(UNASSIGNED));
        }

        logger.info("start the test1 replica shards");
        routingNodes = clusterState.getRoutingNodes();
        prevRoutingTable = routingTable;
        routingTable = strategy.applyStartedShards(clusterState, routingNodes.shardsWithState("test1", INITIALIZING)).routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();
        routingNodes = clusterState.getRoutingNodes();

        for (int i = 0; i < routingTable.index("test1").shards().size(); i++) {
            assertThat(routingTable.index("test1").shard(i).shards().size(), equalTo(2));
            assertThat(routingTable.index("test1").shard(i).primaryShard().state(), equalTo(STARTED));
            assertThat(routingTable.index("test1").shard(i).replicaShards().get(0).state(), equalTo(STARTED));
        }

        for (int i = 0; i < routingTable.index("test2").shards().size(); i++) {
            assertThat(routingTable.index("test2").shard(i).shards().size(), equalTo(2));
            assertThat(routingTable.index("test2").shard(i).primaryShard().state(), equalTo(INITIALIZING));
            assertThat(routingTable.index("test2").shard(i).replicaShards().get(0).state(), equalTo(UNASSIGNED));
        }

        logger.info("now, start 1 more node, check that rebalancing will not happen (for test1) because we set it to primaries_active");
        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder(clusterState.nodes())
                .put(newNode("node3")))
                .build();
        prevRoutingTable = routingTable;
        routingTable = strategy.reroute(clusterState).routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();
        routingNodes = clusterState.getRoutingNodes();

        assertThat(routingNodes.node("node3").isEmpty(), equalTo(true));
    }

    @Test
    public void testClusterAllActive1() {
        AllocationService strategy = createAllocationService(settingsBuilder().put(ClusterRebalanceAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ALLOW_REBALANCE,
                ClusterRebalanceAllocationDecider.ClusterRebalanceType.INDICES_ALL_ACTIVE.toString()).build());

        MetaData metaData = MetaData.builder()
                .put(IndexMetaData.builder("test1").settings(settings(Version.CURRENT)).numberOfShards(1).numberOfReplicas(1))
                .put(IndexMetaData.builder("test2").settings(settings(Version.CURRENT)).numberOfShards(1).numberOfReplicas(1))
                .build();

        RoutingTable routingTable = RoutingTable.builder()
                .addAsNew(metaData.index("test1"))
                .addAsNew(metaData.index("test2"))
                .build();

        ClusterState clusterState = ClusterState.builder(org.elasticsearch.cluster.ClusterName.DEFAULT).metaData(metaData).routingTable(routingTable).build();

        logger.info("start two nodes");
        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder().put(newNode("node1")).put(newNode("node2"))).build();
        RoutingTable prevRoutingTable = routingTable;
        routingTable = strategy.reroute(clusterState).routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();

        for (int i = 0; i < routingTable.index("test1").shards().size(); i++) {
            assertThat(routingTable.index("test1").shard(i).shards().size(), equalTo(2));
            assertThat(routingTable.index("test1").shard(i).primaryShard().state(), equalTo(INITIALIZING));
            assertThat(routingTable.index("test1").shard(i).replicaShards().get(0).state(), equalTo(UNASSIGNED));
        }

        for (int i = 0; i < routingTable.index("test2").shards().size(); i++) {
            assertThat(routingTable.index("test2").shard(i).shards().size(), equalTo(2));
            assertThat(routingTable.index("test2").shard(i).primaryShard().state(), equalTo(INITIALIZING));
            assertThat(routingTable.index("test2").shard(i).replicaShards().get(0).state(), equalTo(UNASSIGNED));
        }

        logger.info("start all the primary shards for test1, replicas will start initializing");
        RoutingNodes routingNodes = clusterState.getRoutingNodes();
        prevRoutingTable = routingTable;
        routingTable = strategy.applyStartedShards(clusterState, routingNodes.shardsWithState("test1", INITIALIZING)).routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();
        routingNodes = clusterState.getRoutingNodes();

        for (int i = 0; i < routingTable.index("test1").shards().size(); i++) {
            assertThat(routingTable.index("test1").shard(i).shards().size(), equalTo(2));
            assertThat(routingTable.index("test1").shard(i).primaryShard().state(), equalTo(STARTED));
            assertThat(routingTable.index("test1").shard(i).replicaShards().get(0).state(), equalTo(INITIALIZING));
        }

        for (int i = 0; i < routingTable.index("test2").shards().size(); i++) {
            assertThat(routingTable.index("test2").shard(i).shards().size(), equalTo(2));
            assertThat(routingTable.index("test2").shard(i).primaryShard().state(), equalTo(INITIALIZING));
            assertThat(routingTable.index("test2").shard(i).replicaShards().get(0).state(), equalTo(UNASSIGNED));
        }

        logger.info("start the test1 replica shards");
        routingNodes = clusterState.getRoutingNodes();
        prevRoutingTable = routingTable;
        routingTable = strategy.applyStartedShards(clusterState, routingNodes.shardsWithState("test1", INITIALIZING)).routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();
        routingNodes = clusterState.getRoutingNodes();

        for (int i = 0; i < routingTable.index("test1").shards().size(); i++) {
            assertThat(routingTable.index("test1").shard(i).shards().size(), equalTo(2));
            assertThat(routingTable.index("test1").shard(i).primaryShard().state(), equalTo(STARTED));
            assertThat(routingTable.index("test1").shard(i).replicaShards().get(0).state(), equalTo(STARTED));
        }

        for (int i = 0; i < routingTable.index("test2").shards().size(); i++) {
            assertThat(routingTable.index("test2").shard(i).shards().size(), equalTo(2));
            assertThat(routingTable.index("test2").shard(i).primaryShard().state(), equalTo(INITIALIZING));
            assertThat(routingTable.index("test2").shard(i).replicaShards().get(0).state(), equalTo(UNASSIGNED));
        }

        logger.info("start all the primary shards for test2, replicas will start initializing");
        routingNodes = clusterState.getRoutingNodes();
        prevRoutingTable = routingTable;
        routingTable = strategy.applyStartedShards(clusterState, routingNodes.shardsWithState("test2", INITIALIZING)).routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();
        routingNodes = clusterState.getRoutingNodes();

        for (int i = 0; i < routingTable.index("test1").shards().size(); i++) {
            assertThat(routingTable.index("test1").shard(i).shards().size(), equalTo(2));
            assertThat(routingTable.index("test1").shard(i).primaryShard().state(), equalTo(STARTED));
            assertThat(routingTable.index("test1").shard(i).replicaShards().get(0).state(), equalTo(STARTED));
        }

        for (int i = 0; i < routingTable.index("test2").shards().size(); i++) {
            assertThat(routingTable.index("test2").shard(i).shards().size(), equalTo(2));
            assertThat(routingTable.index("test2").shard(i).primaryShard().state(), equalTo(STARTED));
            assertThat(routingTable.index("test2").shard(i).replicaShards().get(0).state(), equalTo(INITIALIZING));
        }

        logger.info("start the test2 replica shards");
        routingNodes = clusterState.getRoutingNodes();
        prevRoutingTable = routingTable;
        routingTable = strategy.applyStartedShards(clusterState, routingNodes.shardsWithState("test2", INITIALIZING)).routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();
        routingNodes = clusterState.getRoutingNodes();

        for (int i = 0; i < routingTable.index("test1").shards().size(); i++) {
            assertThat(routingTable.index("test1").shard(i).shards().size(), equalTo(2));
            assertThat(routingTable.index("test1").shard(i).primaryShard().state(), equalTo(STARTED));
            assertThat(routingTable.index("test1").shard(i).replicaShards().get(0).state(), equalTo(STARTED));
        }

        for (int i = 0; i < routingTable.index("test2").shards().size(); i++) {
            assertThat(routingTable.index("test2").shard(i).shards().size(), equalTo(2));
            assertThat(routingTable.index("test2").shard(i).primaryShard().state(), equalTo(STARTED));
            assertThat(routingTable.index("test2").shard(i).replicaShards().get(0).state(), equalTo(STARTED));
        }

        logger.info("now, start 1 more node, check that rebalancing happen (for test1) because we set it to all_active");
        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder(clusterState.nodes())
                .put(newNode("node3")))
                .build();
        prevRoutingTable = routingTable;
        routingTable = strategy.reroute(clusterState).routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();
        routingNodes = clusterState.getRoutingNodes();

        assertThat(routingNodes.node("node3").size(), equalTo(1));
        assertThat(routingNodes.node("node3").get(0).shardId().index().name(), anyOf(equalTo("test1"), equalTo("test2")));
    }

    @Test
    public void testClusterAllActive2() {
        AllocationService strategy = createAllocationService(settingsBuilder().put(ClusterRebalanceAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ALLOW_REBALANCE,
                ClusterRebalanceAllocationDecider.ClusterRebalanceType.INDICES_ALL_ACTIVE.toString()).build());

        MetaData metaData = MetaData.builder()
                .put(IndexMetaData.builder("test1").settings(settings(Version.CURRENT)).numberOfShards(1).numberOfReplicas(1))
                .put(IndexMetaData.builder("test2").settings(settings(Version.CURRENT)).numberOfShards(1).numberOfReplicas(1))
                .build();

        RoutingTable routingTable = RoutingTable.builder()
                .addAsNew(metaData.index("test1"))
                .addAsNew(metaData.index("test2"))
                .build();

        ClusterState clusterState = ClusterState.builder(org.elasticsearch.cluster.ClusterName.DEFAULT).metaData(metaData).routingTable(routingTable).build();

        logger.info("start two nodes");
        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder().put(newNode("node1")).put(newNode("node2"))).build();
        RoutingTable prevRoutingTable = routingTable;
        routingTable = strategy.reroute(clusterState).routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();

        for (int i = 0; i < routingTable.index("test1").shards().size(); i++) {
            assertThat(routingTable.index("test1").shard(i).shards().size(), equalTo(2));
            assertThat(routingTable.index("test1").shard(i).primaryShard().state(), equalTo(INITIALIZING));
            assertThat(routingTable.index("test1").shard(i).replicaShards().get(0).state(), equalTo(UNASSIGNED));
        }

        for (int i = 0; i < routingTable.index("test2").shards().size(); i++) {
            assertThat(routingTable.index("test2").shard(i).shards().size(), equalTo(2));
            assertThat(routingTable.index("test2").shard(i).primaryShard().state(), equalTo(INITIALIZING));
            assertThat(routingTable.index("test2").shard(i).replicaShards().get(0).state(), equalTo(UNASSIGNED));
        }

        logger.info("start all the primary shards for test1, replicas will start initializing");
        RoutingNodes routingNodes = clusterState.getRoutingNodes();
        prevRoutingTable = routingTable;
        routingTable = strategy.applyStartedShards(clusterState, routingNodes.shardsWithState("test1", INITIALIZING)).routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();
        routingNodes = clusterState.getRoutingNodes();

        for (int i = 0; i < routingTable.index("test1").shards().size(); i++) {
            assertThat(routingTable.index("test1").shard(i).shards().size(), equalTo(2));
            assertThat(routingTable.index("test1").shard(i).primaryShard().state(), equalTo(STARTED));
            assertThat(routingTable.index("test1").shard(i).replicaShards().get(0).state(), equalTo(INITIALIZING));
        }

        for (int i = 0; i < routingTable.index("test2").shards().size(); i++) {
            assertThat(routingTable.index("test2").shard(i).shards().size(), equalTo(2));
            assertThat(routingTable.index("test2").shard(i).primaryShard().state(), equalTo(INITIALIZING));
            assertThat(routingTable.index("test2").shard(i).replicaShards().get(0).state(), equalTo(UNASSIGNED));
        }

        logger.info("start the test1 replica shards");
        routingNodes = clusterState.getRoutingNodes();
        prevRoutingTable = routingTable;
        routingTable = strategy.applyStartedShards(clusterState, routingNodes.shardsWithState("test1", INITIALIZING)).routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();
        routingNodes = clusterState.getRoutingNodes();

        for (int i = 0; i < routingTable.index("test1").shards().size(); i++) {
            assertThat(routingTable.index("test1").shard(i).shards().size(), equalTo(2));
            assertThat(routingTable.index("test1").shard(i).primaryShard().state(), equalTo(STARTED));
            assertThat(routingTable.index("test1").shard(i).replicaShards().get(0).state(), equalTo(STARTED));
        }

        for (int i = 0; i < routingTable.index("test2").shards().size(); i++) {
            assertThat(routingTable.index("test2").shard(i).shards().size(), equalTo(2));
            assertThat(routingTable.index("test2").shard(i).primaryShard().state(), equalTo(INITIALIZING));
            assertThat(routingTable.index("test2").shard(i).replicaShards().get(0).state(), equalTo(UNASSIGNED));
        }

        logger.info("now, start 1 more node, check that rebalancing will not happen (for test1) because we set it to all_active");
        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder(clusterState.nodes())
                .put(newNode("node3")))
                .build();
        prevRoutingTable = routingTable;
        routingTable = strategy.reroute(clusterState).routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();
        routingNodes = clusterState.getRoutingNodes();

        assertThat(routingNodes.node("node3").isEmpty(), equalTo(true));
    }

    @Test
    public void testClusterAllActive3() {
        AllocationService strategy = createAllocationService(settingsBuilder().put(ClusterRebalanceAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ALLOW_REBALANCE,
                ClusterRebalanceAllocationDecider.ClusterRebalanceType.INDICES_ALL_ACTIVE.toString()).build());

        MetaData metaData = MetaData.builder()
                .put(IndexMetaData.builder("test1").settings(settings(Version.CURRENT)).numberOfShards(1).numberOfReplicas(1))
                .put(IndexMetaData.builder("test2").settings(settings(Version.CURRENT)).numberOfShards(1).numberOfReplicas(1))
                .build();

        RoutingTable routingTable = RoutingTable.builder()
                .addAsNew(metaData.index("test1"))
                .addAsNew(metaData.index("test2"))
                .build();

        ClusterState clusterState = ClusterState.builder(org.elasticsearch.cluster.ClusterName.DEFAULT).metaData(metaData).routingTable(routingTable).build();

        logger.info("start two nodes");
        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder().put(newNode("node1")).put(newNode("node2"))).build();
        RoutingTable prevRoutingTable = routingTable;
        routingTable = strategy.reroute(clusterState).routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();

        for (int i = 0; i < routingTable.index("test1").shards().size(); i++) {
            assertThat(routingTable.index("test1").shard(i).shards().size(), equalTo(2));
            assertThat(routingTable.index("test1").shard(i).primaryShard().state(), equalTo(INITIALIZING));
            assertThat(routingTable.index("test1").shard(i).replicaShards().get(0).state(), equalTo(UNASSIGNED));
        }

        for (int i = 0; i < routingTable.index("test2").shards().size(); i++) {
            assertThat(routingTable.index("test2").shard(i).shards().size(), equalTo(2));
            assertThat(routingTable.index("test2").shard(i).primaryShard().state(), equalTo(INITIALIZING));
            assertThat(routingTable.index("test2").shard(i).replicaShards().get(0).state(), equalTo(UNASSIGNED));
        }

        logger.info("start all the primary shards for test1, replicas will start initializing");
        RoutingNodes routingNodes = clusterState.getRoutingNodes();
        prevRoutingTable = routingTable;
        routingTable = strategy.applyStartedShards(clusterState, routingNodes.shardsWithState("test1", INITIALIZING)).routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();
        routingNodes = clusterState.getRoutingNodes();

        for (int i = 0; i < routingTable.index("test1").shards().size(); i++) {
            assertThat(routingTable.index("test1").shard(i).shards().size(), equalTo(2));
            assertThat(routingTable.index("test1").shard(i).primaryShard().state(), equalTo(STARTED));
            assertThat(routingTable.index("test1").shard(i).replicaShards().get(0).state(), equalTo(INITIALIZING));
        }

        for (int i = 0; i < routingTable.index("test2").shards().size(); i++) {
            assertThat(routingTable.index("test2").shard(i).shards().size(), equalTo(2));
            assertThat(routingTable.index("test2").shard(i).primaryShard().state(), equalTo(INITIALIZING));
            assertThat(routingTable.index("test2").shard(i).replicaShards().get(0).state(), equalTo(UNASSIGNED));
        }

        logger.info("start the test1 replica shards");
        routingNodes = clusterState.getRoutingNodes();
        prevRoutingTable = routingTable;
        routingTable = strategy.applyStartedShards(clusterState, routingNodes.shardsWithState("test1", INITIALIZING)).routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();
        routingNodes = clusterState.getRoutingNodes();

        for (int i = 0; i < routingTable.index("test1").shards().size(); i++) {
            assertThat(routingTable.index("test1").shard(i).shards().size(), equalTo(2));
            assertThat(routingTable.index("test1").shard(i).primaryShard().state(), equalTo(STARTED));
            assertThat(routingTable.index("test1").shard(i).replicaShards().get(0).state(), equalTo(STARTED));
        }

        for (int i = 0; i < routingTable.index("test2").shards().size(); i++) {
            assertThat(routingTable.index("test2").shard(i).shards().size(), equalTo(2));
            assertThat(routingTable.index("test2").shard(i).primaryShard().state(), equalTo(INITIALIZING));
            assertThat(routingTable.index("test2").shard(i).replicaShards().get(0).state(), equalTo(UNASSIGNED));
        }

        logger.info("start all the primary shards for test2, replicas will start initializing");
        routingNodes = clusterState.getRoutingNodes();
        prevRoutingTable = routingTable;
        routingTable = strategy.applyStartedShards(clusterState, routingNodes.shardsWithState("test2", INITIALIZING)).routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();
        routingNodes = clusterState.getRoutingNodes();

        for (int i = 0; i < routingTable.index("test1").shards().size(); i++) {
            assertThat(routingTable.index("test1").shard(i).shards().size(), equalTo(2));
            assertThat(routingTable.index("test1").shard(i).primaryShard().state(), equalTo(STARTED));
            assertThat(routingTable.index("test1").shard(i).replicaShards().get(0).state(), equalTo(STARTED));
        }

        for (int i = 0; i < routingTable.index("test2").shards().size(); i++) {
            assertThat(routingTable.index("test2").shard(i).shards().size(), equalTo(2));
            assertThat(routingTable.index("test2").shard(i).primaryShard().state(), equalTo(STARTED));
            assertThat(routingTable.index("test2").shard(i).replicaShards().get(0).state(), equalTo(INITIALIZING));
        }

        logger.info("now, start 1 more node, check that rebalancing will not happen (for test1) because we set it to all_active");
        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder(clusterState.nodes())
                .put(newNode("node3")))
                .build();
        prevRoutingTable = routingTable;
        routingTable = strategy.reroute(clusterState).routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();
        routingNodes = clusterState.getRoutingNodes();

        assertThat(routingNodes.node("node3").isEmpty(), equalTo(true));
    }
}