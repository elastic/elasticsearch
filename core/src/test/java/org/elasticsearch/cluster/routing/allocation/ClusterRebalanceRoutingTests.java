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

import org.apache.logging.log4j.Logger;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.RoutingNodes;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.cluster.routing.allocation.decider.ClusterRebalanceAllocationDecider;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.cluster.ESAllocationTestCase;
import org.elasticsearch.test.gateway.NoopGatewayAllocator;

import java.util.concurrent.atomic.AtomicBoolean;

import static org.elasticsearch.cluster.routing.ShardRoutingState.INITIALIZING;
import static org.elasticsearch.cluster.routing.ShardRoutingState.RELOCATING;
import static org.elasticsearch.cluster.routing.ShardRoutingState.STARTED;
import static org.elasticsearch.cluster.routing.ShardRoutingState.UNASSIGNED;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.equalTo;

public class ClusterRebalanceRoutingTests extends ESAllocationTestCase {
    private final Logger logger = Loggers.getLogger(ClusterRebalanceRoutingTests.class);

    public void testAlways() {
        AllocationService strategy = createAllocationService(Settings.builder().put(ClusterRebalanceAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ALLOW_REBALANCE_SETTING.getKey(),
                ClusterRebalanceAllocationDecider.ClusterRebalanceType.ALWAYS.toString()).build());

        MetaData metaData = MetaData.builder()
                .put(IndexMetaData.builder("test1").settings(settings(Version.CURRENT)).numberOfShards(1).numberOfReplicas(1))
                .put(IndexMetaData.builder("test2").settings(settings(Version.CURRENT)).numberOfShards(1).numberOfReplicas(1))
                .build();

        RoutingTable initialRoutingTable = RoutingTable.builder()
                .addAsNew(metaData.index("test1"))
                .addAsNew(metaData.index("test2"))
                .build();

        ClusterState clusterState = ClusterState.builder(org.elasticsearch.cluster.ClusterName.CLUSTER_NAME_SETTING.getDefault(Settings.EMPTY)).metaData(metaData).routingTable(initialRoutingTable).build();

        logger.info("start two nodes");
        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder().add(newNode("node1")).add(newNode("node2"))).build();
        RoutingAllocation.Result routingResult = strategy.reroute(clusterState, "reroute");
        clusterState = ClusterState.builder(clusterState).routingResult(routingResult).build();

        for (int i = 0; i < clusterState.routingTable().index("test1").shards().size(); i++) {
            assertThat(clusterState.routingTable().index("test1").shard(i).shards().size(), equalTo(2));
            assertThat(clusterState.routingTable().index("test1").shard(i).primaryShard().state(), equalTo(INITIALIZING));
            assertThat(clusterState.routingTable().index("test1").shard(i).replicaShards().get(0).state(), equalTo(UNASSIGNED));
        }

        for (int i = 0; i < clusterState.routingTable().index("test2").shards().size(); i++) {
            assertThat(clusterState.routingTable().index("test2").shard(i).shards().size(), equalTo(2));
            assertThat(clusterState.routingTable().index("test2").shard(i).primaryShard().state(), equalTo(INITIALIZING));
            assertThat(clusterState.routingTable().index("test2").shard(i).replicaShards().get(0).state(), equalTo(UNASSIGNED));
        }

        logger.info("start all the primary shards for test1, replicas will start initializing");
        RoutingNodes routingNodes = clusterState.getRoutingNodes();
        routingResult = strategy.applyStartedShards(clusterState, routingNodes.shardsWithState("test1", INITIALIZING));
        clusterState = ClusterState.builder(clusterState).routingResult(routingResult).build();
        routingNodes = clusterState.getRoutingNodes();

        for (int i = 0; i < clusterState.routingTable().index("test1").shards().size(); i++) {
            assertThat(clusterState.routingTable().index("test1").shard(i).shards().size(), equalTo(2));
//            assertThat(clusterState.routingTable().index("test1").shard(i).primaryShard().state(), equalTo(STARTED));
            assertThat(clusterState.routingTable().index("test1").shard(i).replicaShards().get(0).state(), equalTo(INITIALIZING));
        }

        for (int i = 0; i < clusterState.routingTable().index("test2").shards().size(); i++) {
            assertThat(clusterState.routingTable().index("test2").shard(i).shards().size(), equalTo(2));
            assertThat(clusterState.routingTable().index("test2").shard(i).primaryShard().state(), equalTo(INITIALIZING));
            assertThat(clusterState.routingTable().index("test2").shard(i).replicaShards().get(0).state(), equalTo(UNASSIGNED));
        }

        logger.info("start the test1 replica shards");
        routingNodes = clusterState.getRoutingNodes();
        routingResult = strategy.applyStartedShards(clusterState, routingNodes.shardsWithState("test1", INITIALIZING));
        clusterState = ClusterState.builder(clusterState).routingResult(routingResult).build();
        routingNodes = clusterState.getRoutingNodes();

        for (int i = 0; i < clusterState.routingTable().index("test1").shards().size(); i++) {
            assertThat(clusterState.routingTable().index("test1").shard(i).shards().size(), equalTo(2));
            assertThat(clusterState.routingTable().index("test1").shard(i).primaryShard().state(), equalTo(STARTED));
            assertThat(clusterState.routingTable().index("test1").shard(i).replicaShards().get(0).state(), equalTo(STARTED));
        }

        for (int i = 0; i < clusterState.routingTable().index("test2").shards().size(); i++) {
            assertThat(clusterState.routingTable().index("test2").shard(i).shards().size(), equalTo(2));
            assertThat(clusterState.routingTable().index("test2").shard(i).primaryShard().state(), equalTo(INITIALIZING));
            assertThat(clusterState.routingTable().index("test2").shard(i).replicaShards().get(0).state(), equalTo(UNASSIGNED));
        }

        logger.info("now, start 1 more node, check that rebalancing will happen (for test1) because we set it to always");
        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder(clusterState.nodes())
                .add(newNode("node3")))
                .build();
        routingResult = strategy.reroute(clusterState, "reroute");
        clusterState = ClusterState.builder(clusterState).routingResult(routingResult).build();
        routingNodes = clusterState.getRoutingNodes();

        assertThat(routingNodes.node("node3").size(), equalTo(1));
        assertThat(routingNodes.node("node3").iterator().next().shardId().getIndex().getName(), equalTo("test1"));
    }


    public void testClusterPrimariesActive1() {
        AllocationService strategy = createAllocationService(Settings.builder().put(ClusterRebalanceAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ALLOW_REBALANCE_SETTING.getKey(),
                ClusterRebalanceAllocationDecider.ClusterRebalanceType.INDICES_PRIMARIES_ACTIVE.toString()).build());

        MetaData metaData = MetaData.builder()
                .put(IndexMetaData.builder("test1").settings(settings(Version.CURRENT)).numberOfShards(1).numberOfReplicas(1))
                .put(IndexMetaData.builder("test2").settings(settings(Version.CURRENT)).numberOfShards(1).numberOfReplicas(1))
                .build();

        RoutingTable initialRoutingTable = RoutingTable.builder()
                .addAsNew(metaData.index("test1"))
                .addAsNew(metaData.index("test2"))
                .build();

        ClusterState clusterState = ClusterState.builder(org.elasticsearch.cluster.ClusterName.CLUSTER_NAME_SETTING.getDefault(Settings.EMPTY)).metaData(metaData).routingTable(initialRoutingTable).build();

        logger.info("start two nodes");
        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder().add(newNode("node1")).add(newNode("node2"))).build();
        RoutingAllocation.Result routingResult = strategy.reroute(clusterState, "reroute");
        clusterState = ClusterState.builder(clusterState).routingResult(routingResult).build();

        for (int i = 0; i < clusterState.routingTable().index("test1").shards().size(); i++) {
            assertThat(clusterState.routingTable().index("test1").shard(i).size(), equalTo(2));
            assertThat(clusterState.routingTable().index("test1").shard(i).primaryShard().state(), equalTo(INITIALIZING));
            assertThat(clusterState.routingTable().index("test1").shard(i).replicaShards().get(0).state(), equalTo(UNASSIGNED));
        }

        for (int i = 0; i < clusterState.routingTable().index("test2").shards().size(); i++) {
            assertThat(clusterState.routingTable().index("test2").shard(i).size(), equalTo(2));
            assertThat(clusterState.routingTable().index("test2").shard(i).primaryShard().state(), equalTo(INITIALIZING));
            assertThat(clusterState.routingTable().index("test2").shard(i).replicaShards().get(0).state(), equalTo(UNASSIGNED));
        }

        logger.info("start all the primary shards for test1, replicas will start initializing");
        RoutingNodes routingNodes = clusterState.getRoutingNodes();
        routingResult = strategy.applyStartedShards(clusterState, routingNodes.shardsWithState("test1", INITIALIZING));
        clusterState = ClusterState.builder(clusterState).routingResult(routingResult).build();
        routingNodes = clusterState.getRoutingNodes();

        for (int i = 0; i < clusterState.routingTable().index("test1").shards().size(); i++) {
            assertThat(clusterState.routingTable().index("test1").shard(i).size(), equalTo(2));
            assertThat(clusterState.routingTable().index("test1").shard(i).primaryShard().state(), equalTo(STARTED));
            assertThat(clusterState.routingTable().index("test1").shard(i).replicaShards().get(0).state(), equalTo(INITIALIZING));
        }

        for (int i = 0; i < clusterState.routingTable().index("test2").shards().size(); i++) {
            assertThat(clusterState.routingTable().index("test2").shard(i).size(), equalTo(2));
            assertThat(clusterState.routingTable().index("test2").shard(i).primaryShard().state(), equalTo(INITIALIZING));
            assertThat(clusterState.routingTable().index("test2").shard(i).replicaShards().get(0).state(), equalTo(UNASSIGNED));
        }

        logger.info("start the test1 replica shards");
        routingNodes = clusterState.getRoutingNodes();
        routingResult = strategy.applyStartedShards(clusterState, routingNodes.shardsWithState("test1", INITIALIZING));
        clusterState = ClusterState.builder(clusterState).routingResult(routingResult).build();
        routingNodes = clusterState.getRoutingNodes();

        for (int i = 0; i < clusterState.routingTable().index("test1").shards().size(); i++) {
            assertThat(clusterState.routingTable().index("test1").shard(i).size(), equalTo(2));
            assertThat(clusterState.routingTable().index("test1").shard(i).primaryShard().state(), equalTo(STARTED));
            assertThat(clusterState.routingTable().index("test1").shard(i).replicaShards().get(0).state(), equalTo(STARTED));
        }

        for (int i = 0; i < clusterState.routingTable().index("test2").shards().size(); i++) {
            assertThat(clusterState.routingTable().index("test2").shard(i).size(), equalTo(2));
            assertThat(clusterState.routingTable().index("test2").shard(i).primaryShard().state(), equalTo(INITIALIZING));
            assertThat(clusterState.routingTable().index("test2").shard(i).replicaShards().get(0).state(), equalTo(UNASSIGNED));
        }

        logger.info("start all the primary shards for test2, replicas will start initializing");
        routingNodes = clusterState.getRoutingNodes();
        routingResult = strategy.applyStartedShards(clusterState, routingNodes.shardsWithState("test2", INITIALIZING));
        clusterState = ClusterState.builder(clusterState).routingResult(routingResult).build();
        routingNodes = clusterState.getRoutingNodes();

        for (int i = 0; i < clusterState.routingTable().index("test1").shards().size(); i++) {
            assertThat(clusterState.routingTable().index("test1").shard(i).size(), equalTo(2));
            assertThat(clusterState.routingTable().index("test1").shard(i).primaryShard().state(), equalTo(STARTED));
            assertThat(clusterState.routingTable().index("test1").shard(i).replicaShards().get(0).state(), equalTo(STARTED));
        }

        for (int i = 0; i < clusterState.routingTable().index("test2").shards().size(); i++) {
            assertThat(clusterState.routingTable().index("test2").shard(i).size(), equalTo(2));
            assertThat(clusterState.routingTable().index("test2").shard(i).primaryShard().state(), equalTo(STARTED));
            assertThat(clusterState.routingTable().index("test2").shard(i).replicaShards().get(0).state(), equalTo(INITIALIZING));
        }

        logger.info("now, start 1 more node, check that rebalancing happen (for test1) because we set it to primaries_active");
        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder(clusterState.nodes())
                .add(newNode("node3")))
                .build();
        routingResult = strategy.reroute(clusterState, "reroute");
        clusterState = ClusterState.builder(clusterState).routingResult(routingResult).build();
        routingNodes = clusterState.getRoutingNodes();

        assertThat(routingNodes.node("node3").size(), equalTo(1));
        assertThat(routingNodes.node("node3").iterator().next().shardId().getIndex().getName(), equalTo("test1"));
    }

    public void testClusterPrimariesActive2() {
        AllocationService strategy = createAllocationService(Settings.builder().put(ClusterRebalanceAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ALLOW_REBALANCE_SETTING.getKey(),
                ClusterRebalanceAllocationDecider.ClusterRebalanceType.INDICES_PRIMARIES_ACTIVE.toString()).build());

        MetaData metaData = MetaData.builder()
                .put(IndexMetaData.builder("test1").settings(settings(Version.CURRENT)).numberOfShards(1).numberOfReplicas(1))
                .put(IndexMetaData.builder("test2").settings(settings(Version.CURRENT)).numberOfShards(1).numberOfReplicas(1))
                .build();

        RoutingTable initialRoutingTable = RoutingTable.builder()
                .addAsNew(metaData.index("test1"))
                .addAsNew(metaData.index("test2"))
                .build();

        ClusterState clusterState = ClusterState.builder(org.elasticsearch.cluster.ClusterName.CLUSTER_NAME_SETTING.getDefault(Settings.EMPTY)).metaData(metaData).routingTable(initialRoutingTable).build();

        logger.info("start two nodes");
        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder().add(newNode("node1")).add(newNode("node2"))).build();
        RoutingAllocation.Result routingResult = strategy.reroute(clusterState, "reroute");
        clusterState = ClusterState.builder(clusterState).routingResult(routingResult).build();

        for (int i = 0; i < clusterState.routingTable().index("test1").shards().size(); i++) {
            assertThat(clusterState.routingTable().index("test1").shard(i).shards().size(), equalTo(2));
            assertThat(clusterState.routingTable().index("test1").shard(i).primaryShard().state(), equalTo(INITIALIZING));
            assertThat(clusterState.routingTable().index("test1").shard(i).replicaShards().get(0).state(), equalTo(UNASSIGNED));
        }

        for (int i = 0; i < clusterState.routingTable().index("test2").shards().size(); i++) {
            assertThat(clusterState.routingTable().index("test2").shard(i).shards().size(), equalTo(2));
            assertThat(clusterState.routingTable().index("test2").shard(i).primaryShard().state(), equalTo(INITIALIZING));
            assertThat(clusterState.routingTable().index("test2").shard(i).replicaShards().get(0).state(), equalTo(UNASSIGNED));
        }

        logger.info("start all the primary shards for test1, replicas will start initializing");
        RoutingNodes routingNodes = clusterState.getRoutingNodes();
        routingResult = strategy.applyStartedShards(clusterState, routingNodes.shardsWithState("test1", INITIALIZING));
        clusterState = ClusterState.builder(clusterState).routingResult(routingResult).build();
        routingNodes = clusterState.getRoutingNodes();

        for (int i = 0; i < clusterState.routingTable().index("test1").shards().size(); i++) {
            assertThat(clusterState.routingTable().index("test1").shard(i).shards().size(), equalTo(2));
            assertThat(clusterState.routingTable().index("test1").shard(i).primaryShard().state(), equalTo(STARTED));
            assertThat(clusterState.routingTable().index("test1").shard(i).replicaShards().get(0).state(), equalTo(INITIALIZING));
        }

        for (int i = 0; i < clusterState.routingTable().index("test2").shards().size(); i++) {
            assertThat(clusterState.routingTable().index("test2").shard(i).shards().size(), equalTo(2));
            assertThat(clusterState.routingTable().index("test2").shard(i).primaryShard().state(), equalTo(INITIALIZING));
            assertThat(clusterState.routingTable().index("test2").shard(i).replicaShards().get(0).state(), equalTo(UNASSIGNED));
        }

        logger.info("start the test1 replica shards");
        routingNodes = clusterState.getRoutingNodes();
        routingResult = strategy.applyStartedShards(clusterState, routingNodes.shardsWithState("test1", INITIALIZING));
        clusterState = ClusterState.builder(clusterState).routingResult(routingResult).build();
        routingNodes = clusterState.getRoutingNodes();

        for (int i = 0; i < clusterState.routingTable().index("test1").shards().size(); i++) {
            assertThat(clusterState.routingTable().index("test1").shard(i).shards().size(), equalTo(2));
            assertThat(clusterState.routingTable().index("test1").shard(i).primaryShard().state(), equalTo(STARTED));
            assertThat(clusterState.routingTable().index("test1").shard(i).replicaShards().get(0).state(), equalTo(STARTED));
        }

        for (int i = 0; i < clusterState.routingTable().index("test2").shards().size(); i++) {
            assertThat(clusterState.routingTable().index("test2").shard(i).shards().size(), equalTo(2));
            assertThat(clusterState.routingTable().index("test2").shard(i).primaryShard().state(), equalTo(INITIALIZING));
            assertThat(clusterState.routingTable().index("test2").shard(i).replicaShards().get(0).state(), equalTo(UNASSIGNED));
        }

        logger.info("now, start 1 more node, check that rebalancing will not happen (for test1) because we set it to primaries_active");
        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder(clusterState.nodes())
                .add(newNode("node3")))
                .build();
        routingResult = strategy.reroute(clusterState, "reroute");
        clusterState = ClusterState.builder(clusterState).routingResult(routingResult).build();
        routingNodes = clusterState.getRoutingNodes();

        assertThat(routingNodes.node("node3").isEmpty(), equalTo(true));
    }

    public void testClusterAllActive1() {
        AllocationService strategy = createAllocationService(Settings.builder().put(ClusterRebalanceAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ALLOW_REBALANCE_SETTING.getKey(),
                ClusterRebalanceAllocationDecider.ClusterRebalanceType.INDICES_ALL_ACTIVE.toString()).build());

        MetaData metaData = MetaData.builder()
                .put(IndexMetaData.builder("test1").settings(settings(Version.CURRENT)).numberOfShards(1).numberOfReplicas(1))
                .put(IndexMetaData.builder("test2").settings(settings(Version.CURRENT)).numberOfShards(1).numberOfReplicas(1))
                .build();

        RoutingTable initialRoutingTable = RoutingTable.builder()
                .addAsNew(metaData.index("test1"))
                .addAsNew(metaData.index("test2"))
                .build();

        ClusterState clusterState = ClusterState.builder(org.elasticsearch.cluster.ClusterName.CLUSTER_NAME_SETTING.getDefault(Settings.EMPTY)).metaData(metaData).routingTable(initialRoutingTable).build();

        logger.info("start two nodes");
        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder().add(newNode("node1")).add(newNode("node2"))).build();
        RoutingAllocation.Result routingResult = strategy.reroute(clusterState, "reroute");
        clusterState = ClusterState.builder(clusterState).routingResult(routingResult).build();

        for (int i = 0; i < clusterState.routingTable().index("test1").shards().size(); i++) {
            assertThat(clusterState.routingTable().index("test1").shard(i).shards().size(), equalTo(2));
            assertThat(clusterState.routingTable().index("test1").shard(i).primaryShard().state(), equalTo(INITIALIZING));
            assertThat(clusterState.routingTable().index("test1").shard(i).replicaShards().get(0).state(), equalTo(UNASSIGNED));
        }

        for (int i = 0; i < clusterState.routingTable().index("test2").shards().size(); i++) {
            assertThat(clusterState.routingTable().index("test2").shard(i).shards().size(), equalTo(2));
            assertThat(clusterState.routingTable().index("test2").shard(i).primaryShard().state(), equalTo(INITIALIZING));
            assertThat(clusterState.routingTable().index("test2").shard(i).replicaShards().get(0).state(), equalTo(UNASSIGNED));
        }

        logger.info("start all the primary shards for test1, replicas will start initializing");
        RoutingNodes routingNodes = clusterState.getRoutingNodes();
        routingResult = strategy.applyStartedShards(clusterState, routingNodes.shardsWithState("test1", INITIALIZING));
        clusterState = ClusterState.builder(clusterState).routingResult(routingResult).build();
        routingNodes = clusterState.getRoutingNodes();

        for (int i = 0; i < clusterState.routingTable().index("test1").shards().size(); i++) {
            assertThat(clusterState.routingTable().index("test1").shard(i).shards().size(), equalTo(2));
            assertThat(clusterState.routingTable().index("test1").shard(i).primaryShard().state(), equalTo(STARTED));
            assertThat(clusterState.routingTable().index("test1").shard(i).replicaShards().get(0).state(), equalTo(INITIALIZING));
        }

        for (int i = 0; i < clusterState.routingTable().index("test2").shards().size(); i++) {
            assertThat(clusterState.routingTable().index("test2").shard(i).shards().size(), equalTo(2));
            assertThat(clusterState.routingTable().index("test2").shard(i).primaryShard().state(), equalTo(INITIALIZING));
            assertThat(clusterState.routingTable().index("test2").shard(i).replicaShards().get(0).state(), equalTo(UNASSIGNED));
        }

        logger.info("start the test1 replica shards");
        routingNodes = clusterState.getRoutingNodes();
        routingResult = strategy.applyStartedShards(clusterState, routingNodes.shardsWithState("test1", INITIALIZING));
        clusterState = ClusterState.builder(clusterState).routingResult(routingResult).build();
        routingNodes = clusterState.getRoutingNodes();

        for (int i = 0; i < clusterState.routingTable().index("test1").shards().size(); i++) {
            assertThat(clusterState.routingTable().index("test1").shard(i).shards().size(), equalTo(2));
            assertThat(clusterState.routingTable().index("test1").shard(i).primaryShard().state(), equalTo(STARTED));
            assertThat(clusterState.routingTable().index("test1").shard(i).replicaShards().get(0).state(), equalTo(STARTED));
        }

        for (int i = 0; i < clusterState.routingTable().index("test2").shards().size(); i++) {
            assertThat(clusterState.routingTable().index("test2").shard(i).shards().size(), equalTo(2));
            assertThat(clusterState.routingTable().index("test2").shard(i).primaryShard().state(), equalTo(INITIALIZING));
            assertThat(clusterState.routingTable().index("test2").shard(i).replicaShards().get(0).state(), equalTo(UNASSIGNED));
        }

        logger.info("start all the primary shards for test2, replicas will start initializing");
        routingNodes = clusterState.getRoutingNodes();
        routingResult = strategy.applyStartedShards(clusterState, routingNodes.shardsWithState("test2", INITIALIZING));
        clusterState = ClusterState.builder(clusterState).routingResult(routingResult).build();
        routingNodes = clusterState.getRoutingNodes();

        for (int i = 0; i < clusterState.routingTable().index("test1").shards().size(); i++) {
            assertThat(clusterState.routingTable().index("test1").shard(i).shards().size(), equalTo(2));
            assertThat(clusterState.routingTable().index("test1").shard(i).primaryShard().state(), equalTo(STARTED));
            assertThat(clusterState.routingTable().index("test1").shard(i).replicaShards().get(0).state(), equalTo(STARTED));
        }

        for (int i = 0; i < clusterState.routingTable().index("test2").shards().size(); i++) {
            assertThat(clusterState.routingTable().index("test2").shard(i).shards().size(), equalTo(2));
            assertThat(clusterState.routingTable().index("test2").shard(i).primaryShard().state(), equalTo(STARTED));
            assertThat(clusterState.routingTable().index("test2").shard(i).replicaShards().get(0).state(), equalTo(INITIALIZING));
        }

        logger.info("start the test2 replica shards");
        routingNodes = clusterState.getRoutingNodes();
        routingResult = strategy.applyStartedShards(clusterState, routingNodes.shardsWithState("test2", INITIALIZING));
        clusterState = ClusterState.builder(clusterState).routingResult(routingResult).build();
        routingNodes = clusterState.getRoutingNodes();

        for (int i = 0; i < clusterState.routingTable().index("test1").shards().size(); i++) {
            assertThat(clusterState.routingTable().index("test1").shard(i).shards().size(), equalTo(2));
            assertThat(clusterState.routingTable().index("test1").shard(i).primaryShard().state(), equalTo(STARTED));
            assertThat(clusterState.routingTable().index("test1").shard(i).replicaShards().get(0).state(), equalTo(STARTED));
        }

        for (int i = 0; i < clusterState.routingTable().index("test2").shards().size(); i++) {
            assertThat(clusterState.routingTable().index("test2").shard(i).shards().size(), equalTo(2));
            assertThat(clusterState.routingTable().index("test2").shard(i).primaryShard().state(), equalTo(STARTED));
            assertThat(clusterState.routingTable().index("test2").shard(i).replicaShards().get(0).state(), equalTo(STARTED));
        }

        logger.info("now, start 1 more node, check that rebalancing happen (for test1) because we set it to all_active");
        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder(clusterState.nodes())
                .add(newNode("node3")))
                .build();
        routingResult = strategy.reroute(clusterState, "reroute");
        clusterState = ClusterState.builder(clusterState).routingResult(routingResult).build();
        routingNodes = clusterState.getRoutingNodes();

        assertThat(routingNodes.node("node3").size(), equalTo(1));
        assertThat(routingNodes.node("node3").iterator().next().shardId().getIndex().getName(), anyOf(equalTo("test1"), equalTo("test2")));
    }

    public void testClusterAllActive2() {
        AllocationService strategy = createAllocationService(Settings.builder().put(ClusterRebalanceAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ALLOW_REBALANCE_SETTING.getKey(),
                ClusterRebalanceAllocationDecider.ClusterRebalanceType.INDICES_ALL_ACTIVE.toString()).build());

        MetaData metaData = MetaData.builder()
                .put(IndexMetaData.builder("test1").settings(settings(Version.CURRENT)).numberOfShards(1).numberOfReplicas(1))
                .put(IndexMetaData.builder("test2").settings(settings(Version.CURRENT)).numberOfShards(1).numberOfReplicas(1))
                .build();

        RoutingTable initialRoutingTable = RoutingTable.builder()
                .addAsNew(metaData.index("test1"))
                .addAsNew(metaData.index("test2"))
                .build();

        ClusterState clusterState = ClusterState.builder(org.elasticsearch.cluster.ClusterName.CLUSTER_NAME_SETTING.getDefault(Settings.EMPTY)).metaData(metaData).routingTable(initialRoutingTable).build();

        logger.info("start two nodes");
        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder().add(newNode("node1")).add(newNode("node2"))).build();
        RoutingAllocation.Result routingResult = strategy.reroute(clusterState, "reroute");
        clusterState = ClusterState.builder(clusterState).routingResult(routingResult).build();

        for (int i = 0; i < clusterState.routingTable().index("test1").shards().size(); i++) {
            assertThat(clusterState.routingTable().index("test1").shard(i).shards().size(), equalTo(2));
            assertThat(clusterState.routingTable().index("test1").shard(i).primaryShard().state(), equalTo(INITIALIZING));
            assertThat(clusterState.routingTable().index("test1").shard(i).replicaShards().get(0).state(), equalTo(UNASSIGNED));
        }

        for (int i = 0; i < clusterState.routingTable().index("test2").shards().size(); i++) {
            assertThat(clusterState.routingTable().index("test2").shard(i).shards().size(), equalTo(2));
            assertThat(clusterState.routingTable().index("test2").shard(i).primaryShard().state(), equalTo(INITIALIZING));
            assertThat(clusterState.routingTable().index("test2").shard(i).replicaShards().get(0).state(), equalTo(UNASSIGNED));
        }

        logger.info("start all the primary shards for test1, replicas will start initializing");
        RoutingNodes routingNodes = clusterState.getRoutingNodes();
        routingResult = strategy.applyStartedShards(clusterState, routingNodes.shardsWithState("test1", INITIALIZING));
        clusterState = ClusterState.builder(clusterState).routingResult(routingResult).build();
        routingNodes = clusterState.getRoutingNodes();

        for (int i = 0; i < clusterState.routingTable().index("test1").shards().size(); i++) {
            assertThat(clusterState.routingTable().index("test1").shard(i).shards().size(), equalTo(2));
            assertThat(clusterState.routingTable().index("test1").shard(i).primaryShard().state(), equalTo(STARTED));
            assertThat(clusterState.routingTable().index("test1").shard(i).replicaShards().get(0).state(), equalTo(INITIALIZING));
        }

        for (int i = 0; i < clusterState.routingTable().index("test2").shards().size(); i++) {
            assertThat(clusterState.routingTable().index("test2").shard(i).shards().size(), equalTo(2));
            assertThat(clusterState.routingTable().index("test2").shard(i).primaryShard().state(), equalTo(INITIALIZING));
            assertThat(clusterState.routingTable().index("test2").shard(i).replicaShards().get(0).state(), equalTo(UNASSIGNED));
        }

        logger.info("start the test1 replica shards");
        routingNodes = clusterState.getRoutingNodes();
        routingResult = strategy.applyStartedShards(clusterState, routingNodes.shardsWithState("test1", INITIALIZING));
        clusterState = ClusterState.builder(clusterState).routingResult(routingResult).build();
        routingNodes = clusterState.getRoutingNodes();

        for (int i = 0; i < clusterState.routingTable().index("test1").shards().size(); i++) {
            assertThat(clusterState.routingTable().index("test1").shard(i).shards().size(), equalTo(2));
            assertThat(clusterState.routingTable().index("test1").shard(i).primaryShard().state(), equalTo(STARTED));
            assertThat(clusterState.routingTable().index("test1").shard(i).replicaShards().get(0).state(), equalTo(STARTED));
        }

        for (int i = 0; i < clusterState.routingTable().index("test2").shards().size(); i++) {
            assertThat(clusterState.routingTable().index("test2").shard(i).shards().size(), equalTo(2));
            assertThat(clusterState.routingTable().index("test2").shard(i).primaryShard().state(), equalTo(INITIALIZING));
            assertThat(clusterState.routingTable().index("test2").shard(i).replicaShards().get(0).state(), equalTo(UNASSIGNED));
        }

        logger.info("now, start 1 more node, check that rebalancing will not happen (for test1) because we set it to all_active");
        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder(clusterState.nodes())
                .add(newNode("node3")))
                .build();
        routingResult = strategy.reroute(clusterState, "reroute");
        clusterState = ClusterState.builder(clusterState).routingResult(routingResult).build();
        routingNodes = clusterState.getRoutingNodes();

        assertThat(routingNodes.node("node3").isEmpty(), equalTo(true));
    }

    public void testClusterAllActive3() {
        AllocationService strategy = createAllocationService(Settings.builder().put(ClusterRebalanceAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ALLOW_REBALANCE_SETTING.getKey(),
                ClusterRebalanceAllocationDecider.ClusterRebalanceType.INDICES_ALL_ACTIVE.toString()).build());

        MetaData metaData = MetaData.builder()
                .put(IndexMetaData.builder("test1").settings(settings(Version.CURRENT)).numberOfShards(1).numberOfReplicas(1))
                .put(IndexMetaData.builder("test2").settings(settings(Version.CURRENT)).numberOfShards(1).numberOfReplicas(1))
                .build();

        RoutingTable initialRoutingTable = RoutingTable.builder()
                .addAsNew(metaData.index("test1"))
                .addAsNew(metaData.index("test2"))
                .build();

        ClusterState clusterState = ClusterState.builder(org.elasticsearch.cluster.ClusterName.CLUSTER_NAME_SETTING.getDefault(Settings.EMPTY)).metaData(metaData).routingTable(initialRoutingTable).build();

        logger.info("start two nodes");
        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder().add(newNode("node1")).add(newNode("node2"))).build();
        RoutingAllocation.Result routingResult = strategy.reroute(clusterState, "reroute");
        clusterState = ClusterState.builder(clusterState).routingResult(routingResult).build();

        for (int i = 0; i < clusterState.routingTable().index("test1").shards().size(); i++) {
            assertThat(clusterState.routingTable().index("test1").shard(i).shards().size(), equalTo(2));
            assertThat(clusterState.routingTable().index("test1").shard(i).primaryShard().state(), equalTo(INITIALIZING));
            assertThat(clusterState.routingTable().index("test1").shard(i).replicaShards().get(0).state(), equalTo(UNASSIGNED));
        }

        for (int i = 0; i < clusterState.routingTable().index("test2").shards().size(); i++) {
            assertThat(clusterState.routingTable().index("test2").shard(i).shards().size(), equalTo(2));
            assertThat(clusterState.routingTable().index("test2").shard(i).primaryShard().state(), equalTo(INITIALIZING));
            assertThat(clusterState.routingTable().index("test2").shard(i).replicaShards().get(0).state(), equalTo(UNASSIGNED));
        }

        logger.info("start all the primary shards for test1, replicas will start initializing");
        RoutingNodes routingNodes = clusterState.getRoutingNodes();
        routingResult = strategy.applyStartedShards(clusterState, routingNodes.shardsWithState("test1", INITIALIZING));
        clusterState = ClusterState.builder(clusterState).routingResult(routingResult).build();
        routingNodes = clusterState.getRoutingNodes();

        for (int i = 0; i < clusterState.routingTable().index("test1").shards().size(); i++) {
            assertThat(clusterState.routingTable().index("test1").shard(i).shards().size(), equalTo(2));
            assertThat(clusterState.routingTable().index("test1").shard(i).primaryShard().state(), equalTo(STARTED));
            assertThat(clusterState.routingTable().index("test1").shard(i).replicaShards().get(0).state(), equalTo(INITIALIZING));
        }

        for (int i = 0; i < clusterState.routingTable().index("test2").shards().size(); i++) {
            assertThat(clusterState.routingTable().index("test2").shard(i).shards().size(), equalTo(2));
            assertThat(clusterState.routingTable().index("test2").shard(i).primaryShard().state(), equalTo(INITIALIZING));
            assertThat(clusterState.routingTable().index("test2").shard(i).replicaShards().get(0).state(), equalTo(UNASSIGNED));
        }

        logger.info("start the test1 replica shards");
        routingNodes = clusterState.getRoutingNodes();
        routingResult = strategy.applyStartedShards(clusterState, routingNodes.shardsWithState("test1", INITIALIZING));
        clusterState = ClusterState.builder(clusterState).routingResult(routingResult).build();
        routingNodes = clusterState.getRoutingNodes();

        for (int i = 0; i < clusterState.routingTable().index("test1").shards().size(); i++) {
            assertThat(clusterState.routingTable().index("test1").shard(i).shards().size(), equalTo(2));
            assertThat(clusterState.routingTable().index("test1").shard(i).primaryShard().state(), equalTo(STARTED));
            assertThat(clusterState.routingTable().index("test1").shard(i).replicaShards().get(0).state(), equalTo(STARTED));
        }

        for (int i = 0; i < clusterState.routingTable().index("test2").shards().size(); i++) {
            assertThat(clusterState.routingTable().index("test2").shard(i).shards().size(), equalTo(2));
            assertThat(clusterState.routingTable().index("test2").shard(i).primaryShard().state(), equalTo(INITIALIZING));
            assertThat(clusterState.routingTable().index("test2").shard(i).replicaShards().get(0).state(), equalTo(UNASSIGNED));
        }

        logger.info("start all the primary shards for test2, replicas will start initializing");
        routingNodes = clusterState.getRoutingNodes();
        routingResult = strategy.applyStartedShards(clusterState, routingNodes.shardsWithState("test2", INITIALIZING));
        clusterState = ClusterState.builder(clusterState).routingResult(routingResult).build();
        routingNodes = clusterState.getRoutingNodes();

        for (int i = 0; i < clusterState.routingTable().index("test1").shards().size(); i++) {
            assertThat(clusterState.routingTable().index("test1").shard(i).shards().size(), equalTo(2));
            assertThat(clusterState.routingTable().index("test1").shard(i).primaryShard().state(), equalTo(STARTED));
            assertThat(clusterState.routingTable().index("test1").shard(i).replicaShards().get(0).state(), equalTo(STARTED));
        }

        for (int i = 0; i < clusterState.routingTable().index("test2").shards().size(); i++) {
            assertThat(clusterState.routingTable().index("test2").shard(i).shards().size(), equalTo(2));
            assertThat(clusterState.routingTable().index("test2").shard(i).primaryShard().state(), equalTo(STARTED));
            assertThat(clusterState.routingTable().index("test2").shard(i).replicaShards().get(0).state(), equalTo(INITIALIZING));
        }

        logger.info("now, start 1 more node, check that rebalancing will not happen (for test1) because we set it to all_active");
        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder(clusterState.nodes())
                .add(newNode("node3")))
                .build();
        routingResult = strategy.reroute(clusterState, "reroute");
        clusterState = ClusterState.builder(clusterState).routingResult(routingResult).build();
        routingNodes = clusterState.getRoutingNodes();

        assertThat(routingNodes.node("node3").isEmpty(), equalTo(true));
    }

    public void testRebalanceWithIgnoredUnassignedShards() {
        final AtomicBoolean allocateTest1 = new AtomicBoolean(false);

        AllocationService strategy = createAllocationService(Settings.EMPTY, new NoopGatewayAllocator() {
            @Override
            public void allocateUnassigned(RoutingAllocation allocation) {
                if (allocateTest1.get() == false) {
                    RoutingNodes.UnassignedShards unassigned = allocation.routingNodes().unassigned();
                    RoutingNodes.UnassignedShards.UnassignedIterator iterator = unassigned.iterator();
                    while (iterator.hasNext()) {
                        ShardRouting next = iterator.next();
                        if ("test1".equals(next.index().getName())) {
                            iterator.removeAndIgnore(UnassignedInfo.AllocationStatus.NO_ATTEMPT, allocation.changes());
                        }

                    }
                }
                super.allocateUnassigned(allocation);
            }
        });

        MetaData metaData = MetaData.builder()
                .put(IndexMetaData.builder("test").settings(settings(Version.CURRENT)).numberOfShards(2).numberOfReplicas(0))
                .put(IndexMetaData.builder("test1").settings(settings(Version.CURRENT)).numberOfShards(2).numberOfReplicas(0))
                .build();

        RoutingTable initialRoutingTable = RoutingTable.builder()
                .addAsNew(metaData.index("test"))
                .addAsNew(metaData.index("test1"))
                .build();

        ClusterState clusterState = ClusterState.builder(org.elasticsearch.cluster.ClusterName.CLUSTER_NAME_SETTING.getDefault(Settings.EMPTY)).metaData(metaData).routingTable(initialRoutingTable).build();

        logger.info("start two nodes");
        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder().add(newNode("node1"))).build();
        RoutingAllocation.Result routingResult = strategy.reroute(clusterState, "reroute");
        clusterState = ClusterState.builder(clusterState).routingResult(routingResult).build();

        for (int i = 0; i < clusterState.routingTable().index("test").shards().size(); i++) {
            assertThat(clusterState.routingTable().index("test").shard(i).shards().size(), equalTo(1));
            assertThat(clusterState.routingTable().index("test").shard(i).primaryShard().state(), equalTo(INITIALIZING));
        }

        logger.debug("start all the primary shards for test");
        RoutingNodes routingNodes = clusterState.getRoutingNodes();
        routingResult = strategy.applyStartedShards(clusterState, routingNodes.shardsWithState("test", INITIALIZING));
        clusterState = ClusterState.builder(clusterState).routingResult(routingResult).build();

        for (int i = 0; i < clusterState.routingTable().index("test").shards().size(); i++) {
            assertThat(clusterState.routingTable().index("test").shard(i).shards().size(), equalTo(1));
            assertThat(clusterState.routingTable().index("test").shard(i).primaryShard().state(), equalTo(STARTED));
        }

        logger.debug("now, start 1 more node, check that rebalancing will not happen since we unassigned shards");
        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder(clusterState.nodes())
                .add(newNode("node2")))
                .build();
        logger.debug("reroute and check that nothing has changed");
        RoutingAllocation.Result reroute = strategy.reroute(clusterState, "reroute");
        assertFalse(reroute.changed());
        routingResult = reroute;
        clusterState = ClusterState.builder(clusterState).routingResult(routingResult).build();

        for (int i = 0; i < clusterState.routingTable().index("test").shards().size(); i++) {
            assertThat(clusterState.routingTable().index("test").shard(i).shards().size(), equalTo(1));
            assertThat(clusterState.routingTable().index("test").shard(i).primaryShard().state(), equalTo(STARTED));
        }
        for (int i = 0; i < clusterState.routingTable().index("test1").shards().size(); i++) {
            assertThat(clusterState.routingTable().index("test1").shard(i).shards().size(), equalTo(1));
            assertThat(clusterState.routingTable().index("test1").shard(i).primaryShard().state(), equalTo(UNASSIGNED));
        }
        logger.debug("now set allocateTest1 to true and reroute we should see the [test1] index initializing");
        allocateTest1.set(true);
        reroute = strategy.reroute(clusterState, "reroute");
        assertTrue(reroute.changed());
        routingResult = reroute;
        clusterState = ClusterState.builder(clusterState).routingResult(routingResult).build();
        for (int i = 0; i < clusterState.routingTable().index("test1").shards().size(); i++) {
            assertThat(clusterState.routingTable().index("test1").shard(i).shards().size(), equalTo(1));
            assertThat(clusterState.routingTable().index("test1").shard(i).primaryShard().state(), equalTo(INITIALIZING));
        }

        logger.debug("now start initializing shards and expect exactly one rebalance from node1 to node 2 since index [test] is all on node1");

        routingNodes = clusterState.getRoutingNodes();
        routingResult = strategy.applyStartedShards(clusterState, routingNodes.shardsWithState("test1", INITIALIZING));
        clusterState = ClusterState.builder(clusterState).routingResult(routingResult).build();

        for (int i = 0; i < clusterState.routingTable().index("test1").shards().size(); i++) {
            assertThat(clusterState.routingTable().index("test1").shard(i).shards().size(), equalTo(1));
            assertThat(clusterState.routingTable().index("test1").shard(i).primaryShard().state(), equalTo(STARTED));
        }
        int numStarted = 0;
        int numRelocating = 0;
        for (int i = 0; i < clusterState.routingTable().index("test").shards().size(); i++) {
            assertThat(clusterState.routingTable().index("test").shard(i).shards().size(), equalTo(1));
            if (clusterState.routingTable().index("test").shard(i).primaryShard().state() == STARTED) {
                numStarted++;
            } else if (clusterState.routingTable().index("test").shard(i).primaryShard().state() == RELOCATING) {
                numRelocating++;
            }
        }
        assertEquals(numStarted, 1);
        assertEquals(numRelocating, 1);

    }

    public void testRebalanceWhileShardFetching() {
        final AtomicBoolean hasFetches = new AtomicBoolean(true);
        AllocationService strategy = createAllocationService(Settings.builder().put(ClusterRebalanceAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ALLOW_REBALANCE_SETTING.getKey(),
                ClusterRebalanceAllocationDecider.ClusterRebalanceType.ALWAYS.toString()).build(), new NoopGatewayAllocator() {
            @Override
            public void allocateUnassigned(RoutingAllocation allocation) {
                if (hasFetches.get()) {
                    allocation.setHasPendingAsyncFetch();
                }
                super.allocateUnassigned(allocation);
            }
        });

        MetaData metaData = MetaData.builder()
                .put(IndexMetaData.builder("test").settings(settings(Version.CURRENT)).numberOfShards(2).numberOfReplicas(0))
                .put(IndexMetaData.builder("test1").settings(settings(Version.CURRENT).put(IndexMetaData.INDEX_ROUTING_EXCLUDE_GROUP_SETTING.getKey() + "_id", "node1,node2")).numberOfShards(2).numberOfReplicas(0))
                .build();

        // we use a second index here (test1) that never gets assigned otherwise allocateUnassigned is never called if we don't have unassigned shards.
        RoutingTable initialRoutingTable = RoutingTable.builder()
                .addAsNew(metaData.index("test"))
                .addAsNew(metaData.index("test1"))
                .build();

        ClusterState clusterState = ClusterState.builder(org.elasticsearch.cluster.ClusterName.CLUSTER_NAME_SETTING.getDefault(Settings.EMPTY)).metaData(metaData).routingTable(initialRoutingTable).build();

        logger.info("start two nodes");
        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder().add(newNode("node1"))).build();
        RoutingAllocation.Result routingResult = strategy.reroute(clusterState, "reroute");
        clusterState = ClusterState.builder(clusterState).routingResult(routingResult).build();

        for (int i = 0; i < clusterState.routingTable().index("test").shards().size(); i++) {
            assertThat(clusterState.routingTable().index("test").shard(i).shards().size(), equalTo(1));
            assertThat(clusterState.routingTable().index("test").shard(i).primaryShard().state(), equalTo(INITIALIZING));
        }

        logger.debug("start all the primary shards for test");
        RoutingNodes routingNodes = clusterState.getRoutingNodes();
        routingResult = strategy.applyStartedShards(clusterState, routingNodes.shardsWithState("test", INITIALIZING));
        clusterState = ClusterState.builder(clusterState).routingResult(routingResult).build();

        for (int i = 0; i < clusterState.routingTable().index("test").shards().size(); i++) {
            assertThat(clusterState.routingTable().index("test").shard(i).shards().size(), equalTo(1));
            assertThat(clusterState.routingTable().index("test").shard(i).primaryShard().state(), equalTo(STARTED));
        }

        logger.debug("now, start 1 more node, check that rebalancing will not happen since we have shard sync going on");
        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder(clusterState.nodes())
                .add(newNode("node2")))
                .build();
        logger.debug("reroute and check that nothing has changed");
        routingResult = strategy.reroute(clusterState, "reroute");
        assertFalse(routingResult.changed());

        for (int i = 0; i < clusterState.routingTable().index("test").shards().size(); i++) {
            assertThat(clusterState.routingTable().index("test").shard(i).shards().size(), equalTo(1));
            assertThat(clusterState.routingTable().index("test").shard(i).primaryShard().state(), equalTo(STARTED));
        }
        for (int i = 0; i < clusterState.routingTable().index("test1").shards().size(); i++) {
            assertThat(clusterState.routingTable().index("test1").shard(i).shards().size(), equalTo(1));
            assertThat(clusterState.routingTable().index("test1").shard(i).primaryShard().state(), equalTo(UNASSIGNED));
        }
        logger.debug("now set hasFetches to true and reroute we should now see exactly one relocating shard");
        hasFetches.set(false);
        routingResult = strategy.reroute(clusterState, "reroute");
        assertTrue(routingResult.changed());
        clusterState = ClusterState.builder(clusterState).routingResult(routingResult).build();
        int numStarted = 0;
        int numRelocating = 0;
        for (int i = 0; i < clusterState.routingTable().index("test").shards().size(); i++) {

            assertThat(clusterState.routingTable().index("test").shard(i).shards().size(), equalTo(1));
            if (clusterState.routingTable().index("test").shard(i).primaryShard().state() == STARTED) {
                numStarted++;
            } else if (clusterState.routingTable().index("test").shard(i).primaryShard().state() == RELOCATING) {
                numRelocating++;
            }
        }
        for (int i = 0; i < clusterState.routingTable().index("test1").shards().size(); i++) {
            assertThat(clusterState.routingTable().index("test1").shard(i).shards().size(), equalTo(1));
            assertThat(clusterState.routingTable().index("test1").shard(i).primaryShard().state(), equalTo(UNASSIGNED));
        }
        assertEquals(numStarted, 1);
        assertEquals(numRelocating, 1);
    }
}
