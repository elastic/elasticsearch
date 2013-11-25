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
package org.elasticsearch.benchmark.cluster;

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.RoutingNodes;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.cluster.routing.allocation.decider.ClusterRebalanceAllocationDecider;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;

import static org.elasticsearch.cluster.routing.ShardRoutingState.INITIALIZING;
import static org.elasticsearch.cluster.routing.allocation.RoutingAllocationTests.newNode;
import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;

public class MassiveClusterRebalanceBenchmark {

    private static final ESLogger logger = Loggers.getLogger(MassiveClusterRebalanceBenchmark.class);

    public static void main(String[] args) {
        int numIndices = 5 * 365; // five years
        int numShards = 6;
        int numReplicas = 2;
        AllocationService strategy = new AllocationService(settingsBuilder()
                .put("cluster.routing.allocation.allow_rebalance", ClusterRebalanceAllocationDecider.ClusterRebalanceType.ALWAYS.toString())
                .put("cluster.routing.allocation.node_initial_primaries_recoveries", 16384)
                .put("cluster.routing.allocation.node_concurrent_recoveries", 16384)
                .build());


        long start = System.currentTimeMillis();
        logger.info("Start massive cluster test.");
        MetaData.Builder mb = MetaData.builder();
        for (int i = 1; i <= numIndices; i++)
            mb.put(IndexMetaData.builder("test_" + i).numberOfShards(numShards).numberOfReplicas(numReplicas));

        MetaData metaData = mb.build();

        logger.info("Buidling MetaData took " + (System.currentTimeMillis() - start) + "ms.");
        start = System.currentTimeMillis();


        RoutingTable.Builder rb = RoutingTable.builder();
        for (int i = 1; i <= numIndices; i++)
            rb.addAsNew(metaData.index("test_" + i));

        RoutingTable routingTable = rb.build();

        logger.info("Buidling RoutingTable took " + (System.currentTimeMillis() - start) + "ms.");
        start = System.currentTimeMillis();

        ClusterState clusterState = ClusterState.builder().metaData(metaData).routingTable(routingTable).build();

        logger.info("Buidling ClusterState took " + (System.currentTimeMillis() - start) + "ms.");
        start = System.currentTimeMillis();

        logger.info("start two nodes");
        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder().put(newNode("node1")).put(newNode("node2"))).build();
        logger.info("Buidling ClusterState took " + (System.currentTimeMillis() - start) + "ms.");
        start = System.currentTimeMillis();

        RoutingTable prevRoutingTable = routingTable;

        routingTable = strategy.reroute(clusterState).routingTable();
        logger.info("Buidling new RoutingTable took " + (System.currentTimeMillis() - start) + "ms.");
        start = System.currentTimeMillis();

        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();
        logger.info("Buidling new ClusterState took " + (System.currentTimeMillis() - start) + "ms.");
        start = System.currentTimeMillis();


        logger.info("start all the primary shards for test1, replicas will start initializing");
        RoutingNodes routingNodes = clusterState.routingNodes();
        prevRoutingTable = routingTable;
        routingTable = strategy.applyStartedShards(clusterState, routingNodes.shardsWithState("test_1", INITIALIZING)).routingTable();
        logger.info("Buidling new RoutingTable took " + (System.currentTimeMillis() - start) + "ms.");
        start = System.currentTimeMillis();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();
        logger.info("Buidling new ClusterState took " + (System.currentTimeMillis() - start) + "ms.");
        start = System.currentTimeMillis();
        routingNodes = clusterState.routingNodes();

        logger.info("start the test1 replica shards");
        routingNodes = clusterState.routingNodes();
        prevRoutingTable = routingTable;
        routingTable = strategy.applyStartedShards(clusterState, routingNodes.shardsWithState("test_1", INITIALIZING)).routingTable();
        logger.info("Buidling new RoutingTable took " + (System.currentTimeMillis() - start) + "ms.");
        start = System.currentTimeMillis();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();
        logger.info("Buidling new ClusterState took " + (System.currentTimeMillis() - start) + "ms.");
        start = System.currentTimeMillis();
        routingNodes = clusterState.routingNodes();


        logger.info("now, start 1 more node, check that rebalancing will happen (for test1) because we set it to always");
        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder(clusterState.nodes())
                .put(newNode("node3")))
                .build();
        prevRoutingTable = routingTable;
        routingTable = strategy.reroute(clusterState).routingTable();
        logger.info("Buidling new RoutingTable took " + (System.currentTimeMillis() - start) + "ms.");
        start = System.currentTimeMillis();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();
        logger.info("Buidling new ClusterState took " + (System.currentTimeMillis() - start) + "ms.");
        start = System.currentTimeMillis();
        routingNodes = clusterState.routingNodes();

        logger.info("now, start 33 more node, check that rebalancing will happen (for test1) because we set it to always");
        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder(clusterState.nodes())
                .put(newNode("node4"))
                .put(newNode("node5"))
                .put(newNode("node6"))
                .put(newNode("node7"))
                .put(newNode("node8"))
                .put(newNode("node9"))
                .put(newNode("node10"))
                .put(newNode("node11"))
                .put(newNode("node12"))
                .put(newNode("node13"))
                .put(newNode("node15"))
                .put(newNode("node16"))
                .put(newNode("node17"))
                .put(newNode("node18"))
                .put(newNode("node19"))
                .put(newNode("node20"))
                .put(newNode("node21"))
                .put(newNode("node22"))
                .put(newNode("node23"))
                .put(newNode("node24"))
                .put(newNode("node25"))
                .put(newNode("node26"))
                .put(newNode("node27"))
                .put(newNode("node28"))
                .put(newNode("node29"))
                .put(newNode("node30"))
                .put(newNode("node31"))
                .put(newNode("node32"))
                .put(newNode("node33"))
                .put(newNode("node34"))
                .put(newNode("node35"))
                .put(newNode("node36")))
                .build();
        prevRoutingTable = routingTable;
        routingTable = strategy.reroute(clusterState).routingTable();
        logger.info("Buidling new RoutingTable took " + (System.currentTimeMillis() - start) + "ms.");
        start = System.currentTimeMillis();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();
        logger.info("Buidling new ClusterState took " + (System.currentTimeMillis() - start) + "ms.");
        start = System.currentTimeMillis();
        routingNodes = clusterState.routingNodes();

    }
}