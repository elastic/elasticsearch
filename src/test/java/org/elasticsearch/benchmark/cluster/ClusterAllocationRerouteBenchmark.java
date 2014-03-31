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
package org.elasticsearch.benchmark.cluster;

import com.google.common.collect.ImmutableMap;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.test.ElasticsearchAllocationTestCase;

import java.util.Random;

import static org.elasticsearch.cluster.routing.ShardRoutingState.INITIALIZING;

public class ClusterAllocationRerouteBenchmark {

    private static final ESLogger logger = Loggers.getLogger(ClusterAllocationRerouteBenchmark.class);

    public static void main(String[] args) {
        final int numberOfRuns = 1;
        final int numIndices = 5 * 365; // five years
        final int numShards = 6;
        final int numReplicas = 2;
        final int numberOfNodes = 30;
        final int numberOfTags = 2;
        AllocationService strategy = ElasticsearchAllocationTestCase.createAllocationService(ImmutableSettings.builder()
                .put("cluster.routing.allocation.awareness.attributes", "tag")
                .build(), new Random(1));

        MetaData.Builder mb = MetaData.builder();
        for (int i = 1; i <= numIndices; i++) {
            mb.put(IndexMetaData.builder("test_" + i).numberOfShards(numShards).numberOfReplicas(numReplicas));
        }
        MetaData metaData = mb.build();
        RoutingTable.Builder rb = RoutingTable.builder();
        for (int i = 1; i <= numIndices; i++) {
            rb.addAsNew(metaData.index("test_" + i));
        }
        RoutingTable routingTable = rb.build();
        DiscoveryNodes.Builder nb = DiscoveryNodes.builder();
        for (int i = 1; i <= numberOfNodes; i++) {
            nb.put(ElasticsearchAllocationTestCase.newNode("node" + i, numberOfTags == 0 ? ImmutableMap.<String, String>of() : ImmutableMap.of("tag", "tag_" + (i % numberOfTags))));
        }
        ClusterState initialClusterState = ClusterState.builder(ClusterName.DEFAULT).metaData(metaData).routingTable(routingTable).nodes(nb).build();

        long start = System.currentTimeMillis();
        for (int i = 0; i < numberOfRuns; i++) {
            logger.info("[{}] starting... ", i);
            long runStart = System.currentTimeMillis();
            ClusterState clusterState = initialClusterState;
            while (clusterState.readOnlyRoutingNodes().hasUnassignedShards()) {
                logger.info("[{}] remaining unassigned {}", i, clusterState.readOnlyRoutingNodes().unassigned().size());
                RoutingAllocation.Result result = strategy.applyStartedShards(clusterState, clusterState.readOnlyRoutingNodes().shardsWithState(INITIALIZING));
                clusterState = ClusterState.builder(clusterState).routingResult(result).build();
                result = strategy.reroute(clusterState);
                clusterState = ClusterState.builder(clusterState).routingResult(result).build();
            }
            logger.info("[{}] took {}", i, TimeValue.timeValueMillis(System.currentTimeMillis() - runStart));
        }
        long took = System.currentTimeMillis() - start;
        logger.info("total took {}, AVG {}", TimeValue.timeValueMillis(took), TimeValue.timeValueMillis(took / numberOfRuns));
    }
}