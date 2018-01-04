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
package org.elasticsearch.benchmark.routing.allocation;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.settings.Settings;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

@Fork(3)
@Warmup(iterations = 10)
@Measurement(iterations = 10)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
@SuppressWarnings("unused") //invoked by benchmarking framework
public class AllocationBenchmark {
    // Do NOT make any field final (even if it is not annotated with @Param)! See also
    // http://hg.openjdk.java.net/code-tools/jmh/file/tip/jmh-samples/src/main/java/org/openjdk/jmh/samples/JMHSample_10_ConstantFold.java

    // we cannot use individual @Params as some will lead to invalid combinations which do not let the benchmark terminate. JMH offers no
    // support to constrain the combinations of benchmark parameters and we do not want to rely on OptionsBuilder as each benchmark would
    // need its own main method and we cannot execute more than one class with a main method per JAR.
    @Param({
        // indices| shards| replicas| nodes
        "       10|      1|        0|     1",
        "       10|      3|        0|     1",
        "       10|     10|        0|     1",
        "      100|      1|        0|     1",
        "      100|      3|        0|     1",
        "      100|     10|        0|     1",

        "       10|      1|        0|    10",
        "       10|      3|        0|    10",
        "       10|     10|        0|    10",
        "      100|      1|        0|    10",
        "      100|      3|        0|    10",
        "      100|     10|        0|    10",

        "       10|      1|        1|    10",
        "       10|      3|        1|    10",
        "       10|     10|        1|    10",
        "      100|      1|        1|    10",
        "      100|      3|        1|    10",
        "      100|     10|        1|    10",

        "       10|      1|        2|    10",
        "       10|      3|        2|    10",
        "       10|     10|        2|    10",
        "      100|      1|        2|    10",
        "      100|      3|        2|    10",
        "      100|     10|        2|    10",
        
        "       10|      1|        1|    20",
        "       10|      3|        1|    20",
        "       10|     10|        1|    20",
        "      100|      1|        1|    20",
        "      100|      3|        1|    20",
        "      100|     10|        1|    20",

        "       10|      1|        0|    50",
        "       10|      3|        0|    50",
        "       10|     10|        0|    50",
        "      100|      1|        0|    50",
        "      100|      3|        0|    50",
        "      100|     10|        0|    50",

        "       10|      1|        1|    50",
        "       10|      3|        1|    50",
        "       10|     10|        1|    50",
        "      100|      1|        1|    50",
        "      100|      3|        1|    50",
        "      100|     10|        1|    50",

        "       10|      1|        2|    50",
        "       10|      3|        2|    50",
        "       10|     10|        2|    50",
        "      100|      1|        2|    50",
        "      100|      3|        2|    50",
        "      100|     10|        2|    50",
        
        "       50|     60|        1|    200",
        "      500|     60|        1|    400"
        
    })
    public String indicesShardsReplicasNodes = "10|1|0|1";

    public int numTags = 2;
    public int numZone = 3;
    public int concurrentRecoveries = 6;
    public int numIndices;
    public int numShards;
    public int numReplicas;
    public int numNodes;

    private AllocationService strategy;
    private AllocationService clusterExcludeStrategy;
    private ClusterState initialClusterState;
    
    @Setup
    public void setUp() throws Exception {
        final String[] params = indicesShardsReplicasNodes.split("\\|");

        numIndices = toInt(params[0]);
        numShards = toInt(params[1]);
        numReplicas = toInt(params[2]);
        numNodes = toInt(params[3]);

        int totalShardCount = (numReplicas + 1) * numShards * numIndices;

        strategy = Allocators.createAllocationService(Settings.builder()
        .put("cluster.routing.allocation.awareness.attributes", "zone")
        .put("cluster.routing.allocation.node_concurrent_recoveries", "30")
        .put("cluster.routing.allocation.exclude.tag", "tag_0").build());
        
        //We'll try to move nodes from tag_1 to tag_0
        clusterExcludeStrategy = Allocators.createAllocationService(
        Settings.builder()
        .put("cluster.routing.allocation.node_concurrent_recoveries", String.valueOf(concurrentRecoveries))
        .put("cluster.routing.allocation.awareness.attributes", "zone")
        .put("cluster.routing.allocation.exclude.tag", "tag_1").build());

        MetaData.Builder mb = MetaData.builder();
        for (int i = 1; i <= numIndices; i++) {
            mb.put(IndexMetaData.builder("test_" + i).settings(Settings.builder().put("index.version.created", Version.CURRENT))
            .numberOfShards(numShards).numberOfReplicas(numReplicas));
        }
        MetaData metaData = mb.build();
        RoutingTable.Builder rb = RoutingTable.builder();
        for (int i = 1; i <= numIndices; i++) {
            rb.addAsNew(metaData.index("test_" + i));
        }
        RoutingTable routingTable = rb.build();
        DiscoveryNodes.Builder nb = DiscoveryNodes.builder();
        for (int i = 1; i <= numNodes; i++) {
            Map<String, String> attributes = new HashMap<>();
            attributes.put("tag", "tag_" + (i % numTags));
            attributes.put("zone", "zone_" + (i % numZone));
            nb.add(Allocators.newNode("node_" + i, attributes));
        }

        initialClusterState = ClusterState.builder(ClusterName.CLUSTER_NAME_SETTING.getDefault(Settings.EMPTY)).metaData(metaData)
        .routingTable(routingTable).nodes(nb).build();
        // Start all unassigned shards
        while (initialClusterState.getRoutingNodes().hasUnassignedShards()) {
            initialClusterState = strategy.applyStartedShards(initialClusterState,
            initialClusterState.getRoutingNodes().shardsWithState(ShardRoutingState.INITIALIZING));
            initialClusterState = strategy.reroute(initialClusterState, "reroute");
        }
        // Ensure all shards are started
        while (initialClusterState.getRoutingNodes().shardsWithState(ShardRoutingState.INITIALIZING).size() > 0) {
            initialClusterState = strategy.applyStartedShards(initialClusterState,
            initialClusterState.getRoutingNodes().shardsWithState(ShardRoutingState.INITIALIZING));

        }
        assert(initialClusterState.getRoutingNodes().shardsWithState(ShardRoutingState.STARTED).size() == totalShardCount);
        assert(initialClusterState.getRoutingNodes().shardsWithState(ShardRoutingState.INITIALIZING).size() == 0);
        assert(initialClusterState.getRoutingNodes().shardsWithState(ShardRoutingState.RELOCATING).size() == 0);
        // make sure shards are only allocated on tag1
        for (ShardRouting startedShard : initialClusterState.getRoutingNodes().shardsWithState(ShardRoutingState.STARTED)) {
            assert(Integer.valueOf(startedShard.currentNodeId().split("_")[1]) % numTags == 1);
        }
        // Ensure shards are relocating at max concurrent recoveries
        ClusterState clusterState = initialClusterState;
        clusterState = clusterExcludeStrategy.applyStartedShards(clusterState,
        clusterState.getRoutingNodes().shardsWithState(ShardRoutingState.INITIALIZING));
        clusterState = clusterExcludeStrategy.reroute(clusterState, "reroute");
        assert (clusterState.getRoutingNodes().shardsWithState(ShardRoutingState.RELOCATING).size() == numNodes / numTags
        * concurrentRecoveries);

    }

    private int toInt(String v) {
        return Integer.valueOf(v.trim());
    }

    @Benchmark
    public ClusterState measureExclusionAllocationApplyStartedShardZoneAware() {
        ClusterState clusterState = initialClusterState;
        clusterState = clusterExcludeStrategy.applyStartedShards(clusterState,
        clusterState.getRoutingNodes().shardsWithState(ShardRoutingState.INITIALIZING));
        clusterState = clusterExcludeStrategy.reroute(clusterState, "reroute");
        return clusterState;

    }
    
    @Benchmark
    public ClusterState measureExclusionAllocationApplyStartedShardZoneUnaware() throws Exception{
        AllocationService clusterExcludeZoneUnawareStrategy = Allocators.createAllocationService(
        Settings.builder()
        .put("cluster.routing.allocation.node_concurrent_recoveries", String.valueOf(concurrentRecoveries))
        .put("cluster.routing.allocation.exclude.tag", "tag_1").build());
        ClusterState clusterState = initialClusterState;
        clusterState = clusterExcludeZoneUnawareStrategy.applyStartedShards(clusterState,
        clusterState.getRoutingNodes().shardsWithState(ShardRoutingState.INITIALIZING));
        clusterState = clusterExcludeZoneUnawareStrategy.reroute(clusterState, "reroute");
        return clusterState;

    }

   

}