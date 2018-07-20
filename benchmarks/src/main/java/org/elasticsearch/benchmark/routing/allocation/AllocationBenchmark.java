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

import org.apache.log4j.Logger;
import org.apache.logging.log4j.LogManager;
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
import org.elasticsearch.cluster.routing.allocation.decider.ConcurrentRecoveriesAllocationDecider;
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
@Warmup(iterations = 3)
@Measurement(iterations = 3)
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
     //    indices| shards| replicas|  source| target| concurrentRecoveries
        "       10|      2|        0|       1|      1|      1|",
        "       10|      3|        0|       1|      1|      2|",
        "       10|     10|        0|       1|      1|      5|",
        "      100|      1|        0|       1|      1|     10|",
        "      100|      3|        0|       1|      1|     10|",
        "      100|     10|        0|       1|      1|     10|",

        "       10|      2|        0|      10|     10|      1|",
        "       10|      3|        0|      10|      5|      2|",
        "       10|     10|        0|      10|      5|      5|",
        "      100|      1|        0|       5|     10|      5|",
        "      100|      3|        0|      10|      5|      5|",
        "      100|     10|        0|      10|     20|      6|",

        "       10|      1|        1|      10|     10|      1|",
        "       10|      3|        1|      10|      3|      3|",
        "       10|     10|        1|       5|     12|      5|",
        "      100|      1|        1|      10|     10|      6|",
        "      100|      3|        1|      10|      5|      8|",
        "      100|     10|        1|       8|     17|      8|",

        "       10|      1|        2|      10|     10|      1|",
        "       10|      3|        2|      10|      5|      3|",
        "       10|     10|        2|       5|     10|      5|",
        "      100|      1|        2|      10|      8|      7|",
        "      100|      3|        2|      13|     17|      5|",
        "      100|     10|        2|      10|     20|      8|",
        
        "       10|      2|        1|      20|     20|      1|",
        "       10|      3|        1|      20|     30|      1|",
        "       10|     10|        1|      20|     10|      3|",
        "      100|      1|        1|      20|      5|      5|",
        "      100|      3|        1|      20|     23|      6|",
        "      100|     10|        1|      40|     20|      8|",

        "       10|      3|        2|      50|     30|      1|",
        "       10|      3|        2|      50|     25|      1|",
        "       10|     10|        1|      50|     33|      2|",
        "      100|      1|        1|      40|     50|      2|",
        "      100|      3|        1|      50|     70|      3|",
        "      100|     10|        1|      60|     50|      3|",

        "       10|     10|        2|      50|     50|      1|",
        "       10|      3|        2|      50|     30|      1|",
        "       10|     10|        2|      50|     40|      2|",
        "      100|      1|        2|      40|     50|      2|",
        "      100|      3|        2|      50|     30|      6|",
        "      100|     10|        2|      33|     55|      6|",
        
        "       500|     60|       1|     100|    100|     12|",
        "       500|     60|       1|     100|     40|     12|",
        "       500|     60|       1|      40|    100|     12|",
        
        "       50|      60|       1|     100|    100|      6|",
        "       50|      60|       1|     100|     40|      6|", 
        "       50|      60|       1|      40|    100|      6|"
    })
    public String indicesShardsReplicasSourceTargetRecoveries = "10|1|0|1|1|1";
    public int numTags = 2;
    public int numZone = 3;
    public int concurrentRecoveries;
    public int numIndices;
    public int numShards;
    public int numReplicas;
    public int sourceNodes;
    public int targetNodes;
    public int clusterConcurrentRecoveries;

    private AllocationService initialClusterStrategy;
    private AllocationService clusterExcludeStrategy;
    private AllocationService clusterZoneAwareExcludeStrategy;
    private ClusterState initialClusterState;
    private static org.apache.logging.log4j.Logger LOG = LogManager.getLogger(AllocationBenchmark.class);
    
    @Setup
    public void setUp() throws Exception {
        final String[] params = indicesShardsReplicasSourceTargetRecoveries.split("\\|");
        numIndices = toInt(params[0]);
        numShards = toInt(params[1]);
        numReplicas = toInt(params[2]);
        sourceNodes = toInt(params[3]);
        targetNodes = toInt(params[4]);
        concurrentRecoveries = toInt(params[5]);

        int totalShardCount = (numReplicas + 1) * numShards * numIndices;
        
        initialClusterStrategy = Allocators.createAllocationService(Settings.builder()
        .put("cluster.routing.allocation.awareness.attributes", "zone")
        .put("cluster.routing.allocation.node_concurrent_recoveries", "20")
        .put("cluster.routing.allocation.exclude.tag", "tag_0").build());
        
        //We'll try to move nodes from tag_1 to tag_0
        clusterConcurrentRecoveries = Math.min(sourceNodes, targetNodes) * concurrentRecoveries;
       
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
        initialClusterState = ClusterState.builder(ClusterName.CLUSTER_NAME_SETTING.getDefault(Settings.EMPTY)).metaData(metaData)
        .routingTable(routingTable).nodes(setUpClusterNodes(sourceNodes, targetNodes)).build();
        // Start all unassigned shards
        initialClusterState = initialClusterStrategy.reroute(initialClusterState, "reroute");
        while (initialClusterState.getRoutingNodes().hasUnassignedShards()) {
            initialClusterState = initialClusterStrategy.applyStartedShards(initialClusterState,
            initialClusterState.getRoutingNodes().shardsWithState(ShardRoutingState.INITIALIZING));
            
        }
        // Ensure all shards are started
        while (initialClusterState.getRoutingNodes().shardsWithState(ShardRoutingState.INITIALIZING).size() > 0) {
            initialClusterState = initialClusterStrategy.applyStartedShards(initialClusterState,
            initialClusterState.getRoutingNodes().shardsWithState(ShardRoutingState.INITIALIZING));
        }
        
        assert(initialClusterState.getRoutingNodes().shardsWithState(ShardRoutingState.STARTED).size() == totalShardCount);
        assert(initialClusterState.getRoutingNodes().shardsWithState(ShardRoutingState.INITIALIZING).size() == 0);
        assert(initialClusterState.getRoutingNodes().shardsWithState(ShardRoutingState.RELOCATING).size() == 0);
        // make sure shards are only allocated on tag1
        for (ShardRouting startedShard : initialClusterState.getRoutingNodes().shardsWithState(ShardRoutingState.STARTED)) {
            assert (initialClusterState.getRoutingNodes().node(startedShard.currentNodeId()).node().getAttributes().get("tag"))
            .equals("tag_1");
        }
        
    }

    private int toInt(String v) {
        return Integer.valueOf(v.trim());
    }
    

    @Benchmark
    public ClusterState measureExclusionOnZoneAwareStartedShard() throws Exception {
        ClusterState clusterState = initialClusterState;
        clusterZoneAwareExcludeStrategy = Allocators
        .createAllocationService(Settings.builder().put("cluster.routing.allocation.awareness.attributes", "zone")
        .put("cluster.routing.allocation.cluster_concurrent_recoveries",String.valueOf(clusterConcurrentRecoveries))
        .put("cluster.routing.allocation.node_concurrent_recoveries", String.valueOf(concurrentRecoveries))
        .put("cluster.routing.allocation.exclude.tag", "tag_1").build());
        clusterState = clusterZoneAwareExcludeStrategy.reroute(clusterState, "reroute");
        return clusterState;

    }
    
    @Benchmark
    public ClusterState measureShardRelocationComplete() throws Exception{
        ClusterState clusterState = initialClusterState;
        clusterZoneAwareExcludeStrategy = Allocators
        .createAllocationService(Settings.builder().put("cluster.routing.allocation.awareness.attributes", "zone")
        .put("cluster.routing.allocation.node_concurrent_recoveries", String.valueOf(concurrentRecoveries))
        .put("cluster.routing.allocation.cluster_concurrent_recoveries",String.valueOf(clusterConcurrentRecoveries))
        .put("cluster.routing.allocation.exclude.tag", "tag_1").build());
        clusterState = clusterZoneAwareExcludeStrategy.reroute(clusterState, "reroute");
        while (clusterState.getRoutingNodes().shardsWithState(ShardRoutingState.INITIALIZING).size() > 0) {
            clusterState = clusterZoneAwareExcludeStrategy.applyStartedShards(clusterState,
            clusterState.getRoutingNodes().shardsWithState(ShardRoutingState.INITIALIZING));
        }
        for (ShardRouting startedShard : clusterState.getRoutingNodes().shardsWithState(ShardRoutingState.STARTED)) {
            assert (clusterState.getRoutingNodes().node(startedShard.currentNodeId()).node().getAttributes().get("tag"))
            .equals("tag_0");
        }
        return clusterState;

    
    }
    
    private DiscoveryNodes.Builder setUpClusterNodes(int sourceNodes, int targetNodes) {
        DiscoveryNodes.Builder nb = DiscoveryNodes.builder();
        for (int i = 1; i <= sourceNodes; i++) {
            Map<String, String> attributes = new HashMap<>();
            attributes.put("tag", "tag_" + 1);
            attributes.put("zone", "zone_" + (i % numZone));
            nb.add(Allocators.newNode("node_s_" + i, attributes));
        }
        for (int j = 1; j <= targetNodes; j++) {
            Map<String, String> attributes = new HashMap<>();
            attributes.put("tag", "tag_" + 0);
            attributes.put("zone", "zone_" + (j % numZone));
            nb.add(Allocators.newNode("node_t_" + j, attributes));
        }
        return nb;
    }
}