/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.benchmark.routing.allocation;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
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
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

@Fork(3)
@Warmup(iterations = 10)
@Measurement(iterations = 10)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
@SuppressWarnings("unused") // invoked by benchmarking framework
public class AllocationBenchmark {
    // Do NOT make any field final (even if it is not annotated with @Param)! See also
    // http://hg.openjdk.java.net/code-tools/jmh/file/tip/jmh-samples/src/main/java/org/openjdk/jmh/samples/JMHSample_10_ConstantFold.java

    // we cannot use individual @Params as some will lead to invalid combinations which do not let the benchmark terminate. JMH offers no
    // support to constrain the combinations of benchmark parameters and we do not want to rely on OptionsBuilder as each benchmark would
    // need its own main method and we cannot execute more than one class with a main method per JAR.
    @Param(
        {
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
            "      100|     10|        2|    50" }
    )
    public String indicesShardsReplicasNodes = "10|1|0|1";

    public int numTags = 2;

    private AllocationService strategy;
    private ClusterState initialClusterState;

    @Setup
    public void setUp() throws Exception {
        final String[] params = indicesShardsReplicasNodes.split("\\|");

        int numIndices = toInt(params[0]);
        int numShards = toInt(params[1]);
        int numReplicas = toInt(params[2]);
        int numNodes = toInt(params[3]);

        strategy = Allocators.createAllocationService(
            Settings.builder().put("cluster.routing.allocation.awareness.attributes", "tag").build()
        );

        Metadata.Builder mb = Metadata.builder();
        for (int i = 1; i <= numIndices; i++) {
            mb.put(
                IndexMetadata.builder("test_" + i)
                    .settings(Settings.builder().put("index.version.created", Version.CURRENT))
                    .numberOfShards(numShards)
                    .numberOfReplicas(numReplicas)
            );
        }
        Metadata metadata = mb.build();
        RoutingTable.Builder rb = RoutingTable.builder(TestShardRoutingRoleStrategies.DEFAULT_ROLE_ONLY);
        for (int i = 1; i <= numIndices; i++) {
            rb.addAsNew(metadata.index("test_" + i));
        }
        RoutingTable routingTable = rb.build();
        DiscoveryNodes.Builder nb = DiscoveryNodes.builder();
        Map<String, TransportVersion> transportVersions = new HashMap<>();
        for (int i = 1; i <= numNodes; i++) {
            String id = "node" + i;
            nb.add(Allocators.newNode(id, Collections.singletonMap("tag", "tag_" + (i % numTags))));
            transportVersions.put(id, TransportVersion.CURRENT);
        }
        initialClusterState = ClusterState.builder(ClusterName.DEFAULT)
            .metadata(metadata)
            .routingTable(routingTable)
            .nodes(nb)
            .transportVersions(transportVersions)
            .build();
    }

    private int toInt(String v) {
        return Integer.valueOf(v.trim());
    }

    /**
     * Once we use DesiredBalanceShardsAllocator this only measures reconciliation, not the balance calculation
     */
    @Benchmark
    public ClusterState measureAllocation() {
        ClusterState clusterState = initialClusterState;
        while (clusterState.getRoutingNodes().hasUnassignedShards()) {
            clusterState = strategy.applyStartedShards(
                clusterState,
                clusterState.getRoutingNodes()
                    .stream()
                    .flatMap(shardRoutings -> StreamSupport.stream(shardRoutings.spliterator(), false))
                    .filter(ShardRouting::initializing)
                    .collect(Collectors.toList())
            );
            clusterState = strategy.reroute(clusterState, "reroute", ActionListener.noop());
        }
        return clusterState;
    }
}
