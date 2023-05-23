/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.benchmark.routing.allocation;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.cluster.routing.allocation.DataTier;
import org.elasticsearch.cluster.routing.allocation.ShardsAvailabilityHealthIndicatorService;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.health.HealthIndicatorResult;
import org.elasticsearch.health.node.HealthInfo;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.SystemIndices;
import org.elasticsearch.tasks.TaskManager;
import org.elasticsearch.threadpool.ThreadPool;
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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.cluster.metadata.IndexMetadata.INDEX_ROUTING_REQUIRE_GROUP_SETTING;

@Fork(3)
@Warmup(iterations = 10)
@Measurement(iterations = 10)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
@SuppressWarnings("unused") // invoked by benchmarking framework
public class ShardsAvailabilityHealthIndicatorBenchmark {

    @Param(
        {
            // indices| shards| replicas| nodes
            "     10000|        1|        0|    3",
            "     20000|        1|        0|    3",
            "     50000|        1|        0|    3",
            "     100000|       1|        0|    3",
            "     200000|       1|        0|    3",

            "     10000|        1|        1|    3",
            "     20000|        1|        1|    3",
            "     50000|        1|        1|    3",
            "     100000|       1|        1|    3",
            "     200000|       1|        1|    3" }
    )
    public String indicesShardsReplicasNodes = "10|1|0|1";

    private ShardsAvailabilityHealthIndicatorService indicatorService;

    @Setup
    public void setUp() throws Exception {
        final String[] params = indicesShardsReplicasNodes.split("\\|");

        int numIndices = toInt(params[0]);
        int numShards = toInt(params[1]);
        int numReplicas = toInt(params[2]);
        int numNodes = toInt(params[3]);

        AllocationService allocationService = Allocators.createAllocationService(Settings.EMPTY);

        Metadata.Builder mb = Metadata.builder();
        RoutingTable.Builder rb = RoutingTable.builder();

        DiscoveryNodes.Builder nb = DiscoveryNodes.builder();
        Set<String> failedNodeIds = new HashSet<>();
        for (int i = 1; i <= numNodes; i++) {
            String nodeId = "node" + i;
            nb.add(Allocators.newNode(nodeId, Map.of()));
            failedNodeIds.add(nodeId);
        }

        UnassignedInfo decidersNoUnassignedInfo = new UnassignedInfo(
            UnassignedInfo.Reason.ALLOCATION_FAILED,
            null,
            null,
            failedNodeIds.size(),
            System.nanoTime(),
            System.currentTimeMillis(),
            false,
            UnassignedInfo.AllocationStatus.DECIDERS_NO,
            failedNodeIds,
            null
        );

        RoutingTable.Builder routingTable = RoutingTable.builder();
        for (int i = 1; i <= numIndices; i++) {
            IndexMetadata indexMetadata = IndexMetadata.builder("test_" + i)
                .settings(
                    Settings.builder()
                        .put("index.version.created", Version.CURRENT)
                        .put(DataTier.TIER_PREFERENCE_SETTING.getKey(), "data_warm")
                        .put(INDEX_ROUTING_REQUIRE_GROUP_SETTING.getKey() + "data", "warm")
                )
                .numberOfShards(numShards)
                .numberOfReplicas(numReplicas)
                .build();

            final IndexRoutingTable.Builder indexRountingTableBuilder = IndexRoutingTable.builder(indexMetadata.getIndex());
            for (int shardIdNumber = 0; shardIdNumber < numShards; shardIdNumber++) {
                ShardId shardId = new ShardId(indexMetadata.getIndex(), shardIdNumber);
                final IndexShardRoutingTable.Builder shardBuilder = new IndexShardRoutingTable.Builder(shardId);
                ShardRouting shardRouting = ShardRouting.newUnassigned(
                    shardId,
                    true,
                    RecoverySource.ExistingStoreRecoverySource.INSTANCE,
                    decidersNoUnassignedInfo,
                    ShardRouting.Role.DEFAULT
                );
                shardBuilder.addShard(shardRouting);
                if (shardIdNumber < numReplicas) {
                    shardBuilder.addShard(
                        ShardRouting.newUnassigned(
                            shardId,
                            false,
                            RecoverySource.EmptyStoreRecoverySource.INSTANCE,
                            decidersNoUnassignedInfo,
                            ShardRouting.Role.DEFAULT
                        )
                    );
                }
                indexRountingTableBuilder.addIndexShard(shardBuilder);
            }

            routingTable.add(indexRountingTableBuilder);
            mb.put(indexMetadata, false);
        }

        ClusterState initialClusterState = ClusterState.builder(ClusterName.DEFAULT)
            .metadata(mb)
            .routingTable(routingTable)
            .nodes(nb)
            .build();

        Settings settings = Settings.builder().put("node.name", ShardsAvailabilityHealthIndicatorBenchmark.class.getSimpleName()).build();
        ThreadPool threadPool = new ThreadPool(settings);

        ClusterService clusterService = new ClusterService(
            Settings.EMPTY,
            new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS),
            threadPool,
            new TaskManager(Settings.EMPTY, threadPool, Collections.emptySet())
        );
        clusterService.getClusterApplierService().setInitialState(initialClusterState);
        indicatorService = new ShardsAvailabilityHealthIndicatorService(clusterService, allocationService, new SystemIndices(List.of()));
    }

    private int toInt(String v) {
        return Integer.parseInt(v.trim());
    }

    @Benchmark
    public HealthIndicatorResult measureCalculate() {
        return indicatorService.calculate(true, HealthInfo.EMPTY_HEALTH_INFO);
    }

}
