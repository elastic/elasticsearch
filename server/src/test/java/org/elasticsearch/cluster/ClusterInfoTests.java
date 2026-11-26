/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.cluster;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.AbstractChunkedSerializingTestCase;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.test.TransportVersionUtils;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.equalTo;

public class ClusterInfoTests extends AbstractWireSerializingTestCase<ClusterInfo> {

    public void testShardHeapUsageIsDefaultedForMissingShards() {
        ShardAndIndexHeapUsage defaultHeapUsage = randomShardAndIndexHeapUsage();
        ClusterInfo clusterInfo = ClusterInfo.builder()
            .estimatedShardHeapUsages(Map.of())
            .defaultShardHeapUsageForShardsWithoutMetrics(defaultHeapUsage)
            .build();

        assertThat(
            clusterInfo.getEstimatedShardHeapUsage(new ShardId(new Index(randomIndexName(), "_na_"), randomNonNegativeInt())),
            equalTo(defaultHeapUsage)
        );
    }

    public void testCacheUsageFieldsAreTransportVersionGated() throws Exception {
        final var shardCacheRequirements = Map.of(randomShardId(), new BoostedAndUnboostedCacheRequirements(10L, 20L));
        final var nodeCacheSizeAndCommitments = Map.of(randomIdentifier(), new NodeCacheSizeAndCommitments(100L, 10L, 30L));
        final var clusterInfo = ClusterInfo.builder()
            .shardCacheRequirements(shardCacheRequirements)
            .nodeCacheSizeAndCommitments(nodeCacheSizeAndCommitments)
            .build();

        final var currentVersionCopy = copyInstance(clusterInfo, TransportVersion.current());
        assertThat(currentVersionCopy.getShardCacheRequirements(), equalTo(shardCacheRequirements));
        assertThat(currentVersionCopy.getNodeCacheSizeAndCommitments(), equalTo(nodeCacheSizeAndCommitments));

        final var preCacheUsageVersion = TransportVersionUtils.getPreviousVersion(ClusterInfo.CACHE_METADATA_IN_CLUSTER_INFO);
        final var preCacheUsageCopy = copyInstance(clusterInfo, preCacheUsageVersion);
        assertThat(preCacheUsageCopy.getShardCacheRequirements(), equalTo(Map.of()));
        assertThat(preCacheUsageCopy.getNodeCacheSizeAndCommitments(), equalTo(Map.of()));
    }

    public void testHostedShardsFieldsAreTransportVersionGated() throws Exception {
        final var clusterInfo = ClusterInfo.builder().hostedShardsPartitionSizeByNodeId(randomHostedShardsPartitionSizes()).build();

        final var currentVersionCopy = copyInstance(clusterInfo, TransportVersion.current());
        assertThat(currentVersionCopy.getHostedShardsPartitionSizeByNodeId(), equalTo(clusterInfo.getHostedShardsPartitionSizeByNodeId()));

        final var preCacheUsageVersion = TransportVersionUtils.getPreviousVersion(ClusterInfo.PARTITION_SIZES_IN_CLUSTER_INFO);
        final var preCacheUsageCopy = copyInstance(clusterInfo, preCacheUsageVersion);
        assertThat(preCacheUsageCopy.getHostedShardsPartitionSizeByNodeId(), equalTo(Map.of()));
    }

    public void testSearchLaneRequirementsAreTransportVersionGated() throws Exception {
        final var clusterInfo = ClusterInfo.builder().shardSearchLaneRequirements(randomPerShardDoubles()).build();

        final var currentVersionCopy = copyInstance(clusterInfo, TransportVersion.current());
        assertThat(currentVersionCopy.getShardSearchLaneRequirements(), equalTo(clusterInfo.getShardSearchLaneRequirements()));

        final var preLaneVersion = TransportVersionUtils.getPreviousVersion(ClusterInfo.SEARCH_LANE_REQUIREMENTS_IN_CLUSTER_INFO);
        final var preLaneCopy = copyInstance(clusterInfo, preLaneVersion);
        assertThat(preLaneCopy.getShardSearchLaneRequirements(), equalTo(Map.of()));
    }

    public void testSearchLoadMetricsAreTransportVersionGated() throws Exception {
        final var nodeAndShard = new ClusterInfo.NodeAndShard(randomIdentifier(), randomShardId());
        final var searchCopyDemands = Map.of(nodeAndShard, new SearchCopyDemand(randomDouble(), randomDouble()));
        final var nodeSearchCapacities = Map.of(
            randomIdentifier(),
            new NodeSearchCapacity(randomDoubleBetween(0.1, 64.0, true), randomDoubleBetween(0.1, 64.0, true))
        );
        final var clusterInfo = ClusterInfo.builder()
            .searchCopyDemands(searchCopyDemands)
            .nodeSearchCapacities(nodeSearchCapacities)
            .build();

        final var currentVersionCopy = copyInstance(clusterInfo, TransportVersion.current());
        assertThat(currentVersionCopy.getSearchCopyDemands(), equalTo(searchCopyDemands));
        assertThat(currentVersionCopy.getNodeSearchCapacities(), equalTo(nodeSearchCapacities));

        final var preSearchLoadMetricsVersion = TransportVersionUtils.getPreviousVersion(ClusterInfo.SEARCH_LOAD_METRICS_IN_CLUSTER_INFO);
        final var preSearchLoadMetricsCopy = copyInstance(clusterInfo, preSearchLoadMetricsVersion);
        assertThat(preSearchLoadMetricsCopy.getSearchCopyDemands(), equalTo(Map.of()));
        assertThat(preSearchLoadMetricsCopy.getNodeSearchCapacities(), equalTo(Map.of()));
    }

    public void testSearchLoadsAreTransportVersionGated() throws Exception {
        final var clusterInfo = ClusterInfo.builder().shardSearchLoads(randomPerShardDoubles()).build();

        final var currentVersionCopy = copyInstance(clusterInfo, TransportVersion.current());
        assertThat(currentVersionCopy.getShardSearchLoads(), equalTo(clusterInfo.getShardSearchLoads()));

        final var preSearchLoadVersion = TransportVersionUtils.getPreviousVersion(ClusterInfo.SHARD_SEARCH_LOAD_IN_CLUSTER_INFO);
        final var preSearchLoadCopy = copyInstance(clusterInfo, preSearchLoadVersion);
        assertThat(preSearchLoadCopy.getShardSearchLoads(), equalTo(Map.of()));
    }

    @Override
    protected Writeable.Reader<ClusterInfo> instanceReader() {
        return ClusterInfo::new;
    }

    @Override
    protected ClusterInfo createTestInstance() {
        return randomClusterInfo();
    }

    @Override
    protected ClusterInfo mutateInstance(ClusterInfo instance) {
        var leastAvailableSpaceUsage = instance.getNodeLeastAvailableDiskUsages();
        var mostAvailableSpaceUsage = instance.getNodeMostAvailableDiskUsages();
        var shardSizes = instance.shardSizes;
        var shardDataSetSizes = instance.shardDataSetSizes;
        var dataPath = instance.dataPath;
        var reservedSpace = instance.reservedSpace;
        var nodeHeapMetrics = instance.getNodeHeapMetrics();
        var estimatedShardHeapUsages = instance.getEstimatedShardHeapUsages();
        var defaultShardHeapUsage = instance.getDefaultShardHeapUsageForShardsWithoutMetrics();
        var nodeUsageStatsForThreadPools = instance.getNodeUsageStatsForThreadPools();
        var shardWriteLoads = instance.getShardWriteLoads();
        var maxHeapSizePerNode = instance.getMaxHeapSizePerNode();
        var nodeIdsWriteLoadHotspotting = instance.nodeIdsWriteLoadHotspotting;
        var nodeCacheSizeAndCommitments = instance.getNodeCacheSizeAndCommitments();
        var shardCacheRequirements = instance.getShardCacheRequirements();
        var hostedShardsPartitionSizeByNodeId = instance.getHostedShardsPartitionSizeByNodeId();
        var shardSearchLaneRequirements = instance.getShardSearchLaneRequirements();
        var shardSearchLoads = instance.getShardSearchLoads();

        // Each serialized field is chosen independently so equals() is checked field by field.
        if (randomBoolean()) {
            leastAvailableSpaceUsage = randomValueOtherThan(leastAvailableSpaceUsage, ClusterInfoTests::randomDiskUsage);
        }
        if (randomBoolean()) {
            mostAvailableSpaceUsage = randomValueOtherThan(mostAvailableSpaceUsage, ClusterInfoTests::randomDiskUsage);
        }
        if (randomBoolean()) {
            shardSizes = randomValueOtherThan(shardSizes, ClusterInfoTests::randomShardSizes);
        }
        if (randomBoolean()) {
            shardDataSetSizes = randomValueOtherThan(shardDataSetSizes, ClusterInfoTests::randomDataSetSizes);
        }
        if (randomBoolean()) {
            dataPath = randomValueOtherThan(dataPath, ClusterInfoTests::randomRoutingToDataPath);
        }
        if (randomBoolean()) {
            reservedSpace = randomValueOtherThan(reservedSpace, ClusterInfoTests::randomReservedSpace);
        }
        if (randomBoolean()) {
            nodeHeapMetrics = randomValueOtherThan(nodeHeapMetrics, ClusterInfoTests::randomNodeHeapUsage);
        }
        if (randomBoolean()) {
            estimatedShardHeapUsages = randomValueOtherThan(estimatedShardHeapUsages, ClusterInfoTests::randomShardHeapUsages);
        }
        if (randomBoolean()) {
            defaultShardHeapUsage = randomValueOtherThan(defaultShardHeapUsage, ClusterInfoTests::randomShardAndIndexHeapUsage);
        }
        if (randomBoolean()) {
            nodeUsageStatsForThreadPools = randomValueOtherThan(
                nodeUsageStatsForThreadPools,
                ClusterInfoTests::randomNodeUsageStatsForThreadPools
            );
        }
        if (randomBoolean()) {
            shardWriteLoads = randomValueOtherThan(shardWriteLoads, ClusterInfoTests::randomPerShardDoubles);
        }
        if (randomBoolean()) {
            maxHeapSizePerNode = randomValueOtherThan(maxHeapSizePerNode, ClusterInfoTests::randomMaxHeapSizes);
        }
        if (randomBoolean()) {
            nodeIdsWriteLoadHotspotting = randomValueOtherThan(
                nodeIdsWriteLoadHotspotting,
                ClusterInfoTests::randomNodeIdsWriteLoadHotspottingSet
            );
        }
        if (randomBoolean()) {
            nodeCacheSizeAndCommitments = randomValueOtherThan(
                nodeCacheSizeAndCommitments,
                ClusterInfoTests::randomNodeCacheSizeAndCommitmentsMap
            );
        }
        if (randomBoolean()) {
            shardCacheRequirements = randomValueOtherThan(shardCacheRequirements, ClusterInfoTests::randomShardCacheRequirements);
        }
        if (randomBoolean()) {
            hostedShardsPartitionSizeByNodeId = randomValueOtherThan(
                hostedShardsPartitionSizeByNodeId,
                ClusterInfoTests::randomHostedShardsPartitionSizes
            );
        }
        if (randomBoolean()) {
            shardSearchLaneRequirements = randomValueOtherThan(shardSearchLaneRequirements, ClusterInfoTests::randomPerShardDoubles);
        }
        if (randomBoolean()) {
            shardSearchLoads = randomValueOtherThan(shardSearchLoads, ClusterInfoTests::randomPerShardDoubles);
        }

        var mutated = new ClusterInfo(
            leastAvailableSpaceUsage,
            mostAvailableSpaceUsage,
            shardSizes,
            shardDataSetSizes,
            dataPath,
            reservedSpace,
            nodeHeapMetrics,
            estimatedShardHeapUsages,
            defaultShardHeapUsage,
            nodeUsageStatsForThreadPools,
            shardWriteLoads,
            maxHeapSizePerNode,
            nodeIdsWriteLoadHotspotting,
            nodeCacheSizeAndCommitments,
            shardCacheRequirements,
            hostedShardsPartitionSizeByNodeId,
            shardSearchLaneRequirements,
            shardSearchLoads
        );
        // randomBoolean() can leave every field unchanged; equals/hashCode requires a difference.
        if (mutated.equals(instance)) {
            return new ClusterInfo(
                randomValueOtherThan(leastAvailableSpaceUsage, ClusterInfoTests::randomDiskUsage),
                mostAvailableSpaceUsage,
                shardSizes,
                shardDataSetSizes,
                dataPath,
                reservedSpace,
                nodeHeapMetrics,
                estimatedShardHeapUsages,
                defaultShardHeapUsage,
                nodeUsageStatsForThreadPools,
                shardWriteLoads,
                maxHeapSizePerNode,
                nodeIdsWriteLoadHotspotting,
                nodeCacheSizeAndCommitments,
                shardCacheRequirements,
                hostedShardsPartitionSizeByNodeId,
                shardSearchLaneRequirements,
                shardSearchLoads
            );
        }
        return mutated;
    }

    public static ClusterInfo randomClusterInfo() {
        return new ClusterInfo(
            randomDiskUsage(),
            randomDiskUsage(),
            randomShardSizes(),
            randomDataSetSizes(),
            randomRoutingToDataPath(),
            randomReservedSpace(),
            randomNodeHeapUsage(),
            randomShardHeapUsages(),
            randomShardAndIndexHeapUsage(),
            randomNodeUsageStatsForThreadPools(),
            randomPerShardDoubles(),
            randomMaxHeapSizes(),
            randomNodeIdsWriteLoadHotspottingSet(),
            randomNodeCacheSizeAndCommitmentsMap(),
            randomShardCacheRequirements(),
            randomHostedShardsPartitionSizes(),
            randomPerShardDoubles(),
            randomPerShardDoubles(),
            randomSearchCopyDemands(),
            randomNodeSearchCapacities()
        );
    }

    private static Map<ClusterInfo.NodeAndShard, SearchCopyDemand> randomSearchCopyDemands() {
        final int numEntries = randomIntBetween(0, 128);
        final Map<ClusterInfo.NodeAndShard, SearchCopyDemand> demands = new HashMap<>(numEntries);
        for (int i = 0; i < numEntries; i++) {
            demands.put(
                new ClusterInfo.NodeAndShard(randomIdentifier(), randomShardId()),
                new SearchCopyDemand(randomDouble(), randomDouble())
            );
        }
        return demands;
    }

    private static Map<String, NodeSearchCapacity> randomNodeSearchCapacities() {
        final int numEntries = randomIntBetween(0, 128);
        final Map<String, NodeSearchCapacity> capacities = new HashMap<>(numEntries);
        for (int i = 0; i < numEntries; i++) {
            capacities.put(
                randomIdentifier(),
                new NodeSearchCapacity(randomDoubleBetween(0.1, 64.0, true), randomDoubleBetween(0.1, 64.0, true))
            );
        }
        return capacities;
    }

    private static Map<ShardId, Double> randomPerShardDoubles() {
        final int numEntries = randomIntBetween(0, 128);
        final Map<ShardId, Double> builder = new HashMap<>(numEntries);
        for (int i = 0; i < numEntries; i++) {
            builder.put(randomShardId(), randomDouble());
        }
        return builder;
    }

    private static Map<String, NodeCacheSizeAndCommitments> randomNodeCacheSizeAndCommitmentsMap() {
        int numEntries = randomIntBetween(0, 128);
        Map<String, NodeCacheSizeAndCommitments> nodeCacheSizeAndCommitments = new HashMap<>(numEntries);
        for (int i = 0; i < numEntries; i++) {
            nodeCacheSizeAndCommitments.put(randomIdentifier(), NodeCacheSizeAndCommitmentsTests.randomNodeCacheSizeAndCommitments());
        }
        return nodeCacheSizeAndCommitments;
    }

    private static Map<ShardId, BoostedAndUnboostedCacheRequirements> randomShardCacheRequirements() {
        int numEntries = randomIntBetween(0, 128);
        Map<ShardId, BoostedAndUnboostedCacheRequirements> shardCacheRequirements = new HashMap<>(numEntries);
        for (int i = 0; i < numEntries; i++) {
            shardCacheRequirements.put(
                randomShardId(),
                new BoostedAndUnboostedCacheRequirements(randomNonNegativeLong(), randomNonNegativeLong())
            );
        }
        return shardCacheRequirements;
    }

    private static Map<String, ByteSizeValue> randomMaxHeapSizes() {
        int numEntries = randomIntBetween(0, 128);
        Map<String, ByteSizeValue> nodeMaxHeapSizes = new HashMap<>(numEntries);
        for (int i = 0; i < numEntries; i++) {
            nodeMaxHeapSizes.put(randomAlphaOfLength(32), randomByteSizeValue());
        }
        return nodeMaxHeapSizes;
    }

    private static Map<ShardId, ShardAndIndexHeapUsage> randomShardHeapUsages() {
        int numEntries = randomIntBetween(0, 128);
        Map<ShardId, ShardAndIndexHeapUsage> shardHeapUsageBuilder = new HashMap<>(numEntries);
        for (int i = 0; i < numEntries; i++) {
            shardHeapUsageBuilder.put(randomShardId(), randomShardAndIndexHeapUsage());
        }
        return shardHeapUsageBuilder;
    }

    private static ShardAndIndexHeapUsage randomShardAndIndexHeapUsage() {
        final long shardHeapUsageBytes = randomNonNegativeLong();
        final long postingsHeapUsageBytes = randomLongBetween(0, shardHeapUsageBytes);
        return new ShardAndIndexHeapUsage(shardHeapUsageBytes, randomNonNegativeLong(), postingsHeapUsageBytes);
    }

    private static Map<String, NodeHeapMetrics> randomNodeHeapUsage() {
        int numEntries = randomIntBetween(0, 128);
        Map<String, NodeHeapMetrics> nodeHeapUsage = new HashMap<>(numEntries);
        for (int i = 0; i < numEntries; i++) {
            String key = randomAlphaOfLength(32);
            final int maxHeapSize = randomIntBetween(0, Integer.MAX_VALUE);
            final long totalHeapUsage = randomLongBetween(0, maxHeapSize);
            final NodeHeapMetrics nodeHeapMetrics = new NodeHeapMetrics(
                randomAlphaOfLength(4),
                maxHeapSize,
                new NodeHeapEstimates(totalHeapUsage, randomLongBetween(0, totalHeapUsage), randomNonNegativeLong())
            );
            nodeHeapUsage.put(key, nodeHeapMetrics);
        }
        return nodeHeapUsage;
    }

    private static Map<String, NodeUsageStatsForThreadPools> randomNodeUsageStatsForThreadPools() {
        int numEntries = randomIntBetween(0, 128);
        Map<String, NodeUsageStatsForThreadPools> nodeUsageStatsForThreadPools = new HashMap<>(numEntries);
        for (int i = 0; i < numEntries; i++) {
            String nodeIdKey = randomAlphaOfLength(32);
            NodeUsageStatsForThreadPools.ThreadPoolUsageStats writeThreadPoolUsageStats =
                new NodeUsageStatsForThreadPools.ThreadPoolUsageStats(/* totalThreadPoolThreads= */ randomIntBetween(1, 16),
                    /* averageThreadPoolUtilization= */ randomFloat(),
                    /* maxThreadPoolQueueLatencyMillis= */ randomLongBetween(0, 50000)
                );
            Map<String, NodeUsageStatsForThreadPools.ThreadPoolUsageStats> usageStatsForThreadPools = new HashMap<>();
            usageStatsForThreadPools.put(ThreadPool.Names.WRITE, writeThreadPoolUsageStats);
            nodeUsageStatsForThreadPools.put(ThreadPool.Names.WRITE, new NodeUsageStatsForThreadPools(nodeIdKey, usageStatsForThreadPools));
        }
        return nodeUsageStatsForThreadPools;
    }

    private static Set<String> randomNodeIdsWriteLoadHotspottingSet() {
        int numEntries = randomIntBetween(0, 128);
        Set<String> nodeIdsWriteLoadHotspotting = new HashSet<>(numEntries);
        for (int i = 0; i < numEntries; i++) {
            String nodeId = randomAlphaOfLength(32);
            nodeIdsWriteLoadHotspotting.add(nodeId);
        }
        return nodeIdsWriteLoadHotspotting;
    }

    private static Map<String, DiskUsage> randomDiskUsage() {
        int numEntries = randomIntBetween(0, 128);
        Map<String, DiskUsage> builder = new HashMap<>(numEntries);
        for (int i = 0; i < numEntries; i++) {
            String key = randomAlphaOfLength(32);
            final int totalBytes = randomIntBetween(0, Integer.MAX_VALUE);
            DiskUsage diskUsage = new DiskUsage(
                randomAlphaOfLength(4),
                randomAlphaOfLength(4),
                randomAlphaOfLength(4),
                totalBytes,
                randomIntBetween(0, totalBytes)
            );
            builder.put(key, diskUsage);
        }
        return builder;
    }

    private static Map<String, Long> randomShardSizes() {
        int numEntries = randomIntBetween(0, 128);
        var builder = Maps.<String, Long>newMapWithExpectedSize(numEntries);
        for (int i = 0; i < numEntries; i++) {
            builder.put(ClusterInfo.shardIdentifierFromRouting(randomShardId(), randomBoolean()), randomLongBetween(0, Integer.MAX_VALUE));
        }
        return builder;
    }

    private static Map<ShardId, Long> randomDataSetSizes() {
        int numEntries = randomIntBetween(0, 128);
        var builder = Maps.<ShardId, Long>newMapWithExpectedSize(numEntries);
        for (int i = 0; i < numEntries; i++) {
            builder.put(randomShardId(), randomLongBetween(0, Integer.MAX_VALUE));
        }
        return builder;
    }

    private static ShardId randomShardId() {
        return new ShardId(randomAlphaOfLength(10), randomAlphaOfLength(10), between(0, Integer.MAX_VALUE));
    }

    private static Map<ClusterInfo.NodeAndShard, String> randomRoutingToDataPath() {
        int numEntries = randomIntBetween(0, 128);
        Map<ClusterInfo.NodeAndShard, String> builder = new HashMap<>(numEntries);
        for (int i = 0; i < numEntries; i++) {
            ShardId shardId = new ShardId(randomAlphaOfLength(32), randomAlphaOfLength(32), randomIntBetween(0, Integer.MAX_VALUE));
            builder.put(new ClusterInfo.NodeAndShard(randomAlphaOfLength(10), shardId), randomAlphaOfLength(32));
        }
        return builder;
    }

    private static Map<ClusterInfo.NodeAndPath, ClusterInfo.ReservedSpace> randomReservedSpace() {
        int numEntries = randomIntBetween(0, 128);
        Map<ClusterInfo.NodeAndPath, ClusterInfo.ReservedSpace> builder = new HashMap<>(numEntries);
        for (int i = 0; i < numEntries; i++) {
            final ClusterInfo.ReservedSpace.Builder valueBuilder = new ClusterInfo.ReservedSpace.Builder();
            for (int j = between(0, 10); j > 0; j--) {
                ShardId shardId = new ShardId(randomAlphaOfLength(32), randomAlphaOfLength(32), randomIntBetween(0, Integer.MAX_VALUE));
                valueBuilder.add(shardId, between(0, Integer.MAX_VALUE));
            }
            builder.put(new ClusterInfo.NodeAndPath(randomAlphaOfLength(10), randomAlphaOfLength(10)), valueBuilder.build());
        }
        return builder;
    }

    private static Map<String, Long> randomHostedShardsPartitionSizes() {
        int numEntries = randomIntBetween(0, 16);
        Map<String, Long> result = new HashMap<>(numEntries);
        for (int i = 0; i < numEntries; i++) {
            result.put(randomAlphaOfLength(10), randomNonNegativeLong());
        }
        return result;
    }

    public void testChunking() {
        AbstractChunkedSerializingTestCase.assertChunkCount(createTestInstance(), ClusterInfoTests::getChunkCount);
    }

    // exposing this to tests in other packages
    public static int getChunkCount(ClusterInfo clusterInfo) {
        return clusterInfo.getChunkCount();
    }
}
