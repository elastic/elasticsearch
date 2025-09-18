/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.routing.allocation.decider;

import org.elasticsearch.action.admin.cluster.node.usage.NodeUsageStatsForThreadPoolsAction;
import org.elasticsearch.action.admin.cluster.node.usage.TransportNodeUsageStatsForThreadPoolsAction;
import org.elasticsearch.action.admin.indices.stats.CommonStats;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsAction;
import org.elasticsearch.action.admin.indices.stats.ShardStats;
import org.elasticsearch.action.admin.indices.stats.TransportIndicesStatsAction;
import org.elasticsearch.cluster.NodeUsageStatsForThreadPools;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.cluster.routing.RoutingNodes;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.cluster.routing.allocation.WriteLoadConstraintSettings;
import org.elasticsearch.cluster.routing.allocation.allocator.DesiredBalanceMetrics;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.DocsStats;
import org.elasticsearch.index.shard.IndexingStats;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.ShardPath;
import org.elasticsearch.index.store.StoreStats;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.PluginsService;
import org.elasticsearch.telemetry.TestTelemetryPlugin;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

import static java.util.stream.IntStream.range;
import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_REPLICAS;
import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_SHARDS;
import static org.elasticsearch.cluster.routing.ShardMovementWriteLoadSimulator.calculateUtilizationForWriteLoad;
import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasSize;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0)
public class WriteLoadConstraintDeciderIT extends ESIntegTestCase {

    @Override
    @SuppressWarnings("unchecked")
    protected Collection<Class<? extends Plugin>> getMockPlugins() {
        return CollectionUtils.appendToCopyNoNullElements(
            super.nodePlugins(),
            MockTransportService.TestPlugin.class,
            TestTelemetryPlugin.class
        );
    }

    /**
     * Uses MockTransportService to set up write load stat responses from the data nodes and tests the allocation decisions made by the
     * balancer, specifically the effect of the {@link WriteLoadConstraintDecider}.
     *
     * Leverages the {@link FilterAllocationDecider} to first start all shards on a Node1, and then eventually force the shards off of
     * Node1 while Node3 is hot-spotting, resulting in reassignment of all shards to Node2.
     */
    public void testHighNodeWriteLoadPreventsNewShardAllocation() {
        int randomUtilizationThresholdPercent = randomIntBetween(50, 100);
        int numberOfWritePoolThreads = randomIntBetween(2, 20);
        float shardWriteLoad = randomFloatBetween(0.0f, 0.01f, false);
        Settings settings = Settings.builder()
            .put(
                WriteLoadConstraintSettings.WRITE_LOAD_DECIDER_ENABLED_SETTING.getKey(),
                WriteLoadConstraintSettings.WriteLoadDeciderStatus.ENABLED
            )
            .put(
                WriteLoadConstraintSettings.WRITE_LOAD_DECIDER_HIGH_UTILIZATION_THRESHOLD_SETTING.getKey(),
                randomUtilizationThresholdPercent + "%"
            )
            .build();

        final String masterName = internalCluster().startMasterOnlyNode(settings);
        final var dataNodes = internalCluster().startDataOnlyNodes(3, settings);
        final String firstDataNodeName = dataNodes.get(0);
        final String secondDataNodeName = dataNodes.get(1);
        final String thirdDataNodeName = dataNodes.get(2);
        final String firstDataNodeId = getNodeId(firstDataNodeName);
        final String secondDataNodeId = getNodeId(secondDataNodeName);
        final String thirdDataNodeId = getNodeId(thirdDataNodeName);
        ensureStableCluster(4);

        logger.info(
            "---> first node name "
                + firstDataNodeName
                + " and ID "
                + firstDataNodeId
                + "; second node name "
                + secondDataNodeName
                + " and ID "
                + secondDataNodeId
                + "; third node name "
                + thirdDataNodeName
                + " and ID "
                + thirdDataNodeId
        );

        /**
         * Exclude assignment of shards to the second and third data nodes via the {@link FilterAllocationDecider} settings.
         * Then create an index with many shards, which will all be assigned to the first data node.
         */

        logger.info("---> Limit shard assignment to node " + firstDataNodeName + " by excluding the other nodes");
        updateClusterSettings(
            Settings.builder().put("cluster.routing.allocation.exclude._name", secondDataNodeName + "," + thirdDataNodeName)
        );

        String indexName = randomIdentifier();
        int randomNumberOfShards = randomIntBetween(10, 20); // Pick a high number of shards, so it is clear assignment is not accidental.

        // Calculate the maximum utilization a node can report while still being able to accept all relocating shards
        double additionalLoadFromAllShards = calculateUtilizationForWriteLoad(
            shardWriteLoad * randomNumberOfShards,
            numberOfWritePoolThreads
        );
        int maxUtilizationPercent = randomUtilizationThresholdPercent - (int) (additionalLoadFromAllShards * 100) - 1;

        var verifyAssignmentToFirstNodeListener = ClusterServiceUtils.addMasterTemporaryStateListener(clusterState -> {
            var indexRoutingTable = clusterState.routingTable().index(indexName);
            if (indexRoutingTable == null) {
                return false;
            }
            return checkShardAssignment(
                clusterState.getRoutingNodes(),
                indexRoutingTable.getIndex(),
                firstDataNodeId,
                secondDataNodeId,
                thirdDataNodeId,
                randomNumberOfShards,
                0,
                0
            );
        });

        createIndex(
            indexName,
            Settings.builder().put(SETTING_NUMBER_OF_SHARDS, randomNumberOfShards).put(SETTING_NUMBER_OF_REPLICAS, 0).build()
        );
        ensureGreen(indexName);

        logger.info("---> Waiting for all shards to be assigned to node " + firstDataNodeName);
        safeAwait(verifyAssignmentToFirstNodeListener);

        /**
         * Override the {@link TransportNodeUsageStatsForThreadPoolsAction} and {@link TransportIndicesStatsAction} actions on the data
         * nodes to supply artificial write load stats. The stats will show the third node hot-spotting, and that all shards have non-empty
         * write load stats (so that the WriteLoadDecider will evaluate assigning them to a node).
         */

        final DiscoveryNode firstDiscoveryNode = getDiscoveryNode(firstDataNodeName);
        final DiscoveryNode secondDiscoveryNode = getDiscoveryNode(secondDataNodeName);
        final DiscoveryNode thirdDiscoveryNode = getDiscoveryNode(thirdDataNodeName);
        final NodeUsageStatsForThreadPools firstNodeNonHotSpottingNodeStats = createNodeUsageStatsForThreadPools(
            firstDiscoveryNode,
            numberOfWritePoolThreads,
            randomIntBetween(0, maxUtilizationPercent) / 100f,
            0
        );
        final NodeUsageStatsForThreadPools secondNodeNonHotSpottingNodeStats = createNodeUsageStatsForThreadPools(
            secondDiscoveryNode,
            numberOfWritePoolThreads,
            randomIntBetween(0, maxUtilizationPercent) / 100f,
            0
        );
        final NodeUsageStatsForThreadPools thirdNodeHotSpottingNodeStats = createNodeUsageStatsForThreadPools(
            thirdDiscoveryNode,
            numberOfWritePoolThreads,
            (randomUtilizationThresholdPercent + 1) / 100f,
            0
        );

        MockTransportService.getInstance(firstDataNodeName).<NodeUsageStatsForThreadPoolsAction.NodeRequest>addRequestHandlingBehavior(
            TransportNodeUsageStatsForThreadPoolsAction.NAME + "[n]",
            (handler, request, channel, task) -> channel.sendResponse(
                new NodeUsageStatsForThreadPoolsAction.NodeResponse(firstDiscoveryNode, firstNodeNonHotSpottingNodeStats)
            )
        );
        MockTransportService.getInstance(secondDataNodeName)
            .addRequestHandlingBehavior(
                TransportNodeUsageStatsForThreadPoolsAction.NAME + "[n]",
                (handler, request, channel, task) -> channel.sendResponse(
                    new NodeUsageStatsForThreadPoolsAction.NodeResponse(secondDiscoveryNode, secondNodeNonHotSpottingNodeStats)
                )
            );
        MockTransportService.getInstance(thirdDataNodeName)
            .addRequestHandlingBehavior(
                TransportNodeUsageStatsForThreadPoolsAction.NAME + "[n]",
                (handler, request, channel, task) -> channel.sendResponse(
                    new NodeUsageStatsForThreadPoolsAction.NodeResponse(thirdDiscoveryNode, thirdNodeHotSpottingNodeStats)
                )
            );

        IndexMetadata indexMetadata = internalCluster().getCurrentMasterNodeInstance(ClusterService.class)
            .state()
            .getMetadata()
            .getProject()
            .index(indexName);
        MockTransportService.getInstance(firstDataNodeName)
            .addRequestHandlingBehavior(IndicesStatsAction.NAME + "[n]", (handler, request, channel, task) -> {
                List<ShardStats> shardStats = new ArrayList<>(indexMetadata.getNumberOfShards());
                for (int i = 0; i < indexMetadata.getNumberOfShards(); i++) {
                    shardStats.add(createShardStats(indexMetadata, i, shardWriteLoad, firstDataNodeId));
                }
                TransportIndicesStatsAction instance = internalCluster().getInstance(TransportIndicesStatsAction.class, firstDataNodeName);
                channel.sendResponse(instance.new NodeResponse(firstDataNodeId, indexMetadata.getNumberOfShards(), shardStats, List.of()));
            });
        MockTransportService.getInstance(secondDataNodeName)
            .addRequestHandlingBehavior(IndicesStatsAction.NAME + "[n]", (handler, request, channel, task) -> {
                // Return no stats for the index because none are assigned to this node.
                TransportIndicesStatsAction instance = internalCluster().getInstance(TransportIndicesStatsAction.class, secondDataNodeName);
                channel.sendResponse(instance.new NodeResponse(secondDataNodeId, 0, List.of(), List.of()));
            });
        MockTransportService.getInstance(thirdDataNodeName)
            .addRequestHandlingBehavior(IndicesStatsAction.NAME + "[n]", (handler, request, channel, task) -> {
                // Return no stats for the index because none are assigned to this node.
                TransportIndicesStatsAction instance = internalCluster().getInstance(TransportIndicesStatsAction.class, thirdDataNodeName);
                channel.sendResponse(instance.new NodeResponse(thirdDataNodeId, 0, List.of(), List.of()));
            });

        /**
         * Provoke a ClusterInfo stats refresh, update the cluster settings to make shard assignment to the first node undesired, and
         * initiate rebalancing via a reroute request. Then wait to see a cluster state update that has all the shards assigned to the
         * second node, since the third is reporting as hot-spotted and should not accept any shards.
         */

        logger.info("---> Refreshing the cluster info to pull in the dummy thread pool stats with a hot-spotting node");
        refreshClusterInfo();

        logger.info(
            "---> Update the filter to exclude " + firstDataNodeName + " so that shards will be reassigned away to the other nodes"
        );
        // Updating the cluster settings will trigger a reroute request, no need to explicitly request one in the test.
        updateClusterSettings(Settings.builder().put("cluster.routing.allocation.exclude._name", firstDataNodeName));

        safeAwait(ClusterServiceUtils.addMasterTemporaryStateListener(clusterState -> {
            Index index = clusterState.routingTable().index(indexName).getIndex();
            return checkShardAssignment(
                clusterState.getRoutingNodes(),
                index,
                firstDataNodeId,
                secondDataNodeId,
                thirdDataNodeId,
                0,
                randomNumberOfShards,
                0
            );
        }));
    }

    public void testMaxQueueLatencyMetricIsPublished() {
        final Settings settings = Settings.builder()
            .put(
                WriteLoadConstraintSettings.WRITE_LOAD_DECIDER_ENABLED_SETTING.getKey(),
                WriteLoadConstraintSettings.WriteLoadDeciderStatus.ENABLED
            )
            .build();
        final var dataNodes = internalCluster().startNodes(3, settings);

        // Refresh cluster info (should trigger polling)
        refreshClusterInfo();

        Map<String, Long> mostRecentQueueLatencyMetrics = getMostRecentQueueLatencyMetrics(dataNodes);
        assertThat(mostRecentQueueLatencyMetrics.keySet(), hasSize(dataNodes.size()));
        assertThat(mostRecentQueueLatencyMetrics.values(), everyItem(greaterThanOrEqualTo(0L)));

        final String dataNodeToDelay = randomFrom(dataNodes);
        final ThreadPool threadPoolToDelay = internalCluster().getInstance(ThreadPool.class, dataNodeToDelay);

        // Fill the write thread pool and block a task for some time
        final int writeThreadPoolSize = threadPoolToDelay.info(ThreadPool.Names.WRITE).getMax();
        final var latch = new CountDownLatch(1);
        final var writeThreadPool = threadPoolToDelay.executor(ThreadPool.Names.WRITE);
        range(0, writeThreadPoolSize + 1).forEach(i -> writeThreadPool.execute(() -> safeAwait(latch)));
        final long delayMillis = randomIntBetween(100, 200);
        safeSleep(delayMillis);
        // Unblock the pool
        latch.countDown();

        refreshClusterInfo();
        mostRecentQueueLatencyMetrics = getMostRecentQueueLatencyMetrics(dataNodes);
        assertThat(mostRecentQueueLatencyMetrics.keySet(), hasSize(dataNodes.size()));
        assertThat(mostRecentQueueLatencyMetrics.get(dataNodeToDelay), greaterThanOrEqualTo(delayMillis));
    }

    private static Map<String, Long> getMostRecentQueueLatencyMetrics(List<String> dataNodes) {
        final Map<String, Long> measurements = new HashMap<>();
        for (String nodeName : dataNodes) {
            PluginsService pluginsService = internalCluster().getInstance(PluginsService.class, nodeName);
            final TestTelemetryPlugin telemetryPlugin = pluginsService.filterPlugins(TestTelemetryPlugin.class).findFirst().orElseThrow();
            telemetryPlugin.collect();
            final var maxLatencyValues = telemetryPlugin.getLongGaugeMeasurement(
                DesiredBalanceMetrics.WRITE_LOAD_DECIDER_MAX_LATENCY_VALUE
            );
            if (maxLatencyValues.isEmpty() == false) {
                measurements.put(nodeName, maxLatencyValues.getLast().getLong());
            }
        }
        return measurements;
    }

    /**
     * Verifies that the {@link RoutingNodes} shows that the expected portion of an index's shards are assigned to each node.
     */
    private boolean checkShardAssignment(
        RoutingNodes routingNodes,
        Index index,
        String firstDataNodeId,
        String secondDataNodeId,
        String thirdDataNodeId,
        int firstDataNodeExpectedNumShards,
        int secondDataNodeExpectedNumShards,
        int thirdDataNodeExpectedNumShards
    ) {

        int firstDataNodeRealNumberOfShards = routingNodes.node(firstDataNodeId).numberOfOwningShardsForIndex(index);
        if (firstDataNodeRealNumberOfShards != firstDataNodeExpectedNumShards) {
            return false;
        }
        int secondDataNodeRealNumberOfShards = routingNodes.node(secondDataNodeId).numberOfOwningShardsForIndex(index);
        if (secondDataNodeRealNumberOfShards != secondDataNodeExpectedNumShards) {
            return false;
        }
        int thirdDataNodeRealNumberOfShards = routingNodes.node(thirdDataNodeId).numberOfOwningShardsForIndex(index);
        if (thirdDataNodeRealNumberOfShards != thirdDataNodeExpectedNumShards) {
            return false;
        }

        return true;
    }

    private DiscoveryNode getDiscoveryNode(String nodeName) {
        final TransportService transportService = internalCluster().getInstance(TransportService.class, nodeName);
        assertNotNull(transportService);
        return transportService.getLocalNode();
    }

    /**
     * Helper to create a {@link NodeUsageStatsForThreadPools} for the given node with the given WRITE thread pool usage stats.
     */
    private NodeUsageStatsForThreadPools createNodeUsageStatsForThreadPools(
        DiscoveryNode discoveryNode,
        int totalWriteThreadPoolThreads,
        float averageWriteThreadPoolUtilization,
        long maxWriteThreadPoolQueueLatencyMillis
    ) {
        var threadPoolUsageMap = Map.of(
            ThreadPool.Names.WRITE,
            new NodeUsageStatsForThreadPools.ThreadPoolUsageStats(
                totalWriteThreadPoolThreads,
                averageWriteThreadPoolUtilization,
                maxWriteThreadPoolQueueLatencyMillis
            )
        );

        return new NodeUsageStatsForThreadPools(discoveryNode.getId(), threadPoolUsageMap);
    }

    /**
     * Helper to create a dummy {@link ShardStats} for the given index shard with the supplied {@code peakWriteLoad} value.
     */
    private static ShardStats createShardStats(IndexMetadata indexMeta, int shardIndex, double peakWriteLoad, String assignedShardNodeId) {
        ShardId shardId = new ShardId(indexMeta.getIndex(), shardIndex);
        Path path = createTempDir().resolve("indices").resolve(indexMeta.getIndexUUID()).resolve(String.valueOf(shardIndex));
        ShardRouting shardRouting = ShardRouting.newUnassigned(
            shardId,
            true,
            RecoverySource.EmptyStoreRecoverySource.INSTANCE,
            new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, null),
            ShardRouting.Role.DEFAULT
        );
        shardRouting = shardRouting.initialize(assignedShardNodeId, null, ShardRouting.UNAVAILABLE_EXPECTED_SHARD_SIZE);
        shardRouting = shardRouting.moveToStarted(ShardRouting.UNAVAILABLE_EXPECTED_SHARD_SIZE);
        CommonStats stats = new CommonStats();
        stats.docs = new DocsStats(100, 0, randomByteSizeValue().getBytes());
        stats.store = new StoreStats();
        stats.indexing = new IndexingStats(
            new IndexingStats.Stats(1, 1, 1, 1, 1, 1, 1, 1, 1, false, 1, 234, 234, 1000, 0.123, peakWriteLoad)
        );
        return new ShardStats(shardRouting, new ShardPath(false, path, path, shardId), stats, null, null, null, false, 0);
    }
}
