/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.routing.allocation.decider;

import org.apache.logging.log4j.Level;
import org.elasticsearch.action.admin.cluster.allocation.DesiredBalanceRequest;
import org.elasticsearch.action.admin.cluster.allocation.DesiredBalanceResponse;
import org.elasticsearch.action.admin.cluster.allocation.TransportGetDesiredBalanceAction;
import org.elasticsearch.action.admin.cluster.node.usage.NodeUsageStatsForThreadPoolsAction;
import org.elasticsearch.action.admin.cluster.node.usage.TransportNodeUsageStatsForThreadPoolsAction;
import org.elasticsearch.action.admin.indices.stats.CommonStats;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsAction;
import org.elasticsearch.action.admin.indices.stats.ShardStats;
import org.elasticsearch.action.admin.indices.stats.TransportIndicesStatsAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.NodeUsageStatsForThreadPools;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.cluster.routing.RoutingNodes;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.cluster.routing.allocation.WriteLoadConstraintSettings;
import org.elasticsearch.cluster.routing.allocation.allocator.BalancedShardsAllocator;
import org.elasticsearch.cluster.routing.allocation.allocator.DesiredBalanceMetrics;
import org.elasticsearch.cluster.routing.allocation.allocator.DesiredBalanceShardsAllocator;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.core.TimeValue;
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
import org.elasticsearch.test.MockLog;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.stream.StreamSupport;

import static java.util.stream.IntStream.range;
import static org.hamcrest.Matchers.equalTo;
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
     * <p>
     * Leverages the {@link FilterAllocationDecider} to first start all shards on a Node1, and then eventually force the shards off of
     * Node1 while Node3 is hot-spotting, resulting in reassignment of all shards to Node2.
     */
    public void testHighNodeWriteLoadPreventsNewShardAllocation() {
        TestHarness harness = setUpThreeTestNodesAndAllIndexShardsOnFirstNode();

        /**
         * Override the {@link TransportNodeUsageStatsForThreadPoolsAction} action on the data nodes to supply artificial thread pool write
         * load stats. The stats will show the third node hot-spotting, while the second node has capacity to receive all the index shards.
         */

        final NodeUsageStatsForThreadPools firstNodeNonHotSpottingNodeStats = createNodeUsageStatsForThreadPools(
            harness.firstDiscoveryNode,
            harness.randomNumberOfWritePoolThreads,
            randomIntBetween(0, harness.maxUtilBelowThresholdThatAllowsAllShardsToRelocate) / 100f,
            0
        );
        final NodeUsageStatsForThreadPools secondNodeNonHotSpottingNodeStats = createNodeUsageStatsForThreadPools(
            harness.secondDiscoveryNode,
            harness.randomNumberOfWritePoolThreads,
            randomIntBetween(0, harness.maxUtilBelowThresholdThatAllowsAllShardsToRelocate) / 100f,
            0
        );
        final NodeUsageStatsForThreadPools thirdNodeHotSpottingNodeStats = createNodeUsageStatsForThreadPools(
            harness.thirdDiscoveryNode,
            harness.randomNumberOfWritePoolThreads,
            (harness.randomUtilizationThresholdPercent + 1) / 100f,
            0
        );

        setUpMockTransportNodeUsageStatsResponse(harness.firstDiscoveryNode, firstNodeNonHotSpottingNodeStats);
        setUpMockTransportNodeUsageStatsResponse(harness.secondDiscoveryNode, secondNodeNonHotSpottingNodeStats);
        setUpMockTransportNodeUsageStatsResponse(harness.thirdDiscoveryNode, thirdNodeHotSpottingNodeStats);

        /**
         * Override the {@link TransportIndicesStatsAction} action on the data nodes to supply artificial shard write load stats. The stats
         * will show that all shards have non-empty write load stats (so that the WriteLoadDecider will evaluate assigning them to a node).
         */

        IndexMetadata indexMetadata = internalCluster().getCurrentMasterNodeInstance(ClusterService.class)
            .state()
            .getMetadata()
            .getProject()
            .index(harness.indexName);
        setUpMockTransportIndicesStatsResponse(
            harness.firstDiscoveryNode,
            indexMetadata.getNumberOfShards(),
            createShardStatsResponseForIndex(indexMetadata, harness.maxShardWriteLoad, harness.firstDataNodeId)
        );
        setUpMockTransportIndicesStatsResponse(harness.secondDiscoveryNode, 0, List.of());
        setUpMockTransportIndicesStatsResponse(harness.thirdDiscoveryNode, 0, List.of());

        /**
         * Provoke a ClusterInfo stats refresh, update the cluster settings to make shard assignment to the first node undesired, and
         * initiate rebalancing via a reroute request. Then wait to see a cluster state update that has all the shards assigned to the
         * second node, since the third is reporting as hot-spotted and should not accept any shards.
         */

        logger.info("--> Refreshing the cluster info to pull in the dummy thread pool stats with a hot-spotting node");
        refreshClusterInfo();

        var temporaryClusterStateListener = ClusterServiceUtils.addMasterTemporaryStateListener(clusterState -> {
            Index index = clusterState.routingTable().index(harness.indexName).getIndex();
            return checkShardAssignment(
                clusterState.getRoutingNodes(),
                index,
                harness.firstDataNodeId,
                harness.secondDataNodeId,
                harness.thirdDataNodeId,
                0,
                harness.randomNumberOfShards,
                0
            );
        });

        try {
            logger.info(
                "--> Update the filter to exclude " + harness.firstDataNodeName + " so shards will be reassigned away to the other nodes"
            );
            // Updating the cluster settings will trigger a reroute request, no need to explicitly request one in the test.
            updateClusterSettings(Settings.builder().put("cluster.routing.allocation.exclude._name", harness.firstDataNodeName));

            safeAwait(temporaryClusterStateListener);
        } catch (AssertionError error) {
            dumpClusterState();
            throw error;
        }
    }

    /**
     * Tests that {@link AllocationDecider#canRemain} returning {@link Decision.Type#NO} for a {@code NodeX} will ignore a
     * {@link AllocationDecider#canAllocate} response of {@link Decision.Type#NOT_PREFERRED} from a {@code NodeY} and reassign the shard
     * when there are no better node options.
     * <p>
     * Uses MockTransportService to set up write load stat responses from the data nodes and tests the allocation decisions made by the
     * balancer. Leverages the {@link FilterAllocationDecider} to first start all shards on a Node1, and then eventually force the shards
     * off of Node1 while Node2 and Node3 are hot-spotting, resulting in overriding not-preferred and relocating shards to Node2 and Node3.
     */
    public void testShardsAreAssignedToNotPreferredWhenAlternativeIsNo() {
        TestHarness harness = setUpThreeTestNodesAndAllIndexShardsOnFirstNode();

        /**
         * Override the {@link TransportNodeUsageStatsForThreadPoolsAction} action on the data nodes to supply artificial thread pool write
         * load stats. The stats will show both the second and third nodes are hot-spotting.
         */

        final NodeUsageStatsForThreadPools firstNodeNonHotSpottingNodeStats = createNodeUsageStatsForThreadPools(
            harness.firstDiscoveryNode,
            harness.randomNumberOfWritePoolThreads,
            randomIntBetween(0, harness.maxUtilBelowThresholdThatAllowsAllShardsToRelocate) / 100f,
            0
        );
        final NodeUsageStatsForThreadPools secondNodeNonHotSpottingNodeStats = createNodeUsageStatsForThreadPools(
            harness.secondDiscoveryNode,
            harness.randomNumberOfWritePoolThreads,
            (harness.randomUtilizationThresholdPercent + 1) / 100f,
            0
        );
        final NodeUsageStatsForThreadPools thirdNodeHotSpottingNodeStats = createNodeUsageStatsForThreadPools(
            harness.thirdDiscoveryNode,
            harness.randomNumberOfWritePoolThreads,
            (harness.randomUtilizationThresholdPercent + 1) / 100f,
            0
        );

        setUpMockTransportNodeUsageStatsResponse(harness.firstDiscoveryNode, firstNodeNonHotSpottingNodeStats);
        setUpMockTransportNodeUsageStatsResponse(harness.secondDiscoveryNode, secondNodeNonHotSpottingNodeStats);
        setUpMockTransportNodeUsageStatsResponse(harness.thirdDiscoveryNode, thirdNodeHotSpottingNodeStats);

        /**
         * Override the {@link TransportIndicesStatsAction} action on the data nodes to supply artificial shard write load stats. The stats
         * will show that all shards have non-empty write load stats (so that the WriteLoadDecider will evaluate assigning them to a node).
         */

        IndexMetadata indexMetadata = internalCluster().getCurrentMasterNodeInstance(ClusterService.class)
            .state()
            .getMetadata()
            .getProject()
            .index(harness.indexName);
        setUpMockTransportIndicesStatsResponse(
            harness.firstDiscoveryNode,
            indexMetadata.getNumberOfShards(),
            createShardStatsResponseForIndex(indexMetadata, harness.maxShardWriteLoad, harness.firstDataNodeId)
        );
        setUpMockTransportIndicesStatsResponse(harness.secondDiscoveryNode, 0, List.of());
        setUpMockTransportIndicesStatsResponse(harness.thirdDiscoveryNode, 0, List.of());

        /**
         * Provoke a ClusterInfo stats refresh, update the cluster settings to make shard assignment to the first node undesired and
         * initiate rebalancing. Then wait to see a cluster state update that has all the shards assigned away from the first node _despite_
         * the second and third node reporting hot-spotting: a canRemain::NO response should override a canAllocate::NOT_PREFERRED answer.
         */

        logger.info("--> Refreshing the cluster info to pull in the dummy thread pool stats with a hot-spotting node");
        refreshClusterInfo();

        var temporaryClusterStateListener = ClusterServiceUtils.addMasterTemporaryStateListener(clusterState -> {
            Index index = clusterState.routingTable().index(harness.indexName).getIndex();
            if (clusterState.getRoutingNodes()
                .node(harness.firstDataNodeId)
                .numberOfOwningShardsForIndex(index) == harness.randomNumberOfShards) {
                return true;
            }
            return false;
        });

        try {
            logger.info("--> Update the filter to remove exclusions so that shards can be reassigned based on the write load decider only");
            // Updating the cluster settings will trigger a reroute request, no need to explicitly request one in the test.
            updateClusterSettings(Settings.builder().putNull("cluster.routing.allocation.exclude._name"));

            safeAwait(temporaryClusterStateListener);
        } catch (AssertionError error) {
            dumpClusterState();
            throw error;
        }
    }

    @TestLogging(
        reason = "track when reconciliation has completed",
        value = "org.elasticsearch.cluster.routing.allocation.allocator.DesiredBalanceShardsAllocator:DEBUG"
    )
    public void testCanRemainNotPreferredIsIgnoredWhenAllOtherNodesReturnNotPreferred() {
        TestHarness harness = setUpThreeTestNodesAndAllIndexShardsOnFirstNode();

        /**
         * Override the {@link TransportNodeUsageStatsForThreadPoolsAction} action on the data nodes to supply artificial thread pool write
         * load stats. The stats will show all the nodes above the high utilization threshold, so they do not accept new shards, while the
         * first node will show queue latency above the threshold and request a shard to move away. However, there will be nowhere to
         * reassign any shards.
         */

        final NodeUsageStatsForThreadPools firstNodeUtilHotSpottingAndQueuingNodeStats = createNodeUsageStatsForThreadPools(
            harness.firstDiscoveryNode,
            harness.randomNumberOfWritePoolThreads,
            (harness.randomUtilizationThresholdPercent + 1) / 100f,
            randomLongBetween(harness.randomQueueLatencyThresholdMillis, harness.randomQueueLatencyThresholdMillis + 1_000)
        );
        final NodeUsageStatsForThreadPools secondNodeUtilHotSpottingNodeStats = createNodeUsageStatsForThreadPools(
            harness.secondDiscoveryNode,
            harness.randomNumberOfWritePoolThreads,
            (harness.randomUtilizationThresholdPercent + 1) / 100f,
            0
        );
        final NodeUsageStatsForThreadPools thirdNodeUtilHotSpottingNodeStats = createNodeUsageStatsForThreadPools(
            harness.thirdDiscoveryNode,
            harness.randomNumberOfWritePoolThreads,
            (harness.randomUtilizationThresholdPercent + 1) / 100f,
            0
        );

        setUpMockTransportNodeUsageStatsResponse(harness.firstDiscoveryNode, firstNodeUtilHotSpottingAndQueuingNodeStats);
        setUpMockTransportNodeUsageStatsResponse(harness.secondDiscoveryNode, secondNodeUtilHotSpottingNodeStats);
        setUpMockTransportNodeUsageStatsResponse(harness.thirdDiscoveryNode, thirdNodeUtilHotSpottingNodeStats);

        /**
         * Override the {@link TransportIndicesStatsAction} action on the data nodes to supply artificial shard write load stats. The stats
         * will show that all shards have non-empty write load stats (so that the WriteLoadDecider will evaluate assigning them to a node).
         */

        IndexMetadata indexMetadata = internalCluster().getCurrentMasterNodeInstance(ClusterService.class)
            .state()
            .getMetadata()
            .getProject()
            .index(harness.indexName);
        setUpMockTransportIndicesStatsResponse(
            harness.firstDiscoveryNode,
            indexMetadata.getNumberOfShards(),
            createShardStatsResponseForIndex(indexMetadata, harness.maxShardWriteLoad, harness.firstDataNodeId)
        );
        setUpMockTransportIndicesStatsResponse(harness.secondDiscoveryNode, 0, List.of());
        setUpMockTransportIndicesStatsResponse(harness.thirdDiscoveryNode, 0, List.of());

        /**
         * Refresh the ClusterInfo to pull in the new dummy hot-spot stats. Then remove the filter restricting the shards to the first node.
         * Then wait for the DesiredBalance computation to finish running after the cluster settings update. All the shards should remain on
         * the first node, despite hot-spotting with queuing, because no other node has below utilization threshold stats.
         */

        // Wait for the DesiredBalance to be recomputed as a result of the ClusterInfo refresh. Ensures no async computation.
        MockLog.awaitLogger(() -> {
            logger.info("--> Refreshing the cluster info to pull in the dummy thread pool stats with hot-spot stats");
            refreshClusterInfo();
        }, DesiredBalanceShardsAllocator.class, createBalancerConvergedSeenEvent());

        // Wait for the DesiredBalance to be recomputed as a result of the settings change.
        MockLog.awaitLogger(() -> {
            logger.info("--> Update the filter to remove exclusions so that shards can be reassigned based on the write load decider only");
            // Updating the cluster settings will trigger a reroute request.
            updateClusterSettings(Settings.builder().putNull("cluster.routing.allocation.exclude._name"));
        }, DesiredBalanceShardsAllocator.class, createBalancerConvergedSeenEvent());

        try {
            // Now check that all the shards remain on the first node because the other two nodes have too high write thread pool
            // utilization to accept additional shards.
            var desiredBalanceResponse = safeGet(
                client().execute(TransportGetDesiredBalanceAction.TYPE, new DesiredBalanceRequest(TEST_REQUEST_TIMEOUT))
            );
            Map<Integer, DesiredBalanceResponse.DesiredShards> shardsMap = desiredBalanceResponse.getRoutingTable().get(harness.indexName);
            logger.info("--> Checking desired shard assignments are still on the first data node. Desired assignments: " + shardsMap);
            for (var desiredShard : shardsMap.values()) {
                for (var desiredNodeId : desiredShard.desired().nodeIds()) {
                    assertEquals("Found a shard assigned to an unexpected node: " + shardsMap, desiredNodeId, harness.firstDataNodeId);
                }
            }
        } catch (AssertionError error) {
            dumpClusterState();
            throw error;
        }
    }

    @TestLogging(
        reason = "track when reconciliation has completed",
        value = "org.elasticsearch.cluster.routing.allocation.allocator.DesiredBalanceShardsAllocator:DEBUG"
    )
    public void testCanRemainRelocatesOneShardWhenAHotSpotOccurs() {
        TestHarness harness = setUpThreeTestNodesAndAllIndexShardsOnFirstNode();

        /**
         * Override the {@link TransportNodeUsageStatsForThreadPoolsAction} action on the data nodes to supply artificial thread pool write
         * load stats. The stats will show all the nodes above the high utilization threshold, so they do not accept new shards, while the
         * first node will show queue latency above the threshold and request a shard to move away. A single shard should move away from the
         * queuing node, no more than one.
         */

        final NodeUsageStatsForThreadPools firstNodeUtilHotSpottingAndQueuingNodeStats = createNodeUsageStatsForThreadPools(
            harness.firstDiscoveryNode,
            harness.randomNumberOfWritePoolThreads,
            (harness.randomUtilizationThresholdPercent + 1) / 100f,
            randomLongBetween(harness.randomQueueLatencyThresholdMillis, harness.randomQueueLatencyThresholdMillis + 1_000)
        );
        final NodeUsageStatsForThreadPools secondNodeNonHotSpottingNodeStats = createNodeUsageStatsForThreadPools(
            harness.secondDiscoveryNode,
            harness.randomNumberOfWritePoolThreads,
            randomIntBetween(0, harness.maxUtilBelowThresholdThatAllowsAllShardsToRelocate) / 100f,
            0
        );
        final NodeUsageStatsForThreadPools thirdNodeNonHotSpottingNodeStats = createNodeUsageStatsForThreadPools(
            harness.thirdDiscoveryNode,
            harness.randomNumberOfWritePoolThreads,
            randomIntBetween(0, harness.maxUtilBelowThresholdThatAllowsAllShardsToRelocate) / 100f,
            0
        );

        setUpMockTransportNodeUsageStatsResponse(harness.firstDiscoveryNode, firstNodeUtilHotSpottingAndQueuingNodeStats);
        setUpMockTransportNodeUsageStatsResponse(harness.secondDiscoveryNode, secondNodeNonHotSpottingNodeStats);
        setUpMockTransportNodeUsageStatsResponse(harness.thirdDiscoveryNode, thirdNodeNonHotSpottingNodeStats);

        /**
         * Override the {@link TransportIndicesStatsAction} action on the data nodes to supply artificial shard write load stats. The stats
         * will show that all shards have non-empty write load stats (so that the WriteLoadDecider will evaluate assigning them to a node).
         */

        final ClusterState originalClusterState = internalCluster().getCurrentMasterNodeInstance(ClusterService.class).state();
        final IndexMetadata indexMetadata = originalClusterState.getMetadata().getProject().index(harness.indexName);
        setUpMockTransportIndicesStatsResponse(
            harness.firstDiscoveryNode,
            indexMetadata.getNumberOfShards(),
            createShardStatsResponseForIndex(indexMetadata, harness.maxShardWriteLoad, harness.firstDataNodeId)
        );
        setUpMockTransportIndicesStatsResponse(harness.secondDiscoveryNode, 0, List.of());
        setUpMockTransportIndicesStatsResponse(harness.thirdDiscoveryNode, 0, List.of());

        /**
         * Refresh the ClusterInfo to pull in the new dummy hot-spot stats. Then remove the filter restricting the shards to the first node.
         * Then wait for the DesiredBalance computation to finish running after the cluster settings update. All the shards should remain on
         * the first node, despite hot-spotting, because no other node has below utilization threshold stats.
         */

        // Wait for the DesiredBalance to be recomputed as a result of the ClusterInfo refresh. This way nothing async is running.
        MockLog.awaitLogger(() -> {
            logger.info("--> Refreshing the cluster info to pull in the dummy thread pool stats with hot-spot stats");
            refreshClusterInfo();
        }, DesiredBalanceShardsAllocator.class, createBalancerConvergedSeenEvent());

        // Wait for the DesiredBalance to be recomputed as a result of the settings change.
        MockLog.awaitLogger(() -> {
            logger.info("--> Update the filter to remove exclusions so that shards can be reassigned based on the write load decider only");
            // Updating the cluster settings will trigger a reroute request.
            updateClusterSettings(Settings.builder().putNull("cluster.routing.allocation.exclude._name"));
        }, DesiredBalanceShardsAllocator.class, createBalancerConvergedSeenEvent());

        try {
            // Now check that all a single shard was moved off of the first node to address the queuing, but the rest of the shards remain.
            var desiredBalanceResponse = safeGet(
                client().execute(TransportGetDesiredBalanceAction.TYPE, new DesiredBalanceRequest(TEST_REQUEST_TIMEOUT))
            );
            Map<Integer, DesiredBalanceResponse.DesiredShards> shardsMap = desiredBalanceResponse.getRoutingTable().get(harness.indexName);
            logger.info("--> Checking desired shard assignments. Desired assignments: " + shardsMap);
            int countShardsStillAssignedToFirstNode = 0;
            for (var desiredShard : shardsMap.values()) {
                for (var desiredNodeId : desiredShard.desired().nodeIds()) {
                    if (desiredNodeId.equals(harness.firstDataNodeId)) {
                        ++countShardsStillAssignedToFirstNode;
                    }
                }
            }
            assertEquals(
                "Expected all shards except one to still be on the first data node: " + shardsMap,
                harness.randomNumberOfShards,
                countShardsStillAssignedToFirstNode + 1
            );
            assertThatTheBestShardWasMoved(harness, originalClusterState, desiredBalanceResponse);
        } catch (AssertionError error) {
            dumpClusterState();
            throw error;
        }
    }

    /**
     * Determine which shard was moved and check that it's the "best" according to
     * {@link org.elasticsearch.cluster.routing.allocation.allocator.BalancedShardsAllocator.Balancer.PrioritiseByShardWriteLoadComparator}
     */
    private void assertThatTheBestShardWasMoved(
        TestHarness harness,
        ClusterState originalClusterState,
        DesiredBalanceResponse desiredBalanceResponse
    ) {
        int movedShardId = desiredBalanceResponse.getRoutingTable().get(harness.indexName).entrySet().stream().filter(e -> {
            Set<String> desiredNodeIds = e.getValue().desired().nodeIds();
            return desiredNodeIds.contains(harness.secondDiscoveryNode.getId())
                || desiredNodeIds.contains(harness.thirdDiscoveryNode.getId());
        }).findFirst().map(Map.Entry::getKey).orElseThrow(() -> new AssertionError("No shard was moved to a non-hot-spotting node"));

        final BalancedShardsAllocator.Balancer.PrioritiseByShardWriteLoadComparator comparator =
            new BalancedShardsAllocator.Balancer.PrioritiseByShardWriteLoadComparator(
                desiredBalanceResponse.getClusterInfo(),
                originalClusterState.getRoutingNodes().node(harness.firstDataNodeId)
            );

        final List<ShardRouting> bestShardsToMove = StreamSupport.stream(
            originalClusterState.getRoutingNodes().node(harness.firstDataNodeId).spliterator(),
            false
        ).sorted(comparator).toList();

        // The moved shard should be at the head of the sorted list
        assertThat(movedShardId, equalTo(bestShardsToMove.get(0).shardId().id()));
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

    private void setUpMockTransportNodeUsageStatsResponse(DiscoveryNode node, NodeUsageStatsForThreadPools nodeUsageStats) {
        MockTransportService.getInstance(node.getName())
            .addRequestHandlingBehavior(
                TransportNodeUsageStatsForThreadPoolsAction.NAME + "[n]",
                (handler, request, channel, task) -> channel.sendResponse(
                    new NodeUsageStatsForThreadPoolsAction.NodeResponse(node, nodeUsageStats)
                )
            );
    }

    private void setUpMockTransportIndicesStatsResponse(DiscoveryNode node, int totalShards, List<ShardStats> shardStats) {
        MockTransportService.getInstance(node.getName())
            .addRequestHandlingBehavior(IndicesStatsAction.NAME + "[n]", (handler, request, channel, task) -> {
                TransportIndicesStatsAction instance = internalCluster().getInstance(TransportIndicesStatsAction.class, node.getName());
                channel.sendResponse(instance.new NodeResponse(node.getId(), totalShards, shardStats, List.of()));
            });
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

    /**
     * Enables the write load decider and overrides other write load decider settings.
     * @param utilizationThresholdPercent Sets the write thread pool utilization threshold, controlling when canAllocate starts returning
     *                                    not-preferred.
     * @param queueLatencyThresholdMillis Sets the queue latency threshold, controlling when canRemain starts returning not-preferred.
     */
    private Settings enabledWriteLoadDeciderSettings(int utilizationThresholdPercent, long queueLatencyThresholdMillis) {
        return Settings.builder()
            .put(
                WriteLoadConstraintSettings.WRITE_LOAD_DECIDER_ENABLED_SETTING.getKey(),
                WriteLoadConstraintSettings.WriteLoadDeciderStatus.ENABLED
            )
            .put(
                WriteLoadConstraintSettings.WRITE_LOAD_DECIDER_HIGH_UTILIZATION_THRESHOLD_SETTING.getKey(),
                utilizationThresholdPercent + "%"
            )
            .put(
                WriteLoadConstraintSettings.WRITE_LOAD_DECIDER_QUEUE_LATENCY_THRESHOLD_SETTING.getKey(),
                TimeValue.timeValueMillis(queueLatencyThresholdMillis)
            )
            // Disable rebalancing so that testing can see Decider change outcomes only.
            .put(EnableAllocationDecider.CLUSTER_ROUTING_REBALANCE_ENABLE_SETTING.getKey(), "none")
            .build();
    }

    /**
     * The utilization percent overhead needed to add the given amount of shard write load to a node with the given number of write pool
     * threads.
     */
    private int shardLoadUtilizationOverhead(float totalShardWriteLoad, int numberOfWritePoolThreads) {
        float totalWriteLoadPerThread = totalShardWriteLoad / numberOfWritePoolThreads;
        return (int) (100 * totalWriteLoadPerThread);
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
     * Helper to create a list of dummy {@link ShardStats} for the given index, each shard being randomly allocated a peak write load
     * between 0 and {@code maximumShardWriteLoad}. There will always be at least one shard reporting the specified
     * {@code maximumShardWriteLoad}.
     */
    private List<ShardStats> createShardStatsResponseForIndex(
        IndexMetadata indexMetadata,
        float maximumShardWriteLoad,
        String assignedShardNodeId
    ) {
        // Randomly distribute shards' peak write-loads so that we can check later that shard movements are prioritized correctly
        final double writeLoadThreshold = maximumShardWriteLoad
            * BalancedShardsAllocator.Balancer.PrioritiseByShardWriteLoadComparator.THRESHOLD_RATIO;
        final List<Double> shardPeakWriteLoads = new ArrayList<>();
        // Need at least one with the maximum write-load
        shardPeakWriteLoads.add((double) maximumShardWriteLoad);
        final int remainingShards = indexMetadata.getNumberOfShards() - 1;
        // Some over-threshold, some under
        for (int i = 0; i < remainingShards; ++i) {
            if (randomBoolean()) {
                shardPeakWriteLoads.add(randomDoubleBetween(writeLoadThreshold, maximumShardWriteLoad, true));
            } else {
                shardPeakWriteLoads.add(randomDoubleBetween(0.0, writeLoadThreshold, true));
            }
        }
        assertThat(shardPeakWriteLoads, hasSize(indexMetadata.getNumberOfShards()));
        Collections.shuffle(shardPeakWriteLoads, random());
        final List<ShardStats> shardStats = new ArrayList<>(indexMetadata.getNumberOfShards());
        for (int i = 0; i < indexMetadata.getNumberOfShards(); i++) {
            shardStats.add(createShardStats(indexMetadata, i, shardPeakWriteLoads.get(i), assignedShardNodeId));
        }
        return shardStats;
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

    /**
     * Creates a MockLog.SeenEventExpectation for a log message indicating that the balancer computation has converged / finalized.
     */
    private MockLog.SeenEventExpectation createBalancerConvergedSeenEvent() {
        return new MockLog.SeenEventExpectation(
            "desired balance computation ran and completed",
            DesiredBalanceShardsAllocator.class.getCanonicalName(),
            Level.DEBUG,
            "Desired balance computation for * is completed, scheduling reconciliation"
        );
    }

    /**
     * Sets up common test infrastructure to deduplicate code across tests.
     * <p>
     * Starts three data nodes and creates an index with many shards, then forces shard assignment to only the first data node via the
     * {@link FilterAllocationDecider}.
     */
    private TestHarness setUpThreeTestNodesAndAllIndexShardsOnFirstNode() {
        int randomUtilizationThresholdPercent = randomIntBetween(50, 100);
        int randomNumberOfWritePoolThreads = randomIntBetween(2, 20);
        long randomQueueLatencyThresholdMillis = randomLongBetween(1, 20_000);
        float maximumShardWriteLoad = randomFloatBetween(0.0f, 0.01f, false);
        Settings settings = enabledWriteLoadDeciderSettings(randomUtilizationThresholdPercent, randomQueueLatencyThresholdMillis);

        internalCluster().startMasterOnlyNode(settings);
        final var dataNodes = internalCluster().startDataOnlyNodes(3, settings);
        final String firstDataNodeName = dataNodes.get(0);
        final String secondDataNodeName = dataNodes.get(1);
        final String thirdDataNodeName = dataNodes.get(2);
        final String firstDataNodeId = getNodeId(firstDataNodeName);
        final String secondDataNodeId = getNodeId(secondDataNodeName);
        final String thirdDataNodeId = getNodeId(thirdDataNodeName);
        ensureStableCluster(4);

        final DiscoveryNode firstDiscoveryNode = getDiscoveryNode(firstDataNodeName);
        final DiscoveryNode secondDiscoveryNode = getDiscoveryNode(secondDataNodeName);
        final DiscoveryNode thirdDiscoveryNode = getDiscoveryNode(thirdDataNodeName);

        logger.info(
            "--> first node name "
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

        logger.info(
            "--> utilization threshold: "
                + randomUtilizationThresholdPercent
                + ",  write threads: "
                + randomNumberOfWritePoolThreads
                + ", maximum shard write load: "
                + maximumShardWriteLoad
        );

        /**
         * Exclude assignment of shards to the second and third data nodes via the {@link FilterAllocationDecider} settings.
         * Then create an index with many shards, which will all be assigned to the first data node.
         */

        logger.info("--> Limit shard assignment to node " + firstDataNodeName + " by excluding the other nodes");
        updateClusterSettings(
            Settings.builder().put("cluster.routing.allocation.exclude._name", secondDataNodeName + "," + thirdDataNodeName)
        );

        String indexName = randomIdentifier();
        int randomNumberOfShards = randomIntBetween(10, 20); // Pick a high number of shards, so it is clear assignment is not accidental.

        // Calculate the maximum utilization a node can report while still being able to accept all relocating shards
        int shardWriteLoadOverhead = shardLoadUtilizationOverhead(
            maximumShardWriteLoad * randomNumberOfShards,
            randomNumberOfWritePoolThreads
        );
        int maxUtilBelowThresholdThatAllowsAllShardsToRelocate = randomUtilizationThresholdPercent - shardWriteLoadOverhead - 1;

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

        createIndex(indexName, randomNumberOfShards, 0);
        ensureGreen(indexName);

        logger.info("--> Waiting for all [" + randomNumberOfShards + "] shards to be assigned to node " + firstDataNodeName);
        try {
            safeAwait(verifyAssignmentToFirstNodeListener);
        } catch (AssertionError error) {
            dumpClusterState();
            throw error;
        }

        return new TestHarness(
            firstDataNodeName,
            secondDataNodeName,
            thirdDataNodeName,
            firstDataNodeId,
            secondDataNodeId,
            thirdDataNodeId,
            firstDiscoveryNode,
            secondDiscoveryNode,
            thirdDiscoveryNode,
            randomUtilizationThresholdPercent,
            randomNumberOfWritePoolThreads,
            randomQueueLatencyThresholdMillis,
            maximumShardWriteLoad,
            indexName,
            randomNumberOfShards,
            maxUtilBelowThresholdThatAllowsAllShardsToRelocate
        );
    }

    private void dumpClusterState() {
        logger.info("--> Failed to reach expected allocation state. Dumping cluster state");
        logClusterState();
    }

    /**
     * Carries set-up state from {@link #setUpThreeTestNodesAndAllIndexShardsOnFirstNode()} to the testing logic.
     */
    record TestHarness(
        String firstDataNodeName,
        String secondDataNodeName,
        String thirdDataNodeName,
        String firstDataNodeId,
        String secondDataNodeId,
        String thirdDataNodeId,
        DiscoveryNode firstDiscoveryNode,
        DiscoveryNode secondDiscoveryNode,
        DiscoveryNode thirdDiscoveryNode,
        int randomUtilizationThresholdPercent,
        int randomNumberOfWritePoolThreads,
        long randomQueueLatencyThresholdMillis,
        float maxShardWriteLoad,
        String indexName,
        int randomNumberOfShards,
        int maxUtilBelowThresholdThatAllowsAllShardsToRelocate
    ) {};

}
