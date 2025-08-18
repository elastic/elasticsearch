/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.routing.allocation;

import org.apache.logging.log4j.Level;
import org.elasticsearch.action.support.replication.ClusterStateCreationUtils;
import org.elasticsearch.cluster.ClusterInfo;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.NodeUsageStatsForThreadPools;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.GlobalRoutingTable;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.RerouteService;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.TestShardRouting;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.gateway.GatewayService;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.MockLog;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;
import java.util.stream.Collectors;

import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_VERSION_CREATED;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

public class WriteLoadConstraintMonitorTests extends ESTestCase {

    public void testRerouteIsCalledWhenAHotSpotIsDetected() {
        final TestState testState = createRandomTestStateThatWillTriggerReroute();
        final WriteLoadConstraintMonitor writeLoadConstraintMonitor = new WriteLoadConstraintMonitor(
            testState.clusterSettings,
            testState.currentTimeSupplier,
            () -> testState.clusterState,
            testState.rerouteService
        );

        writeLoadConstraintMonitor.onNewInfo(testState.clusterInfo);
        verify(testState.rerouteService).reroute(anyString(), eq(Priority.NORMAL), any());
    }

    @TestLogging(
        value = "org.elasticsearch.cluster.routing.allocation.WriteLoadConstraintMonitor:DEBUG",
        reason = "ensure we're skipping reroute for the right reason"
    )
    public void testRerouteIsNotCalledWhenStateIsNotRecovered() {
        final TestState testState = createRandomTestStateThatWillTriggerReroute();
        final WriteLoadConstraintMonitor writeLoadConstraintMonitor = new WriteLoadConstraintMonitor(
            testState.clusterSettings,
            testState.currentTimeSupplier,
            () -> ClusterState.builder(testState.clusterState)
                .blocks(ClusterBlocks.builder().addGlobalBlock(GatewayService.STATE_NOT_RECOVERED_BLOCK).build())
                .build(),
            testState.rerouteService
        );

        try (MockLog mockLog = MockLog.capture(WriteLoadConstraintMonitor.class)) {
            mockLog.addExpectation(
                new MockLog.SeenEventExpectation(
                    "don't reroute due to global block",
                    WriteLoadConstraintMonitor.class.getCanonicalName(),
                    Level.DEBUG,
                    "skipping monitor as the cluster state is not recovered yet"
                )
            );

            writeLoadConstraintMonitor.onNewInfo(testState.clusterInfo);
            mockLog.assertAllExpectationsMatched();
            verifyNoInteractions(testState.rerouteService);
        }
    }

    @TestLogging(
        value = "org.elasticsearch.cluster.routing.allocation.WriteLoadConstraintMonitor:DEBUG",
        reason = "ensure we're skipping reroute for the right reason"
    )
    public void testRerouteIsNotCalledDeciderIsNotEnabled() {
        final TestState testState = createRandomTestStateThatWillTriggerReroute();
        final WriteLoadConstraintMonitor writeLoadConstraintMonitor = new WriteLoadConstraintMonitor(
            createClusterSettings(
                randomValueOtherThan(
                    WriteLoadConstraintSettings.WriteLoadDeciderStatus.ENABLED,
                    () -> randomFrom(WriteLoadConstraintSettings.WriteLoadDeciderStatus.values())
                ),
                testState.latencyThresholdMillis,
                testState.highUtilizationThresholdPercent
            ),
            testState.currentTimeSupplier,
            () -> testState.clusterState,
            testState.rerouteService
        );

        try (MockLog mockLog = MockLog.capture(WriteLoadConstraintMonitor.class)) {
            mockLog.addExpectation(
                new MockLog.SeenEventExpectation(
                    "don't reroute due to decider being disabled",
                    WriteLoadConstraintMonitor.class.getCanonicalName(),
                    Level.DEBUG,
                    "skipping monitor because the write load decider is disabled"
                )
            );

            writeLoadConstraintMonitor.onNewInfo(testState.clusterInfo);
            mockLog.assertAllExpectationsMatched();
            verifyNoInteractions(testState.rerouteService);
        }
    }

    @TestLogging(
        value = "org.elasticsearch.cluster.routing.allocation.WriteLoadConstraintMonitor:DEBUG",
        reason = "ensure we're skipping reroute for the right reason"
    )
    public void testRerouteIsNotCalledNoNodesAreHotSpotting() {
        final TestState testState = createRandomTestStateThatWillTriggerReroute();
        final WriteLoadConstraintMonitor writeLoadConstraintMonitor = new WriteLoadConstraintMonitor(
            testState.clusterSettings,
            testState.currentTimeSupplier,
            () -> testState.clusterState,
            testState.rerouteService
        );

        try (MockLog mockLog = MockLog.capture(WriteLoadConstraintMonitor.class)) {
            mockLog.addExpectation(
                new MockLog.SeenEventExpectation(
                    "don't reroute due to no nodes hot-spotting",
                    WriteLoadConstraintMonitor.class.getCanonicalName(),
                    Level.DEBUG,
                    "No nodes exceeding latency threshold"
                )
            );

            writeLoadConstraintMonitor.onNewInfo(
                createClusterInfo(testState.clusterState, 0, testState.latencyThresholdMillis, testState.highUtilizationThresholdPercent)
            );
            mockLog.assertAllExpectationsMatched();
            verifyNoInteractions(testState.rerouteService);
        }
    }

    @TestLogging(
        value = "org.elasticsearch.cluster.routing.allocation.WriteLoadConstraintMonitor:DEBUG",
        reason = "ensure we're skipping reroute for the right reason"
    )
    public void testRerouteIsNotCalledWhenAllHotSpottingNodesHaveRelocationInProgress() {
        final TestState testState = createRandomTestStateThatWillTriggerReroute();

        // Add another project to the cluster state containing a relocating shard on each hot-spotted node
        final ClusterState.Builder clusterStateWithReroutes = ClusterState.builder(testState.clusterState);
        final ProjectId projectIdNotInState = randomAdditionalProjectId(testState.clusterState);
        final ProjectMetadata.Builder projectMetadata = ProjectMetadata.builder(projectIdNotInState);
        final RoutingTable.Builder routingTableForProjectNotInState = RoutingTable.builder();
        for (NodeUsageStatsForThreadPools stats : testState.clusterInfo.getNodeUsageStatsForThreadPools().values()) {
            final boolean nodeIsHotSpotting = stats.threadPoolUsageStatsMap()
                .get(ThreadPool.Names.WRITE)
                .maxThreadPoolQueueLatencyMillis() > testState.latencyThresholdMillis;
            if (nodeIsHotSpotting) {
                final Index index = new Index(randomIdentifier(), randomUUID());
                final ShardId shardId = new ShardId(index, 0);
                projectMetadata.put(
                    IndexMetadata.builder(index.getName())
                        .settings(Settings.builder().put(SETTING_VERSION_CREATED, IndexVersion.current()).build())
                        .numberOfShards(1)
                        .numberOfReplicas(0)
                );
                routingTableForProjectNotInState.add(
                    IndexRoutingTable.builder(index)
                        .addIndexShard(
                            IndexShardRoutingTable.builder(shardId)
                                .addShard(
                                    TestShardRouting.newShardRouting(shardId, stats.nodeId(), true, ShardRoutingState.STARTED)
                                        .relocate(randomIdentifier(), randomNonNegativeLong())
                                )
                        )
                );
            }
        }
        final GlobalRoutingTable.Builder routingTableBuilder = GlobalRoutingTable.builder(testState.clusterState.globalRoutingTable());
        routingTableBuilder.put(projectIdNotInState, routingTableForProjectNotInState.build());
        clusterStateWithReroutes.routingTable(routingTableBuilder.build());
        clusterStateWithReroutes.putProjectMetadata(projectMetadata.build());
        final WriteLoadConstraintMonitor writeLoadConstraintMonitor = new WriteLoadConstraintMonitor(
            testState.clusterSettings,
            testState.currentTimeSupplier,
            clusterStateWithReroutes::build,
            testState.rerouteService
        );

        try (MockLog mockLog = MockLog.capture(WriteLoadConstraintMonitor.class)) {
            mockLog.addExpectation(
                new MockLog.SeenEventExpectation(
                    "don't reroute due to hot-spotting nodes all relocating",
                    WriteLoadConstraintMonitor.class.getCanonicalName(),
                    Level.DEBUG,
                    "All nodes over threshold have relocation in progress"
                )
            );

            writeLoadConstraintMonitor.onNewInfo(testState.clusterInfo);
            mockLog.assertAllExpectationsMatched();
            verifyNoInteractions(testState.rerouteService);
        }
    }

    @TestLogging(
        value = "org.elasticsearch.cluster.routing.allocation.WriteLoadConstraintMonitor:DEBUG",
        reason = "ensure we're skipping reroute for the right reason"
    )
    public void testRerouteIsNotCalledWhenNoNodeIsUnderUtilizationThreshold() {
        final TestState testState = createRandomTestStateThatWillTriggerReroute();

        // Transform the node usage stats so that all nodes are at the high-utilization threshold
        final var nodeUsageStatsWithHighUtilization = Maps.transformValues(
            testState.clusterInfo.getNodeUsageStatsForThreadPools(),
            stats -> new NodeUsageStatsForThreadPools(
                stats.nodeId(),
                Maps.transformValues(
                    stats.threadPoolUsageStatsMap(),
                    tpStats -> new NodeUsageStatsForThreadPools.ThreadPoolUsageStats(
                        tpStats.totalThreadPoolThreads(),
                        (testState.highUtilizationThresholdPercent + 1) / 100f,
                        tpStats.maxThreadPoolQueueLatencyMillis()
                    )
                )
            )
        );

        final WriteLoadConstraintMonitor writeLoadConstraintMonitor = new WriteLoadConstraintMonitor(
            testState.clusterSettings,
            testState.currentTimeSupplier,
            () -> testState.clusterState,
            testState.rerouteService
        );

        try (MockLog mockLog = MockLog.capture(WriteLoadConstraintMonitor.class)) {
            mockLog.addExpectation(
                new MockLog.SeenEventExpectation(
                    "don't reroute due to all nodes exceeding utilization threshold",
                    WriteLoadConstraintMonitor.class.getCanonicalName(),
                    Level.DEBUG,
                    "No nodes below utilization threshold that are not exceeding latency threshold"
                )
            );
            writeLoadConstraintMonitor.onNewInfo(
                ClusterInfo.builder().nodeUsageStatsForThreadPools(nodeUsageStatsWithHighUtilization).build()
            );
            mockLog.assertAllExpectationsMatched();
            verifyNoInteractions(testState.rerouteService);
        }
    }

    @TestLogging(
        value = "org.elasticsearch.cluster.routing.allocation.WriteLoadConstraintMonitor:DEBUG",
        reason = "ensure we're skipping reroute for the right reason"
    )
    public void testRerouteIsNotCalledWhenNoNodeIsUnderLatencyThreshold() {
        final TestState testState = createRandomTestStateThatWillTriggerReroute();

        final ClusterInfo clusterInfoWithAllNodesOverLatencyThreshold = createClusterInfo(
            testState.clusterState,
            testState.numberOfNodes,
            testState.latencyThresholdMillis,
            testState.highUtilizationThresholdPercent
        );

        final WriteLoadConstraintMonitor writeLoadConstraintMonitor = new WriteLoadConstraintMonitor(
            testState.clusterSettings,
            testState.currentTimeSupplier,
            () -> testState.clusterState,
            testState.rerouteService
        );

        try (MockLog mockLog = MockLog.capture(WriteLoadConstraintMonitor.class)) {
            mockLog.addExpectation(
                new MockLog.SeenEventExpectation(
                    "don't reroute due to all nodes exceeding latency threshold",
                    WriteLoadConstraintMonitor.class.getCanonicalName(),
                    Level.DEBUG,
                    "No nodes below utilization threshold that are not exceeding latency threshold"
                )
            );
            writeLoadConstraintMonitor.onNewInfo(clusterInfoWithAllNodesOverLatencyThreshold);
            mockLog.assertAllExpectationsMatched();
            verifyNoInteractions(testState.rerouteService);
        }
    }

    @TestLogging(
        value = "org.elasticsearch.cluster.routing.allocation.WriteLoadConstraintMonitor:DEBUG",
        reason = "ensure we're skipping reroute for the right reason"
    )
    public void testRerouteIsNotCalledAgainBeforeMinimumIntervalHasPassed() {
        final TestState testState = createRandomTestStateThatWillTriggerReroute();
        final TimeValue minimumInterval = testState.clusterSettings.get(
            WriteLoadConstraintSettings.WRITE_LOAD_DECIDER_REROUTE_INTERVAL_SETTING
        );
        final long nowMillis = System.currentTimeMillis();
        final AtomicLong currentTimeMillis = new AtomicLong(nowMillis);

        final WriteLoadConstraintMonitor writeLoadConstraintMonitor = new WriteLoadConstraintMonitor(
            testState.clusterSettings,
            currentTimeMillis::get,
            () -> testState.clusterState,
            testState.rerouteService
        );

        // We should trigger a re-route @ nowMillis
        writeLoadConstraintMonitor.onNewInfo(testState.clusterInfo);
        verify(testState.rerouteService).reroute(anyString(), eq(Priority.NORMAL), any());
        reset(testState.rerouteService);

        while (currentTimeMillis.get() < nowMillis + minimumInterval.millis()) {
            try (MockLog mockLog = MockLog.capture(WriteLoadConstraintMonitor.class)) {
                mockLog.addExpectation(
                    new MockLog.SeenEventExpectation(
                        "don't reroute due to reroute being called recently",
                        WriteLoadConstraintMonitor.class.getCanonicalName(),
                        Level.DEBUG,
                        "Not calling reroute because we called reroute recently and there are no new hot spots"
                    )
                );
                writeLoadConstraintMonitor.onNewInfo(testState.clusterInfo);
                mockLog.assertAllExpectationsMatched();
                verifyNoInteractions(testState.rerouteService);
            }

            currentTimeMillis.addAndGet(randomLongBetween(500, 1_000));
        }

        // We're now passed the minimum interval
        writeLoadConstraintMonitor.onNewInfo(testState.clusterInfo);
        verify(testState.rerouteService).reroute(anyString(), eq(Priority.NORMAL), any());
    }

    @TestLogging(
        value = "org.elasticsearch.cluster.routing.allocation.WriteLoadConstraintMonitor:DEBUG",
        reason = "ensure we're skipping reroute for the right reason"
    )
    public void testRerouteIsCalledBeforeMinimumIntervalHasPassedIfNewNodesBecomeHotSpotted() {
        final TestState testState = createRandomTestStateThatWillTriggerReroute();
        final long currentTime = System.currentTimeMillis();
        final LongSupplier currentTimeSupplier = () -> currentTime;

        final WriteLoadConstraintMonitor writeLoadConstraintMonitor = new WriteLoadConstraintMonitor(
            testState.clusterSettings,
            currentTimeSupplier,
            () -> testState.clusterState,
            testState.rerouteService
        );

        // We should trigger a re-route @ currentTime
        writeLoadConstraintMonitor.onNewInfo(testState.clusterInfo);
        verify(testState.rerouteService).reroute(anyString(), eq(Priority.NORMAL), any());
        reset(testState.rerouteService);

        // Now update cluster info to add to the hot-spotted nodes
        final AtomicBoolean thresholdIncreased = new AtomicBoolean(false);
        var nodeUsageStatsWithExtraHotSpot = Maps.transformValues(testState.clusterInfo.getNodeUsageStatsForThreadPools(), stats -> {
            if (thresholdIncreased.get() == false && nodeExceedsQueueLatencyThreshold(stats, testState.latencyThresholdMillis) == false) {
                thresholdIncreased.set(true);
                return new NodeUsageStatsForThreadPools(
                    stats.nodeId(),
                    Maps.transformValues(
                        stats.threadPoolUsageStatsMap(),
                        tpStats -> new NodeUsageStatsForThreadPools.ThreadPoolUsageStats(
                            tpStats.totalThreadPoolThreads(),
                            tpStats.averageThreadPoolUtilization(),
                            testState.latencyThresholdMillis + 1000
                        )
                    )
                );
            }
            return stats;
        });

        // We should reroute again despite the clock being the same
        writeLoadConstraintMonitor.onNewInfo(ClusterInfo.builder().nodeUsageStatsForThreadPools(nodeUsageStatsWithExtraHotSpot).build());
        verify(testState.rerouteService).reroute(anyString(), eq(Priority.NORMAL), any());
    }

    private static ProjectId randomAdditionalProjectId(ClusterState clusterState) {
        return ProjectId.fromId(
            randomValueOtherThanMany(
                projectId -> clusterState.metadata().hasProject(ProjectId.fromId(projectId)),
                ESTestCase::randomIdentifier
            )
        );
    }

    private boolean nodeExceedsQueueLatencyThreshold(NodeUsageStatsForThreadPools nodeUsageStats, long latencyThresholdMillis) {
        return nodeUsageStats.threadPoolUsageStatsMap()
            .get(ThreadPool.Names.WRITE)
            .maxThreadPoolQueueLatencyMillis() > latencyThresholdMillis;
    }

    private TestState createRandomTestStateThatWillTriggerReroute() {
        final long latencyThresholdMillis = randomLongBetween(1000, 5000);
        final int highUtilizationThresholdPercent = randomIntBetween(70, 100);
        final int numberOfNodes = randomIntBetween(3, 10);
        final ClusterSettings clusterSettings = createClusterSettings(
            WriteLoadConstraintSettings.WriteLoadDeciderStatus.ENABLED,
            latencyThresholdMillis,
            highUtilizationThresholdPercent
        );
        final LongSupplier currentTimeSupplier = ESTestCase::randomNonNegativeLong;
        final ClusterState state = ClusterStateCreationUtils.state(
            numberOfNodes,
            new String[] { randomIdentifier() },
            randomIntBetween(1, numberOfNodes)
        );
        final RerouteService rerouteService = mock(RerouteService.class);
        final ClusterInfo clusterInfo = createClusterInfo(
            state,
            randomIntBetween(1, numberOfNodes - 2),
            latencyThresholdMillis,
            highUtilizationThresholdPercent
        );
        return new TestState(
            latencyThresholdMillis,
            highUtilizationThresholdPercent,
            numberOfNodes,
            clusterSettings,
            currentTimeSupplier,
            state,
            rerouteService,
            clusterInfo
        );
    }

    private static ClusterSettings createClusterSettings(
        WriteLoadConstraintSettings.WriteLoadDeciderStatus status,
        long latencyThresholdMillis,
        int highUtilizationThresholdPercent
    ) {
        return ClusterSettings.createBuiltInClusterSettings(
            Settings.builder()
                .put(WriteLoadConstraintSettings.WRITE_LOAD_DECIDER_ENABLED_SETTING.getKey(), status.name())
                .put(
                    WriteLoadConstraintSettings.WRITE_LOAD_DECIDER_QUEUE_LATENCY_THRESHOLD_SETTING.getKey(),
                    TimeValue.timeValueMillis(latencyThresholdMillis)
                )
                .put(
                    WriteLoadConstraintSettings.WRITE_LOAD_DECIDER_HIGH_UTILIZATION_THRESHOLD_SETTING.getKey(),
                    highUtilizationThresholdPercent + "%"
                )
                .build()
        );
    }

    /**
     * Create a {@link ClusterInfo}
     *
     * @param state The cluster state
     * @param numberOfNodesHotSpotting The number of nodes that should be hot-spotting
     * @param latencyThresholdMillis The latency threshold in milliseconds
     * @param highUtilizationThresholdPercent The high utilization threshold as a percentage
     * @return a ClusterInfo with the given parameters
     */
    private static ClusterInfo createClusterInfo(
        ClusterState state,
        int numberOfNodesHotSpotting,
        long latencyThresholdMillis,
        int highUtilizationThresholdPercent
    ) {
        final float maxRatioForUnderUtilised = (highUtilizationThresholdPercent - 1) / 100.0f;
        final AtomicInteger hotSpottingNodes = new AtomicInteger(numberOfNodesHotSpotting);
        return ClusterInfo.builder()
            .nodeUsageStatsForThreadPools(state.nodes().stream().collect(Collectors.toMap(DiscoveryNode::getId, node -> {
                if (hotSpottingNodes.getAndDecrement() > 0) {
                    // hot-spotting node
                    return new NodeUsageStatsForThreadPools(
                        node.getId(),
                        Map.of(
                            ThreadPool.Names.WRITE,
                            new NodeUsageStatsForThreadPools.ThreadPoolUsageStats(
                                randomNonNegativeInt(),
                                randomFloatBetween(0f, 1f, true),
                                randomLongBetween(latencyThresholdMillis + 1, latencyThresholdMillis * 2)
                            )
                        )
                    );
                } else {
                    // not-hot-spotting node
                    return new NodeUsageStatsForThreadPools(
                        node.getId(),
                        Map.of(
                            ThreadPool.Names.WRITE,
                            new NodeUsageStatsForThreadPools.ThreadPoolUsageStats(
                                randomNonNegativeInt(),
                                randomFloatBetween(0f, maxRatioForUnderUtilised, true),
                                randomLongBetween(0, latencyThresholdMillis)
                            )
                        )
                    );
                }
            })))
            .build();
    }

    private record TestState(
        long latencyThresholdMillis,
        int highUtilizationThresholdPercent,
        int numberOfNodes,
        ClusterSettings clusterSettings,
        LongSupplier currentTimeSupplier,
        ClusterState clusterState,
        RerouteService rerouteService,
        ClusterInfo clusterInfo
    ) {}
}
