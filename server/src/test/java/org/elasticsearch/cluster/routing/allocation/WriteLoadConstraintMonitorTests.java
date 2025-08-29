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
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.RerouteService;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.gateway.GatewayService;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.MockLog;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
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
            testState.mockRerouteService
        );

        writeLoadConstraintMonitor.onNewInfo(testState.clusterInfo);
        verify(testState.mockRerouteService).reroute(anyString(), eq(Priority.NORMAL), any());
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
            testState.mockRerouteService
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
            verifyNoInteractions(testState.mockRerouteService);
        }
    }

    @TestLogging(
        value = "org.elasticsearch.cluster.routing.allocation.WriteLoadConstraintMonitor:DEBUG",
        reason = "ensure we're skipping reroute for the right reason"
    )
    public void testRerouteIsNotCalledWhenDeciderIsNotEnabled() {
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
            testState.mockRerouteService
        );

        try (MockLog mockLog = MockLog.capture(WriteLoadConstraintMonitor.class)) {
            mockLog.addExpectation(
                new MockLog.SeenEventExpectation(
                    "don't reroute due to decider being disabled",
                    WriteLoadConstraintMonitor.class.getCanonicalName(),
                    Level.DEBUG,
                    "skipping monitor because the write load decider is not fully enabled"
                )
            );

            writeLoadConstraintMonitor.onNewInfo(testState.clusterInfo);
            mockLog.assertAllExpectationsMatched();
            verifyNoInteractions(testState.mockRerouteService);
        }
    }

    @TestLogging(
        value = "org.elasticsearch.cluster.routing.allocation.WriteLoadConstraintMonitor:DEBUG",
        reason = "ensure we're skipping reroute for the right reason"
    )
    public void testRerouteIsNotCalledWhenNoNodesAreHotSpotting() {
        final TestState testState = createRandomTestStateThatWillTriggerReroute();
        final WriteLoadConstraintMonitor writeLoadConstraintMonitor = new WriteLoadConstraintMonitor(
            testState.clusterSettings,
            testState.currentTimeSupplier,
            () -> testState.clusterState,
            testState.mockRerouteService
        );

        try (MockLog mockLog = MockLog.capture(WriteLoadConstraintMonitor.class)) {
            mockLog.addExpectation(
                new MockLog.SeenEventExpectation(
                    "don't reroute due to no nodes hot-spotting",
                    WriteLoadConstraintMonitor.class.getCanonicalName(),
                    Level.DEBUG,
                    "No hot-spotting nodes detected"
                )
            );

            writeLoadConstraintMonitor.onNewInfo(
                createClusterInfoWithHotSpots(
                    testState.clusterState,
                    0,
                    testState.latencyThresholdMillis,
                    testState.highUtilizationThresholdPercent
                )
            );
            mockLog.assertAllExpectationsMatched();
            verifyNoInteractions(testState.mockRerouteService);
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
        assertThat(minimumInterval, greaterThan(TimeValue.ZERO));
        final long nowMillis = System.currentTimeMillis();
        final AtomicLong currentTimeMillis = new AtomicLong(nowMillis);

        final WriteLoadConstraintMonitor writeLoadConstraintMonitor = new WriteLoadConstraintMonitor(
            testState.clusterSettings,
            currentTimeMillis::get,
            () -> testState.clusterState,
            testState.mockRerouteService
        );

        // We should trigger a re-route @ nowMillis
        writeLoadConstraintMonitor.onNewInfo(testState.clusterInfo);
        verify(testState.mockRerouteService).reroute(anyString(), eq(Priority.NORMAL), any());
        reset(testState.mockRerouteService);

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
                verifyNoInteractions(testState.mockRerouteService);
            }

            currentTimeMillis.addAndGet(randomLongBetween(500, 1_000));
        }

        // We're now passed the minimum interval
        writeLoadConstraintMonitor.onNewInfo(testState.clusterInfo);
        verify(testState.mockRerouteService).reroute(anyString(), eq(Priority.NORMAL), any());
    }

    @TestLogging(
        value = "org.elasticsearch.cluster.routing.allocation.WriteLoadConstraintMonitor:DEBUG",
        reason = "ensure we're skipping reroute for the right reason"
    )
    public void testRerouteIsCalledBeforeMinimumIntervalHasPassedIfNewNodesBecomeHotSpotted() {
        final TestState testState = createRandomTestStateThatWillTriggerReroute();
        final AtomicLong currentTimeMillis = new AtomicLong(System.currentTimeMillis());
        final TimeValue minimumInterval = testState.clusterSettings.get(
            WriteLoadConstraintSettings.WRITE_LOAD_DECIDER_REROUTE_INTERVAL_SETTING
        );
        assertThat(minimumInterval, greaterThan(TimeValue.ZERO));

        final WriteLoadConstraintMonitor writeLoadConstraintMonitor = new WriteLoadConstraintMonitor(
            testState.clusterSettings,
            currentTimeMillis::get,
            () -> testState.clusterState,
            testState.mockRerouteService
        );

        // We should trigger a re-route @ currentTime
        writeLoadConstraintMonitor.onNewInfo(testState.clusterInfo);
        verify(testState.mockRerouteService).reroute(anyString(), eq(Priority.NORMAL), any());
        reset(testState.mockRerouteService);

        assertThat(
            "Test setup should leave at least two nodes not hot-spotted",
            testState.clusterInfo.getNodeUsageStatsForThreadPools().size() - testState.clusterInfo.getNodeUsageStatsForThreadPools()
                .values()
                .stream()
                .filter(stats -> nodeExceedsQueueLatencyThreshold(stats, testState.latencyThresholdMillis))
                .count(),
            greaterThanOrEqualTo(2L)
        );

        // Now update cluster info to add another hot-spotted node
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
                            testState.latencyThresholdMillis + randomLongBetween(1, 100_000)
                        )
                    )
                );
            }
            return stats;
        });

        // Advance the clock by less than the re-route interval
        currentTimeMillis.addAndGet(randomLongBetween(0, minimumInterval.millis() - 1));

        // We should reroute again despite the minimum interval not having passed
        writeLoadConstraintMonitor.onNewInfo(ClusterInfo.builder().nodeUsageStatsForThreadPools(nodeUsageStatsWithExtraHotSpot).build());
        verify(testState.mockRerouteService).reroute(anyString(), eq(Priority.NORMAL), any());
    }

    private boolean nodeExceedsQueueLatencyThreshold(NodeUsageStatsForThreadPools nodeUsageStats, long latencyThresholdMillis) {
        return nodeUsageStats.threadPoolUsageStatsMap()
            .get(ThreadPool.Names.WRITE)
            .maxThreadPoolQueueLatencyMillis() > latencyThresholdMillis;
    }

    private TestState createRandomTestStateThatWillTriggerReroute() {
        final long queueLatencyThresholdMillis = randomLongBetween(1000, 5000);
        final int highUtilizationThresholdPercent = randomIntBetween(70, 100);
        final int numberOfNodes = randomIntBetween(3, 10);
        final ClusterSettings clusterSettings = createClusterSettings(
            WriteLoadConstraintSettings.WriteLoadDeciderStatus.ENABLED,
            queueLatencyThresholdMillis,
            highUtilizationThresholdPercent
        );
        final ClusterState state = ClusterStateCreationUtils.state(
            numberOfNodes,
            new String[] { randomIdentifier() },
            randomIntBetween(1, numberOfNodes)
        );
        final RerouteService rerouteService = mock(RerouteService.class);
        final ClusterInfo clusterInfo = createClusterInfoWithHotSpots(
            state,
            randomIntBetween(1, numberOfNodes - 2),
            queueLatencyThresholdMillis,
            highUtilizationThresholdPercent
        );
        return new TestState(
            queueLatencyThresholdMillis,
            highUtilizationThresholdPercent,
            numberOfNodes,
            clusterSettings,
            System::currentTimeMillis,
            state,
            rerouteService,
            clusterInfo
        );
    }

    private static ClusterSettings createClusterSettings(
        WriteLoadConstraintSettings.WriteLoadDeciderStatus status,
        long queueLatencyThresholdMillis,
        int highUtilizationThresholdPercent
    ) {
        return ClusterSettings.createBuiltInClusterSettings(
            Settings.builder()
                .put(WriteLoadConstraintSettings.WRITE_LOAD_DECIDER_ENABLED_SETTING.getKey(), status.name())
                .put(
                    WriteLoadConstraintSettings.WRITE_LOAD_DECIDER_QUEUE_LATENCY_THRESHOLD_SETTING.getKey(),
                    TimeValue.timeValueMillis(queueLatencyThresholdMillis)
                )
                .put(
                    WriteLoadConstraintSettings.WRITE_LOAD_DECIDER_HIGH_UTILIZATION_THRESHOLD_SETTING.getKey(),
                    highUtilizationThresholdPercent + "%"
                )
                .put(
                    WriteLoadConstraintSettings.WRITE_LOAD_DECIDER_REROUTE_INTERVAL_SETTING.getKey(),
                    randomTimeValue(1, 30, TimeUnit.SECONDS)
                )
                .build()
        );
    }

    /**
     * Create a {@link ClusterInfo} with the specified number of hot spotting nodes,
     * all other nodes will have no queue latency and have utilization below the specified
     * high-utilization threshold.
     *
     * @param state The cluster state
     * @param numberOfNodesHotSpotting The number of nodes that should be hot-spotting
     * @param queueLatencyThresholdMillis The latency threshold in milliseconds
     * @param highUtilizationThresholdPercent The high utilization threshold as a percentage
     * @return a ClusterInfo with the given parameters
     */
    private static ClusterInfo createClusterInfoWithHotSpots(
        ClusterState state,
        int numberOfNodesHotSpotting,
        long queueLatencyThresholdMillis,
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
                                randomLongBetween(queueLatencyThresholdMillis + 1, queueLatencyThresholdMillis * 2)
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
                                randomLongBetween(0, queueLatencyThresholdMillis)
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
        RerouteService mockRerouteService,
        ClusterInfo clusterInfo
    ) {}
}
