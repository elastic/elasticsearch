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
import org.elasticsearch.cluster.coordination.CoordinationMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.routing.RerouteService;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.gateway.GatewayService;
import org.elasticsearch.telemetry.InstrumentType;
import org.elasticsearch.telemetry.Measurement;
import org.elasticsearch.telemetry.MetricRecorder;
import org.elasticsearch.telemetry.RecordingMeterRegistry;
import org.elasticsearch.telemetry.metric.Instrument;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.MockLog;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.LongSupplier;
import java.util.stream.Collectors;

import static org.elasticsearch.cluster.routing.allocation.WriteLoadConstraintMonitor.HOTSPOT_DURATION_METRIC_NAME;
import static org.elasticsearch.cluster.routing.allocation.WriteLoadConstraintMonitor.HOTSPOT_NODES_COUNT_METRIC_NAME;
import static org.hamcrest.Matchers.greaterThan;
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
        value = "org.elasticsearch.cluster.routing.allocation.WriteLoadConstraintMonitor:TRACE",
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
                    Level.TRACE,
                    "skipping monitor as the cluster state is not recovered yet"
                )
            );

            writeLoadConstraintMonitor.onNewInfo(testState.clusterInfo);
            mockLog.assertAllExpectationsMatched();
            verifyNoInteractions(testState.mockRerouteService);
        }
    }

    @TestLogging(
        value = "org.elasticsearch.cluster.routing.allocation.WriteLoadConstraintMonitor:TRACE",
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
                    Level.TRACE,
                    "skipping monitor because the write load decider is not fully enabled"
                )
            );

            writeLoadConstraintMonitor.onNewInfo(testState.clusterInfo);
            mockLog.assertAllExpectationsMatched();
            verifyNoInteractions(testState.mockRerouteService);
        }
    }

    @TestLogging(
        value = "org.elasticsearch.cluster.routing.allocation.WriteLoadConstraintMonitor:TRACE",
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
                    Level.TRACE,
                    "No hot-spotting write nodes detected"
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
    public void testRerouteIsNotCalledInAnAllNodesAreHotSpottingCluster() {
        final int numberOfIndexNodes = randomIntBetween(1, 5);
        final TestState testState = createTestStateWithNumberOfNodesAndHotSpots(
            numberOfIndexNodes,
            randomIntBetween(1, 5), // Search nodes should not be considered to address write load hot-spots.
            randomIntBetween(1, 5), // ML nodes should not be considered to address write load hot-spots.
            numberOfIndexNodes
        );
        final WriteLoadConstraintMonitor writeLoadConstraintMonitor = new WriteLoadConstraintMonitor(
            testState.clusterSettings,
            testState.currentTimeSupplier,
            () -> testState.clusterState,
            testState.mockRerouteService
        );
        try (MockLog mockLog = MockLog.capture(WriteLoadConstraintMonitor.class)) {
            mockLog.addExpectation(
                new MockLog.SeenEventExpectation(
                    "don't reroute when all nodes are hot-spotting",
                    WriteLoadConstraintMonitor.class.getCanonicalName(),
                    Level.DEBUG,
                    "Nodes * are above the queue latency threshold, but there are no write nodes below the threshold. "
                        + "Cannot rebalance shards."
                )
            );

            writeLoadConstraintMonitor.onNewInfo(
                createClusterInfoWithHotSpots(
                    testState.clusterState,
                    numberOfIndexNodes,
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
                        "Not calling reroute because we called reroute * ago and there are no new hot spots"
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
        value = "org.elasticsearch.cluster.routing.allocation.WriteLoadConstraintMonitor:TRACE",
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

        // Now update cluster info to add another hot-spotted node
        final AtomicBoolean thresholdIncreased = new AtomicBoolean(false);
        Map<String, NodeUsageStatsForThreadPools> nodeUsageStatsWithExtraHotSpot = new HashMap<>();
        for (var entry : testState.clusterInfo.getNodeUsageStatsForThreadPools().entrySet()) {
            if (thresholdIncreased.get() == false
                && indexingNodeBelowQueueLatencyThreshold(
                    testState.clusterState,
                    entry.getKey(),
                    entry.getValue(),
                    testState.latencyThresholdMillis
                )) {
                thresholdIncreased.set(true);
                nodeUsageStatsWithExtraHotSpot.put(
                    entry.getKey(),
                    new NodeUsageStatsForThreadPools(
                        entry.getKey(),
                        Maps.transformValues(
                            entry.getValue().threadPoolUsageStatsMap(),
                            tpStats -> new NodeUsageStatsForThreadPools.ThreadPoolUsageStats(
                                tpStats.totalThreadPoolThreads(),
                                tpStats.averageThreadPoolUtilization(),
                                testState.latencyThresholdMillis + randomLongBetween(1, 100_000)
                            )
                        )
                    )
                );
            } else {
                nodeUsageStatsWithExtraHotSpot.put(entry.getKey(), entry.getValue());
            }
        }

        // Advance the clock by less than the re-route interval
        currentTimeMillis.addAndGet(randomLongBetween(0, minimumInterval.millis() - 1));

        // We should reroute again despite the minimum interval not having passed
        writeLoadConstraintMonitor.onNewInfo(ClusterInfo.builder().nodeUsageStatsForThreadPools(nodeUsageStatsWithExtraHotSpot).build());
        verify(testState.mockRerouteService).reroute(anyString(), eq(Priority.NORMAL), any());
    }

    public void testEmptyHotspotCount() {
        // test that there is no count or histogram issued when there have been no onNewInfo calls
        TestState testState = createTestStateWithNumberOfNodesAndHotSpots(10, 1, 1, 0);
        final ClusterState clusterState = testState.clusterState();

        final RecordingMeterRegistry recordingMeterRegistry = new RecordingMeterRegistry();
        final WriteLoadConstraintMonitor writeLoadConstraintMonitor = new WriteLoadConstraintMonitor(
            testState.clusterSettings,
            testState.currentTimeSupplier,
            () -> clusterState,
            testState.mockRerouteService,
            recordingMeterRegistry
        );

        recordingMeterRegistry.getRecorder().collect();
        assertMetricsCollected(recordingMeterRegistry, List.of(), List.of());
    }

    public void testZeroHotspotCount() {
        // test a zero count is issued when there has been an onNewInfo call
        TestState testState = createTestStateWithNumberOfNodesAndHotSpots(10, 1, 1, 0);
        final ClusterState clusterState = testState.clusterState();

        final RecordingMeterRegistry recordingMeterRegistry = new RecordingMeterRegistry();
        final WriteLoadConstraintMonitor writeLoadConstraintMonitor = new WriteLoadConstraintMonitor(
            testState.clusterSettings,
            testState.currentTimeSupplier,
            () -> clusterState,
            testState.mockRerouteService,
            recordingMeterRegistry
        );

        writeLoadConstraintMonitor.onNewInfo(testState.clusterInfo);

        recordingMeterRegistry.getRecorder().collect();
        assertMetricsCollected(recordingMeterRegistry, List.of(0L), List.of());
    }

    public void testHotspotCountTurnsOff() {
        /* Test that collecting metrics without calling WriteLoadConstraintMonitor::onNewInfo returns no new data,
        and that changing the term on cluster state clears the hotspot duration table */
        TestState testState = createTestStateWithNumberOfNodesAndHotSpots(10, 1, 1, 2, true);

        final long nowMillis = System.currentTimeMillis();
        final AtomicLong currentTimeMillis = new AtomicLong(nowMillis);
        final AtomicReference<ClusterState> clusterStateRef = new AtomicReference<>(testState.clusterState());

        final RecordingMeterRegistry recordingMeterRegistry = new RecordingMeterRegistry();
        final WriteLoadConstraintMonitor writeLoadConstraintMonitor = new WriteLoadConstraintMonitor(
            testState.clusterSettings,
            currentTimeMillis::get,
            clusterStateRef::get,
            testState.mockRerouteService,
            recordingMeterRegistry
        );

        writeLoadConstraintMonitor.onNewInfo(testState.clusterInfo);

        recordingMeterRegistry.getRecorder().collect();
        assertMetricsCollected(recordingMeterRegistry, List.of(2L), List.of());

        // remove one of two nodes from the hotspot, to create one finished duration and one in-progress
        String removeId = randomFrom(testState.hotspotNodeIds());
        testState = testState.removeFromClusterInfoHotspot(List.of(removeId));
        long duration = randomLongBetween(500, 2000);
        currentTimeMillis.addAndGet(duration);

        writeLoadConstraintMonitor.onNewInfo(testState.clusterInfo);

        recordingMeterRegistry.getRecorder().collect();
        assertMetricsCollected(recordingMeterRegistry, List.of(2L, 1L), List.of(duration / 1000.0));

        // no count is issued for this collection round, as onNewInfo hasn't been called
        recordingMeterRegistry.getRecorder().collect();
        assertMetricsCollected(recordingMeterRegistry, List.of(2L, 1L), List.of(duration / 1000.0));

        // change cluster state term, and see that the hotspot table is reset
        testState = testState.removeFromClusterInfoHotspot(testState.hotspotNodeIds());
        testState = testState.incrementClusterStateTerm();
        clusterStateRef.set(testState.clusterState());

        writeLoadConstraintMonitor.onNewInfo(testState.clusterInfo);
        recordingMeterRegistry.getRecorder().collect();
        assertMetricsCollected(recordingMeterRegistry, List.of(2L, 1L, 0L), List.of(duration / 1000.0));
    }

    public void testHotspotDurationsAreRecorded() {
        TestState testState = createTestStateWithNumberOfNodesAndHotSpots(10, 1, 1, 5);

        final long nowMillis = System.currentTimeMillis();
        final AtomicLong currentTimeMillis = new AtomicLong(nowMillis);
        final ClusterState clusterState = testState.clusterState();

        final RecordingMeterRegistry recordingMeterRegistry = new RecordingMeterRegistry();
        final WriteLoadConstraintMonitor writeLoadConstraintMonitor = new WriteLoadConstraintMonitor(
            testState.clusterSettings,
            currentTimeMillis::get,
            () -> clusterState,
            testState.mockRerouteService,
            recordingMeterRegistry
        );

        final Set<String> firstWaveHotspotNodes = testState.hotspotNodeIds();
        final String removeHotspotId = randomFrom(testState.hotspotNodeIds());
        final List<Long> hotspotSizes = new ArrayList<>();
        hotspotSizes.add((long) testState.hotspotNodeIds().size());

        writeLoadConstraintMonitor.onNewInfo(testState.clusterInfo);
        verify(testState.mockRerouteService).reroute(anyString(), eq(Priority.NORMAL), any());
        reset(testState.mockRerouteService);

        // check hotspot currently is set up in the counter
        recordingMeterRegistry.getRecorder().collect();
        assertMetricsCollected(recordingMeterRegistry, hotspotSizes, List.of());

        // add a node, and see hotspot count go up
        long millisAddedFirst = randomLongBetween(500, 1_000);
        currentTimeMillis.addAndGet(millisAddedFirst);
        testState = testState.addToClusterInfoHotspot(1);
        hotspotSizes.add((long) testState.hotspotNodeIds().size());

        writeLoadConstraintMonitor.onNewInfo(testState.clusterInfo());
        verify(testState.mockRerouteService).reroute(anyString(), eq(Priority.NORMAL), any());
        reset(testState.mockRerouteService);

        recordingMeterRegistry.getRecorder().collect();
        assertMetricsCollected(recordingMeterRegistry, hotspotSizes, List.of());

        // remove a node, and see the count go down and a duration issued
        long millisAddedSecond = randomLongBetween(500, 1_000);
        currentTimeMillis.addAndGet(millisAddedSecond);
        testState = testState.removeFromClusterInfoHotspot(List.of(removeHotspotId));
        hotspotSizes.add((long) testState.hotspotNodeIds().size());

        writeLoadConstraintMonitor.onNewInfo(testState.clusterInfo());
        recordingMeterRegistry.getRecorder().collect();
        assertMetricsCollected(recordingMeterRegistry, hotspotSizes, List.of((millisAddedFirst + millisAddedSecond) / 1000.0));

        // remove all the nodes from the first series, and see all but one durations issued
        long millisAddedThird = randomLongBetween(500, 1_000);
        currentTimeMillis.addAndGet(millisAddedThird);
        testState = testState.removeFromClusterInfoHotspot(firstWaveHotspotNodes);
        hotspotSizes.add(1L);

        writeLoadConstraintMonitor.onNewInfo(testState.clusterInfo());

        List<Double> hotspotDurations = new ArrayList<>();
        hotspotDurations.add((millisAddedFirst + millisAddedSecond) / 1000.0);
        for (int i = 0; i < firstWaveHotspotNodes.size() - 1; i++) {
            hotspotDurations.add((millisAddedFirst + millisAddedSecond + millisAddedThird) / 1000.0);
        }
        recordingMeterRegistry.getRecorder().collect();
        assertMetricsCollected(recordingMeterRegistry, hotspotSizes, hotspotDurations);

        // remove the last node from the series, and see the last duration issued
        long millisAddedFourth = randomLongBetween(500, 1_000);
        currentTimeMillis.addAndGet(millisAddedFourth);
        testState = testState.removeFromClusterInfoHotspot(testState.hotspotNodeIds());
        hotspotSizes.add(0L);

        writeLoadConstraintMonitor.onNewInfo(testState.clusterInfo());
        hotspotDurations.add((millisAddedSecond + millisAddedThird + millisAddedFourth) / 1000.0);
        recordingMeterRegistry.getRecorder().collect();
        assertMetricsCollected(recordingMeterRegistry, hotspotSizes, hotspotDurations);
    }

    private boolean indexingNodeBelowQueueLatencyThreshold(
        ClusterState clusterState,
        String nodeId,
        NodeUsageStatsForThreadPools nodeUsageStats,
        long latencyThresholdMillis
    ) {
        final var nodeRoles = clusterState.getNodes().get(nodeId).getRoles();
        return nodeRoles.contains(DiscoveryNodeRole.SEARCH_ROLE) == false
            && nodeRoles.contains(DiscoveryNodeRole.ML_ROLE) == false
            && nodeUsageStats.threadPoolUsageStatsMap()
                .get(ThreadPool.Names.WRITE)
                .maxThreadPoolQueueLatencyMillis() < latencyThresholdMillis;
    }

    private TestState createRandomTestStateThatWillTriggerReroute() {
        int numberOfNodes = randomIntBetween(3, 10);
        int numberOfHotSpottingNodes = numberOfNodes - 2; // Leave at least 2 non-hot-spotting nodes.
        return createTestStateWithNumberOfNodesAndHotSpots(
            numberOfNodes,
            randomIntBetween(0, 5), // search nodes
            randomIntBetween(0, 2), // ML nodes
            numberOfHotSpottingNodes
        );
    }

    private TestState createTestStateWithNumberOfNodesAndHotSpots(
        int numberOfIndexNodes,
        int numberOfSearchNodes,
        int numberOfMLNodes,
        int numberOfHotSpottingNodes
    ) {
        return createTestStateWithNumberOfNodesAndHotSpots(
            numberOfIndexNodes,
            numberOfSearchNodes,
            numberOfMLNodes,
            numberOfHotSpottingNodes,
            false
        );
    }

    private TestState createTestStateWithNumberOfNodesAndHotSpots(
        int numberOfIndexNodes,
        int numberOfSearchNodes,
        int numberOfMLNodes,
        int numberOfHotSpottingNodes,
        boolean exactHotspotCount
    ) {
        assert numberOfHotSpottingNodes <= numberOfIndexNodes;
        final long queueLatencyThresholdMillis = randomLongBetween(1000, 5000);
        final int highUtilizationThresholdPercent = randomIntBetween(70, 100);
        final ClusterSettings clusterSettings = createClusterSettings(
            WriteLoadConstraintSettings.WriteLoadDeciderStatus.ENABLED,
            queueLatencyThresholdMillis,
            highUtilizationThresholdPercent
        );
        final ClusterState state = ClusterStateCreationUtils.buildServerlessRoleNodes(
            randomIdentifier(), // index name
            randomIntBetween(1, numberOfIndexNodes),  // num shard primaries
            numberOfIndexNodes,
            numberOfSearchNodes,
            numberOfMLNodes
        );

        final RerouteService rerouteService = mock(RerouteService.class);

        final Set<String> hotspotNodes;
        if (numberOfHotSpottingNodes > 0) {
            if (exactHotspotCount == false) {
                numberOfHotSpottingNodes = randomIntBetween(1, numberOfHotSpottingNodes);
            }
            hotspotNodes = new HashSet<>(randomSubsetOf(numberOfHotSpottingNodes, indexNodeIds(state)));
        } else {
            hotspotNodes = Collections.emptySet();
        }
        final ClusterInfo clusterInfo = createClusterInfoWithHotSpots(
            state,
            hotspotNodes,
            queueLatencyThresholdMillis,
            highUtilizationThresholdPercent
        );
        return new TestState(
            queueLatencyThresholdMillis,
            highUtilizationThresholdPercent,
            numberOfIndexNodes,
            hotspotNodes,
            clusterSettings,
            System::currentTimeMillis,
            state,
            rerouteService,
            clusterInfo
        );
    }

    private static Set<String> indexNodeIds(ClusterState clusterState) {
        return clusterState.nodes()
            .stream()
            .filter(node -> node.getRoles().contains(DiscoveryNodeRole.INDEX_ROLE))
            .map(node -> node.getId())
            .collect(Collectors.toSet());
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
     * Create a {@link ClusterInfo} with the specified number of hot spotting index nodes,
     * all other index nodes will have no queue latency and have utilization below the specified
     * high-utilization threshold. Any search or ML nodes in the cluster will have zero usage
     * write load stats.
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
        Set<String> nodeIds = indexNodeIds(state);
        assert numberOfNodesHotSpotting <= nodeIds.size()
            : "Requested "
                + numberOfNodesHotSpotting
                + " hot spotting nodes, but there are only "
                + nodeIds.size()
                + " nodes in the cluster";

        final Set<String> hotspotNodes = new HashSet<>(randomSubsetOf(numberOfNodesHotSpotting, nodeIds));
        return createClusterInfoWithHotSpots(state, hotspotNodes, queueLatencyThresholdMillis, highUtilizationThresholdPercent);
    }

    private static ClusterInfo createClusterInfoWithHotSpots(
        ClusterState state,
        Set<String> hotspotNodes,
        long queueLatencyThresholdMillis,
        int highUtilizationThresholdPercent
    ) {
        final Set<String> hotspotNodesSet = new HashSet<>(hotspotNodes);
        final float maxRatioForUnderUtilised = (highUtilizationThresholdPercent - 1) / 100.0f;
        ClusterInfo clusterInfo = ClusterInfo.builder()
            .nodeUsageStatsForThreadPools(state.nodes().stream().collect(Collectors.toMap(DiscoveryNode::getId, node -> {
                if (node.getRoles().contains(DiscoveryNodeRole.SEARCH_ROLE) || node.getRoles().contains(DiscoveryNodeRole.ML_ROLE)) {
                    // Search & ML nodes are skipped for write load hot-spots.
                    return new NodeUsageStatsForThreadPools(node.getId(), ZERO_USAGE_THREAD_POOL_USAGE_MAP);
                }
                if (hotspotNodesSet.remove(node.getId())) {
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

        assert hotspotNodesSet.isEmpty() : "hotspot nodes set should be empty";
        return clusterInfo;
    }

    private record TestState(
        long latencyThresholdMillis,
        int highUtilizationThresholdPercent,
        int numberOfNodes,
        Set<String> hotspotNodeIds,
        ClusterSettings clusterSettings,
        LongSupplier currentTimeSupplier,
        ClusterState clusterState,
        RerouteService mockRerouteService,
        ClusterInfo clusterInfo
    ) {
        private TestState addToClusterInfoHotspot(int addHotspotNodes) {
            Set<String> nodeIds = indexNodeIds(clusterState);
            Set<String> newHotspotNodeIds = new HashSet<>(randomSubsetOf(addHotspotNodes, Sets.difference(nodeIds, hotspotNodeIds)));
            newHotspotNodeIds.addAll(hotspotNodeIds);

            return new TestState(
                latencyThresholdMillis,
                highUtilizationThresholdPercent,
                numberOfNodes,
                newHotspotNodeIds,
                clusterSettings,
                currentTimeSupplier,
                clusterState,
                mockRerouteService,
                createClusterInfoWithHotSpots(clusterState, newHotspotNodeIds, latencyThresholdMillis, highUtilizationThresholdPercent)
            );
        }

        private TestState removeFromClusterInfoHotspot(Collection<String> removeHotspotNodes) {
            Set<String> newHotspotNodeIds = new HashSet<>(hotspotNodeIds);
            newHotspotNodeIds.removeAll(removeHotspotNodes);

            return new TestState(
                latencyThresholdMillis,
                highUtilizationThresholdPercent,
                numberOfNodes,
                newHotspotNodeIds,
                clusterSettings,
                currentTimeSupplier,
                clusterState,
                mockRerouteService,
                createClusterInfoWithHotSpots(clusterState, newHotspotNodeIds, latencyThresholdMillis, highUtilizationThresholdPercent)
            );
        }

        private TestState incrementClusterStateTerm() {
            ClusterState state = ClusterState.builder(clusterState).metadata(
                Metadata.builder(clusterState.metadata()).coordinationMetadata(
                    CoordinationMetadata.builder(clusterState.metadata().coordinationMetadata())
                        .term(clusterState.term() + 1)
                        .build()))
                .build();

            return new TestState(
                latencyThresholdMillis,
                highUtilizationThresholdPercent,
                numberOfNodes,
                hotspotNodeIds,
                clusterSettings,
                currentTimeSupplier,
                state,
                mockRerouteService,
                clusterInfo
            );
        }
    }

    public static final Map<String, NodeUsageStatsForThreadPools.ThreadPoolUsageStats> ZERO_USAGE_THREAD_POOL_USAGE_MAP = Map.of(
        ThreadPool.Names.WRITE,
        new NodeUsageStatsForThreadPools.ThreadPoolUsageStats(5, 0, 0)
    );

    private void assertMetricsCollected(
        RecordingMeterRegistry recordingMeterRegistry,
        List<Long> hotspotCounts,
        List<Double> hotspotDurations
    ) {
        MetricRecorder<Instrument> metricRecorder = recordingMeterRegistry.getRecorder();

        List<Measurement> measuredHotspotCounts = metricRecorder.getMeasurements(
            InstrumentType.LONG_GAUGE,
            HOTSPOT_NODES_COUNT_METRIC_NAME
        );
        List<Long> measuredHotspotCountValues = Measurement.getMeasurementValues(
            measuredHotspotCounts,
            (measurement -> measurement.getLong())
        );
        assertEquals(hotspotCounts, measuredHotspotCountValues);

        List<Measurement> measuredHotspotDurations = metricRecorder.getMeasurements(
            InstrumentType.DOUBLE_HISTOGRAM,
            HOTSPOT_DURATION_METRIC_NAME
        );
        List<Double> measuredHotspotDurationValues = Measurement.getMeasurementValues(
            measuredHotspotDurations,
            (measurement -> measurement.getDouble())
        );
        assertEquals(hotspotDurations, measuredHotspotDurationValues);
    }
}
