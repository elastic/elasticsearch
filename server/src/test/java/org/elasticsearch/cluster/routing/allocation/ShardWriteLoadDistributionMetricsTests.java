/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.routing.allocation;

import org.elasticsearch.action.support.replication.ClusterStateCreationUtils;
import org.elasticsearch.cluster.ClusterInfo;
import org.elasticsearch.cluster.ClusterModule;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.RoutingChangesObserver;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.allocation.allocator.BalancedShardsAllocator;
import org.elasticsearch.cluster.routing.allocation.decider.AllocationDeciders;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.component.Lifecycle;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.snapshots.SnapshotShardSizeInfo;
import org.elasticsearch.telemetry.InstrumentType;
import org.elasticsearch.telemetry.Measurement;
import org.elasticsearch.telemetry.RecordingMeterRegistry;
import org.elasticsearch.test.ESTestCase;
import org.hamcrest.Matchers;

import java.math.BigDecimal;
import java.math.MathContext;
import java.math.RoundingMode;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ShardWriteLoadDistributionMetricsTests extends ESTestCase {

    public void testShardWriteLoadDistributionMetrics() {
        final var testInfrastructure = createTestInfrastructure();

        testInfrastructure.shardWriteLoadDistributionMetrics.onNewInfo(testInfrastructure.clusterInfo);
        testInfrastructure.meterRegistry.getRecorder().collect();

        final var writeLoadDistributionMeasurements = testInfrastructure.meterRegistry.getRecorder()
            .getMeasurements(InstrumentType.DOUBLE_GAUGE, ShardWriteLoadDistributionMetrics.WRITE_LOAD_DISTRIBUTION_METRIC_NAME);
        final var writeLoadPrioritisationThresholdMeasurements = testInfrastructure.meterRegistry.getRecorder()
            .getMeasurements(
                InstrumentType.DOUBLE_GAUGE,
                ShardWriteLoadDistributionMetrics.WRITE_LOAD_PRIORITISATION_THRESHOLD_METRIC_NAME
            );
        final var countAboveThresholdMeasurements = testInfrastructure.meterRegistry.getRecorder()
            .getMeasurements(
                InstrumentType.LONG_GAUGE,
                ShardWriteLoadDistributionMetrics.WRITE_LOAD_PRIORITISATION_THRESHOLD_PERCENTILE_RANK_METRIC_NAME
            );
        final var shardWriteLoadSumMeasurements = testInfrastructure.meterRegistry.getRecorder()
            .getMeasurements(InstrumentType.DOUBLE_GAUGE, ShardWriteLoadDistributionMetrics.WRITE_LOAD_SUM_METRIC_NAME);

        logger.info(
            "Generated maximums p50={}/p90={}/p100={}",
            testInfrastructure.maxP50,
            testInfrastructure.maxP90,
            testInfrastructure.maxP100
        );
        assertEquals(8, writeLoadDistributionMeasurements.size());
        for (String nodeId : List.of("node_0", "node_1")) {
            assertThat(measurementForPercentile(writeLoadDistributionMeasurements, nodeId, 0.0), greaterThanOrEqualTo(0.0));
            assertRoughlyInRange(
                testInfrastructure.numberOfSignificantDigits,
                measurementForPercentile(writeLoadDistributionMeasurements, nodeId, 50.0),
                0.0,
                testInfrastructure.maxP50
            );
            assertRoughlyInRange(
                testInfrastructure.numberOfSignificantDigits,
                measurementForPercentile(writeLoadDistributionMeasurements, nodeId, 90.0),
                testInfrastructure.maxP50,
                testInfrastructure.maxP90
            );
            assertRoughlyInRange(
                testInfrastructure.numberOfSignificantDigits,
                measurementForPercentile(writeLoadDistributionMeasurements, nodeId, 100.0),
                testInfrastructure.maxP90,
                testInfrastructure.maxP100
            );

            assertThat(
                measurementForNode(writeLoadPrioritisationThresholdMeasurements, nodeId).getDouble(),
                Matchers.allOf(greaterThan(0.5 * testInfrastructure.maxP90), lessThanOrEqualTo(0.5 * testInfrastructure.maxP100))
            );
            assertThat(measurementForNode(countAboveThresholdMeasurements, nodeId).getLong(), greaterThan(0L));
            assertEquals(
                getTotalWriteLoadForNode(testInfrastructure.clusterInfo, testInfrastructure.clusterService.state(), nodeId),
                measurementForNode(shardWriteLoadSumMeasurements, nodeId).getDouble(),
                0.001
            );
        }
    }

    private double getTotalWriteLoadForNode(ClusterInfo clusterInfo, ClusterState clusterState, String nodeId) {
        return clusterState.getRoutingNodes()
            .node(nodeId)
            .shardsWithState(ShardRoutingState.STARTED)
            .mapToDouble(shardRouting -> clusterInfo.getShardWriteLoads().getOrDefault(shardRouting.shardId(), 0.0))
            .sum();
    }

    public void testShardWriteLoadDistributionMetricsDisabled() {
        final var testInfrastructure = createTestInfrastructure();
        testInfrastructure.clusterService()
            .getClusterSettings()
            .applySettings(
                Settings.builder().put(ShardWriteLoadDistributionMetrics.SHARD_WRITE_LOAD_METRICS_ENABLED_SETTING.getKey(), false).build()
            );

        testInfrastructure.shardWriteLoadDistributionMetrics.onNewInfo(testInfrastructure.clusterInfo);
        testInfrastructure.meterRegistry.getRecorder().collect();

        assertNoMetricsPublished(testInfrastructure);
    }

    public void testShardWriteLoadDistributionMetricsNoShardWriteLoads() {
        final var testInfrastructure = createTestInfrastructure();

        testInfrastructure.shardWriteLoadDistributionMetrics.onNewInfo(ClusterInfo.EMPTY);
        testInfrastructure.meterRegistry.getRecorder().collect();

        assertNoMetricsPublished(testInfrastructure);
    }

    public void testShardWriteLoadDistributionMetricsClusterNotStarted() {
        final var testInfrastructure = createTestInfrastructure();

        when(testInfrastructure.clusterService.lifecycleState()).thenReturn(
            randomFrom(Lifecycle.State.INITIALIZED, Lifecycle.State.STOPPED)
        );
        testInfrastructure.meterRegistry.getRecorder().collect();

        assertNoMetricsPublished(testInfrastructure);
    }

    public void testMetricsForNodeWithNoShards() {
        final var testInfrastructure = createTestInfrastructure();

        final var originalClusterState = testInfrastructure.clusterService.state();
        final var additionalNodeId = "node_2";
        final var nodesWithNodeAdded = DiscoveryNodes.builder(originalClusterState.nodes())
            .add(DiscoveryNodeUtils.create(additionalNodeId))
            .build();
        final var clusterStateWithNodeAdded = ClusterState.builder(originalClusterState).nodes(nodesWithNodeAdded).build();
        when(testInfrastructure.clusterService.state()).thenReturn(clusterStateWithNodeAdded);

        testInfrastructure.shardWriteLoadDistributionMetrics.onNewInfo(testInfrastructure.clusterInfo);
        testInfrastructure.meterRegistry.getRecorder().collect();

        final var writeLoadDistributionMeasurements = testInfrastructure.meterRegistry.getRecorder()
            .getMeasurements(InstrumentType.DOUBLE_GAUGE, ShardWriteLoadDistributionMetrics.WRITE_LOAD_DISTRIBUTION_METRIC_NAME);
        final var writeLoadPrioritisationThresholdMeasurements = testInfrastructure.meterRegistry.getRecorder()
            .getMeasurements(
                InstrumentType.DOUBLE_GAUGE,
                ShardWriteLoadDistributionMetrics.WRITE_LOAD_PRIORITISATION_THRESHOLD_METRIC_NAME
            );
        final var countAboveThresholdMeasurements = testInfrastructure.meterRegistry.getRecorder()
            .getMeasurements(
                InstrumentType.LONG_GAUGE,
                ShardWriteLoadDistributionMetrics.WRITE_LOAD_PRIORITISATION_THRESHOLD_PERCENTILE_RANK_METRIC_NAME
            );
        final var shardWriteLoadSumMeasurements = testInfrastructure.meterRegistry.getRecorder()
            .getMeasurements(InstrumentType.DOUBLE_GAUGE, ShardWriteLoadDistributionMetrics.WRITE_LOAD_SUM_METRIC_NAME);

        assertNoMetricsPublished(writeLoadDistributionMeasurements, additionalNodeId);
        assertNoMetricsPublished(writeLoadPrioritisationThresholdMeasurements, additionalNodeId);
        assertNoMetricsPublished(countAboveThresholdMeasurements, additionalNodeId);
        assertThat(measurementForNode(shardWriteLoadSumMeasurements, additionalNodeId).getDouble(), Matchers.is(0.0));
    }

    private static void assertNoMetricsPublished(List<Measurement> measurements, String nodeId) {
        assertThat(measurements.stream().filter(m -> m.attributes().get("node_id").equals(nodeId)).toList(), Matchers.empty());
    }

    private static void assertNoMetricsPublished(TestInfrastructure testInfrastructure) {
        assertThat(
            testInfrastructure.meterRegistry.getRecorder()
                .getMeasurements(InstrumentType.DOUBLE_GAUGE, ShardWriteLoadDistributionMetrics.WRITE_LOAD_DISTRIBUTION_METRIC_NAME),
            Matchers.empty()
        );
        assertThat(
            testInfrastructure.meterRegistry.getRecorder()
                .getMeasurements(
                    InstrumentType.DOUBLE_GAUGE,
                    ShardWriteLoadDistributionMetrics.WRITE_LOAD_PRIORITISATION_THRESHOLD_METRIC_NAME
                ),
            Matchers.empty()
        );
        assertThat(
            testInfrastructure.meterRegistry.getRecorder()
                .getMeasurements(
                    InstrumentType.LONG_GAUGE,
                    ShardWriteLoadDistributionMetrics.WRITE_LOAD_PRIORITISATION_THRESHOLD_PERCENTILE_RANK_METRIC_NAME
                ),
            Matchers.empty()
        );
    }

    public TestInfrastructure createTestInfrastructure() {
        final RecordingMeterRegistry meterRegistry = new RecordingMeterRegistry();
        final ClusterService clusterService = mock(ClusterService.class);
        when(clusterService.getClusterSettings()).thenReturn(
            ClusterSettings.createBuiltInClusterSettings(
                Settings.builder().put(ShardWriteLoadDistributionMetrics.SHARD_WRITE_LOAD_METRICS_ENABLED_SETTING.getKey(), true).build()
            )
        );
        when(clusterService.lifecycleState()).thenReturn(Lifecycle.State.STARTED);
        final var indexName = randomIdentifier();
        final var clusterState = balanceShardsByCount(ClusterStateCreationUtils.state(indexName, 2, 200));
        when(clusterService.state()).thenReturn(clusterState);
        final int numberOfSignificantDigits = randomIntBetween(2, 3);
        final ShardWriteLoadDistributionMetrics shardWriteLoadDistributionMetrics = new ShardWriteLoadDistributionMetrics(
            meterRegistry,
            clusterService,
            numberOfSignificantDigits,
            0,
            50,
            90,
            100
        );
        final double maxp50 = randomDoubleBetween(0, 10, true);
        final double maxp90 = randomDoubleBetween(maxp50, 30, true);
        final double maxp100 = randomDoubleBetween(maxp90, 50, true);

        final var clusterInfo = ClusterInfo.builder().shardWriteLoads(randomWriteLoads(clusterState, maxp50, maxp90, maxp100)).build();
        return new TestInfrastructure(
            clusterService,
            meterRegistry,
            clusterInfo,
            shardWriteLoadDistributionMetrics,
            numberOfSignificantDigits,
            maxp50,
            maxp90,
            maxp100
        );
    }

    public record TestInfrastructure(
        ClusterService clusterService,
        RecordingMeterRegistry meterRegistry,
        ClusterInfo clusterInfo,
        ShardWriteLoadDistributionMetrics shardWriteLoadDistributionMetrics,
        int numberOfSignificantDigits,
        double maxP50,
        double maxP90,
        double maxP100
    ) {}

    private static ClusterState balanceShardsByCount(ClusterState state) {
        final var routingAllocation = new RoutingAllocation(
            new AllocationDeciders(List.of()),
            state.getRoutingNodes().mutableCopy(),
            state,
            ClusterInfo.EMPTY,
            SnapshotShardSizeInfo.EMPTY,
            System.nanoTime()
        );
        final var shardsAllocator = new BalancedShardsAllocator(
            Settings.builder().put(ClusterModule.SHARDS_ALLOCATOR_TYPE_SETTING.getKey(), ClusterModule.BALANCED_ALLOCATOR).build()
        );
        shardsAllocator.allocate(routingAllocation);
        final var initializingShards = routingAllocation.routingNodes()
            .stream()
            .flatMap(rn -> rn.shardsWithState(ShardRoutingState.INITIALIZING))
            .toList();
        initializingShards.forEach(shardRouting -> routingAllocation.routingNodes().startShard(shardRouting, new RoutingChangesObserver() {
        }, randomNonNegativeLong()));
        return ClusterState.builder(state)
            .routingTable(state.globalRoutingTable().rebuild(routingAllocation.routingNodes(), routingAllocation.metadata()))
            .build();
    }

    /**
     * HDR histograms are accurate to a number of significant digits, so it's possible the values might be slightly off. This comparison
     * accounts for the configured significant digits to prevent test flakiness.
     */
    private static void assertRoughlyInRange(int numberOfSignificantDigits, double value, double min, double max) {
        final double valueLow = roundDown(value, numberOfSignificantDigits);
        final double valueHigh = roundUp(value, numberOfSignificantDigits);
        final double maxHigh = roundUp(max, numberOfSignificantDigits);
        final double minLow = roundDown(min, numberOfSignificantDigits);

        assertThat(valueHigh, greaterThanOrEqualTo(minLow));
        assertThat(valueLow, lessThanOrEqualTo(maxHigh));
    }

    private static double roundUp(double value, int significantDigits) {
        return BigDecimal.valueOf(value).multiply(BigDecimal.ONE, new MathContext(significantDigits, RoundingMode.CEILING)).doubleValue();
    }

    private static double roundDown(double value, int significantDigits) {
        return BigDecimal.valueOf(value).multiply(BigDecimal.ONE, new MathContext(significantDigits, RoundingMode.FLOOR)).doubleValue();
    }

    private static Measurement measurementForNode(List<Measurement> measurements, String nodeId) {
        return measurements.stream().filter(m -> m.attributes().get("node_id").equals(nodeId)).findFirst().orElseThrow();
    }

    private static double measurementForPercentile(List<Measurement> measurements, String nodeId, double percentile) {
        return measurements.stream()
            .filter(
                m -> m.attributes().get("percentile").equals(String.valueOf(percentile)) && m.attributes().get("node_id").equals(nodeId)
            )
            .findFirst()
            .orElseThrow()
            .getDouble();
    }

    private static Map<ShardId, Double> randomWriteLoads(ClusterState clusterState, double p50, double p90, double p100) {
        final var node1Shards = shardsOnNode(clusterState, "node_0");
        final var node2Shards = shardsOnNode(clusterState, "node_1");
        assertEquals(100, node1Shards.size());
        assertEquals(100, node2Shards.size());

        final Map<ShardId, Double> shardWriteLoads = new HashMap<>();
        for (List<ShardId> shardIds : List.of(node1Shards, node2Shards)) {
            final var shardIterator = shardIds.iterator();
            for (int i = 0; i < 50; i++) {
                shardWriteLoads.put(shardIterator.next(), randomDoubleBetween(0, p50, true));
            }
            for (int i = 0; i < 40; i++) {
                shardWriteLoads.put(shardIterator.next(), randomDoubleBetween(p50, p90, true));
            }
            for (int i = 0; i < 10; i++) {
                shardWriteLoads.put(shardIterator.next(), randomDoubleBetween(p90, p100, true));
            }
        }

        assertEquals(200, shardWriteLoads.size());
        return shardWriteLoads;
    }

    private static List<ShardId> shardsOnNode(ClusterState clusterState, String nodeId) {
        return clusterState.routingTable(ProjectId.DEFAULT)
            .allShards()
            .filter(shardRouting -> nodeId.equals(shardRouting.currentNodeId()))
            .map(ShardRouting::shardId)
            .toList();
    }
}
