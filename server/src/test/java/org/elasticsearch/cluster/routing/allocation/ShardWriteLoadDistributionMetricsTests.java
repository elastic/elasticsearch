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
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.RoutingChangesObserver;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.allocation.allocator.BalancedShardsAllocator;
import org.elasticsearch.cluster.routing.allocation.decider.AllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.AllocationDeciders;
import org.elasticsearch.cluster.routing.allocation.decider.Decision;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.component.Lifecycle;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.Maps;
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
import java.util.Set;

import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.not;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ShardWriteLoadDistributionMetricsTests extends ESTestCase {

    public void testShardWriteLoadDistributionMetrics() {
        final var testInfrastructure = createTestInfrastructure();

        testInfrastructure.shardWriteLoadDistributionMetrics.onNewInfo(testInfrastructure.clusterInfo);
        testInfrastructure.meterRegistry.getRecorder().collect();

        final var p0writeLoadMeasurements = testInfrastructure.meterRegistry.getRecorder()
            .getMeasurements(InstrumentType.DOUBLE_GAUGE, ShardWriteLoadDistributionMetrics.shardWriteLoadDistributionMetricName(0));
        final var p50writeLoadMeasurements = testInfrastructure.meterRegistry.getRecorder()
            .getMeasurements(InstrumentType.DOUBLE_GAUGE, ShardWriteLoadDistributionMetrics.shardWriteLoadDistributionMetricName(50));
        final var p90writeLoadMeasurements = testInfrastructure.meterRegistry.getRecorder()
            .getMeasurements(InstrumentType.DOUBLE_GAUGE, ShardWriteLoadDistributionMetrics.shardWriteLoadDistributionMetricName(90));
        final var p100writeLoadMeasurements = testInfrastructure.meterRegistry.getRecorder()
            .getMeasurements(InstrumentType.DOUBLE_GAUGE, ShardWriteLoadDistributionMetrics.shardWriteLoadDistributionMetricName(100));
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
        assertEquals(2, p0writeLoadMeasurements.size());
        assertEquals(2, p50writeLoadMeasurements.size());
        assertEquals(2, p90writeLoadMeasurements.size());
        assertEquals(2, p100writeLoadMeasurements.size());
        for (String nodeId : List.of("index_0", "index_1")) {
            assertThat(measurementForNode(p0writeLoadMeasurements, nodeId).getDouble(), greaterThanOrEqualTo(0.0));
            assertRoughlyInRange(
                testInfrastructure.numberOfSignificantDigits,
                measurementForNode(p50writeLoadMeasurements, nodeId).getDouble(),
                0.0,
                testInfrastructure.maxP50
            );
            assertRoughlyInRange(
                testInfrastructure.numberOfSignificantDigits,
                measurementForNode(p90writeLoadMeasurements, nodeId).getDouble(),
                testInfrastructure.maxP50,
                testInfrastructure.maxP90
            );
            assertRoughlyInRange(
                testInfrastructure.numberOfSignificantDigits,
                measurementForNode(p100writeLoadMeasurements, nodeId).getDouble(),
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
        final var additionalNodeId = "index_2";
        final var nodesWithNodeAdded = DiscoveryNodes.builder(originalClusterState.nodes())
            .add(DiscoveryNodeUtils.builder(additionalNodeId).roles(Set.of(DiscoveryNodeRole.INDEX_ROLE)).build())
            .build();
        final var clusterStateWithNodeAdded = ClusterState.builder(originalClusterState).nodes(nodesWithNodeAdded).build();
        when(testInfrastructure.clusterService.state()).thenReturn(clusterStateWithNodeAdded);

        testInfrastructure.shardWriteLoadDistributionMetrics.onNewInfo(testInfrastructure.clusterInfo);
        testInfrastructure.meterRegistry.getRecorder().collect();

        final var p0writeLoadMeasurements = testInfrastructure.meterRegistry.getRecorder()
            .getMeasurements(InstrumentType.DOUBLE_GAUGE, ShardWriteLoadDistributionMetrics.shardWriteLoadDistributionMetricName(0));
        final var p50writeLoadMeasurements = testInfrastructure.meterRegistry.getRecorder()
            .getMeasurements(InstrumentType.DOUBLE_GAUGE, ShardWriteLoadDistributionMetrics.shardWriteLoadDistributionMetricName(50));
        final var p90writeLoadMeasurements = testInfrastructure.meterRegistry.getRecorder()
            .getMeasurements(InstrumentType.DOUBLE_GAUGE, ShardWriteLoadDistributionMetrics.shardWriteLoadDistributionMetricName(90));
        final var p100writeLoadMeasurements = testInfrastructure.meterRegistry.getRecorder()
            .getMeasurements(InstrumentType.DOUBLE_GAUGE, ShardWriteLoadDistributionMetrics.shardWriteLoadDistributionMetricName(100));
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

        assertNoMetricsPublished(p0writeLoadMeasurements, additionalNodeId);
        assertNoMetricsPublished(p50writeLoadMeasurements, additionalNodeId);
        assertNoMetricsPublished(p90writeLoadMeasurements, additionalNodeId);
        assertNoMetricsPublished(p100writeLoadMeasurements, additionalNodeId);
        assertNoMetricsPublished(writeLoadPrioritisationThresholdMeasurements, additionalNodeId);
        assertNoMetricsPublished(countAboveThresholdMeasurements, additionalNodeId);
        assertThat(measurementForNode(shardWriteLoadSumMeasurements, additionalNodeId).getDouble(), Matchers.is(0.0));
    }

    public void testMetricsAreNotReportedForNonIndexNodes() {
        final var testInfrastructure = createTestInfrastructure();
        testInfrastructure.shardWriteLoadDistributionMetrics.onNewInfo(testInfrastructure.clusterInfo);
        testInfrastructure.meterRegistry.getRecorder().collect();

        final var p0writeLoadMeasurements = testInfrastructure.meterRegistry.getRecorder()
            .getMeasurements(InstrumentType.DOUBLE_GAUGE, ShardWriteLoadDistributionMetrics.shardWriteLoadDistributionMetricName(0));
        final var p50writeLoadMeasurements = testInfrastructure.meterRegistry.getRecorder()
            .getMeasurements(InstrumentType.DOUBLE_GAUGE, ShardWriteLoadDistributionMetrics.shardWriteLoadDistributionMetricName(50));
        final var p90writeLoadMeasurements = testInfrastructure.meterRegistry.getRecorder()
            .getMeasurements(InstrumentType.DOUBLE_GAUGE, ShardWriteLoadDistributionMetrics.shardWriteLoadDistributionMetricName(90));
        final var p100writeLoadMeasurements = testInfrastructure.meterRegistry.getRecorder()
            .getMeasurements(InstrumentType.DOUBLE_GAUGE, ShardWriteLoadDistributionMetrics.shardWriteLoadDistributionMetricName(100));
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

        final var nonIndexNodes = testInfrastructure.clusterService.state()
            .nodes()
            .stream()
            .filter(node -> node.getRoles().contains(DiscoveryNodeRole.INDEX_ROLE) == false)
            .toList();
        assertThat(nonIndexNodes, not(empty()));
        for (DiscoveryNode node : nonIndexNodes) {
            assertNoMetricsPublished(p0writeLoadMeasurements, node.getId());
            assertNoMetricsPublished(p50writeLoadMeasurements, node.getId());
            assertNoMetricsPublished(p90writeLoadMeasurements, node.getId());
            assertNoMetricsPublished(p100writeLoadMeasurements, node.getId());
            assertNoMetricsPublished(writeLoadPrioritisationThresholdMeasurements, node.getId());
            assertNoMetricsPublished(countAboveThresholdMeasurements, node.getId());
            assertNoMetricsPublished(shardWriteLoadSumMeasurements, node.getId());
        }
    }

    public void testMetricsAreNotRecalculatedWhenThereAreUncollectedMetrics() {
        final var testInfrastructure = createTestInfrastructure();

        // Calculate metrics
        testInfrastructure.shardWriteLoadDistributionMetrics.onNewInfo(testInfrastructure.clusterInfo);

        // Calculate again with all very low write-loads - should be a no-op and not overwrite previous values
        final var clusterInfoWithLowWriteLoads = ClusterInfo.builder()
            .shardWriteLoads(Maps.transformValues(testInfrastructure.clusterInfo.getShardWriteLoads(), v -> 0.1))
            .build();
        testInfrastructure.shardWriteLoadDistributionMetrics.onNewInfo(clusterInfoWithLowWriteLoads);

        // Collect should publish the first set of (not low) write-loads
        testInfrastructure.meterRegistry.getRecorder().collect();

        final var p100writeLoadMeasurements = testInfrastructure.meterRegistry.getRecorder()
            .getMeasurements(InstrumentType.DOUBLE_GAUGE, ShardWriteLoadDistributionMetrics.shardWriteLoadDistributionMetricName(100));

        assertRoughlyInRange(
            testInfrastructure.numberOfSignificantDigits,
            measurementForNode(p100writeLoadMeasurements, "index_0").getDouble(),
            testInfrastructure.maxP90,
            testInfrastructure.maxP100
        );

        // This time the metrics should be updated
        testInfrastructure.shardWriteLoadDistributionMetrics.onNewInfo(clusterInfoWithLowWriteLoads);

        testInfrastructure.meterRegistry.getRecorder().resetCalls();
        testInfrastructure.meterRegistry.getRecorder().collect();

        final var lowerP100writeLoadMeasurements = testInfrastructure.meterRegistry.getRecorder()
            .getMeasurements(InstrumentType.DOUBLE_GAUGE, ShardWriteLoadDistributionMetrics.shardWriteLoadDistributionMetricName(100));

        assertEquals(measurementForNode(lowerP100writeLoadMeasurements, "index_0").getDouble(), 0.1, 0.01);
    }

    private static void assertNoMetricsPublished(List<Measurement> measurements, String nodeId) {
        assertThat(measurements.stream().filter(m -> m.attributes().get("es_node_id").equals(nodeId)).toList(), empty());
    }

    private static void assertNoMetricsPublished(TestInfrastructure testInfrastructure) {
        for (int percentile : new int[] { 0, 50, 90, 100 }) {
            assertThat(
                testInfrastructure.meterRegistry.getRecorder()
                    .getMeasurements(
                        InstrumentType.DOUBLE_GAUGE,
                        ShardWriteLoadDistributionMetrics.shardWriteLoadDistributionMetricName(percentile)
                    ),
                empty()
            );
        }
        assertThat(
            testInfrastructure.meterRegistry.getRecorder()
                .getMeasurements(
                    InstrumentType.DOUBLE_GAUGE,
                    ShardWriteLoadDistributionMetrics.WRITE_LOAD_PRIORITISATION_THRESHOLD_METRIC_NAME
                ),
            empty()
        );
        assertThat(
            testInfrastructure.meterRegistry.getRecorder()
                .getMeasurements(
                    InstrumentType.LONG_GAUGE,
                    ShardWriteLoadDistributionMetrics.WRITE_LOAD_PRIORITISATION_THRESHOLD_PERCENTILE_RANK_METRIC_NAME
                ),
            empty()
        );
    }

    public TestInfrastructure createTestInfrastructure() {
        final RecordingMeterRegistry meterRegistry = new RecordingMeterRegistry();
        final ClusterService clusterService = mock(ClusterService.class);
        final var settings = Settings.builder()
            .put(ShardWriteLoadDistributionMetrics.SHARD_WRITE_LOAD_METRICS_ENABLED_SETTING.getKey(), true)
            .put(DiscoveryNode.STATELESS_ENABLED_SETTING_NAME, true)
            .build();
        when(clusterService.getClusterSettings()).thenReturn(ClusterSettings.createBuiltInClusterSettings(settings));
        when(clusterService.getSettings()).thenReturn(settings);
        when(clusterService.lifecycleState()).thenReturn(Lifecycle.State.STARTED);
        final var indexName = randomIdentifier();
        final var clusterState = balanceShardsByCount(
            ClusterStateCreationUtils.buildServerlessRoleNodes(indexName, 200, 2, randomIntBetween(1, 2), randomIntBetween(1, 2))
        );
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
        final var routingAllocation = new RoutingAllocation(new AllocationDeciders(List.<AllocationDecider>of(new AllocationDecider() {
            @Override
            public Decision canAllocate(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
                // We are simulating stateless here, but we don't have the StatelessAllocationDecider in scope
                return node.node().getRoles().contains(DiscoveryNodeRole.INDEX_ROLE) ? Decision.YES : Decision.NO;
            }
        })), state.getRoutingNodes().mutableCopy(), state, ClusterInfo.EMPTY, SnapshotShardSizeInfo.EMPTY, System.nanoTime());
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
        return measurements.stream().filter(m -> m.attributes().get("es_node_id").equals(nodeId)).findFirst().orElseThrow();
    }

    private static Map<ShardId, Double> randomWriteLoads(ClusterState clusterState, double p50, double p90, double p100) {
        final var node1Shards = shardsOnNode(clusterState, "index_0");
        final var node2Shards = shardsOnNode(clusterState, "index_1");
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
