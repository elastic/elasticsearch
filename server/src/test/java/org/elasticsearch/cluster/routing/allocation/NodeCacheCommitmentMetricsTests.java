/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.routing.allocation;

import org.elasticsearch.cluster.ClusterInfo;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.NodeCacheSizeAndCommitments;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.component.Lifecycle;
import org.elasticsearch.telemetry.InstrumentType;
import org.elasticsearch.telemetry.Measurement;
import org.elasticsearch.telemetry.RecordingMeterRegistry;
import org.elasticsearch.test.ESTestCase;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class NodeCacheCommitmentMetricsTests extends ESTestCase {

    public void testCommitmentFractionsAreComputedCorrectly() {
        final var env = createTestEnvironment();

        final long cacheSize = randomLongBetween(1_000, 1_000_000);
        final long boosted = randomLongBetween(0, cacheSize * 2);
        final long unboosted = randomLongBetween(0, cacheSize);

        final var clusterInfo = ClusterInfo.builder()
            .nodeCacheSizeAndCommitments(
                Map.of(
                    env.node1.getId(),
                    new NodeCacheSizeAndCommitments(cacheSize, boosted, unboosted),
                    env.node2.getId(),
                    new NodeCacheSizeAndCommitments(cacheSize, boosted, unboosted)
                )
            )
            .build();

        env.metrics.onNewInfo(clusterInfo);
        env.registry.getRecorder().collect();

        final var boostedMeasurements = boostedMeasurements(env);
        final var totalMeasurements = totalMeasurements(env);

        assertThat(boostedMeasurements.size(), equalTo(2));
        assertThat(totalMeasurements.size(), equalTo(2));

        final double expectedBoosted = (double) boosted / cacheSize;
        final double expectedTotal = (double) (boosted + unboosted) / cacheSize;

        for (String nodeId : List.of(env.node1.getId(), env.node2.getId())) {
            assertThat(measurementForNode(boostedMeasurements, nodeId).getDouble(), closeTo(expectedBoosted, 1e-9));
            assertThat(measurementForNode(totalMeasurements, nodeId).getDouble(), closeTo(expectedTotal, 1e-9));
            assertThat(
                measurementForNode(totalMeasurements, nodeId).getDouble(),
                greaterThanOrEqualTo(measurementForNode(boostedMeasurements, nodeId).getDouble())
            );
        }
    }

    public void testNodeAttributesArePopulated() {
        final var env = createTestEnvironment();

        final var clusterInfo = ClusterInfo.builder()
            .nodeCacheSizeAndCommitments(Map.of(env.node1.getId(), new NodeCacheSizeAndCommitments(1000L, 100L, 200L)))
            .build();

        env.metrics.onNewInfo(clusterInfo);
        env.registry.getRecorder().collect();

        final var boostedMeasurement = measurementForNode(boostedMeasurements(env), env.node1.getId());
        assertThat(boostedMeasurement.attributes().get("es_node_id"), equalTo(env.node1.getId()));
        assertThat(boostedMeasurement.attributes().get("es_node_name"), equalTo(env.node1.getName()));

        final var totalMeasurement = measurementForNode(totalMeasurements(env), env.node1.getId());
        assertThat(totalMeasurement.attributes().get("es_node_id"), equalTo(env.node1.getId()));
        assertThat(totalMeasurement.attributes().get("es_node_name"), equalTo(env.node1.getName()));
    }

    public void testEmptyNodeCacheSizeAndCommitmentsProducesNoMeasurements() {
        final var env = createTestEnvironment();

        env.metrics.onNewInfo(ClusterInfo.EMPTY);
        env.registry.getRecorder().collect();

        assertThat(boostedMeasurements(env), empty());
        assertThat(totalMeasurements(env), empty());
    }

    public void testNodeWithZeroCacheSizeIsSkipped() {
        final var env = createTestEnvironment();

        final var clusterInfo = ClusterInfo.builder()
            .nodeCacheSizeAndCommitments(
                Map.of(
                    env.node1.getId(),
                    new NodeCacheSizeAndCommitments(0L, 0L, 0L),
                    env.node2.getId(),
                    new NodeCacheSizeAndCommitments(1000L, 100L, 200L)
                )
            )
            .build();

        env.metrics.onNewInfo(clusterInfo);
        env.registry.getRecorder().collect();

        final var boostedMeasurements = boostedMeasurements(env);
        assertThat(boostedMeasurements.size(), equalTo(1));
        assertThat(boostedMeasurements.getFirst().attributes().get("es_node_id"), equalTo(env.node2.getId()));
    }

    public void testNodeAbsentFromClusterStateIsSkipped() {
        final var env = createTestEnvironment();

        final var clusterInfo = ClusterInfo.builder()
            .nodeCacheSizeAndCommitments(
                Map.of(
                    "unknown-node-id",
                    new NodeCacheSizeAndCommitments(1000L, 100L, 200L),
                    env.node1.getId(),
                    new NodeCacheSizeAndCommitments(1000L, 100L, 200L)
                )
            )
            .build();

        env.metrics.onNewInfo(clusterInfo);
        env.registry.getRecorder().collect();

        final var boostedMeasurements = boostedMeasurements(env);
        assertThat(boostedMeasurements.size(), equalTo(1));
        assertThat(boostedMeasurements.getFirst().attributes().get("es_node_id"), equalTo(env.node1.getId()));
    }

    public void testNoMeasurementsWhenClusterNotStarted() {
        final var env = createTestEnvironment();
        when(env.clusterService.lifecycleState()).thenReturn(randomFrom(Lifecycle.State.INITIALIZED, Lifecycle.State.STOPPED));

        final var clusterInfo = ClusterInfo.builder()
            .nodeCacheSizeAndCommitments(Map.of(env.node1.getId(), new NodeCacheSizeAndCommitments(1000L, 100L, 200L)))
            .build();

        env.metrics.onNewInfo(clusterInfo);
        env.registry.getRecorder().collect();

        assertThat(boostedMeasurements(env), empty());
        assertThat(totalMeasurements(env), empty());
    }

    public void testLatestClusterInfoWinsAndMetricsAreConsumedOnPoll() {
        final var env = createTestEnvironment();

        final var firstInfo = ClusterInfo.builder()
            .nodeCacheSizeAndCommitments(Map.of(env.node1.getId(), new NodeCacheSizeAndCommitments(1000L, 300L, 100L)))
            .build();
        final var secondInfo = ClusterInfo.builder()
            .nodeCacheSizeAndCommitments(Map.of(env.node1.getId(), new NodeCacheSizeAndCommitments(1000L, 50L, 10L)))
            .build();

        env.metrics.onNewInfo(firstInfo);
        // Second call before collection — latest wins
        env.metrics.onNewInfo(secondInfo);
        env.registry.getRecorder().collect();

        // Second call's data (boosted = 50/1000 = 0.05) must be reported
        assertThat(measurementForNode(boostedMeasurements(env), env.node1.getId()).getDouble(), closeTo(0.05, 1e-9));

        // Metrics are consumed on poll — a second collect without a new onNewInfo call produces nothing
        env.registry.getRecorder().resetCalls();
        env.registry.getRecorder().collect();
        assertThat(boostedMeasurements(env), empty());
        assertThat(totalMeasurements(env), empty());
    }

    public void testBoostedAndTotalMetricsAreDerivedFromSameClusterInfoWhenNewInfoArrivesInTheInterim() {
        final var env = createTestEnvironment();

        final var v1Commitments = randomNodeCacheSizeAndCommitments();
        final var v1 = ClusterInfo.builder().nodeCacheSizeAndCommitments(Map.of(env.node1.getId(), v1Commitments)).build();
        final var v2Commitments = randomValueOtherThan(v1Commitments, this::randomNodeCacheSizeAndCommitments);
        final var v2 = ClusterInfo.builder().nodeCacheSizeAndCommitments(Map.of(env.node1.getId(), v2Commitments)).build();

        env.metrics.onNewInfo(v1);

        // Simulate the first APM gauge poll: computation is triggered from v1 and boosted metrics are returned
        final var boostedFromFirstPoll = env.metrics.getBoostedCommitmentMetrics();

        // A new ClusterInfo arrives before the second gauge is polled
        env.metrics.onNewInfo(v2);

        // Simulate the second APM gauge poll: total metrics must come from v1, not v2
        final var totalFromSecondPoll = env.metrics.getTotalCommitmentMetrics();

        final var node1Boosted = boostedFromFirstPoll.stream()
            .filter(m -> env.node1.getId().equals(m.attributes().get("es_node_id")))
            .findFirst()
            .orElseThrow();
        assertThat(
            node1Boosted.value(),
            closeTo(v1Commitments.boostedCacheCommitmentInBytes() / (double) v1Commitments.cacheSizeInBytes(), 1e-9)
        );

        // total must be from v1, not v2
        final var node1Total = totalFromSecondPoll.stream()
            .filter(m -> env.node1.getId().equals(m.attributes().get("es_node_id")))
            .findFirst()
            .orElseThrow();
        assertThat(
            node1Total.value(),
            closeTo(v1Commitments.totalCacheCommitmentInBytes() / (double) v1Commitments.cacheSizeInBytes(), 1e-9)
        );
    }

    private NodeCacheSizeAndCommitments randomNodeCacheSizeAndCommitments() {
        long cacheSizeInBytes = randomLongBetween(1_000_000, 5_000_000);
        long boostedCacheCommitmentInBytes = randomLongBetween(1_000, 10_000_000);
        long unboostedCacheCommitmentInBytes = randomLongBetween(1_000, 10_000_000);
        return new NodeCacheSizeAndCommitments(cacheSizeInBytes, boostedCacheCommitmentInBytes, unboostedCacheCommitmentInBytes);
    }

    // --- helpers ---

    private record TestEnvironment(
        ClusterService clusterService,
        RecordingMeterRegistry registry,
        NodeCacheCommitmentMetrics metrics,
        DiscoveryNode node1,
        DiscoveryNode node2
    ) {}

    private TestEnvironment createTestEnvironment() {
        final var node1 = DiscoveryNodeUtils.builder("search_0").roles(Set.of(DiscoveryNodeRole.SEARCH_ROLE)).build();
        final var node2 = DiscoveryNodeUtils.builder("search_1").roles(Set.of(DiscoveryNodeRole.SEARCH_ROLE)).build();
        final var clusterState = ClusterState.builder(ClusterName.DEFAULT)
            .nodes(DiscoveryNodes.builder().add(node1).add(node2).build())
            .build();

        final var clusterService = mock(ClusterService.class);
        when(clusterService.lifecycleState()).thenReturn(Lifecycle.State.STARTED);
        when(clusterService.state()).thenReturn(clusterState);

        final var registry = new RecordingMeterRegistry();
        final var metrics = new NodeCacheCommitmentMetrics(registry, clusterService);
        return new TestEnvironment(clusterService, registry, metrics, node1, node2);
    }

    private static List<Measurement> boostedMeasurements(TestEnvironment env) {
        return env.registry.getRecorder()
            .getMeasurements(InstrumentType.DOUBLE_GAUGE, NodeCacheCommitmentMetrics.BOOSTED_CACHE_COMMITMENT_METRIC_NAME);
    }

    private static List<Measurement> totalMeasurements(TestEnvironment env) {
        return env.registry.getRecorder()
            .getMeasurements(InstrumentType.DOUBLE_GAUGE, NodeCacheCommitmentMetrics.TOTAL_CACHE_COMMITMENT_METRIC_NAME);
    }

    private static Measurement measurementForNode(List<Measurement> measurements, String nodeId) {
        return measurements.stream().filter(m -> nodeId.equals(m.attributes().get("es_node_id"))).findFirst().orElseThrow();
    }
}
