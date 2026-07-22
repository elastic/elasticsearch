/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.allocation;

import org.apache.logging.log4j.Level;
import org.elasticsearch.cluster.ClusterInfo;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.InternalClusterInfoService;
import org.elasticsearch.cluster.NodeHeapEstimates;
import org.elasticsearch.cluster.NodeHeapMetrics;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.routing.RerouteService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.gateway.GatewayService;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.MockLog;
import org.elasticsearch.test.junit.annotations.TestLogging;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;

@TestLogging(value = "org.elasticsearch.xpack.stateless.allocation.EstimatedHeapUsageMonitor:DEBUG", reason = "debug log for test")
public class EstimatedHeapUsageMonitorTests extends ESTestCase {

    private long totalBytesPerNode;
    private RerouteService rerouteService;

    public void setUp() throws Exception {
        super.setUp();
        totalBytesPerNode = ByteSizeUnit.GB.toBytes(between(2, 16));
        rerouteService = mock(RerouteService.class);
    }

    public void testRerouteIsNotCalledWhenStateIsNotRecovered() {
        final EstimatedHeapUsageMonitor monitor = createMonitor(
            randomBoolean(),
            between(1, 100),
            () -> ClusterState.builder(ClusterState.EMPTY_STATE)
                .blocks(ClusterBlocks.builder().addGlobalBlock(GatewayService.STATE_NOT_RECOVERED_BLOCK).build())
                .build()
        );
        try (MockLog mockLog = MockLog.capture(EstimatedHeapUsageMonitor.class)) {
            mockLog.addExpectation(
                new MockLog.SeenEventExpectation(
                    "don't reroute due to global block",
                    EstimatedHeapUsageMonitor.class.getCanonicalName(),
                    Level.DEBUG,
                    "skipping monitor as the cluster state is not recovered yet"
                )
            );
            monitor.onNewInfo(createClusterInfo(between(1, 99), between(0, 3)));
            mockLog.assertAllExpectationsMatched();
            verifyNoInteractions(rerouteService);
        }
    }

    public void testRerouteIsNotCalledWhenThresholdIsDisabled() {
        final EstimatedHeapUsageMonitor monitor = createMonitor(false, between(1, 100), () -> ClusterState.EMPTY_STATE);
        try (MockLog mockLog = MockLog.capture(EstimatedHeapUsageMonitor.class)) {
            mockLog.addExpectation(
                new MockLog.SeenEventExpectation(
                    "don't reroute due to threshold disabled",
                    EstimatedHeapUsageMonitor.class.getCanonicalName(),
                    Level.DEBUG,
                    "skipping monitor as the estimated heap usage threshold is disabled"
                )
            );
            monitor.onNewInfo(createClusterInfo(between(1, 99), between(0, 3)));
            mockLog.assertAllExpectationsMatched();
            verifyNoInteractions(rerouteService);
        }
    }

    public void testRerouteIsNotCalledWhenHighWatermarkDisabled() {
        final int highWatermarkPercentage = between(2, 97);
        final int lowWatermarkPercentage = between(1, highWatermarkPercentage - 1);
        final EstimatedHeapUsageMonitor monitor = createMonitor(
            true,
            lowWatermarkPercentage,
            false,
            highWatermarkPercentage,
            () -> ClusterState.EMPTY_STATE
        );

        // All nodes are above the high watermark, but the feature is disabled — no reroute should be triggered.
        final Map<String, NodeHeapMetrics> nodeHeapMetrics = new HashMap<>();
        randNodeIds(between(1, 3)).forEach(
            nodeId -> nodeHeapMetrics.put(
                nodeId,
                new NodeHeapMetrics(
                    nodeId,
                    totalBytesPerNode,
                    validHeapEstimate(totalBytesPerNode * between(highWatermarkPercentage + 1, 100) / 100)
                )
            )
        );
        monitor.onNewInfo(ClusterInfo.builder().nodeHeapMetrics(nodeHeapMetrics).build());

        verifyNoInteractions(rerouteService);
    }

    public void testRerouteCalledWhenNodeExceedsHighWatermark() {
        final int highWatermarkPercentage = between(2, 97);
        final int lowWatermarkPercentage = between(1, highWatermarkPercentage - 1);
        final EstimatedHeapUsageMonitor monitor = createMonitor(
            true,
            lowWatermarkPercentage,
            true,
            highWatermarkPercentage,
            () -> ClusterState.EMPTY_STATE
        );

        try (MockLog mockLog = MockLog.capture(EstimatedHeapUsageMonitor.class)) {
            final var expectation = new MockLog.EventuallySeenEventExpectation(
                "reroute due to heap usages exceeded high watermark",
                EstimatedHeapUsageMonitor.class.getCanonicalName(),
                Level.DEBUG,
                Strings.format(
                    "estimated heap usages exceeded the high watermark [%.2f] for nodes * triggering reroute",
                    (double) highWatermarkPercentage
                )
            );
            mockLog.addExpectation(expectation);

            // Initially all nodes are below the high watermark — no reroute.
            final Map<String, NodeHeapMetrics> initialUsages = new HashMap<>();
            final var nodeIds = randNodeIds(between(2, 3));
            nodeIds.forEach(
                nodeId -> initialUsages.put(
                    nodeId,
                    new NodeHeapMetrics(
                        nodeId,
                        totalBytesPerNode,
                        validHeapEstimate(totalBytesPerNode * between(0, highWatermarkPercentage - 1) / 100)
                    )
                )
            );
            monitor.onNewInfo(ClusterInfo.builder().nodeHeapMetrics(initialUsages).build());
            mockLog.assertAllExpectationsMatched();

            // One node exceeds the high watermark — reroute should fire.
            final Map<String, NodeHeapMetrics> updatedUsages = new HashMap<>(initialUsages);
            updatedUsages.put(
                nodeIds.get(0),
                new NodeHeapMetrics(
                    nodeIds.get(0),
                    totalBytesPerNode,
                    validHeapEstimate(totalBytesPerNode * between(highWatermarkPercentage + 1, 100) / 100)
                )
            );
            expectation.setExpectSeen();
            monitor.onNewInfo(ClusterInfo.builder().nodeHeapMetrics(updatedUsages).build());
            mockLog.assertAllExpectationsMatched();

            // An additional node exceeds the high watermark — reroute should fire again.
            mockLog.addExpectation(
                new MockLog.SeenEventExpectation(
                    "reroute due to additional node exceeding high watermark",
                    EstimatedHeapUsageMonitor.class.getCanonicalName(),
                    Level.DEBUG,
                    Strings.format(
                        "estimated heap usages exceeded the high watermark [%.2f] for nodes * triggering reroute",
                        (double) highWatermarkPercentage
                    )
                )
            );
            final Map<String, NodeHeapMetrics> moreUsages = new HashMap<>(updatedUsages);
            moreUsages.put(
                nodeIds.get(1),
                new NodeHeapMetrics(
                    nodeIds.get(1),
                    totalBytesPerNode,
                    validHeapEstimate(totalBytesPerNode * between(highWatermarkPercentage + 1, 100) / 100)
                )
            );
            monitor.onNewInfo(ClusterInfo.builder().nodeHeapMetrics(moreUsages).build());
            mockLog.assertAllExpectationsMatched();

            // One node drops below the high watermark — no reroute (set shrank, not grew).
            mockLog.addExpectation(
                new MockLog.UnseenEventExpectation(
                    "no reroute when a node drops below the high watermark",
                    EstimatedHeapUsageMonitor.class.getCanonicalName(),
                    Level.DEBUG,
                    "estimated heap usages exceeded the high watermark * triggering reroute"
                )
            );
            final Map<String, NodeHeapMetrics> reducedUsages = new HashMap<>(moreUsages);
            reducedUsages.put(
                nodeIds.get(0),
                new NodeHeapMetrics(
                    nodeIds.get(0),
                    totalBytesPerNode,
                    validHeapEstimate(totalBytesPerNode * between(0, highWatermarkPercentage) / 100)
                )
            );
            monitor.onNewInfo(ClusterInfo.builder().nodeHeapMetrics(reducedUsages).build());
            mockLog.assertAllExpectationsMatched();
        }
    }

    public void testRerouteBasedOnEstimatedHeapUsage() {
        final int lowWatermarkPercentage = between(1, 99);
        final EstimatedHeapUsageMonitor monitor = createMonitor(true, lowWatermarkPercentage, () -> ClusterState.EMPTY_STATE);

        try (MockLog mockLog = MockLog.capture(EstimatedHeapUsageMonitor.class)) {
            final var expectation = new MockLog.EventuallySeenEventExpectation(
                "reroute due to heap usages dropped below low watermark",
                EstimatedHeapUsageMonitor.class.getCanonicalName(),
                Level.DEBUG,
                Strings.format(
                    "estimated heap usages dropped below the low watermark [%.2f] for nodes * triggering reroute",
                    (double) lowWatermarkPercentage
                )
            );
            mockLog.addExpectation(expectation);

            // Initially goes above the low watermark, no reroute
            final ClusterInfo clusterInfo = createClusterInfo(lowWatermarkPercentage, 2);
            monitor.onNewInfo(clusterInfo);
            mockLog.assertAllExpectationsMatched();

            // Heap usages from one or more nodes drop below the low watermark, triggering reroute
            final AtomicBoolean heapUsageReduced = new AtomicBoolean(false);
            final var updatedClusterInfo = ClusterInfo.builder()
                .nodeHeapMetrics(
                    clusterInfo.getNodeHeapMetrics().entrySet().stream().collect(Collectors.toUnmodifiableMap(Map.Entry::getKey, entry -> {
                        final NodeHeapMetrics nodeHeapMetrics = entry.getValue();
                        if (nodeHeapMetrics.estimatedUsageAsPercentage() > lowWatermarkPercentage
                            && (heapUsageReduced.compareAndSet(false, true) || randomBoolean())) {
                            return new NodeHeapMetrics(
                                nodeHeapMetrics.nodeId(),
                                nodeHeapMetrics.totalBytes(),
                                validHeapEstimate(nodeHeapMetrics.totalBytes() * between(0, lowWatermarkPercentage) / 100)
                            );
                        } else {
                            return nodeHeapMetrics;
                        }
                    }))
                )
                .build();
            expectation.setExpectSeen();
            monitor.onNewInfo(updatedClusterInfo);
            mockLog.assertAllExpectationsMatched();
        }
    }

    private EstimatedHeapUsageMonitor createMonitor(boolean enabled, int lowWatermarkPercent, Supplier<ClusterState> clusterStateSupplier) {
        // High watermark defaults to 100% (unreachable) so it never fires unless explicitly configured.
        return createMonitor(enabled, lowWatermarkPercent, true, 100, clusterStateSupplier);
    }

    private EstimatedHeapUsageMonitor createMonitor(
        boolean enabled,
        int lowWatermarkPercent,
        boolean highWatermarkEnabled,
        int highWatermarkPercent,
        Supplier<ClusterState> clusterStateSupplier
    ) {
        final var clusterSettings = new ClusterSettings(
            Settings.builder()
                .put(InternalClusterInfoService.CLUSTER_ROUTING_ALLOCATION_ESTIMATED_HEAP_THRESHOLD_DECIDER_ENABLED.getKey(), enabled)
                .put(
                    EstimatedHeapUsageAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ESTIMATED_HEAP_LOW_WATERMARK.getKey(),
                    lowWatermarkPercent + "%"
                )
                .put(
                    EstimatedHeapUsageAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ESTIMATED_HEAP_HIGH_WATERMARK_ENABLED.getKey(),
                    highWatermarkEnabled
                )
                .put(
                    EstimatedHeapUsageAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ESTIMATED_HEAP_HIGH_WATERMARK.getKey(),
                    highWatermarkPercent + "%"
                )
                .build(),
            Set.of(
                InternalClusterInfoService.CLUSTER_ROUTING_ALLOCATION_ESTIMATED_HEAP_THRESHOLD_DECIDER_ENABLED,
                EstimatedHeapUsageAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ESTIMATED_HEAP_LOW_WATERMARK,
                EstimatedHeapUsageAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ESTIMATED_HEAP_HIGH_WATERMARK_ENABLED,
                EstimatedHeapUsageAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ESTIMATED_HEAP_HIGH_WATERMARK
            )
        );
        return new EstimatedHeapUsageMonitor(clusterSettings, clusterStateSupplier, rerouteService);
    }

    private ClusterInfo createClusterInfo(int lowWatermarkPercentage, int numNodesAboveLowWatermark) {
        assert lowWatermarkPercentage >= 0 && lowWatermarkPercentage < 100 : lowWatermarkPercentage;
        final Map<String, NodeHeapMetrics> nodeHeapMetrics = new HashMap<>();
        final var nodeIdsAboveLowWatermark = randNodeIds(numNodesAboveLowWatermark);
        nodeIdsAboveLowWatermark.forEach(nodeId -> {
            nodeHeapMetrics.put(
                nodeId,
                new NodeHeapMetrics(
                    nodeId,
                    totalBytesPerNode,
                    validHeapEstimate(totalBytesPerNode * between(lowWatermarkPercentage + 1, 100) / 100)
                )
            );
        });

        if (nodeIdsAboveLowWatermark.isEmpty() || randomBoolean()) {
            randOtherNodeIds(between(1, 3)).forEach(nodeId -> {
                nodeHeapMetrics.put(
                    nodeId,
                    new NodeHeapMetrics(
                        nodeId,
                        totalBytesPerNode,
                        validHeapEstimate(totalBytesPerNode * between(0, lowWatermarkPercentage) / 100)
                    )
                );
            });
        }

        return ClusterInfo.builder().nodeHeapMetrics(nodeHeapMetrics).build();
    }

    private List<String> randNodeIds(int n) {
        return IntStream.range(0, n).mapToObj(id -> "node-" + id).toList();
    }

    private List<String> randOtherNodeIds(int n) {
        return IntStream.range(0, n).mapToObj(id -> "other-node-" + id).toList();
    }

    private NodeHeapEstimates validHeapEstimate(long totalHeapUsageBytes) {
        return new NodeHeapEstimates(totalHeapUsageBytes, randomLongBetween(0, totalHeapUsageBytes));
    }
}
