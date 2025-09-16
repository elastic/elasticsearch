/*
 * ELASTICSEARCH CONFIDENTIAL
 * __________________
 *
 * Copyright Elasticsearch B.V. All rights reserved.
 *
 * NOTICE:  All information contained herein is, and remains
 * the property of Elasticsearch B.V. and its suppliers, if any.
 * The intellectual and technical concepts contained herein
 * are proprietary to Elasticsearch B.V. and its suppliers and
 * may be covered by U.S. and Foreign Patents, patents in
 * process, and are protected by trade secret or copyright
 * law.  Dissemination of this information or reproduction of
 * this material is strictly forbidden unless prior written
 * permission is obtained from Elasticsearch B.V.
 */

package co.elastic.elasticsearch.stateless.allocation;

import org.apache.logging.log4j.Level;
import org.elasticsearch.cluster.ClusterInfo;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.EstimatedHeapUsage;
import org.elasticsearch.cluster.InternalClusterInfoService;
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

@TestLogging(value = "co.elastic.elasticsearch.stateless.allocation.EstimatedHeapUsageMonitor:DEBUG", reason = "debug log for test")
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
                    (lowWatermarkPercentage * 100 / 100.0)
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
                .estimatedHeapUsages(
                    clusterInfo.getEstimatedHeapUsages()
                        .entrySet()
                        .stream()
                        .collect(Collectors.toUnmodifiableMap(Map.Entry::getKey, entry -> {
                            final EstimatedHeapUsage estimatedHeapUsage = entry.getValue();
                            if (estimatedHeapUsage.estimatedUsageAsPercentage() > lowWatermarkPercentage
                                && (heapUsageReduced.compareAndSet(false, true) || randomBoolean())) {
                                return new EstimatedHeapUsage(
                                    estimatedHeapUsage.nodeId(),
                                    estimatedHeapUsage.totalBytes(),
                                    estimatedHeapUsage.totalBytes() * between(0, lowWatermarkPercentage) / 100
                                );
                            } else {
                                return estimatedHeapUsage;
                            }
                        }))
                )
                .build();
            expectation.setExpectSeen();
            monitor.onNewInfo(updatedClusterInfo);
            mockLog.assertAllExpectationsMatched();
        }
    }

    private EstimatedHeapUsageMonitor createMonitor(boolean enabled, int percent, Supplier<ClusterState> clusterStateSupplier) {
        final var clusterSettings = new ClusterSettings(
            Settings.builder()
                .put(InternalClusterInfoService.CLUSTER_ROUTING_ALLOCATION_ESTIMATED_HEAP_THRESHOLD_DECIDER_ENABLED.getKey(), enabled)
                .put(EstimatedHeapUsageAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ESTIMATED_HEAP_LOW_WATERMARK.getKey(), percent + "%")
                .build(),
            Set.of(
                InternalClusterInfoService.CLUSTER_ROUTING_ALLOCATION_ESTIMATED_HEAP_THRESHOLD_DECIDER_ENABLED,
                EstimatedHeapUsageAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ESTIMATED_HEAP_LOW_WATERMARK
            )
        );
        return new EstimatedHeapUsageMonitor(clusterSettings, clusterStateSupplier, rerouteService);
    }

    private ClusterInfo createClusterInfo(int lowWatermarkPercentage, int numNodesAboveLowWatermark) {
        assert lowWatermarkPercentage >= 0 && lowWatermarkPercentage < 100 : lowWatermarkPercentage;
        final Map<String, EstimatedHeapUsage> estimatedHeapUsages = new HashMap<>();
        final var nodeIdsAboveLowWatermark = randNodeIds(numNodesAboveLowWatermark);
        nodeIdsAboveLowWatermark.forEach(nodeId -> {
            estimatedHeapUsages.put(
                nodeId,
                new EstimatedHeapUsage(nodeId, totalBytesPerNode, totalBytesPerNode * between(lowWatermarkPercentage + 1, 100) / 100)
            );
        });

        if (nodeIdsAboveLowWatermark.isEmpty() || randomBoolean()) {
            randOtherNodeIds(between(1, 3)).forEach(nodeId -> {
                estimatedHeapUsages.put(
                    nodeId,
                    new EstimatedHeapUsage(nodeId, totalBytesPerNode, totalBytesPerNode * between(0, lowWatermarkPercentage) / 100)
                );
            });
        }

        return ClusterInfo.builder().estimatedHeapUsages(estimatedHeapUsages).build();
    }

    private List<String> randNodeIds(int n) {
        return IntStream.range(0, n).mapToObj(id -> "node-" + id).toList();
    }

    private List<String> randOtherNodeIds(int n) {
        return IntStream.range(0, n).mapToObj(id -> "other-node-" + id).toList();
    }
}
