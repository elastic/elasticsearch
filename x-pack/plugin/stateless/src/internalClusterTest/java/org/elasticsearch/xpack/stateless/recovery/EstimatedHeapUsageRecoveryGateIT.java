/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.recovery;

import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.cluster.ClusterInfo;
import org.elasticsearch.cluster.InternalClusterInfoService;
import org.elasticsearch.cluster.NodeHeapMetrics;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.recovery.RecoveryStats;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.indices.recovery.CompositeRecoverySchedulingListener;
import org.elasticsearch.indices.recovery.RecoveryGate;
import org.elasticsearch.indices.recovery.RecoveryGateMonitor;
import org.elasticsearch.indices.recovery.RecoveryMetricsCollector;
import org.elasticsearch.indices.recovery.TestRecoverySchedulingListener;
import org.elasticsearch.indices.recovery.ThrottlingRecoveryService;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.telemetry.Measurement;
import org.elasticsearch.telemetry.TestTelemetryPlugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.InternalSettingsPlugin;
import org.elasticsearch.xpack.stateless.AbstractStatelessPluginIntegTestCase;
import org.elasticsearch.xpack.stateless.allocation.EstimatedHeapUsageAllocationDecider;
import org.elasticsearch.xpack.stateless.memory.ShardsMappingSizeCollector;
import org.elasticsearch.xpack.stateless.memory.StatelessMemoryMetricsService;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.function.Predicate;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.not;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0)
public class EstimatedHeapUsageRecoveryGateIT extends AbstractStatelessPluginIntegTestCase {

    /// Recovery gates are disabled by default; [InternalSettingsPlugin] registers the enable flag so tests can turn it on.
    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        final List<Class<? extends Plugin>> plugins = new ArrayList<>(super.nodePlugins());
        plugins.add(InternalSettingsPlugin.class);
        plugins.add(TestTelemetryPlugin.class);
        return plugins;
    }

    @Override
    protected Settings.Builder nodeSettings() {
        return super.nodeSettings().put(RecoveryGateMonitor.ENABLE_RECOVERY_GATES_SETTING.getKey(), true)
            .put(InternalClusterInfoService.CLUSTER_ROUTING_ALLOCATION_ESTIMATED_HEAP_THRESHOLD_DECIDER_ENABLED.getKey(), true)
            // Ensure the gate is enabled even for the small (512 MB) test JVM.
            .put(EstimatedHeapUsageAllocationDecider.MINIMUM_HEAP_SIZE_FOR_ENABLEMENT.getKey(), "100mb")
            // Publish memory metrics quickly so the master's view catches up fast.
            .put(ShardsMappingSizeCollector.PUBLISHING_FREQUENCY_SETTING.getKey(), TimeValue.timeValueMillis(10));
    }

    public void testDefersRecoveriesUntilEstimateDropsBelowWatermark() throws Exception {
        final String indexNodeName = startGateBlockedIndexNode();
        final TestTelemetryPlugin telemetry = getTelemetryPlugin(indexNodeName);
        telemetry.resetMeter();
        final String indexName = createIndexWithBlockedRecovery(indexNodeName);

        telemetry.collect();
        assertThat(getLastLongGaugeValue(ThrottlingRecoveryService.RECOVERY_GATE_BLOCKED_CURRENT_METRIC, telemetry), equalTo(1L));
        final List<Measurement> blockCount = telemetry.getLongCounterMeasurement(
            RecoveryMetricsCollector.RECOVERY_GATE_BLOCKED_TOTAL_METRIC
        );
        assertThat(blockCount, hasSize(1));
        assertThat(blockCount.getFirst().getLong(), equalTo(1L));
        assertThat(
            blockCount.getFirst().attributes().get(RecoveryMetricsCollector.RECOVERY_GATE_NAME_ATTRIBUTE_KEY),
            equalTo("estimated_heap")
        );
        assertThat(getLastLongGaugeValue(EstimatedHeapUsageRecoveryGate.ESTIMATED_HEAP_USAGE_METRIC, telemetry), greaterThan(0L));
        assertThat(getLastLongGaugeValue(EstimatedHeapUsageRecoveryGate.ESTIMATED_HEAP_USAGE_DELTA_METRIC, telemetry), lessThan(0L));
        assertThat(
            telemetry.getLongHistogramMeasurement(EstimatedHeapUsageRecoveryGate.ESTIMATED_HEAP_COMPUTATION_TIME_METRIC_IN_MILLIS),
            not(empty())
        );

        // Drop the node's estimate below the watermark: the gate opens and the monitor's periodic re-evaluation resumes the held
        // recovery
        final CountDownLatch gateOpened = awaitGateOutcome(indexNodeName, RecoveryGate.Outcome.RUN);
        setWorkloadMemoryOverheadOverride(indexNodeName, 100);
        safeAwait(gateOpened);
        // shards are started
        ensureGreen(indexName);

        telemetry.collect();
        assertThat(getLastLongGaugeValue(ThrottlingRecoveryService.RECOVERY_GATE_BLOCKED_CURRENT_METRIC, telemetry), equalTo(0L));
        assertThat(getLastLongGaugeValue(EstimatedHeapUsageRecoveryGate.ESTIMATED_HEAP_USAGE_DELTA_METRIC, telemetry), greaterThan(0L));
        assertThat(telemetry.getLongHistogramMeasurement(RecoveryMetricsCollector.RECOVERY_GATE_BLOCKED_DURATION_METRIC), hasSize(1));
    }

    public void testDisablingThresholdSettingReleasesGate() throws Exception {
        final String indexNodeName = startGateBlockedIndexNode();
        final String indexName = createIndexWithBlockedRecovery(indexNodeName);

        // The shared threshold setting is the operational kill switch for heap intervention: disabling it releases the gate too.
        final CountDownLatch gateOpened = awaitGateOutcome(indexNodeName, RecoveryGate.Outcome.RUN);
        updateClusterSettings(
            Settings.builder()
                .put(InternalClusterInfoService.CLUSTER_ROUTING_ALLOCATION_ESTIMATED_HEAP_THRESHOLD_DECIDER_ENABLED.getKey(), false)
        );
        safeAwait(gateOpened);
        // shards are started
        ensureGreen(indexName);
    }

    /// The gate's safety invariant against live, randomized data: once publications catch up, the master's estimate for a node is
    /// never below the node's own estimate. A node-local estimate above the master's would let the gate defer recoveries on a node
    /// the master considers healthy and would never rebalance away.
    public void testNodeLocalEstimateEventuallyAtMostMasterEstimate() throws Exception {
        runNodeLocalEstimateAtMostMasterEstimate(false);
    }

    /// Same invariant with the adaptive per-shard estimate (segments, fields, postings...), which is what serverless configures
    /// instead of the fixed 6MB default.
    public void testNodeLocalEstimateEventuallyAtMostMasterEstimateWithAdaptiveOverhead() throws Exception {
        runNodeLocalEstimateAtMostMasterEstimate(true);
    }

    private void runNodeLocalEstimateAtMostMasterEstimate(boolean adaptiveShardOverhead) throws Exception {
        startMasterOnlyNode();
        final List<String> indexNodeNames = List.of(startIndexNode(), startIndexNode());
        ensureStableCluster(3);
        // Uniform override on every node, as in production where the workload overhead is a constant; small enough that the
        // decider and gate stay below the watermark on 512 MB test heaps.
        for (StatelessMemoryMetricsService service : internalCluster().getInstances(StatelessMemoryMetricsService.class)) {
            service.setWorkloadMemoryOverheadOverrideForTesting(100);
        }
        refreshClusterInfo();

        if (adaptiveShardOverhead) {
            // The adaptive minimum threshold stays off: its per-shard floor (~31MB) would exceed the watermark on 512 MB test
            // heaps — the unit tests cover its parity and direction.
            updateClusterSettings(
                Settings.builder().put(StatelessMemoryMetricsService.FIXED_SHARD_MEMORY_OVERHEAD_SETTING.getKey(), "-1b")
            );
        }

        // Index random data. The TOTAL shard count is bounded so that even a maximally skewed distribution stays below the low
        // watermark on 512 MB test heaps (60 shards × 6MB fixed overhead ≈ 70% on a single node), and so below the high watermark
        // the gate uses
        final int indexCount = between(1, 6);
        for (int i = 0; i < indexCount; i++) {
            final String indexName = randomIdentifier();
            createIndex(indexName, indexSettings(between(1, 10), 0).build());
            final int docCount = between(0, 500);
            if (docCount > 0) {
                final var bulk = client().prepareBulk();
                for (int doc = 0; doc < docCount; doc++) {
                    bulk.add(prepareIndex(indexName).setSource("field", randomAlphaOfLengthBetween(10, 100)));
                }
                assertNoFailures(bulk.get());
            }
            if (randomBoolean()) {
                flush(indexName);
            }
        }
        ensureGreen();
        // Settle and publish the final stats: the refresh updates every shard's field stats and triggers their publication.
        refresh();

        // Compare against ClusterInfo — the exact values the master's allocation decider consumes, covering the full pipeline
        // (publication, StatelessHeapUsageReader, InternalClusterInfoService). Publication is asynchronous, so
        // a fresher local view may transiently exceed the master's — that freshness is the gate's purpose; each retry refreshes
        // ClusterInfo so the two sides converge.
        assertBusy(() -> {
            final ClusterInfo clusterInfo = refreshClusterInfo();
            for (String indexNodeName : indexNodeNames) {
                final long localEstimate = nodeLocalEstimate(indexNodeName);
                final String nodeId = internalCluster().clusterService(indexNodeName).localNode().getId();
                final NodeHeapMetrics nodeHeapMetrics = clusterInfo.getNodeHeapMetrics().get(nodeId);
                assertNotNull("no heap metrics for node " + indexNodeName + " in ClusterInfo yet", nodeHeapMetrics);
                assertThat(
                    "node-local estimate must not exceed the master's estimate for node " + indexNodeName,
                    localEstimate,
                    lessThanOrEqualTo(nodeHeapMetrics.nodeHeapEstimates().totalHeapUsage())
                );
            }
        });
    }

    private long nodeLocalEstimate(String nodeName) {
        return internalCluster().getInstance(EstimatedHeapUsageRecoveryGate.class, nodeName).currentEstimateBytes();
    }

    /// Starts a master and one index node whose local heap estimate is pushed permanently over the high watermark: the overridden
    /// workload overhead alone exceeds any test JVM heap. The master's own instance gets a small override so its estimates stay
    /// below the watermark and the allocation decider keeps assigning shards to the node — isolating the gate.
    private String startGateBlockedIndexNode() {
        final String masterNodeName = startMasterOnlyNode();
        final String indexNodeName = startIndexNode();
        ensureStableCluster(2);
        setWorkloadMemoryOverheadOverride(masterNodeName, 100);
        setWorkloadMemoryOverheadOverride(indexNodeName, ByteSizeValue.ofGb(4).getBytes());
        // The startup ClusterInfo refresh may have captured estimates from before the overrides; refresh so the master's
        // allocation decider deterministically sees the small master-side estimates and keeps assigning shards to the node.
        refreshClusterInfo();
        return indexNodeName;
    }

    /// Creates an index without waiting for it and asserts its recovery is deferred: the node's gate decides BLOCK and the
    /// index does not go green while the gate stays closed.
    private String createIndexWithBlockedRecovery(String indexNodeName) throws Exception {
        safeAwait(awaitGateOutcome(indexNodeName, RecoveryGate.Outcome.BLOCK));
        final RecoveryGate.Decision decision = gateDecision(indexNodeName);
        assertFalse("expected the gate to block while the estimate exceeds the watermark", decision.mayRun());
        assertEquals("estimated_heap", decision.gateName());

        final String indexName = randomIdentifier();
        prepareCreate(indexName).setSettings(indexSettings(1, 0).build()).setWaitForActiveShards(ActiveShardCount.NONE).get();
        awaitRecoveryStats(
            indexNodeName,
            indexName,
            stats -> stats.currentFromStoreQueued() + stats.currentAsTargetQueued() == 1
                && stats.currentFromStore() + stats.currentAsTarget() == 0
        );
        ensureRed(indexName);
        return indexName;
    }

    /// A latch released once the node's combined gate decision next evaluates to the given outcome; the monitor re-evaluates
    /// periodically while the callback waits, so no external trigger is needed.
    private CountDownLatch awaitGateOutcome(String nodeName, RecoveryGate.Outcome outcome) {
        final CountDownLatch latch = new CountDownLatch(1);
        internalCluster().getInstance(RecoveryGateMonitor.class, nodeName).addCallback(outcome, latch::countDown);
        return latch;
    }

    /// Event-driven wait on the index's recovery stats: re-checked on every recovery scheduling change on the node (enqueue,
    /// start, cancellation, completion) instead of polling.
    private void awaitRecoveryStats(String nodeName, String indexName, Predicate<RecoveryStats> predicate) {
        final CountDownLatch conditionLatch = new CountDownLatch(1);
        final var schedulingListeners = internalCluster().getInstance(CompositeRecoverySchedulingListener.class, nodeName);
        final var listener = new TestRecoverySchedulingListener() {
            @Override
            public void onRecoverySchedulingChange() {
                final RecoveryStats recoveryStats = recoveryStatsOrNull(nodeName, indexName);
                if (recoveryStats != null && predicate.test(recoveryStats)) {
                    conditionLatch.countDown();
                }
            }
        };
        schedulingListeners.addListener(listener);
        try {
            // in case the condition was already met before the listener was registered
            listener.onRecoverySchedulingChange();
            safeAwait(conditionLatch);
        } finally {
            schedulingListeners.removeListener(listener);
        }
    }

    /// Null until the node has created the shard; the scheduling listener re-checks on each event.
    private RecoveryStats recoveryStatsOrNull(String nodeName, String indexName) {
        final var indexService = internalCluster().getInstance(IndicesService.class, nodeName).indexService(resolveIndex(indexName));
        if (indexService == null) {
            return null;
        }
        final var shard = indexService.getShardOrNull(0);
        return shard == null ? null : shard.recoveryStats();
    }

    /// Evaluates the node's [RecoveryGateMonitor] — the combined node-wide decision the recovery scheduler consults, covering the
    /// gate registration wiring as well as the gate itself.
    private RecoveryGate.Decision gateDecision(String nodeName) {
        return internalCluster().getInstance(RecoveryGateMonitor.class, nodeName).evaluate();
    }

    private void setWorkloadMemoryOverheadOverride(String nodeName, long value) {
        internalCluster().getInstance(StatelessMemoryMetricsService.class, nodeName).setWorkloadMemoryOverheadOverrideForTesting(value);
    }
}
