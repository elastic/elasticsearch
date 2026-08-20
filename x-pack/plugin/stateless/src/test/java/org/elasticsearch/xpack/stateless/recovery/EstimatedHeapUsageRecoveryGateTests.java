/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.recovery;

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.InternalClusterInfoService;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.telemetry.InstrumentType;
import org.elasticsearch.telemetry.RecordingMeterRegistry;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.stateless.EstimatedHeapSettings;
import org.elasticsearch.xpack.stateless.allocation.EstimatedHeapUsageAllocationDecider;

import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.ToLongFunction;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;

public class EstimatedHeapUsageRecoveryGateTests extends ESTestCase {

    public void testRunsAndSkipsEstimateWhenHeapDeciderDisabled() {
        final long maxHeap = randomMaxHeapBytes();
        final var gate = newGate(settings(false, randomWatermark(), randomEnabledMinHeap(maxHeap)), maxHeap, state -> {
            throw new AssertionError("estimate must not be computed when the gate is disabled");
        });
        assertRuns(gate);
    }

    public void testRunsAndSkipsEstimateWhenHighWatermarkDisabled() {
        final long maxHeap = randomMaxHeapBytes();
        final Settings settings = Settings.builder()
            .put(settings(true, randomWatermark(), randomEnabledMinHeap(maxHeap)))
            .put(EstimatedHeapUsageAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ESTIMATED_HEAP_HIGH_WATERMARK_ENABLED.getKey(), false)
            .build();
        final var gate = newGate(settings, maxHeap, state -> {
            throw new AssertionError("estimate must not be computed when the high watermark is disabled");
        });
        assertRuns(gate);
    }

    public void testRunsAndSkipsEstimateWhenHeapBelowEnablementThreshold() {
        final long maxHeap = randomMaxHeapBytes();
        final String minHeapAboveMaxHeap = randomLongBetween(maxHeap + 1, maxHeap * 2) + "b";
        final var gate = newGate(settings(true, randomWatermark(), minHeapAboveMaxHeap), maxHeap, state -> {
            throw new AssertionError("estimate must not be computed below the enablement heap threshold");
        });
        assertRuns(gate);
    }

    public void testRunsAndSkipsEstimateWhenMaxHeapIsZero() {
        final var gate = newGate(settings(true, randomWatermark(), "0b"), 0L, state -> {
            throw new AssertionError("estimate must not be computed when max heap is zero");
        });
        assertRuns(gate);
    }

    public void testBlocksWhenEstimateExceedsHighWatermark() {
        final long maxHeap = randomMaxHeapBytes();
        final int watermarkPercent = between(1, 100);
        final int usedPercent = between(watermarkPercent + 1, 200); // an estimate may legitimately exceed the whole heap
        final var gate = newGate(
            settings(true, watermarkPercent + "%", randomEnabledMinHeap(maxHeap)),
            maxHeap,
            state -> bytesOf(maxHeap, usedPercent)
        );
        assertBlocks(gate);
        final var decision = gate.evaluate();
        assertThat(decision.gateName(), equalTo("estimated_heap"));
        assertThat(decision.reason(), containsString("exceeds high watermark [" + watermarkPercent + ".0%]"));
    }

    public void testRunsWhenEstimateAtOrBelowHighWatermark() {
        final long maxHeap = randomMaxHeapBytes();
        final int watermarkPercent = between(2, 100);
        final int usedPercent = between(1, watermarkPercent - 1);
        final var gate = newGate(
            settings(true, watermarkPercent + "%", randomEnabledMinHeap(maxHeap)),
            maxHeap,
            state -> bytesOf(maxHeap, usedPercent)
        );
        assertRuns(gate);

        // The boundary
        final var gateAtDefaultWatermark = newGate(settings(true, "100%", randomEnabledMinHeap(maxHeap)), maxHeap, state -> maxHeap);
        assertRuns(gateAtDefaultWatermark);
    }

    /// Raising the high watermark over the estimate releases the gate with no estimate change
    public void testDynamicHighWatermarkUpdateChangesDecision() {
        final long maxHeap = randomMaxHeapBytes();
        final int initialWatermarkPercent = between(1, 98);
        final int usedPercent = between(initialWatermarkPercent + 1, 99);
        final int raisedWatermarkPercent = between(usedPercent, 100);
        final ClusterSettings clusterSettings = clusterSettings(
            settings(true, initialWatermarkPercent + "%", randomEnabledMinHeap(maxHeap))
        );
        final var gate = new EstimatedHeapUsageRecoveryGate(
            new EstimatedHeapSettings(clusterSettings),
            () -> ClusterState.EMPTY_STATE,
            maxHeap,
            state -> bytesOf(maxHeap, usedPercent),
            () -> 0L,
            TimeValue.ZERO
        );
        assertBlocks(gate);

        clusterSettings.applySettings(
            Settings.builder()
                .put(
                    EstimatedHeapUsageAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ESTIMATED_HEAP_HIGH_WATERMARK.getKey(),
                    raisedWatermarkPercent + "%"
                )
                .build()
        );
        assertRuns(gate);
    }

    /// Once the cached estimate expires, the decision follows the live estimate in both directions, so the RecoveryGateMonitor's
    /// periodic re-evaluation observes changes.
    public void testDecisionTracksLiveEstimateAcrossBothEdges() {
        final long maxHeap = randomMaxHeapBytes();
        final int watermarkPercent = between(2, 100);
        final long underEstimate = bytesOf(maxHeap, between(0, watermarkPercent - 1));
        final long overEstimate = bytesOf(maxHeap, between(watermarkPercent + 1, 200));
        final AtomicLong estimate = new AtomicLong(underEstimate); // start under the watermark
        final var gate = newGate(settings(true, watermarkPercent + "%", randomEnabledMinHeap(maxHeap)), maxHeap, state -> estimate.get());

        assertRuns(gate);

        estimate.set(overEstimate); // now over the watermark
        assertBlocks(gate);

        estimate.set(underEstimate); // back under the watermark
        assertRuns(gate);
    }

    /// Evaluations within the validity window reuse the last estimate (one shard walk per window however hot the dispatch path is);
    /// once the window expires the next evaluation recomputes and the decision follows the live value.
    public void testReusesEstimateWithinValidityWindow() {
        final long maxHeap = randomMaxHeapBytes();
        final int watermarkPercent = between(2, 100);
        final long validityMillis = randomLongBetween(1, 30_000);
        final AtomicLong nowMillis = new AtomicLong();
        final AtomicLong estimate = new AtomicLong(bytesOf(maxHeap, between(0, watermarkPercent - 1)));
        final AtomicInteger computations = new AtomicInteger();
        final var gate = new EstimatedHeapUsageRecoveryGate(
            new EstimatedHeapSettings(clusterSettings(settings(true, watermarkPercent + "%", randomEnabledMinHeap(maxHeap)))),
            () -> ClusterState.EMPTY_STATE,
            maxHeap,
            state -> {
                computations.incrementAndGet();
                return estimate.get();
            },
            nowMillis::get,
            TimeValue.timeValueMillis(validityMillis)
        );

        assertRuns(gate); // computes at t=0
        estimate.set(bytesOf(maxHeap, between(watermarkPercent + 1, 200)));   // live estimate exceeds watermark
        nowMillis.set(randomLongBetween(0, validityMillis)); // anywhere inside the window, inclusive
        assertRuns(gate); // cached estimate, no recompute, stale RUN
        assertThat(computations.get(), equalTo(1));

        nowMillis.set(validityMillis + 1); // strictly past expiry
        assertBlocks(gate); // decision follows the live value again
        assertThat(computations.get(), equalTo(2));
    }

    /// A failed computation is not cached: with the clock frozen inside the validity window, the next evaluation still retries
    /// (a cached failure would otherwise pin the fail-open RUN for the whole window).
    public void testFailedComputationIsNotCached() {
        final long maxHeap = randomMaxHeapBytes();
        final int watermarkPercent = between(1, 100);
        final int usedPercent = between(watermarkPercent + 1, 200);
        final AtomicBoolean failing = new AtomicBoolean(true);
        final var gate = new EstimatedHeapUsageRecoveryGate(
            new EstimatedHeapSettings(clusterSettings(settings(true, watermarkPercent + "%", randomEnabledMinHeap(maxHeap)))),
            () -> ClusterState.EMPTY_STATE,
            maxHeap,
            state -> {
                if (failing.get()) {
                    throw new RuntimeException("simulated estimate failure");
                }
                return bytesOf(maxHeap, usedPercent);
            },
            () -> 0L,
            TimeValue.timeValueMillis(randomLongBetween(1, 30_000))
        );
        assertRuns(gate); // fails open

        failing.set(false);
        assertBlocks(gate); // recomputed despite the frozen clock: the failure left nothing in the cache
    }

    /// The gate fails open: [org.elasticsearch.indices.recovery.RecoveryGate]s must not throw, and a broken estimate (e.g. a shard
    /// closed mid-computation) must not hold recoveries back.
    public void testFailsOpenWhenEstimateComputationThrows() {
        final long maxHeap = randomMaxHeapBytes();
        final var gate = newGate(settings(true, randomWatermark(), randomEnabledMinHeap(maxHeap)), maxHeap, state -> {
            throw new RuntimeException("simulated estimate failure");
        });
        assertRuns(gate);
    }

    public void testRecordsEstimatedHeapMetrics() {
        final long maxHeap = randomMaxHeapBytes();
        final int watermarkPercent = between(1, 98);
        final int usedPercent = between(watermarkPercent + 1, 99);
        final int raisedWatermarkPercent = between(usedPercent + 1, 100);
        final long estimate = bytesOf(maxHeap, usedPercent);
        final long computationTimeMillis = randomLongBetween(1, 1_000);
        final AtomicLong nowMillis = new AtomicLong();
        final RecordingMeterRegistry meterRegistry = new RecordingMeterRegistry();
        final ClusterSettings clusterSettings = clusterSettings(settings(true, watermarkPercent + "%", randomEnabledMinHeap(maxHeap)));
        final var gate = new EstimatedHeapUsageRecoveryGate(
            new EstimatedHeapSettings(clusterSettings),
            () -> ClusterState.EMPTY_STATE,
            maxHeap,
            state -> {
                nowMillis.addAndGet(computationTimeMillis);
                return estimate;
            },
            nowMillis::get,
            TimeValue.timeValueSeconds(1),
            meterRegistry
        );

        assertBlocks(gate);
        assertBlocks(gate); // cached evaluation does not record another computation
        meterRegistry.getRecorder().collect();
        assertThat(
            meterRegistry.getRecorder()
                .getMeasurements(InstrumentType.LONG_GAUGE, EstimatedHeapUsageRecoveryGate.ESTIMATED_HEAP_USAGE_METRIC),
            RecordingMeterRegistry.measures(estimate)
        );
        assertThat(
            meterRegistry.getRecorder()
                .getMeasurements(InstrumentType.LONG_GAUGE, EstimatedHeapUsageRecoveryGate.ESTIMATED_HEAP_USAGE_DELTA_METRIC),
            RecordingMeterRegistry.measures(bytesOf(maxHeap, watermarkPercent) - estimate)
        );
        assertThat(
            meterRegistry.getRecorder()
                .getMeasurements(
                    InstrumentType.LONG_HISTOGRAM,
                    EstimatedHeapUsageRecoveryGate.ESTIMATED_HEAP_COMPUTATION_TIME_METRIC_IN_MILLIS
                ),
            RecordingMeterRegistry.measures(computationTimeMillis)
        );

        meterRegistry.getRecorder().resetCalls();
        clusterSettings.applySettings(
            Settings.builder()
                .put(
                    EstimatedHeapUsageAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ESTIMATED_HEAP_HIGH_WATERMARK.getKey(),
                    raisedWatermarkPercent + "%"
                )
                .build()
        );
        assertRuns(gate);
        meterRegistry.getRecorder().collect();
        assertThat(
            meterRegistry.getRecorder()
                .getMeasurements(InstrumentType.LONG_GAUGE, EstimatedHeapUsageRecoveryGate.ESTIMATED_HEAP_USAGE_METRIC),
            RecordingMeterRegistry.measures(estimate)
        );
        assertThat(
            meterRegistry.getRecorder()
                .getMeasurements(InstrumentType.LONG_GAUGE, EstimatedHeapUsageRecoveryGate.ESTIMATED_HEAP_USAGE_DELTA_METRIC),
            RecordingMeterRegistry.measures(bytesOf(maxHeap, raisedWatermarkPercent) - estimate)
        );
        assertThat(
            meterRegistry.getRecorder()
                .getMeasurements(
                    InstrumentType.LONG_HISTOGRAM,
                    EstimatedHeapUsageRecoveryGate.ESTIMATED_HEAP_COMPUTATION_TIME_METRIC_IN_MILLIS
                ),
            empty()
        );

        gate.close();
        assertFalse(
            meterRegistry.getRecorder()
                .getRegisteredMetrics(InstrumentType.LONG_GAUGE)
                .contains(EstimatedHeapUsageRecoveryGate.ESTIMATED_HEAP_USAGE_METRIC)
        );
        assertFalse(
            meterRegistry.getRecorder()
                .getRegisteredMetrics(InstrumentType.LONG_GAUGE)
                .contains(EstimatedHeapUsageRecoveryGate.ESTIMATED_HEAP_USAGE_DELTA_METRIC)
        );
    }

    public void testRecordsFailedEstimateComputation() {
        final long maxHeap = randomMaxHeapBytes();
        final long computationTimeMillis = randomLongBetween(1, 1_000);
        final AtomicLong nowMillis = new AtomicLong();
        final RecordingMeterRegistry meterRegistry = new RecordingMeterRegistry();
        final var gate = new EstimatedHeapUsageRecoveryGate(
            new EstimatedHeapSettings(clusterSettings(settings(true, randomWatermark(), randomEnabledMinHeap(maxHeap)))),
            () -> ClusterState.EMPTY_STATE,
            maxHeap,
            state -> {
                nowMillis.addAndGet(computationTimeMillis);
                throw new RuntimeException("simulated estimate failure");
            },
            nowMillis::get,
            TimeValue.timeValueSeconds(1),
            meterRegistry
        );

        assertRuns(gate);
        assertThat(
            meterRegistry.getRecorder()
                .getMeasurements(
                    InstrumentType.LONG_COUNTER,
                    EstimatedHeapUsageRecoveryGate.ESTIMATED_HEAP_COMPUTATION_FAILURE_TOTAL_METRIC
                ),
            RecordingMeterRegistry.measures(1)
        );
        assertThat(
            meterRegistry.getRecorder()
                .getMeasurements(
                    InstrumentType.LONG_HISTOGRAM,
                    EstimatedHeapUsageRecoveryGate.ESTIMATED_HEAP_COMPUTATION_TIME_METRIC_IN_MILLIS
                ),
            RecordingMeterRegistry.measures(computationTimeMillis)
        );
    }

    /// Zero validity: every evaluation recomputes, so these tests observe the live estimator. Caching is covered separately.
    private static EstimatedHeapUsageRecoveryGate newGate(Settings settings, long maxHeapBytes, ToLongFunction<ClusterState> estimator) {
        return new EstimatedHeapUsageRecoveryGate(
            new EstimatedHeapSettings(clusterSettings(settings)),
            () -> ClusterState.EMPTY_STATE,
            maxHeapBytes,
            estimator,
            () -> 0L,
            TimeValue.ZERO
        );
    }

    private static ClusterSettings clusterSettings(Settings settings) {
        return new ClusterSettings(
            settings,
            Set.of(
                InternalClusterInfoService.CLUSTER_ROUTING_ALLOCATION_ESTIMATED_HEAP_THRESHOLD_DECIDER_ENABLED,
                EstimatedHeapUsageAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ESTIMATED_HEAP_LOW_WATERMARK,
                EstimatedHeapUsageAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ESTIMATED_HEAP_HIGH_WATERMARK,
                EstimatedHeapUsageAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ESTIMATED_HEAP_HIGH_WATERMARK_ENABLED,
                EstimatedHeapUsageAllocationDecider.MINIMUM_HEAP_SIZE_FOR_ENABLEMENT
            )
        );
    }

    private static Settings settings(boolean enabled, String highWatermark, String minHeapForEnablement) {
        return Settings.builder()
            .put(InternalClusterInfoService.CLUSTER_ROUTING_ALLOCATION_ESTIMATED_HEAP_THRESHOLD_DECIDER_ENABLED.getKey(), enabled)
            .put(EstimatedHeapUsageAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ESTIMATED_HEAP_HIGH_WATERMARK.getKey(), highWatermark)
            .put(EstimatedHeapUsageAllocationDecider.MINIMUM_HEAP_SIZE_FOR_ENABLEMENT.getKey(), minHeapForEnablement)
            .build();
    }

    private static long randomMaxHeapBytes() {
        return randomLongBetween(ByteSizeValue.ofMb(64).getBytes(), ByteSizeValue.ofGb(64).getBytes());
    }

    private static String randomWatermark() {
        return between(1, 100) + "%";
    }

    private static String randomEnabledMinHeap(long maxHeapBytes) {
        return randomLongBetween(0, maxHeapBytes) + "b";
    }

    private static long bytesOf(long maxHeapBytes, double percent) {
        return (long) (maxHeapBytes * percent / 100.0);
    }

    private static void assertRuns(EstimatedHeapUsageRecoveryGate gate) {
        assertTrue(gate.evaluate().mayRun());
    }

    private static void assertBlocks(EstimatedHeapUsageRecoveryGate gate) {
        assertFalse(gate.evaluate().mayRun());
    }
}
