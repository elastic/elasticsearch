/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.painless;

import org.elasticsearch.painless.spi.PainlessTestScript;
import org.elasticsearch.script.ScriptException;
import org.elasticsearch.telemetry.InstrumentType;
import org.elasticsearch.telemetry.Measurement;
import org.elasticsearch.telemetry.RecordingMeterRegistry;

import java.util.List;

/**
 * Per-execution allocation metrics: one count per execution in the bucket its total falls in, no enforcement, and
 * nothing counted when off. Each test builds its own engine and {@link AllocationMetrics}; no shared state.
 */
public class AllocationMetricsTests extends AllocationTestCase {

    /** Allocates a bounded, repeatable amount, comfortably under 1kb so it lands in the underflow bucket. */
    private static final String SMALL = "String s = ''; for (int i = 0; i < 4; ++i) { s = 'abcdefghij'.toUpperCase(); } return s;";

    /** Allocates a single array well over 1kb, so it lands in a named bucket rather than the underflow one. */
    private static final String LARGE = "int[] a = new int[4096]; return a.length;";

    private static AllocationMetrics recordingMetrics(RecordingMeterRegistry registry) {
        return new AllocationMetrics(registry);
    }

    /**
     * The count a bucket's counter reports. The counters are asynchronous and cumulative, so collection appends the
     * running total: the latest observation is the count, not the sum of them.
     */
    private static long bucketCount(RecordingMeterRegistry registry, int bucket) {
        List<Measurement> measurements = registry.getRecorder()
            .getMeasurements(InstrumentType.LONG_ASYNC_COUNTER, AllocationMetrics.metricName(PainlessTestScript.CONTEXT.name, bucket));

        return measurements.isEmpty() ? 0L : measurements.get(measurements.size() - 1).getLong();
    }

    /** Counts reported for the bucket {@code totalBytes} belongs to. */
    private static long countIn(RecordingMeterRegistry registry, long totalBytes) {
        registry.getRecorder().collect();

        return bucketCount(registry, AllocationMetrics.bucketIndex(totalBytes));
    }

    /** Counts reported across every bucket for the test context. */
    private static long totalCount(RecordingMeterRegistry registry) {
        registry.getRecorder().collect();

        long total = 0;

        for (int bucket = 0; bucket < AllocationMetrics.BUCKET_COUNT; ++bucket) {
            total += bucketCount(registry, bucket);
        }

        return total;
    }

    public void testExecutionIsCountedInTheBucketForItsTotal() {
        RecordingMeterRegistry registry = new RecordingMeterRegistry();
        PainlessTestScript script = compileWithMetrics(LARGE, recordingMetrics(registry));
        script.execute();

        long total = ((PainlessScript) script).getAllocBytes();
        assertTrue("the script must allocate past the underflow bucket", AllocationMetrics.bucketIndex(total) > 0);
        assertEquals(1, countIn(registry, total));
        assertEquals(1, totalCount(registry));
    }

    public void testSmallExecutionsLandInTheUnderflowBucket() {
        RecordingMeterRegistry registry = new RecordingMeterRegistry();
        PainlessTestScript script = compileWithMetrics(SMALL, recordingMetrics(registry));
        script.execute();

        assertEquals(0, AllocationMetrics.bucketIndex(((PainlessScript) script).getAllocBytes()));
        assertEquals(1, countIn(registry, 0));
    }

    public void testOneCountPerExecution() {
        // Per execution, not per allocation: the script allocates many times in a loop.
        RecordingMeterRegistry registry = new RecordingMeterRegistry();
        PainlessTestScript script = compileWithMetrics(SMALL, recordingMetrics(registry));
        script.execute();
        script.execute();
        script.execute();

        assertEquals(3, totalCount(registry));
    }

    public void testCounterResetsBetweenExecutions() {
        // Each execution is counted on its own total, so repeated runs land in the same bucket rather than climbing.
        RecordingMeterRegistry registry = new RecordingMeterRegistry();
        PainlessTestScript script = compileWithMetrics(LARGE, recordingMetrics(registry));
        script.execute();
        script.execute();

        assertEquals(2, countIn(registry, ((PainlessScript) script).getAllocBytes()));
        assertEquals(2, totalCount(registry));
    }

    public void testMetricsEnableTrackingWithoutEnforcing() {
        // The point of the mode: the counter reports and a heavily allocating script still completes.
        RecordingMeterRegistry registry = new RecordingMeterRegistry();
        PainlessTestScript script = compileWithMetrics(
            "String s = ''; for (int i = 0; i < 2000; ++i) { s = 'abcdefghij' + i; } return s;",
            recordingMetrics(registry)
        );
        script.execute();

        assertTrue("metrics alone must enable the counter", ((PainlessScript) script).getAllocBytes() > 0L);
        assertEquals(1, totalCount(registry));
    }

    public void testNothingCountedWhenMetricsAreOff() {
        // The first compile proves the registry receives counts; the metrics-off one must then add nothing.
        RecordingMeterRegistry registry = new RecordingMeterRegistry();
        compileWithMetrics(LARGE, recordingMetrics(registry)).execute();
        assertEquals(1, totalCount(registry));

        compile(LARGE, "1mb").execute();
        assertEquals(1, totalCount(registry));
    }

    public void testFailedExecutionIsNotCounted() {
        // Documented gap: counting rides the return path, so an execution that throws contributes nothing. Needs
        // metrics and the limit together, so the execution both counts and can be failed.
        RecordingMeterRegistry registry = new RecordingMeterRegistry();
        PainlessTestScript script = compileWithMetricsAndLimit("int[] a = new int[100000]; return 1;", "1b", recordingMetrics(registry));

        expectThrows(ScriptException.class, script::execute);
        assertEquals(0, totalCount(registry));
    }

    public void testBucketIndexBoundaries() {
        assertEquals(0, AllocationMetrics.bucketIndex(0));
        assertEquals(0, AllocationMetrics.bucketIndex(1023));
        assertEquals(1, AllocationMetrics.bucketIndex(1024));
        assertEquals(1, AllocationMetrics.bucketIndex(2047));
        assertEquals(2, AllocationMetrics.bucketIndex(2048));
        // Everything at or above the top boundary shares the last bucket.
        assertEquals(AllocationMetrics.BUCKET_COUNT - 1, AllocationMetrics.bucketIndex(1L << AllocationMetrics.MAX_BUCKET_EXPONENT));
        assertEquals(AllocationMetrics.BUCKET_COUNT - 1, AllocationMetrics.bucketIndex(Long.MAX_VALUE));
    }

    public void testMetricNamesAreWellFormed() {
        assertEquals(
            "es.script.painless.allocation.execution.painless_test.under_1kb.total",
            AllocationMetrics.metricName("painless_test", 0)
        );
        assertEquals(
            "es.script.painless.allocation.execution.painless_test.from_1kb.total",
            AllocationMetrics.metricName("painless_test", 1)
        );
        assertEquals(
            "es.script.painless.allocation.execution.painless_test.from_16gb.total",
            AllocationMetrics.metricName("painless_test", AllocationMetrics.BUCKET_COUNT - 1)
        );
    }
}
