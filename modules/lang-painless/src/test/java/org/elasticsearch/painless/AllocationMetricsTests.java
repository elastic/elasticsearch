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
import java.util.Map;

/**
 * Per-execution allocation metrics: one sample per execution carrying that execution's total, no enforcement, and nothing
 * recorded when off. Each test builds its own engine and {@link AllocationMetrics}; no shared state.
 */
public class AllocationMetricsTests extends AllocationTestCase {

    /** Allocates a bounded, repeatable amount so the recorded total can be compared against the counter. */
    private static final String ALLOCATING = "String s = ''; for (int i = 0; i < 20; ++i) { s = 'abcdefghij'.toUpperCase(); } return s;";

    private static AllocationMetrics recordingMetrics(RecordingMeterRegistry registry) {
        return new AllocationMetrics(registry);
    }

    private static List<Measurement> samples(RecordingMeterRegistry registry) {
        return registry.getRecorder().getMeasurements(InstrumentType.LONG_HISTOGRAM, AllocationMetrics.METRIC_EXECUTION_ALLOCATION);
    }

    public void testExecutionIsRecordedWithItsContext() {
        RecordingMeterRegistry registry = new RecordingMeterRegistry();
        PainlessTestScript script = compileWithMetrics(ALLOCATING, recordingMetrics(registry));
        script.execute();

        List<Measurement> s = samples(registry);
        assertEquals(1, s.size());
        assertEquals(Map.of(AllocationMetrics.CONTEXT_ATTRIBUTE, PainlessTestScript.CONTEXT.name), s.get(0).attributes());
    }

    public void testRecordedValueIsTheExecutionTotal() {
        // The sample must be the number the counter holds, i.e. recorded while it still describes this execution.
        RecordingMeterRegistry registry = new RecordingMeterRegistry();
        PainlessTestScript script = compileWithMetrics(ALLOCATING, recordingMetrics(registry));
        script.execute();

        List<Measurement> s = samples(registry);
        assertEquals(1, s.size());
        assertEquals(((PainlessScript) script).getAllocBytes(), s.get(0).getLong());
    }

    public void testOneSamplePerExecution() {
        // Per execution, not per allocation: the script above allocates many times in a loop.
        RecordingMeterRegistry registry = new RecordingMeterRegistry();
        PainlessTestScript script = compileWithMetrics(ALLOCATING, recordingMetrics(registry));
        script.execute();
        script.execute();
        script.execute();

        assertEquals(3, samples(registry).size());
    }

    public void testCounterResetsBetweenExecutions() {
        // Repeated runs report the same total, not one climbing with the instance's lifetime.
        RecordingMeterRegistry registry = new RecordingMeterRegistry();
        PainlessTestScript script = compileWithMetrics(ALLOCATING, recordingMetrics(registry));
        script.execute();
        script.execute();

        List<Measurement> s = samples(registry);
        assertEquals(2, s.size());
        assertEquals(s.get(0).getLong(), s.get(1).getLong());
    }

    public void testMetricsEnableTrackingWithoutEnforcing() {
        // The point of the mode: the counter reports and a heavily allocating script still completes.
        RecordingMeterRegistry registry = new RecordingMeterRegistry();
        PainlessTestScript script = compileWithMetrics(
            "String s = ''; for (int i = 0; i < 2000; ++i) { s = 'abcdefghij' + i; } return s;",
            recordingMetrics(registry)
        );
        script.execute();

        List<Measurement> s = samples(registry);
        assertEquals(1, s.size());
        assertTrue("metrics alone must enable the counter", s.get(0).getLong() > 0L);
    }

    public void testNothingRecordedWhenMetricsAreOff() {
        // The first compile proves the registry receives samples; the metrics-off one must then add nothing.
        RecordingMeterRegistry registry = new RecordingMeterRegistry();
        compileWithMetrics(ALLOCATING, recordingMetrics(registry)).execute();
        assertEquals(1, samples(registry).size());

        compile(ALLOCATING, "1mb").execute();
        assertEquals(1, samples(registry).size());
    }

    public void testFailedExecutionIsNotRecorded() {
        // Documented gap: recording rides the return path, so an execution that throws contributes no sample. Needs
        // metrics and the limit together, so the execution both records and can be failed.
        RecordingMeterRegistry registry = new RecordingMeterRegistry();
        PainlessTestScript script = compileWithMetricsAndLimit("int[] a = new int[100000]; return 1;", "1b", recordingMetrics(registry));

        expectThrows(ScriptException.class, script::execute);
        assertTrue(samples(registry).isEmpty());
    }
}
