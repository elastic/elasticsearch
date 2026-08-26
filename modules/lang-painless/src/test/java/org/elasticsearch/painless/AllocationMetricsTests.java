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
 * Per-execution allocation metrics. The interesting properties are that a sample is recorded once per execution carrying
 * that execution's total, that enabling metrics does not enforce anything, and that nothing is recorded when the feature is
 * off. Each test creates its own engine and {@link AllocationMetrics} instance; there is no shared state.
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
        // The sample must be the same number the counter holds, i.e. recorded before the value is read back and while it
        // still describes this execution.
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
        // Each sample describes its own execution, so repeated runs of the same script report the same total rather than a
        // number that climbs with the instance's lifetime.
        RecordingMeterRegistry registry = new RecordingMeterRegistry();
        PainlessTestScript script = compileWithMetrics(ALLOCATING, recordingMetrics(registry));
        script.execute();
        script.execute();

        List<Measurement> s = samples(registry);
        assertEquals(2, s.size());
        assertEquals(s.get(0).getLong(), s.get(1).getLong());
    }

    public void testMetricsEnableTrackingWithoutEnforcing() {
        // The whole point of the mode: the counter runs and reports, and a script that allocates heavily still completes.
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
        // Both compiles run the same script against the same registry, so the second half is only meaningful because the
        // first proves the registry does receive samples. A metrics-off compile must add nothing to it.
        RecordingMeterRegistry registry = new RecordingMeterRegistry();
        compileWithMetrics(ALLOCATING, recordingMetrics(registry)).execute();
        assertEquals(1, samples(registry).size());

        compile(ALLOCATING, "1mb").execute();
        assertEquals(1, samples(registry).size());
    }

    public void testFailedExecutionIsNotRecorded() {
        // Documented gap: recording rides the normal return path, so an execution that throws contributes no sample. A
        // partial total from an aborted execution would skew the distribution the histogram is meant to describe. Needs
        // metrics and the limit on together, so that the execution both records and can be failed.
        RecordingMeterRegistry registry = new RecordingMeterRegistry();
        PainlessTestScript script = compileWithMetricsAndLimit("int[] a = new int[100000]; return 1;", "1b", recordingMetrics(registry));

        expectThrows(ScriptException.class, script::execute);
        assertTrue(samples(registry).isEmpty());
    }
}
