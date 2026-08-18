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
import org.elasticsearch.telemetry.InstrumentType;
import org.elasticsearch.telemetry.Measurement;
import org.elasticsearch.telemetry.RecordingMeterRegistry;
import org.junit.After;
import org.junit.Before;

import java.util.List;
import java.util.Map;

/**
 * Per-execution allocation metrics. The interesting properties are that a sample is recorded once per execution carrying
 * that execution's total, that enabling metrics does not enforce anything, and that nothing is recorded when the feature is
 * off. The recorder is reached from a static helper, so each test installs a recording registry and restores the no-op.
 */
public class AllocationMetricsTests extends AllocationTestCase {

    /** Allocates a bounded, repeatable amount so the recorded total can be compared against the counter. */
    private static final String ALLOCATING = "String s = ''; for (int i = 0; i < 20; ++i) { s = 'abcdefghij'.toUpperCase(); } return s;";

    private RecordingMeterRegistry meterRegistry;

    @Before
    public void installRecordingMetrics() {
        meterRegistry = new RecordingMeterRegistry();
        AllocationMetrics.setInstance(new AllocationMetrics(meterRegistry));
    }

    @After
    public void restoreNoopMetrics() {
        // The instance is static; a recording registry left installed would leak into unrelated tests.
        AllocationMetrics.setInstance(AllocationMetrics.NOOP);
    }

    private List<Measurement> samples() {
        return meterRegistry.getRecorder().getMeasurements(InstrumentType.LONG_HISTOGRAM, AllocationMetrics.METRIC_EXECUTION_ALLOCATION);
    }

    public void testExecutionIsRecordedWithItsContext() {
        PainlessTestScript script = compileWithMetrics(ALLOCATING);
        script.execute();

        List<Measurement> samples = samples();
        assertEquals(1, samples.size());
        assertEquals(Map.of(AllocationMetrics.CONTEXT_ATTRIBUTE, PainlessTestScript.CONTEXT.name), samples.get(0).attributes());
    }

    public void testRecordedValueIsTheExecutionTotal() {
        // The sample must be the same number the counter holds, i.e. recorded before the value is read back and while it
        // still describes this execution.
        PainlessTestScript script = compileWithMetrics(ALLOCATING);
        script.execute();

        assertEquals(1, samples().size());
        assertEquals(((PainlessScript) script).getAllocBytes(), samples().get(0).getLong());
    }

    public void testOneSamplePerExecution() {
        // Per execution, not per allocation: the script above allocates many times in a loop.
        PainlessTestScript script = compileWithMetrics(ALLOCATING);
        script.execute();
        script.execute();
        script.execute();

        assertEquals(3, samples().size());
    }

    public void testCounterResetsBetweenExecutions() {
        // Each sample describes its own execution, so repeated runs of the same script report the same total rather than a
        // number that climbs with the instance's lifetime.
        PainlessTestScript script = compileWithMetrics(ALLOCATING);
        script.execute();
        script.execute();

        List<Measurement> samples = samples();
        assertEquals(2, samples.size());
        assertEquals(samples.get(0).getLong(), samples.get(1).getLong());
    }

    public void testMetricsEnableTrackingWithoutEnforcing() {
        // The whole point of the mode: the counter runs and reports, and a script that allocates heavily still completes.
        PainlessTestScript script = compileWithMetrics("String s = ''; for (int i = 0; i < 2000; ++i) { s = 'abcdefghij' + i; } return s;");
        script.execute();

        assertEquals(1, samples().size());
        assertTrue("metrics alone must enable the counter", samples().get(0).getLong() > 0L);
    }

    public void testNothingRecordedWhenMetricsAreOff() {
        // Compiled through the limit path instead, which leaves the metrics property unset.
        PainlessTestScript script = compile(ALLOCATING, "1mb");
        script.execute();

        assertTrue(samples().isEmpty());
    }

    public void testFailedExecutionIsNotRecorded() {
        // Documented gap: recording rides the normal return path, so an execution that throws contributes no sample. A
        // partial total from an aborted execution would skew the distribution the histogram is meant to describe.
        assertTripsLimit("int[] a = new int[100000]; return 1;", "1b");

        assertTrue(samples().isEmpty());
    }
}
