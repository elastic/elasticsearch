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

import java.util.List;
import java.util.Map;

/**
 * The APM counters for allocation-threshold breaches. A breach is counted from a static helper, so each test installs a
 * recording registry and restores the no-op afterwards.
 */
public class AllocationMetricsTests extends AllocationTestCase {

    /** Allocates past any small threshold but stays bounded, so it completes unless a limit fails it. */
    private static final String ALLOCATING = "String s = ''; for (int i = 0; i < 200; ++i) { s = 'abcdefghij'.toUpperCase(); } return s;";

    private RecordingMeterRegistry meterRegistry;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        meterRegistry = new RecordingMeterRegistry();
        AllocationMetrics.setInstance(new AllocationMetrics(meterRegistry));
    }

    @Override
    public void tearDown() throws Exception {
        // The instance is static; a recording registry left installed would leak into unrelated tests.
        AllocationMetrics.setInstance(AllocationMetrics.NOOP);
        super.tearDown();
    }

    private List<Measurement> measurements(String metricName) {
        return meterRegistry.getRecorder().getMeasurements(InstrumentType.LONG_COUNTER, metricName);
    }

    public void testWarnThresholdBreachIsCounted() {
        PainlessTestScript script = compile(ALLOCATING, null, "1b");
        script.execute();

        List<Measurement> warnings = measurements(AllocationMetrics.METRIC_WARN_EXCEEDED);
        assertEquals(1, warnings.size());
        assertEquals(1L, warnings.get(0).value());
        assertEquals(Map.of(AllocationMetrics.CONTEXT_ATTRIBUTE, PainlessTestScript.CONTEXT.name), warnings.get(0).attributes());
    }

    public void testWarnThresholdCountedOncePerExecution() {
        // Matches the log latch: one count per execution, not per allocation.
        PainlessTestScript script = compile(ALLOCATING, null, "1b");
        script.execute();
        script.execute();

        assertEquals(2, measurements(AllocationMetrics.METRIC_WARN_EXCEEDED).size());
    }

    public void testNothingCountedBelowThreshold() {
        PainlessTestScript script = compile(ALLOCATING, null, "1mb");
        script.execute();

        assertTrue(measurements(AllocationMetrics.METRIC_WARN_EXCEEDED).isEmpty());
        assertTrue(measurements(AllocationMetrics.METRIC_LIMIT_EXCEEDED).isEmpty());
    }

    public void testLimitBreachIsCounted() {
        PainlessTestScript script = compile(ALLOCATING, "2kb");
        expectThrows(Exception.class, script::execute);

        List<Measurement> breaches = measurements(AllocationMetrics.METRIC_LIMIT_EXCEEDED);
        assertEquals(1, breaches.size());
        assertEquals(Map.of(AllocationMetrics.CONTEXT_ATTRIBUTE, PainlessTestScript.CONTEXT.name), breaches.get(0).attributes());
    }

    public void testWarningOnlyModeCountsNoLimitBreaches() {
        // The mode the threshold exists for: reported, counted, never enforced.
        PainlessTestScript script = compile(ALLOCATING, null, "1b");
        script.execute();

        assertEquals(1, measurements(AllocationMetrics.METRIC_WARN_EXCEEDED).size());
        assertTrue(measurements(AllocationMetrics.METRIC_LIMIT_EXCEEDED).isEmpty());
    }

    public void testBothCountedWhenBothThresholdsBreached() {
        PainlessTestScript script = compile(ALLOCATING, "2kb", "1b");
        expectThrows(Exception.class, script::execute);

        assertEquals(1, measurements(AllocationMetrics.METRIC_WARN_EXCEEDED).size());
        assertEquals(1, measurements(AllocationMetrics.METRIC_LIMIT_EXCEEDED).size());
    }

    public void testNoopInstanceIsUsableWithoutTelemetry() {
        // A node without telemetry keeps the no-op, and a breach must still work.
        AllocationMetrics.setInstance(AllocationMetrics.NOOP);
        PainlessTestScript script = compile(ALLOCATING, null, "1b");
        assertEquals("ABCDEFGHIJ", script.execute());
    }
}
