/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.painless;

import org.apache.logging.log4j.Level;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.MockLog;

import java.util.BitSet;

import static org.hamcrest.Matchers.containsString;

/**
 * Exercises the allocation-tracking runtime scaffolding directly: the {@link PainlessScript} counter defaults and
 * {@link AllocationGuard#checkAllocation}, which owns the warn/throw decision for every charge. The generated
 * {@code $checkAllocBytes} override only charges the total and calls into it; the end-to-end path is covered by the
 * pre-check, warning-threshold, and metrics tests.
 */
public class AllocationGuardTests extends ESTestCase {

    public void testDefaultsWhenTrackingDisabled() {
        // A script that does not opt in keeps the interface defaults: a zero total, no usable increment, and a no-op check.
        PainlessScript script = script();

        assertEquals(0L, script.getAllocBytes());
        expectThrows(UnsupportedOperationException.class, () -> script.$incAllocBytes(10L));
        // The check is a no-op when tracking is disabled, so it must not throw.
        script.$checkAllocBytes(1_000_000L);
    }

    public void testAllocationLimitExceededThrows() {
        PainlessError error = expectThrows(
            PainlessError.class,
            () -> AllocationGuard.allocationLimitExceeded("painless_test", 20L, 110L, 100L)
        );

        // PainlessError is an Error, not an Exception, so a script cannot catch it.
        assertFalse("must not be catchable as an Exception", Exception.class.isAssignableFrom(error.getClass()));
        // The message carries the byte values for diagnostics.
        assertThat(error.getMessage(), containsString("[20] bytes"));
        assertThat(error.getMessage(), containsString("[110] bytes"));
        assertThat(error.getMessage(), containsString("limit of [100] bytes"));
    }

    public void testCheckAllocationDoesNothingBelowBothThresholds() {
        assertFalse(AllocationGuard.checkAllocation(script(), "painless_test", 20L, 50L, false, 100L, 200L));
    }

    public void testCheckAllocationLatchesTheWarningAfterTheFirstBreach() {
        // The latch is the caller's to store, so the first breach returns true and a later one is asked not to warn again.
        assertTrue(AllocationGuard.checkAllocation(script(), "painless_test", 20L, 150L, false, 100L, -1L));
        assertTrue(AllocationGuard.checkAllocation(script(), "painless_test", 20L, 160L, true, 100L, -1L));
    }

    public void testCheckAllocationTreatsMinusOneAsOff() {
        // A threshold left unconfigured is passed as -1 and must never fire, even though every total exceeds it.
        assertFalse(AllocationGuard.checkAllocation(script(), "painless_test", 20L, Long.MAX_VALUE / 2, false, -1L, -1L));
    }

    public void testCheckAllocationThrowsOnTheLimitRegardlessOfTheWarning() {
        // Enforcement-only: no warning threshold configured, limit still fails the script.
        expectThrows(PainlessError.class, () -> AllocationGuard.checkAllocation(script(), "painless_test", 20L, 110L, false, -1L, 100L));
    }

    public void testCheckAllocationWarnsBeforeThrowingWhenBothAreCrossed() {
        // Ordering matters: the warning is reported for the allocation that also trips the limit, not skipped by the throw.
        try (MockLog mockLog = MockLog.capture(AllocationGuard.class)) {
            mockLog.addExpectation(
                new MockLog.SeenEventExpectation("warning", AllocationGuard.class.getCanonicalName(), Level.WARN, "*warning*")
            );
            expectThrows(
                PainlessError.class,
                () -> AllocationGuard.checkAllocation(script(), "painless_test", 20L, 300L, false, 100L, 200L)
            );
            mockLog.assertAllExpectationsMatched();
        }
    }

    public void testSanitizeEstimatePassesThroughSaneValues() {
        assertEquals(0L, AllocationGuard.sanitizeEstimate(0L));
        assertEquals(42L, AllocationGuard.sanitizeEstimate(42L));
    }

    public void testSanitizeEstimateClampsNegativeToZero() {
        // A negative estimate (an estimator bug) must not credit the running total.
        assertEquals(0L, AllocationGuard.sanitizeEstimate(-1L));
        assertEquals(0L, AllocationGuard.sanitizeEstimate(Long.MIN_VALUE));
    }

    public void testSanitizeEstimateClampsHugeValues() {
        // Huge values are clamped so charging them cannot overflow the running total, while still tripping any real limit.
        assertEquals(Long.MAX_VALUE / 2, AllocationGuard.sanitizeEstimate(Long.MAX_VALUE));
        assertEquals(Long.MAX_VALUE / 2, AllocationGuard.sanitizeEstimate(Long.MAX_VALUE / 2));
        assertEquals(Long.MAX_VALUE / 2 - 1, AllocationGuard.sanitizeEstimate(Long.MAX_VALUE / 2 - 1));
    }

    /** A script with tracking off, standing in for the real generated class where only its name and source are read. */
    private static PainlessScript script() {
        return new PainlessScript() {
            @Override
            public String getName() {
                return "test";
            }

            @Override
            public String getSource() {
                return "<source>";
            }

            @Override
            public BitSet getStatements() {
                return new BitSet();
            }
        };
    }
}
