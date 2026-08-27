/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.painless;

import org.elasticsearch.test.ESTestCase;

import java.util.BitSet;

import static org.hamcrest.Matchers.containsString;

/**
 * Exercises the allocation-tracking runtime scaffolding directly: the {@link PainlessScript} counter defaults and the
 * {@link AllocationGuard#allocationLimitExceeded} log-and-throw helper. The charge-and-check itself lives on the generated
 * {@code $checkAllocBytes} override and is exercised end to end by the pre-check tests.
 */
public class AllocationGuardTests extends ESTestCase {

    public void testDefaultsWhenTrackingDisabled() {
        // A script that does not opt in keeps the interface defaults: a zero total, no usable increment, and a no-op check.
        PainlessScript script = new PainlessScript() {
            @Override
            public String getName() {
                return "disabled";
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

        assertEquals(0L, script.getAllocBytes());
        expectThrows(UnsupportedOperationException.class, () -> script.$incAllocBytes(10L));
        // The check is a no-op when tracking is disabled, so it must not throw.
        script.$checkAllocBytes(1_000_000L);
    }

    public void testAllocationLimitExceededThrows() {
        PainlessError error = expectThrows(PainlessError.class, () -> AllocationGuard.allocationLimitExceeded(20L, 110L, 100L));

        // PainlessError is an Error, not an Exception, so a script cannot catch it.
        assertFalse("must not be catchable as an Exception", Exception.class.isAssignableFrom(error.getClass()));
        // The message carries the byte values for diagnostics.
        assertThat(error.getMessage(), containsString("[20] bytes"));
        assertThat(error.getMessage(), containsString("[110] bytes"));
        assertThat(error.getMessage(), containsString("limit of [100] bytes"));
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
}
