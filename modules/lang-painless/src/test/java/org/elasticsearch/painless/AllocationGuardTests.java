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
 * Exercises the allocation-tracking runtime scaffolding directly (not yet wired into bytecode): the {@link PainlessScript}
 * counter methods and {@link AllocationGuard#checkAlloc}.
 */
public class AllocationGuardTests extends ESTestCase {

    /** Minimal {@link PainlessScript} whose only behaviour is a mutable per-instance allocation counter. */
    private static class CountingScript implements PainlessScript {
        private long allocBytes;

        @Override
        public long $incAllocBytes(long bytes) {
            allocBytes += bytes;
            return allocBytes;
        }

        @Override
        public long getAllocBytes() {
            return allocBytes;
        }

        @Override
        public String getName() {
            return "counting";
        }

        @Override
        public String getSource() {
            return "<source>";
        }

        @Override
        public BitSet getStatements() {
            return new BitSet();
        }
    }

    public void testDefaultsWhenTrackingDisabled() {
        // A script that does not opt in keeps the interface defaults: a zero total and no usable increment.
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
    }

    public void testCheckAllocUnderLimitAccumulates() {
        CountingScript script = new CountingScript();

        assertEquals(10L, AllocationGuard.checkAlloc(script, 10L, 100L, "site-a"));
        assertEquals(30L, AllocationGuard.checkAlloc(script, 20L, 100L, "site-b"));
        assertEquals(30L, script.getAllocBytes());
    }

    public void testCheckAllocAtLimitDoesNotTrip() {
        CountingScript script = new CountingScript();
        // Exactly reaching the limit is allowed; only exceeding it trips.
        assertEquals(100L, AllocationGuard.checkAlloc(script, 100L, 100L, "site"));
    }

    public void testCheckAllocOverLimitThrows() {
        CountingScript script = new CountingScript();
        AllocationGuard.checkAlloc(script, 90L, 100L, "first");

        PainlessError error = expectThrows(PainlessError.class, () -> AllocationGuard.checkAlloc(script, 20L, 100L, "second"));

        // PainlessError is an Error, not an Exception, so a script cannot catch it.
        assertFalse("must not be catchable as an Exception", Exception.class.isAssignableFrom(error.getClass()));
        // The message carries the known byte values and site for diagnostics.
        assertThat(error.getMessage(), containsString("[20] bytes at [second]"));
        assertThat(error.getMessage(), containsString("[110] bytes"));
        assertThat(error.getMessage(), containsString("limit of [100] bytes"));
        // The counter still reflects the rejected allocation; the guard charges before checking.
        assertEquals(110L, script.getAllocBytes());
    }
}
