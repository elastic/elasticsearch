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

import java.math.BigInteger;

/**
 * End-to-end tests for the {@code BigInteger} {@code @allocates_dynamic} estimators: results are sized from the operands' bit
 * lengths (object + backing {@code int[]}). Covers exact charges for small operations and confirms the exponential-growth
 * operations ({@code pow}, {@code shiftLeft}) trip a realistic limit before exhausting the heap.
 */
public class AllocationBigIntegerTests extends AllocationTestCase {

    private void assertTripsUnder(String source, String limit) {
        PainlessTestScript script = compile(source, limit);
        ScriptException e = expectThrows(ScriptException.class, script::execute);
        for (Throwable t = e; t != null; t = t.getCause()) {
            if (t.getMessage() != null && t.getMessage().contains("allocation limit exceeded")) {
                return;
            }
        }
        throw new AssertionError("expected an allocation limit error for [" + source + "], but got: " + e, e);
    }

    public void testFromStringCharged() {
        assertEquals(
            AllocationEstimators.bigIntegerFromStringBytes("123456789"),
            allocatedBytes("new BigInteger('123456789'); return 'x';")
        );
    }

    public void testPowChargedFromOperands() {
        BigInteger a = new BigInteger("7");
        long expected = AllocationEstimators.bigIntegerFromStringBytes("7") + AllocationEstimators.bigIntegerPowBytes(a, 20);
        assertEquals(expected, allocatedBytes("BigInteger a = new BigInteger('7'); a.pow(20); return 'x';"));
    }

    public void testMultiplyChargedFromOperands() {
        BigInteger a = new BigInteger("123456");
        long expected = AllocationEstimators.bigIntegerFromStringBytes("123456") + AllocationEstimators.bigIntegerMultiplyBytes(a, a);
        assertEquals(expected, allocatedBytes("BigInteger a = new BigInteger('123456'); a.multiply(a); return 'x';"));
    }

    public void testPowTripsLimit() {
        // 2.pow(100_000_000) is ~100M bits (~12MB) — preempted under a 1mb limit while the tiny constructor passes.
        assertTripsUnder("new BigInteger('2').pow(100000000); return 'x';", "1mb");
    }

    public void testShiftLeftTripsLimit() {
        assertTripsUnder("new BigInteger('1').shiftLeft(100000000); return 'x';", "1mb");
    }
}
