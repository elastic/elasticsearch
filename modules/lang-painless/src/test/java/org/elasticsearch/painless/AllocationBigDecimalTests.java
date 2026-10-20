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

import java.math.BigDecimal;

/**
 * End-to-end tests for the {@code BigDecimal} {@code @allocates} estimators: results are sized from the operands'
 * unscaled digit counts ({@code precision()}) and scales. Covers exact charges for small operations and confirms the scale-
 * blow-up operations ({@code pow}, {@code setScale}, {@code movePointRight}) trip a realistic limit before exhausting the heap.
 */
public class AllocationBigDecimalTests extends AllocationTestCase {

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
        assertEquals(AllocationEstimators.bigDecimalFromStringBytes("123.456"), allocatedBytes("new BigDecimal('123.456'); return 'x';"));
    }

    public void testMultiplyChargedFromOperands() {
        BigDecimal a = new BigDecimal("123.456");
        long expected = AllocationEstimators.bigDecimalFromStringBytes("123.456") + AllocationEstimators.bigDecimalBinaryBytes(a, a);
        assertEquals(expected, allocatedBytes("BigDecimal a = new BigDecimal('123.456'); a.multiply(a); return 'x';"));
    }

    public void testPowTripsLimit() {
        assertTripsUnder("new BigDecimal('1.1').pow(100000000); return 'x';", "1mb");
    }

    public void testSetScaleTripsLimit() {
        assertTripsUnder("new BigDecimal('1').setScale(100000000); return 'x';", "1mb");
    }

    public void testMovePointRightTripsLimit() {
        assertTripsUnder("new BigDecimal('1').movePointRight(100000000); return 'x';", "1mb");
    }
}
