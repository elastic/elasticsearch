/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.math;

import org.elasticsearch.test.ESTestCase;

public class RoundFunctionTests extends ESTestCase {

    public void testRoundFunction() {
        assertEquals(123, Round.process(123, null));
        assertEquals(123, Round.process(123, randomIntBetween(0, 1024)));
        assertEquals(120, Round.process(123, -1));
        assertEquals(123.5, Round.process(123.45, 1));
        assertEquals(123.0, Round.process(123.45, 0));
        assertEquals(123.0, Round.process(123.45, null));
        assertEquals(123L, Round.process(123L, 0));
        assertEquals(123L, Round.process(123L, 5));
        assertEquals(120L, Round.process(123L, -1));
        assertEquals(100L, Round.process(123L, -2));
        assertEquals(0L, Round.process(123L, -3));
        assertEquals(0L, Round.process(123L, -100));
        assertEquals(1000L, Round.process(999L, -1));
        assertEquals(1000.0, Round.process(999.0, -1));
        assertEquals(130L, Round.process(125L, -1));
        assertEquals(12400L, Round.process(12350L, -2));
        assertEquals(12400.0, Round.process(12350.0, -2));
        assertEquals(12300.0, Round.process(12349.0, -2));
        assertEquals(-12300L, Round.process(-12349L, -2));
        assertEquals(-12400L, Round.process(-12350L, -2));
        assertEquals(-12400.0, Round.process(-12350.0, -2));
        assertEquals(-100L, Round.process(-123L, -2));
        assertEquals(-120.0, Round.process(-123.45, -1));
        assertEquals(-123.5, Round.process(-123.45, 1));
        assertEquals(-124.0, Round.process(-123.5, 0));
        assertEquals(-123.0, Round.process(-123.45, null));
        assertNull(Round.process(null, 3));
        assertEquals(123.456, Round.process(123.456, Integer.MAX_VALUE));
        assertEquals(0.0, Round.process(123.456, Integer.MIN_VALUE));
        assertEquals(0L, Round.process(0L, 0));
        assertEquals(0, Round.process(0, 0));
        assertEquals((short) 0, Round.process((short) 0, 0));
        assertEquals((byte) 0, Round.process((byte) 0, 0));
        assertEquals(Long.MAX_VALUE, Round.process(Long.MAX_VALUE, null));
        assertEquals(Long.MAX_VALUE, Round.process(Long.MAX_VALUE, 5));
        assertEquals(Long.MIN_VALUE, Round.process(Long.MIN_VALUE, null));
        assertEquals(Long.MIN_VALUE, Round.process(Long.MIN_VALUE, 5));
    }
}
