/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common;

import com.carrotsearch.randomizedtesting.annotations.Timeout;
import org.elasticsearch.test.ESTestCase;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.concurrent.atomic.AtomicInteger;

public class NumbersTests extends ESTestCase {

    @Timeout(millis = 10000)
    public void testToLong() {
        assertEquals(3L, Numbers.toLong("3", false));
        assertEquals(3L, Numbers.toLong("3.1", true));
        assertEquals(9223372036854775807L, Numbers.toLong("9223372036854775807.00", false));
        assertEquals(-9223372036854775808L, Numbers.toLong("-9223372036854775808.00", false));
        assertEquals(9223372036854775807L, Numbers.toLong("9223372036854775807.00", true));
        assertEquals(-9223372036854775808L, Numbers.toLong("-9223372036854775808.00", true));
        assertEquals(9223372036854775807L, Numbers.toLong("9223372036854775807.99", true));
        assertEquals(-9223372036854775808L, Numbers.toLong("-9223372036854775808.99", true));

        assertEquals("Value [9223372036854775808] is out of range for a long", expectThrows(IllegalArgumentException.class,
            () -> Numbers.toLong("9223372036854775808", false)).getMessage());
        assertEquals("Value [-9223372036854775809] is out of range for a long", expectThrows(IllegalArgumentException.class,
            () -> Numbers.toLong("-9223372036854775809", false)).getMessage());

        assertEquals("Value [1e99999999] is out of range for a long", expectThrows(IllegalArgumentException.class,
            () -> Numbers.toLong("1e99999999", false)).getMessage());
        assertEquals("Value [-1e99999999] is out of range for a long", expectThrows(IllegalArgumentException.class,
            () -> Numbers.toLong("-1e99999999", false)).getMessage());
    }

    public void testToLongExact() {
        assertEquals(3L, Numbers.toLongExact(Long.valueOf(3L)));
        assertEquals(3L, Numbers.toLongExact(Integer.valueOf(3)));
        assertEquals(3L, Numbers.toLongExact(Short.valueOf((short) 3)));
        assertEquals(3L, Numbers.toLongExact(Byte.valueOf((byte) 3)));
        assertEquals(3L, Numbers.toLongExact(3d));
        assertEquals(3L, Numbers.toLongExact(3f));
        assertEquals(3L, Numbers.toLongExact(BigInteger.valueOf(3L)));
        assertEquals(3L, Numbers.toLongExact(BigDecimal.valueOf(3L)));

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
                () -> Numbers.toLongExact(3.1d));
        assertEquals("3.1 is not an integer value", e.getMessage());
        e = expectThrows(IllegalArgumentException.class,
                () -> Numbers.toLongExact(Double.NaN));
        assertEquals("NaN is not an integer value", e.getMessage());
        e = expectThrows(IllegalArgumentException.class,
                () -> Numbers.toLongExact(Double.POSITIVE_INFINITY));
        assertEquals("Infinity is not an integer value", e.getMessage());
        e = expectThrows(IllegalArgumentException.class,
                () -> Numbers.toLongExact(3.1f));
        assertEquals("3.1 is not an integer value", e.getMessage());
        e = expectThrows(IllegalArgumentException.class,
                () -> Numbers.toLongExact(new AtomicInteger(3))); // not supported
        assertEquals("Cannot check whether [3] of class [java.util.concurrent.atomic.AtomicInteger] is actually a long", e.getMessage());
    }

    public void testToIntExact() {
        assertEquals(3L, Numbers.toIntExact(Long.valueOf(3L)));
        assertEquals(3L, Numbers.toIntExact(Integer.valueOf(3)));
        assertEquals(3L, Numbers.toIntExact(Short.valueOf((short) 3)));
        assertEquals(3L, Numbers.toIntExact(Byte.valueOf((byte) 3)));
        assertEquals(3L, Numbers.toIntExact(3d));
        assertEquals(3L, Numbers.toIntExact(3f));
        assertEquals(3L, Numbers.toIntExact(BigInteger.valueOf(3L)));
        assertEquals(3L, Numbers.toIntExact(BigDecimal.valueOf(3L)));

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
                () -> Numbers.toIntExact(3.1d));
        assertEquals("3.1 is not an integer value", e.getMessage());
        e = expectThrows(IllegalArgumentException.class,
                () -> Numbers.toLongExact(Double.NaN));
        assertEquals("NaN is not an integer value", e.getMessage());
        e = expectThrows(IllegalArgumentException.class,
                () -> Numbers.toLongExact(Double.POSITIVE_INFINITY));
        assertEquals("Infinity is not an integer value", e.getMessage());
        e = expectThrows(IllegalArgumentException.class,
                () -> Numbers.toIntExact(3.1f));
        assertEquals("3.1 is not an integer value", e.getMessage());
        ArithmeticException ae = expectThrows(ArithmeticException.class,
                () -> Numbers.toIntExact(1L << 40));
        assertEquals("integer overflow", ae.getMessage());
        e = expectThrows(IllegalArgumentException.class,
                () -> Numbers.toIntExact(new AtomicInteger(3))); // not supported
        assertEquals("Cannot check whether [3] of class [java.util.concurrent.atomic.AtomicInteger] is actually a long", e.getMessage());
    }

    public void testToShortExact() {
        assertEquals(3L, Numbers.toShortExact(Long.valueOf(3L)));
        assertEquals(3L, Numbers.toShortExact(Integer.valueOf(3)));
        assertEquals(3L, Numbers.toShortExact(Short.valueOf((short) 3)));
        assertEquals(3L, Numbers.toShortExact(Byte.valueOf((byte) 3)));
        assertEquals(3L, Numbers.toShortExact(3d));
        assertEquals(3L, Numbers.toShortExact(3f));
        assertEquals(3L, Numbers.toShortExact(BigInteger.valueOf(3L)));
        assertEquals(3L, Numbers.toShortExact(BigDecimal.valueOf(3L)));

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
                () -> Numbers.toShortExact(3.1d));
        assertEquals("3.1 is not an integer value", e.getMessage());
        e = expectThrows(IllegalArgumentException.class,
                () -> Numbers.toLongExact(Double.NaN));
        assertEquals("NaN is not an integer value", e.getMessage());
        e = expectThrows(IllegalArgumentException.class,
                () -> Numbers.toLongExact(Double.POSITIVE_INFINITY));
        assertEquals("Infinity is not an integer value", e.getMessage());
        e = expectThrows(IllegalArgumentException.class,
                () -> Numbers.toShortExact(3.1f));
        assertEquals("3.1 is not an integer value", e.getMessage());
        ArithmeticException ae = expectThrows(ArithmeticException.class,
                () -> Numbers.toShortExact(100000));
        assertEquals("short overflow: " + 100000, ae.getMessage());
        e = expectThrows(IllegalArgumentException.class,
                () -> Numbers.toShortExact(new AtomicInteger(3))); // not supported
        assertEquals("Cannot check whether [3] of class [java.util.concurrent.atomic.AtomicInteger] is actually a long", e.getMessage());
    }

    public void testToByteExact() {
        assertEquals(3L, Numbers.toByteExact(Long.valueOf(3L)));
        assertEquals(3L, Numbers.toByteExact(Integer.valueOf(3)));
        assertEquals(3L, Numbers.toByteExact(Short.valueOf((short) 3)));
        assertEquals(3L, Numbers.toByteExact(Byte.valueOf((byte) 3)));
        assertEquals(3L, Numbers.toByteExact(3d));
        assertEquals(3L, Numbers.toByteExact(3f));
        assertEquals(3L, Numbers.toByteExact(BigInteger.valueOf(3L)));
        assertEquals(3L, Numbers.toByteExact(BigDecimal.valueOf(3L)));

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
                () -> Numbers.toByteExact(3.1d));
        assertEquals("3.1 is not an integer value", e.getMessage());
        e = expectThrows(IllegalArgumentException.class,
                () -> Numbers.toLongExact(Double.NaN));
        assertEquals("NaN is not an integer value", e.getMessage());
        e = expectThrows(IllegalArgumentException.class,
                () -> Numbers.toLongExact(Double.POSITIVE_INFINITY));
        assertEquals("Infinity is not an integer value", e.getMessage());
        e = expectThrows(IllegalArgumentException.class,
                () -> Numbers.toByteExact(3.1f));
        assertEquals("3.1 is not an integer value", e.getMessage());
        ArithmeticException ae = expectThrows(ArithmeticException.class,
                () -> Numbers.toByteExact(300));
        assertEquals("byte overflow: " + 300, ae.getMessage());
        e = expectThrows(IllegalArgumentException.class,
                () -> Numbers.toByteExact(new AtomicInteger(3))); // not supported
        assertEquals("Cannot check whether [3] of class [java.util.concurrent.atomic.AtomicInteger] is actually a long", e.getMessage());
    }
}
