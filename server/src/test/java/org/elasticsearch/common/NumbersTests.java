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

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class NumbersTests extends ESTestCase {

    @Timeout(millis = 10000)
    public void testToLong() {
        assertThat(Numbers.toLong("3", false), equalTo(3L));
        assertThat(Numbers.toLong("3.1", true), equalTo(3L));
        assertThat(Numbers.toLong("9223372036854775807.00", false), equalTo(9223372036854775807L));
        assertThat(Numbers.toLong("-9223372036854775808.00", false), equalTo(-9223372036854775808L));
        assertThat(Numbers.toLong("9223372036854775807.00", true), equalTo(9223372036854775807L));
        assertThat(Numbers.toLong("-9223372036854775808.00", true), equalTo(-9223372036854775808L));
        assertThat(Numbers.toLong("9223372036854775807.99", true), equalTo(9223372036854775807L));
        assertThat(Numbers.toLong("-9223372036854775808.99", true), equalTo(-9223372036854775808L));

        assertEquals(
            "Value [9223372036854775808] is out of range for a long",
            expectThrows(IllegalArgumentException.class, () -> Numbers.toLong("9223372036854775808", false)).getMessage()
        );
        assertEquals(
            "Value [-9223372036854775809] is out of range for a long",
            expectThrows(IllegalArgumentException.class, () -> Numbers.toLong("-9223372036854775809", false)).getMessage()
        );

        assertEquals(
            "Value [1e99999999] is out of range for a long",
            expectThrows(IllegalArgumentException.class, () -> Numbers.toLong("1e99999999", false)).getMessage()
        );
        assertEquals(
            "Value [-1e99999999] is out of range for a long",
            expectThrows(IllegalArgumentException.class, () -> Numbers.toLong("-1e99999999", false)).getMessage()
        );
        assertEquals(
            "Value [12345.6] has a decimal part",
            expectThrows(IllegalArgumentException.class, () -> Numbers.toLong("12345.6", false)).getMessage()
        );
        assertEquals(
            "For input string: \"t12345\"",
            expectThrows(IllegalArgumentException.class, () -> Numbers.toLong("t12345", false)).getMessage()
        );
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

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> Numbers.toLongExact(3.1d));
        assertEquals("3.1 is not an integer value", e.getMessage());
        e = expectThrows(IllegalArgumentException.class, () -> Numbers.toLongExact(Double.NaN));
        assertEquals("NaN is not an integer value", e.getMessage());
        e = expectThrows(IllegalArgumentException.class, () -> Numbers.toLongExact(Double.POSITIVE_INFINITY));
        assertEquals("Infinity is not an integer value", e.getMessage());
        e = expectThrows(IllegalArgumentException.class, () -> Numbers.toLongExact(3.1f));
        assertEquals("3.1 is not an integer value", e.getMessage());
        e = expectThrows(IllegalArgumentException.class, () -> Numbers.toLongExact(new AtomicInteger(3))); // not supported
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

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> Numbers.toIntExact(3.1d));
        assertEquals("3.1 is not an integer value", e.getMessage());
        e = expectThrows(IllegalArgumentException.class, () -> Numbers.toLongExact(Double.NaN));
        assertEquals("NaN is not an integer value", e.getMessage());
        e = expectThrows(IllegalArgumentException.class, () -> Numbers.toLongExact(Double.POSITIVE_INFINITY));
        assertEquals("Infinity is not an integer value", e.getMessage());
        e = expectThrows(IllegalArgumentException.class, () -> Numbers.toIntExact(3.1f));
        assertEquals("3.1 is not an integer value", e.getMessage());
        ArithmeticException ae = expectThrows(ArithmeticException.class, () -> Numbers.toIntExact(1L << 40));
        assertEquals("integer overflow", ae.getMessage());
        e = expectThrows(IllegalArgumentException.class, () -> Numbers.toIntExact(new AtomicInteger(3))); // not supported
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

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> Numbers.toShortExact(3.1d));
        assertEquals("3.1 is not an integer value", e.getMessage());
        e = expectThrows(IllegalArgumentException.class, () -> Numbers.toLongExact(Double.NaN));
        assertEquals("NaN is not an integer value", e.getMessage());
        e = expectThrows(IllegalArgumentException.class, () -> Numbers.toLongExact(Double.POSITIVE_INFINITY));
        assertEquals("Infinity is not an integer value", e.getMessage());
        e = expectThrows(IllegalArgumentException.class, () -> Numbers.toShortExact(3.1f));
        assertEquals("3.1 is not an integer value", e.getMessage());
        ArithmeticException ae = expectThrows(ArithmeticException.class, () -> Numbers.toShortExact(100000));
        assertEquals("short overflow: " + 100000, ae.getMessage());
        e = expectThrows(IllegalArgumentException.class, () -> Numbers.toShortExact(new AtomicInteger(3))); // not supported
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

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> Numbers.toByteExact(3.1d));
        assertEquals("3.1 is not an integer value", e.getMessage());
        e = expectThrows(IllegalArgumentException.class, () -> Numbers.toLongExact(Double.NaN));
        assertEquals("NaN is not an integer value", e.getMessage());
        e = expectThrows(IllegalArgumentException.class, () -> Numbers.toLongExact(Double.POSITIVE_INFINITY));
        assertEquals("Infinity is not an integer value", e.getMessage());
        e = expectThrows(IllegalArgumentException.class, () -> Numbers.toByteExact(3.1f));
        assertEquals("3.1 is not an integer value", e.getMessage());
        ArithmeticException ae = expectThrows(ArithmeticException.class, () -> Numbers.toByteExact(300));
        assertEquals("byte overflow: " + 300, ae.getMessage());
        e = expectThrows(IllegalArgumentException.class, () -> Numbers.toByteExact(new AtomicInteger(3))); // not supported
        assertEquals("Cannot check whether [3] of class [java.util.concurrent.atomic.AtomicInteger] is actually a long", e.getMessage());
    }

    public void testLongToBytes() {
        assertThat(Numbers.longToBytes(123456L), is(new byte[] { 0, 0, 0, 0, 0, 1, -30, 64 }));
        assertThat(Numbers.longToBytes(-123456L), is(new byte[] { -1, -1, -1, -1, -1, -2, 29, -64 }));
        assertThat(Numbers.longToBytes(0L), is(new byte[] { 0, 0, 0, 0, 0, 0, 0, 0 }));
        assertThat(Numbers.longToBytes(Long.MAX_VALUE + 1), is(new byte[] { -128, 0, 0, 0, 0, 0, 0, 0 }));
        assertThat(Numbers.longToBytes(Long.MAX_VALUE + 127), is(new byte[] { -128, 0, 0, 0, 0, 0, 0, 126 }));
        assertThat(Numbers.longToBytes(Long.MIN_VALUE - 1), is(new byte[] { 127, -1, -1, -1, -1, -1, -1, -1 }));
        assertThat(Numbers.longToBytes(Long.MIN_VALUE - 127), is(new byte[] { 127, -1, -1, -1, -1, -1, -1, -127 }));
    }

    public void testBytesToLong() {
        assertThat(Numbers.bytesToLong(new byte[] { 0, 0, 0, 0, 0, 1, -30, 64 }, 0), is(123456L));
        assertThat(Numbers.bytesToLong(new byte[] { -1, -1, -1, -1, -1, -2, 29, -64 }, 0), is(-123456L));
        assertThat(Numbers.bytesToLong(new byte[] { 0, 0, 0, 0, 0, 0, 0, 0 }, 0), is(0L));
        assertThat(Numbers.bytesToLong(new byte[] { -128, 0, 0, 0, 0, 0, 0, 0 }, 0), is(Long.MIN_VALUE));
        assertThat(Numbers.bytesToLong(new byte[] { -128, 0, 0, 0, 0, 0, 0, 126 }, 0), is(Long.MIN_VALUE + 127 - 1));
        assertThat(Numbers.bytesToLong(new byte[] { 127, -1, -1, -1, -1, -1, -1, -1 }, 0), is(Long.MAX_VALUE));
        assertThat(Numbers.bytesToLong(new byte[] { 127, -1, -1, -1, -1, -1, -1, -127, 0 }, 0), is(Long.MAX_VALUE - 127 + 1));

        assertThat(Numbers.bytesToLong(new byte[] { 100, 0, 0, 0, 0, 0, 1, -30, 64 }, 1), is(123456L));
        assertThat(Numbers.bytesToLong(new byte[] { -100, -1, -1, -1, -1, -1, -2, 29, -64 }, 1), is(-123456L));
    }

    public void testIntToBytes() {
        assertThat(Numbers.intToBytes(123456), is(new byte[] { 0, 1, -30, 64 }));
        assertThat(Numbers.intToBytes(-123456), is(new byte[] { -1, -2, 29, -64 }));
        assertThat(Numbers.intToBytes(0), is(new byte[] { 0, 0, 0, 0 }));
        assertThat(Numbers.intToBytes(Integer.MAX_VALUE + 1), is(new byte[] { -128, 0, 0, 0 }));
        assertThat(Numbers.intToBytes(Integer.MAX_VALUE + 127), is(new byte[] { -128, 0, 0, 126 }));
        assertThat(Numbers.intToBytes(Integer.MIN_VALUE - 1), is(new byte[] { 127, -1, -1, -1 }));
        assertThat(Numbers.intToBytes(Integer.MIN_VALUE - 127), is(new byte[] { 127, -1, -1, -127 }));
    }

    public void testBytesToInt() {
        assertThat(Numbers.bytesToInt(new byte[] { 0, 1, -30, 64 }, 0), is(123456));
        assertThat(Numbers.bytesToInt(new byte[] { -1, -2, 29, -64 }, 0), is(-123456));
        assertThat(Numbers.bytesToInt(new byte[] { 0, 0, 0, 0 }, 0), is(0));
        assertThat(Numbers.bytesToInt(new byte[] { -128, 0, 0, 0 }, 0), is(Integer.MIN_VALUE));
        assertThat(Numbers.bytesToInt(new byte[] { -128, 0, 0, 126 }, 0), is(Integer.MIN_VALUE + 127 - 1));
        assertThat(Numbers.bytesToInt(new byte[] { 127, -1, -1, -1 }, 0), is(Integer.MAX_VALUE));
        assertThat(Numbers.bytesToInt(new byte[] { 127, -1, -1, -127, 0 }, 0), is(Integer.MAX_VALUE - 127 + 1));

        assertThat(Numbers.bytesToInt(new byte[] { 100, 0, 1, -30, 64 }, 1), is(123456));
        assertThat(Numbers.bytesToInt(new byte[] { -100, -1, -2, 29, -64 }, 1), is(-123456));
    }

    public void testShortToBytes() {
        assertThat(Numbers.shortToBytes(1234), is(new byte[] { 4, -46 }));
        assertThat(Numbers.shortToBytes(-1234), is(new byte[] { -5, 46 }));
        assertThat(Numbers.shortToBytes(0), is(new byte[] { 0, 0 }));
        assertThat(Numbers.shortToBytes(Short.MAX_VALUE + 1), is(new byte[] { -128, 0 }));
        assertThat(Numbers.shortToBytes(Short.MAX_VALUE + 127), is(new byte[] { -128, 126 }));
        assertThat(Numbers.shortToBytes(Short.MIN_VALUE - 1), is(new byte[] { 127, -1 }));
        assertThat(Numbers.shortToBytes(Short.MIN_VALUE - 127), is(new byte[] { 127, -127 }));
    }

    public void testBytesToShort() {
        assertThat(Numbers.bytesToShort(new byte[] { 4, -46 }, 0), is((short) 1234));
        assertThat(Numbers.bytesToShort(new byte[] { -5, 46 }, 0), is((short) -1234));
        assertThat(Numbers.bytesToShort(new byte[] { 0, 0 }, 0), is((short) 0));
        assertThat(Numbers.bytesToShort(new byte[] { -128, 0 }, 0), is(Short.MIN_VALUE));
        assertThat(Numbers.bytesToShort(new byte[] { -128, 126 }, 0), is((short) (Short.MIN_VALUE + 127 - 1)));
        assertThat(Numbers.bytesToShort(new byte[] { 127, -1 }, 0), is(Short.MAX_VALUE));
        assertThat(Numbers.bytesToShort(new byte[] { 127, -127, 0 }, 0), is((short) (Short.MAX_VALUE - 127 + 1)));

        assertThat(Numbers.bytesToShort(new byte[] { 100, 0, 1, 4, -46 }, 3), is((short) 1234));
        assertThat(Numbers.bytesToShort(new byte[] { -100, -1, -2, -5, 46 }, 3), is((short) -1234));
    }

    public void testDoubleToBytes() {
        assertThat(Numbers.doubleToBytes(-1234.0d), is(new byte[] { -64, -109, 72, 0, 0, 0, 0, 0 }));
        assertThat(Numbers.doubleToBytes(1234.0d), is(new byte[] { 64, -109, 72, 0, 0, 0, 0, 0 }));
        assertThat(Numbers.doubleToBytes(.0d), is(new byte[] { 0, 0, 0, 0, 0, 0, 0, 0 }));
    }

    public void testIsPositiveNumeric() {
        assertTrue(Numbers.isPositiveNumeric(""));
        assertTrue(Numbers.isPositiveNumeric("0"));
        assertTrue(Numbers.isPositiveNumeric("1"));
        assertFalse(Numbers.isPositiveNumeric("1.0"));
        assertFalse(Numbers.isPositiveNumeric("-1"));
        assertFalse(Numbers.isPositiveNumeric("test"));
        assertTrue(Numbers.isPositiveNumeric("9223372036854775807000000"));
        assertEquals(
            """
                Cannot invoke "String.length()" because "string" is null""",
            expectThrows(NullPointerException.class, () -> Numbers.isPositiveNumeric(null)).getMessage()
        );
    }
}
