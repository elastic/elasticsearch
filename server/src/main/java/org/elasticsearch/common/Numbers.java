/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common;

import org.apache.lucene.util.BytesRef;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteOrder;

/**
 * A set of utilities for numbers.
 */
public final class Numbers {

    private static final VarHandle BIG_ENDIAN_SHORT = MethodHandles.byteArrayViewVarHandle(short[].class, ByteOrder.BIG_ENDIAN);

    private static final VarHandle BIG_ENDIAN_INT = MethodHandles.byteArrayViewVarHandle(int[].class, ByteOrder.BIG_ENDIAN);

    private static final VarHandle BIG_ENDIAN_LONG = MethodHandles.byteArrayViewVarHandle(long[].class, ByteOrder.BIG_ENDIAN);

    private static final BigInteger MAX_LONG_VALUE = BigInteger.valueOf(Long.MAX_VALUE);
    private static final BigInteger MIN_LONG_VALUE = BigInteger.valueOf(Long.MIN_VALUE);

    private Numbers() {}

    public static short bytesToShort(byte[] bytes, int offset) {
        return (short) BIG_ENDIAN_SHORT.get(bytes, offset);
    }

    public static int bytesToInt(byte[] bytes, int offset) {
        return (int) BIG_ENDIAN_INT.get(bytes, offset);
    }

    public static long bytesToLong(byte[] bytes, int offset) {
        return (long) BIG_ENDIAN_LONG.get(bytes, offset);
    }

    public static long bytesToLong(BytesRef bytes) {
        return bytesToLong(bytes.bytes, bytes.offset);
    }

    public static byte[] intToBytes(int val) {
        byte[] arr = new byte[4];
        BIG_ENDIAN_INT.set(arr, 0, val);
        return arr;
    }

    /**
     * Converts an int to a byte array.
     *
     * @param val The int to convert to a byte array
     * @return The byte array converted
     */
    public static byte[] shortToBytes(int val) {
        byte[] arr = new byte[2];
        BIG_ENDIAN_SHORT.set(arr, 0, (short) val);
        return arr;
    }

    /**
     * Converts a long to a byte array.
     *
     * @param val The long to convert to a byte array
     * @return The byte array converted
     */
    public static byte[] longToBytes(long val) {
        byte[] arr = new byte[8];
        BIG_ENDIAN_LONG.set(arr, 0, val);
        return arr;
    }

    /**
     * Converts a double to a byte array.
     *
     * @param val The double to convert to a byte array
     * @return The byte array converted
     */
    public static byte[] doubleToBytes(double val) {
        return longToBytes(Double.doubleToRawLongBits(val));
    }

    /** Returns true if value is neither NaN nor infinite. */
    public static boolean isValidDouble(double value) {
        if (Double.isNaN(value) || Double.isInfinite(value)) {
            return false;
        }
        return true;
    }

    /** Return the long that {@code n} stores, or throws an exception if the
     *  stored value cannot be converted to a long that stores the exact same
     *  value. */
    public static long toLongExact(Number n) {
        if (n instanceof Byte || n instanceof Short || n instanceof Integer || n instanceof Long) {
            return n.longValue();
        } else if (n instanceof Float || n instanceof Double) {
            double d = n.doubleValue();
            if (d != Math.round(d)) {
                throw new IllegalArgumentException(n + " is not an integer value");
            }
            return n.longValue();
        } else if (n instanceof BigDecimal) {
            return ((BigDecimal) n).toBigIntegerExact().longValueExact();
        } else if (n instanceof BigInteger) {
            return ((BigInteger) n).longValueExact();
        } else {
            throw new IllegalArgumentException(
                "Cannot check whether [" + n + "] of class [" + n.getClass().getName() + "] is actually a long"
            );
        }
    }

    // weak bounds on the BigDecimal representation to allow for coercion
    private static BigDecimal BIGDECIMAL_GREATER_THAN_LONG_MAX_VALUE = BigDecimal.valueOf(Long.MAX_VALUE).add(BigDecimal.ONE);
    private static BigDecimal BIGDECIMAL_LESS_THAN_LONG_MIN_VALUE = BigDecimal.valueOf(Long.MIN_VALUE).subtract(BigDecimal.ONE);

    /** Return the long that {@code stringValue} stores or throws an exception if the
     *  stored value cannot be converted to a long that stores the exact same
     *  value and {@code coerce} is false. */
    public static long toLong(String stringValue, boolean coerce) {
        try {
            return Long.parseLong(stringValue);
        } catch (NumberFormatException e) {
            // we will try again with BigDecimal
        }

        final BigInteger bigIntegerValue;
        try {
            BigDecimal bigDecimalValue = new BigDecimal(stringValue);
            if (bigDecimalValue.compareTo(BIGDECIMAL_GREATER_THAN_LONG_MAX_VALUE) >= 0
                || bigDecimalValue.compareTo(BIGDECIMAL_LESS_THAN_LONG_MIN_VALUE) <= 0) {
                throw new IllegalArgumentException("Value [" + stringValue + "] is out of range for a long");
            }
            bigIntegerValue = coerce ? bigDecimalValue.toBigInteger() : bigDecimalValue.toBigIntegerExact();
        } catch (ArithmeticException e) {
            throw new IllegalArgumentException("Value [" + stringValue + "] has a decimal part");
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("For input string: \"" + stringValue + "\"");
        }

        if (bigIntegerValue.compareTo(MAX_LONG_VALUE) > 0 || bigIntegerValue.compareTo(MIN_LONG_VALUE) < 0) {
            throw new IllegalArgumentException("Value [" + stringValue + "] is out of range for a long");
        }

        return bigIntegerValue.longValue();
    }

    /** Return the int that {@code n} stores, or throws an exception if the
     *  stored value cannot be converted to an int that stores the exact same
     *  value. */
    public static int toIntExact(Number n) {
        return Math.toIntExact(toLongExact(n));
    }

    /** Return the short that {@code n} stores, or throws an exception if the
     *  stored value cannot be converted to a short that stores the exact same
     *  value. */
    public static short toShortExact(Number n) {
        long l = toLongExact(n);
        if (l != (short) l) {
            throw new ArithmeticException("short overflow: " + l);
        }
        return (short) l;
    }

    /** Return the byte that {@code n} stores, or throws an exception if the
     *  stored value cannot be converted to a byte that stores the exact same
     *  value. */
    public static byte toByteExact(Number n) {
        long l = toLongExact(n);
        if (l != (byte) l) {
            throw new ArithmeticException("byte overflow: " + l);
        }
        return (byte) l;
    }

    /**
     * Checks if the given string can be parsed as a positive integer value.
     */
    public static boolean isPositiveNumeric(String string) {
        for (int i = 0; i < string.length(); ++i) {
            final char c = string.charAt(i);
            if (c < '0' || c > '9') {
                return false;
            }
        }
        return true;
    }
}
