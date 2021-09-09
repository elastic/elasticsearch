/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common;

import org.apache.lucene.util.BytesRef;

import java.math.BigDecimal;
import java.math.BigInteger;

/**
 * A set of utilities for numbers.
 */
public final class Numbers {

    private static final BigInteger MAX_LONG_VALUE = BigInteger.valueOf(Long.MAX_VALUE);
    private static final BigInteger MIN_LONG_VALUE = BigInteger.valueOf(Long.MIN_VALUE);

    private Numbers() {
    }

    public static short bytesToShort(byte[] bytes, int offset) {
        return (short) (((bytes[offset] & 0xFF) << 8) | (bytes[offset + 1] & 0xFF));
    }

    public static int bytesToInt(byte[] bytes, int offset) {
        return ((bytes[offset] & 0xFF) << 24) | ((bytes[offset + 1] & 0xFF) << 16) | ((bytes[offset + 2] & 0xFF) << 8)
                | (bytes[offset + 3] & 0xFF);
    }

    public static long bytesToLong(byte[] bytes, int offset) {
        return (((long) (((bytes[offset] & 0xFF) << 24) | ((bytes[offset + 1] & 0xFF) << 16) | ((bytes[offset + 2] & 0xFF) << 8)
                | (bytes[offset + 3] & 0xFF))) << 32)
                | ((((bytes[offset + 4] & 0xFF) << 24) | ((bytes[offset + 5] & 0xFF) << 16) | ((bytes[offset + 6] & 0xFF) << 8)
                | (bytes[offset + 7] & 0xFF)) & 0xFFFFFFFFL);
    }

    public static long bytesToLong(BytesRef bytes) {
        return bytesToLong(bytes.bytes, bytes.offset);
    }

    public static byte[] intToBytes(int val) {
        byte[] arr = new byte[4];
        arr[0] = (byte) (val >>> 24);
        arr[1] = (byte) (val >>> 16);
        arr[2] = (byte) (val >>> 8);
        arr[3] = (byte) (val);
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
        arr[0] = (byte) (val >>> 8);
        arr[1] = (byte) (val);
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
        arr[0] = (byte) (val >>> 56);
        arr[1] = (byte) (val >>> 48);
        arr[2] = (byte) (val >>> 40);
        arr[3] = (byte) (val >>> 32);
        arr[4] = (byte) (val >>> 24);
        arr[5] = (byte) (val >>> 16);
        arr[6] = (byte) (val >>> 8);
        arr[7] = (byte) (val);
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
        if (n instanceof Byte || n instanceof Short || n instanceof Integer
                || n instanceof Long) {
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
            throw new IllegalArgumentException("Cannot check whether [" + n + "] of class [" + n.getClass().getName()
                    + "] is actually a long");
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
            if (bigDecimalValue.compareTo(BIGDECIMAL_GREATER_THAN_LONG_MAX_VALUE) >= 0 ||
                bigDecimalValue.compareTo(BIGDECIMAL_LESS_THAN_LONG_MIN_VALUE) <= 0) {
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
}
