/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.util;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteOrder;

/** Utility methods to do byte-level encoding. These methods are biased towards little-endian byte order because it is the most
 *  common byte order and reading several bytes at once may be optimizable in the future with the help of sun.mist.Unsafe. */
public enum ByteUtils {
    ;

    public static final VarHandle LITTLE_ENDIAN_CHAR = MethodHandles.byteArrayViewVarHandle(char[].class, ByteOrder.LITTLE_ENDIAN);

    public static final VarHandle LITTLE_ENDIAN_INT = MethodHandles.byteArrayViewVarHandle(int[].class, ByteOrder.LITTLE_ENDIAN);

    public static final VarHandle LITTLE_ENDIAN_LONG = MethodHandles.byteArrayViewVarHandle(long[].class, ByteOrder.LITTLE_ENDIAN);

    public static final VarHandle BIG_ENDIAN_LONG = MethodHandles.byteArrayViewVarHandle(long[].class, ByteOrder.BIG_ENDIAN);

    private static final VarHandle BIG_ENDIAN_SHORT = MethodHandles.byteArrayViewVarHandle(short[].class, ByteOrder.BIG_ENDIAN);

    private static final VarHandle BIG_ENDIAN_INT = MethodHandles.byteArrayViewVarHandle(int[].class, ByteOrder.BIG_ENDIAN);

    /** Zig-zag decode. */
    public static long zigZagDecode(long n) {
        return ((n >>> 1) ^ -(n & 1));
    }

    /** Zig-zag encode: this helps transforming small signed numbers into small positive numbers. */
    public static long zigZagEncode(long n) {
        return (n >> 63) ^ (n << 1);
    }

    /**
     * Converts a long to a byte array in little endian format.
     *
     * @param val The long to convert to a byte array
     * @param arr The byte array to write the long value in little endian layout
     * @param offset The offset where in the array to write to
     */
    public static void writeLongLE(long val, byte[] arr, int offset) {
        LITTLE_ENDIAN_LONG.set(arr, offset, val);
    }

    /**
     * Converts a byte array written in little endian format to a long.
     *
     * @param arr The byte array to read from in little endian layout
     * @param offset The offset where in the array to read from
     */
    public static long readLongLE(byte[] arr, int offset) {
        return (long) LITTLE_ENDIAN_LONG.get(arr, offset);
    }

    /**
     * Converts a long to a byte array in big endian format.
     *
     * @param val The long to convert to a byte array
     * @param arr The byte array to write the long value in big endian layout
     * @param offset The offset where in the array to write to
     */
    public static void writeLongBE(long val, byte[] arr, int offset) {
        BIG_ENDIAN_LONG.set(arr, offset, val);
    }

    /**
     * Converts a byte array written in big endian format to a long.
     *
     * @param arr The byte array to read from in big endian layout
     * @param offset The offset where in the array to read from
     */
    public static long readLongBE(byte[] arr, int offset) {
        return (long) BIG_ENDIAN_LONG.get(arr, offset);
    }

    /**
     * Converts an int to a byte array in little endian format.
     *
     * @param val The int to convert to a byte array
     * @param arr The byte array to write the int value in little endian layout
     * @param offset The offset where in the array to write to
     */
    public static void writeIntLE(int val, byte[] arr, int offset) {
        LITTLE_ENDIAN_INT.set(arr, offset, val);
    }

    /**
     * Converts a byte array written in little endian format to an int.
     *
     * @param arr The byte array to read from in little endian layout
     * @param offset The offset where in the array to read from
     */
    public static int readIntLE(byte[] arr, int offset) {
        return (int) LITTLE_ENDIAN_INT.get(arr, offset);
    }

    /**
     * Converts a double to a byte array in little endian format.
     *
     * @param val The double to convert to a byte array
     * @param arr The byte array to write the double value in little endian layout
     * @param offset The offset where in the array to write to
     */
    public static void writeDoubleLE(double val, byte[] arr, int offset) {
        writeLongLE(Double.doubleToRawLongBits(val), arr, offset);
    }

    /**
     * Converts a byte array written in little endian format to a double.
     *
     * @param arr The byte array to read from in little endian layout
     * @param offset The offset where in the array to read from
     */
    public static double readDoubleLE(byte[] arr, int offset) {
        return Double.longBitsToDouble(readLongLE(arr, offset));
    }

    /**
     * Converts a float to a byte array in little endian format.
     *
     * @param val The float to convert to a byte array
     * @param arr The byte array to write the float value in little endian layout
     * @param offset The offset where in the array to write to
     */
    public static void writeFloatLE(float val, byte[] arr, int offset) {
        writeIntLE(Float.floatToRawIntBits(val), arr, offset);
    }

    /**
     * Converts a byte array written in little endian format to a float.
     *
     * @param arr The byte array to read from in little endian layout
     * @param offset The offset where in the array to read from
     */
    public static float readFloatLE(byte[] arr, int offset) {
        return Float.intBitsToFloat(readIntLE(arr, offset));
    }

    /**
     * Converts an int to a byte array in big endian format.
     *
     * @param val The int to convert to a byte array
     * @param arr The byte array to write the int value in big endian layout
     * @param offset The offset where in the array to write to
     */
    public static void writeIntBE(int val, byte[] arr, int offset) {
        BIG_ENDIAN_INT.set(arr, offset, val);
    }

    /**
     * Converts a byte array written in big endian format to an int.
     *
     * @param arr The byte array to read from in big endian layout
     * @param offset The offset where in the array to read from
     */
    public static int readIntBE(byte[] arr, int offset) {
        return (int) BIG_ENDIAN_INT.get(arr, offset);
    }

    /**
     * Converts a short to a byte array in big endian format.
     *
     * @param val The short to convert to a byte array
     * @param arr The byte array to write the short value in big endian layout
     * @param offset The offset where in the array to write to
     */
    public static void writeShortBE(short val, byte[] arr, int offset) {
        BIG_ENDIAN_SHORT.set(arr, offset, val);
    }

    /**
     * Converts a byte array written in big endian format to a short.
     *
     * @param arr The byte array to read from in big endian layout
     * @param offset The offset where in the array to read from
     */
    public static short readShortBE(byte[] arr, int offset) {
        return (short) BIG_ENDIAN_SHORT.get(arr, offset);
    }
}
