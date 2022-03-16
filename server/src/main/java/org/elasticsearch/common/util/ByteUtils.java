/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.util;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteOrder;

/** Utility methods to do byte-level encoding. These methods are biased towards little-endian byte order because it is the most
 *  common byte order and reading several bytes at once may be optimizable in the future with the help of sun.mist.Unsafe. */
public enum ByteUtils {
    ;

    public static final VarHandle LITTLE_ENDIAN_INT = MethodHandles.byteArrayViewVarHandle(int[].class, ByteOrder.LITTLE_ENDIAN);

    public static final VarHandle LITTLE_ENDIAN_LONG = MethodHandles.byteArrayViewVarHandle(long[].class, ByteOrder.LITTLE_ENDIAN);

    public static final VarHandle BIG_ENDIAN_LONG = MethodHandles.byteArrayViewVarHandle(long[].class, ByteOrder.BIG_ENDIAN);

    /** Zig-zag decode. */
    public static long zigZagDecode(long n) {
        return ((n >>> 1) ^ -(n & 1));
    }

    /** Zig-zag encode: this helps transforming small signed numbers into small positive numbers. */
    public static long zigZagEncode(long n) {
        return (n >> 63) ^ (n << 1);
    }

    /** Write a long in little-endian format. */
    public static void writeLongLE(long l, byte[] arr, int offset) {
        LITTLE_ENDIAN_LONG.set(arr, offset, l);
    }

    /** Write a long in little-endian format. */
    public static long readLongLE(byte[] arr, int offset) {
        return (long) LITTLE_ENDIAN_LONG.get(arr, offset);
    }

    /** Write a long in big-endian format. */
    public static void writeLongBE(long l, byte[] arr, int offset) {
        BIG_ENDIAN_LONG.set(arr, offset, l);
    }

    /** Write a long in big-endian format. */
    public static long readLongBE(byte[] arr, int offset) {
        return (long) BIG_ENDIAN_LONG.get(arr, offset);
    }

    /** Write an int in little-endian format. */
    public static void writeIntLE(int l, byte[] arr, int offset) {
        LITTLE_ENDIAN_INT.set(arr, offset, l);
    }

    /** Read an int in little-endian format. */
    public static int readIntLE(byte[] arr, int offset) {
        return (int) LITTLE_ENDIAN_INT.get(arr, offset);
    }

    /** Write a double in little-endian format. */
    public static void writeDoubleLE(double d, byte[] arr, int offset) {
        writeLongLE(Double.doubleToRawLongBits(d), arr, offset);
    }

    /** Read a double in little-endian format. */
    public static double readDoubleLE(byte[] arr, int offset) {
        return Double.longBitsToDouble(readLongLE(arr, offset));
    }

    /** Write a float in little-endian format. */
    public static void writeFloatLE(float d, byte[] arr, int offset) {
        writeIntLE(Float.floatToRawIntBits(d), arr, offset);
    }

    /** Read a float in little-endian format. */
    public static float readFloatLE(byte[] arr, int offset) {
        return Float.intBitsToFloat(readIntLE(arr, offset));
    }

}
