/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.script.field.vectors;

import org.apache.lucene.util.BitUtil;
import org.apache.lucene.util.Constants;

/**
 * This class consists of a single utility method that provides XOR bit count computed over signed bytes.
 * Remove this class when Lucene version > 9.11 is released, and replace with Lucene's VectorUtil directly.
 */
public class ESVectorUtil {

    /**
     * For xorBitCount we stride over the values as either 64-bits (long) or 32-bits (int) at a time.
     * On ARM Long::bitCount is not vectorized, and therefore produces less than optimal code, when
     * compared to Integer::bitCount. While Long::bitCount is optimal on x64.
     */
    static final boolean XOR_BIT_COUNT_STRIDE_AS_INT = Constants.OS_ARCH.equals("aarch64");

    /**
     * XOR bit count computed over signed bytes.
     *
     * @param a bytes containing a vector
     * @param b bytes containing another vector, of the same dimension
     * @return the value of the XOR bit count of the two vectors
     */
    public static int xorBitCount(byte[] a, byte[] b) {
        if (a.length != b.length) {
            throw new IllegalArgumentException("vector dimensions differ: " + a.length + "!=" + b.length);
        }
        if (XOR_BIT_COUNT_STRIDE_AS_INT) {
            return xorBitCountInt(a, b);
        } else {
            return xorBitCountLong(a, b);
        }
    }

    /** XOR bit count striding over 4 bytes at a time. */
    static int xorBitCountInt(byte[] a, byte[] b) {
        int distance = 0, i = 0;
        for (final int upperBound = a.length & -Integer.BYTES; i < upperBound; i += Integer.BYTES) {
            distance += Integer.bitCount((int) BitUtil.VH_NATIVE_INT.get(a, i) ^ (int) BitUtil.VH_NATIVE_INT.get(b, i));
        }
        // tail:
        for (; i < a.length; i++) {
            distance += Integer.bitCount((a[i] ^ b[i]) & 0xFF);
        }
        return distance;
    }

    /** XOR bit count striding over 8 bytes at a time. */
    static int xorBitCountLong(byte[] a, byte[] b) {
        int distance = 0, i = 0;
        for (final int upperBound = a.length & -Long.BYTES; i < upperBound; i += Long.BYTES) {
            distance += Long.bitCount((long) BitUtil.VH_NATIVE_LONG.get(a, i) ^ (long) BitUtil.VH_NATIVE_LONG.get(b, i));
        }
        // tail:
        for (; i < a.length; i++) {
            distance += Integer.bitCount((a[i] ^ b[i]) & 0xFF);
        }
        return distance;
    }

    private ESVectorUtil() {}
}
