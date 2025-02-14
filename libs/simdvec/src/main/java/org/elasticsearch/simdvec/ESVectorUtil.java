/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.simdvec;

import org.apache.lucene.util.BitUtil;
import org.apache.lucene.util.Constants;
import org.elasticsearch.simdvec.internal.vectorization.ESVectorUtilSupport;
import org.elasticsearch.simdvec.internal.vectorization.ESVectorizationProvider;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;

import static org.elasticsearch.simdvec.internal.vectorization.ESVectorUtilSupport.B_QUERY;

public class ESVectorUtil {

    private static final MethodHandle BIT_COUNT_MH;
    static {
        try {
            // For xorBitCount we stride over the values as either 64-bits (long) or 32-bits (int) at a time.
            // On ARM Long::bitCount is not vectorized, and therefore produces less than optimal code, when
            // compared to Integer::bitCount. While Long::bitCount is optimal on x64. See
            // https://bugs.openjdk.org/browse/JDK-8336000
            BIT_COUNT_MH = Constants.OS_ARCH.equals("aarch64")
                ? MethodHandles.lookup()
                    .findStatic(ESVectorUtil.class, "andBitCountInt", MethodType.methodType(int.class, byte[].class, byte[].class))
                : MethodHandles.lookup()
                    .findStatic(ESVectorUtil.class, "andBitCountLong", MethodType.methodType(int.class, byte[].class, byte[].class));
        } catch (NoSuchMethodException | IllegalAccessException e) {
            throw new AssertionError(e);
        }
    }

    private static final ESVectorUtilSupport IMPL = ESVectorizationProvider.getInstance().getVectorUtilSupport();

    public static long ipByteBinByte(byte[] q, byte[] d) {
        if (q.length != d.length * B_QUERY) {
            throw new IllegalArgumentException("vector dimensions incompatible: " + q.length + "!= " + B_QUERY + " x " + d.length);
        }
        return IMPL.ipByteBinByte(q, d);
    }

    /**
     * Compute the inner product of two vectors, where the query vector is a byte vector and the document vector is a bit vector.
     * This will return the sum of the query vector values using the document vector as a mask.
     * When comparing the bits with the bytes, they are done in "big endian" order. For example, if the byte vector
     * is [1, 2, 3, 4, 5, 6, 7, 8] and the bit vector is [0b10000000], the inner product will be 1.0.
     * @param q the query vector
     * @param d the document vector
     * @return the inner product of the two vectors
     */
    public static int ipByteBit(byte[] q, byte[] d) {
        if (q.length != d.length * Byte.SIZE) {
            throw new IllegalArgumentException("vector dimensions incompatible: " + q.length + "!= " + Byte.SIZE + " x " + d.length);
        }
        return IMPL.ipByteBit(q, d);
    }

    /**
     * Compute the inner product of two vectors, where the query vector is a float vector and the document vector is a bit vector.
     * This will return the sum of the query vector values using the document vector as a mask.
     * When comparing the bits with the floats, they are done in "big endian" order. For example, if the float vector
     * is [1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0] and the bit vector is [0b10000000], the inner product will be 1.0.
     * @param q the query vector
     * @param d the document vector
     * @return the inner product of the two vectors
     */
    public static float ipFloatBit(float[] q, byte[] d) {
        if (q.length != d.length * Byte.SIZE) {
            throw new IllegalArgumentException("vector dimensions incompatible: " + q.length + "!= " + Byte.SIZE + " x " + d.length);
        }
        return IMPL.ipFloatBit(q, d);
    }

    /**
     * AND bit count computed over signed bytes.
     * Copied from Lucene's XOR implementation
     * @param a bytes containing a vector
     * @param b bytes containing another vector, of the same dimension
     * @return the value of the AND bit count of the two vectors
     */
    public static int andBitCount(byte[] a, byte[] b) {
        if (a.length != b.length) {
            throw new IllegalArgumentException("vector dimensions differ: " + a.length + "!=" + b.length);
        }
        try {
            return (int) BIT_COUNT_MH.invokeExact(a, b);
        } catch (Throwable e) {
            if (e instanceof Error err) {
                throw err;
            } else if (e instanceof RuntimeException re) {
                throw re;
            } else {
                throw new RuntimeException(e);
            }
        }
    }

    /** AND bit count striding over 4 bytes at a time. */
    static int andBitCountInt(byte[] a, byte[] b) {
        int distance = 0, i = 0;
        // limit to number of int values in the array iterating by int byte views
        for (final int upperBound = a.length & -Integer.BYTES; i < upperBound; i += Integer.BYTES) {
            distance += Integer.bitCount((int) BitUtil.VH_NATIVE_INT.get(a, i) & (int) BitUtil.VH_NATIVE_INT.get(b, i));
        }
        // tail:
        for (; i < a.length; i++) {
            distance += Integer.bitCount((a[i] & b[i]) & 0xFF);
        }
        return distance;
    }

    /** AND bit count striding over 8 bytes at a time**/
    static int andBitCountLong(byte[] a, byte[] b) {
        int distance = 0, i = 0;
        // limit to number of long values in the array iterating by long byte views
        for (final int upperBound = a.length & -Long.BYTES; i < upperBound; i += Long.BYTES) {
            distance += Long.bitCount((long) BitUtil.VH_NATIVE_LONG.get(a, i) & (long) BitUtil.VH_NATIVE_LONG.get(b, i));
        }
        // tail:
        for (; i < a.length; i++) {
            distance += Integer.bitCount((a[i] & b[i]) & 0xFF);
        }
        return distance;
    }
}
