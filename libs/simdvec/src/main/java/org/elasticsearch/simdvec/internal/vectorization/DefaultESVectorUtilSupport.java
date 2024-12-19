/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.simdvec.internal.vectorization;

import org.apache.lucene.util.BitUtil;
import org.apache.lucene.util.Constants;

final class DefaultESVectorUtilSupport implements ESVectorUtilSupport {

    private static float fma(float a, float b, float c) {
        if (Constants.HAS_FAST_SCALAR_FMA) {
            return Math.fma(a, b, c);
        } else {
            return a * b + c;
        }
    }

    DefaultESVectorUtilSupport() {}

    @Override
    public long ipByteBinByte(byte[] q, byte[] d) {
        return ipByteBinByteImpl(q, d);
    }

    @Override
    public int ipByteBit(byte[] q, byte[] d) {
        return ipByteBitImpl(q, d);
    }

    @Override
    public float ipFloatBit(float[] q, byte[] d) {
        return ipFloatBitImpl(q, d);
    }

    public static int ipByteBitImpl(byte[] q, byte[] d) {
        assert q.length == d.length * Byte.SIZE;
        int acc0 = 0;
        int acc1 = 0;
        int acc2 = 0;
        int acc3 = 0;
        // now combine the two vectors, summing the byte dimensions where the bit in d is `1`
        for (int i = 0; i < d.length; i++) {
            byte mask = d[i];
            // Make sure its just 1 or 0

            acc0 += q[i * Byte.SIZE + 0] * ((mask >> 7) & 1);
            acc1 += q[i * Byte.SIZE + 1] * ((mask >> 6) & 1);
            acc2 += q[i * Byte.SIZE + 2] * ((mask >> 5) & 1);
            acc3 += q[i * Byte.SIZE + 3] * ((mask >> 4) & 1);

            acc0 += q[i * Byte.SIZE + 4] * ((mask >> 3) & 1);
            acc1 += q[i * Byte.SIZE + 5] * ((mask >> 2) & 1);
            acc2 += q[i * Byte.SIZE + 6] * ((mask >> 1) & 1);
            acc3 += q[i * Byte.SIZE + 7] * ((mask >> 0) & 1);
        }
        return acc0 + acc1 + acc2 + acc3;
    }

    public static float ipFloatBitImpl(float[] q, byte[] d) {
        assert q.length == d.length * Byte.SIZE;
        float acc0 = 0;
        float acc1 = 0;
        float acc2 = 0;
        float acc3 = 0;
        // now combine the two vectors, summing the byte dimensions where the bit in d is `1`
        for (int i = 0; i < d.length; i++) {
            byte mask = d[i];
            acc0 = fma(q[i * Byte.SIZE + 0], (mask >> 7) & 1, acc0);
            acc1 = fma(q[i * Byte.SIZE + 1], (mask >> 6) & 1, acc1);
            acc2 = fma(q[i * Byte.SIZE + 2], (mask >> 5) & 1, acc2);
            acc3 = fma(q[i * Byte.SIZE + 3], (mask >> 4) & 1, acc3);

            acc0 = fma(q[i * Byte.SIZE + 4], (mask >> 3) & 1, acc0);
            acc1 = fma(q[i * Byte.SIZE + 5], (mask >> 2) & 1, acc1);
            acc2 = fma(q[i * Byte.SIZE + 6], (mask >> 1) & 1, acc2);
            acc3 = fma(q[i * Byte.SIZE + 7], (mask >> 0) & 1, acc3);
        }
        return acc0 + acc1 + acc2 + acc3;
    }

    public static long ipByteBinByteImpl(byte[] q, byte[] d) {
        long ret = 0;
        int size = d.length;
        for (int i = 0; i < B_QUERY; i++) {
            int r = 0;
            long subRet = 0;
            for (final int upperBound = d.length & -Integer.BYTES; r < upperBound; r += Integer.BYTES) {
                subRet += Integer.bitCount((int) BitUtil.VH_NATIVE_INT.get(q, i * size + r) & (int) BitUtil.VH_NATIVE_INT.get(d, r));
            }
            for (; r < d.length; r++) {
                subRet += Integer.bitCount((q[i * size + r] & d[r]) & 0xFF);
            }
            ret += subRet << i;
        }
        return ret;
    }
}
