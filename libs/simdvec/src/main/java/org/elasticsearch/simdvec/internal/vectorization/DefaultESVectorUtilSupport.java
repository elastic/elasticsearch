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

    @Override
    public float calculateOSQLoss(float[] target, float[] interval, float step, float invStep, float norm2, float lambda) {
        float a = interval[0];
        float b = interval[1];
        float xe = 0f;
        float e = 0f;
        for (float xi : target) {
            // this is quantizing and then dequantizing the vector
            float xiq = fma(step, Math.round((Math.min(Math.max(xi, a), b) - a) * invStep), a);
            // how much does the de-quantized value differ from the original value
            float xiiq = xi - xiq;
            e = fma(xiiq, xiiq, e);
            xe = fma(xi, xiiq, xe);
        }
        return (1f - lambda) * xe * xe / norm2 + lambda * e;
    }

    @Override
    public void calculateOSQGridPoints(float[] target, float[] interval, int points, float invStep, float[] pts) {
        float a = interval[0];
        float b = interval[1];
        float daa = 0;
        float dab = 0;
        float dbb = 0;
        float dax = 0;
        float dbx = 0;
        for (float v : target) {
            float k = Math.round((Math.min(Math.max(v, a), b) - a) * invStep);
            float s = k / (points - 1);
            float ms = 1f - s;
            daa = fma(ms, ms, daa);
            dab = fma(ms, s, dab);
            dbb = fma(s, s, dbb);
            dax = fma(ms, v, dax);
            dbx = fma(s, v, dbx);
        }
        pts[0] = daa;
        pts[1] = dab;
        pts[2] = dbb;
        pts[3] = dax;
        pts[4] = dbx;
    }

    @Override
    public void centerAndCalculateOSQStatsEuclidean(float[] target, float[] centroid, float[] centered, float[] stats) {
        float vecMean = 0;
        float vecVar = 0;
        float norm2 = 0;
        float min = Float.MAX_VALUE;
        float max = -Float.MAX_VALUE;
        for (int i = 0; i < target.length; i++) {
            centered[i] = target[i] - centroid[i];
            min = Math.min(min, centered[i]);
            max = Math.max(max, centered[i]);
            norm2 = fma(centered[i], centered[i], norm2);
            float delta = centered[i] - vecMean;
            vecMean += delta / (i + 1);
            float delta2 = centered[i] - vecMean;
            vecVar = fma(delta, delta2, vecVar);
        }
        stats[0] = vecMean;
        stats[1] = vecVar / target.length;
        stats[2] = norm2;
        stats[3] = min;
        stats[4] = max;
    }

    @Override
    public void centerAndCalculateOSQStatsDp(float[] target, float[] centroid, float[] centered, float[] stats) {
        float vecMean = 0;
        float vecVar = 0;
        float norm2 = 0;
        float centroidDot = 0;
        float min = Float.MAX_VALUE;
        float max = -Float.MAX_VALUE;
        for (int i = 0; i < target.length; i++) {
            centroidDot = fma(target[i], centroid[i], centroidDot);
            centered[i] = target[i] - centroid[i];
            min = Math.min(min, centered[i]);
            max = Math.max(max, centered[i]);
            norm2 = fma(centered[i], centered[i], norm2);
            float delta = centered[i] - vecMean;
            vecMean += delta / (i + 1);
            float delta2 = centered[i] - vecMean;
            vecVar = fma(delta, delta2, vecVar);
        }
        stats[0] = vecMean;
        stats[1] = vecVar / target.length;
        stats[2] = norm2;
        stats[3] = min;
        stats[4] = max;
        stats[5] = centroidDot;
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
