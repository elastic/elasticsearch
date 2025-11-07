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
import org.apache.lucene.util.VectorUtil;

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
    public float ipFloatByte(float[] q, byte[] d) {
        return ipFloatByteImpl(q, d);
    }

    @Override
    public float calculateOSQLoss(
        float[] target,
        float low,
        float high,
        float step,
        float invStep,
        float norm2,
        float lambda,
        int[] quantize
    ) {
        float a = low;
        float b = high;
        float xe = 0f;
        float e = 0f;
        for (int i = 0; i < target.length; ++i) {
            float xi = target[i];
            // this is quantizing and then dequantizing the vector
            quantize[i] = Math.round((Math.min(Math.max(xi, a), b) - a) * invStep);
            float xiq = fma(step, quantize[i], a);
            // how much does the de-quantized value differ from the original value
            float xiiq = xi - xiq;
            e = fma(xiiq, xiiq, e);
            xe = fma(xi, xiiq, xe);
        }
        return (1f - lambda) * xe * xe / norm2 + lambda * e;
    }

    @Override
    public void calculateOSQGridPoints(float[] target, int[] quantize, int points, float[] pts) {
        float daa = 0;
        float dab = 0;
        float dbb = 0;
        float dax = 0;
        float dbx = 0;
        float invPmOnes = 1f / (points - 1f);
        for (int i = 0; i < target.length; ++i) {
            float v = target[i];
            float k = quantize[i];
            float s = k * invPmOnes;
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

    @Override
    public float soarDistance(float[] v1, float[] centroid, float[] originalResidual, float soarLambda, float rnorm) {
        assert v1.length == centroid.length;
        assert v1.length == originalResidual.length;
        float dsq = VectorUtil.squareDistance(v1, centroid);
        float proj = 0;
        for (int i = 0; i < v1.length; i++) {
            float djk = v1[i] - centroid[i];
            proj = fma(djk, originalResidual[i], proj);
        }
        return dsq + soarLambda * proj * proj / rnorm;
    }

    public static int ipByteBitImpl(byte[] q, byte[] d) {
        return ipByteBitImpl(q, d, 0);
    }

    public static int ipByteBitImpl(byte[] q, byte[] d, int start) {
        assert q.length == d.length * Byte.SIZE;
        int acc0 = 0;
        int acc1 = 0;
        int acc2 = 0;
        int acc3 = 0;
        // now combine the two vectors, summing the byte dimensions where the bit in d is `1`
        for (int i = start; i < d.length; i++) {
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
        return ipFloatBitImpl(q, d, 0);
    }

    static float ipFloatBitImpl(float[] q, byte[] d, int start) {
        assert q.length == d.length * Byte.SIZE;
        float acc0 = 0;
        float acc1 = 0;
        float acc2 = 0;
        float acc3 = 0;
        // now combine the two vectors, summing the byte dimensions where the bit in d is `1`
        for (int i = start; i < d.length; i++) {
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

    /**
     * Returns the inner product (aka dot product) between the query vector {@code q}, and the data vector {@code d}.
     * <p>
     * The query vector should be {@link #B_QUERY}-bit quantized and striped, so that the first {@code n} bits
     * of the array are the initial bits of each of the {@code n} vector dimensions; the next {@code n}
     * bits are the second bits of each of the {@code n} vector dimensions, and so on
     * (this algorithm is only valid for vectors with dimensions a multiple of 8).
     * The striping is usually done by {@code ESVectorUtil.transposeHalfByte}.
     * <p>
     * The data vector should be single-bit quantized.
     *
     * <h4>Dot products with bit quantization</h4>
     *
     * The dot product of any vector with a bit vector is a simple selector - each query vector dimension is multiplied
     * by the 0 or 1 in the corresponding data vector dimension; the result is that each dimension value
     * is either kept or ignored, with the dimensions that are kept then summed together.
     *
     * <h4>The algorithm</h4>
     *
     * The transposition already applied to the query vector ensures there's a 1-to-1 correspondence
     * between the data vector bits and query vector bits (see {@code ESVectorUtil.transposeHalfByte)};
     * this means we can use a bitwise {@code &} to keep only the bits of the vector elements we want to sum.
     * Essentially, the data vector is used as a selector for each of the striped bits of each vector dimension
     * as stored, concatenated together, in {@code q}.
     * <p>
     * The final dot product result can be obtained by observing that the sum of each stripe of {@code n} bits
     * can be computed using the bit count of that stripe. Similar to
     * <a href="https://en.wikipedia.org/wiki/Multiplication_algorithm#Long_multiplication">long multiplication</a>,
     * the result of each stripe of {@code n} bits can be added together by shifting the value {@code s} bits to the left,
     * where {@code s} is the stripe number (0-3), then adding to the overall result. Any carry is handled by the add operation.
     *
     * @param q query vector, {@link #B_QUERY}-bit quantized and striped (see {@code ESVectorUtil.transposeHalfByte})
     * @param d data vector, 1-bit quantized
     * @return  inner product result
     */
    public static long ipByteBinByteImpl(byte[] q, byte[] d) {
        long ret = 0;
        int size = d.length;
        for (int s = 0; s < B_QUERY; s++) { // for each stripe of B_QUERY-bit quantization in q...
            int r = 0;
            long stripeRet = 0;
            // bitwise & the query and data vectors together, 32-bits at a time, and counting up the bits still set
            for (final int upperBound = d.length & -Integer.BYTES; r < upperBound; r += Integer.BYTES) {
                stripeRet += Integer.bitCount((int) BitUtil.VH_NATIVE_INT.get(q, s * size + r) & (int) BitUtil.VH_NATIVE_INT.get(d, r));
            }
            // handle any tail
            // Java operations on bytes automatically extend to int, so we need to mask back down again in case it sign-extends the int
            for (; r < d.length; r++) {
                stripeRet += Integer.bitCount((q[s * size + r] & d[r]) & 0xFF);
            }
            // shift the result of the s'th stripe s to the left and add to the result
            ret += stripeRet << s;
        }
        return ret;
    }

    public static float ipFloatByteImpl(float[] q, byte[] d) {
        float ret = 0;
        for (int i = 0; i < q.length; i++) {
            ret += q[i] * d[i];
        }
        return ret;
    }

    @Override
    public int quantizeVectorWithIntervals(float[] vector, int[] destination, float lowInterval, float upperInterval, byte bits) {
        float nSteps = ((1 << bits) - 1);
        float invStep = nSteps / (upperInterval - lowInterval);
        int sumQuery = 0;
        for (int h = 0; h < vector.length; h++) {
            float xi = Math.min(Math.max(vector[h], lowInterval), upperInterval);
            int assignment = Math.round((xi - lowInterval) * invStep);
            sumQuery += assignment;
            destination[h] = assignment;
        }
        return sumQuery;
    }

    @Override
    public void squareDistanceBulk(float[] query, float[] v0, float[] v1, float[] v2, float[] v3, float[] distances) {
        distances[0] = VectorUtil.squareDistance(query, v0);
        distances[1] = VectorUtil.squareDistance(query, v1);
        distances[2] = VectorUtil.squareDistance(query, v2);
        distances[3] = VectorUtil.squareDistance(query, v3);
    }

    @Override
    public void soarDistanceBulk(
        float[] v1,
        float[] c0,
        float[] c1,
        float[] c2,
        float[] c3,
        float[] originalResidual,
        float soarLambda,
        float rnorm,
        float[] distances
    ) {
        distances[0] = soarDistance(v1, c0, originalResidual, soarLambda, rnorm);
        distances[1] = soarDistance(v1, c1, originalResidual, soarLambda, rnorm);
        distances[2] = soarDistance(v1, c2, originalResidual, soarLambda, rnorm);
        distances[3] = soarDistance(v1, c3, originalResidual, soarLambda, rnorm);
    }

    @Override
    public void packDibit(int[] vector, byte[] packed) {
        packDibitImpl(vector, packed);
    }

    @Override
    public void packAsBinary(int[] vector, byte[] packed) {
        packAsBinaryImpl(vector, packed);
    }

    /**
     * Packs two bit vector (values 0-3) into a byte array with lower bits first.
     * The striding is similar to transposeHalfByte
     *
     * @param vector the input vector with values 0-3
     * @param packed the output packed byte array
     */
    public static void packDibitImpl(int[] vector, byte[] packed) {
        int limit = vector.length - 7;
        int i = 0;
        int index = 0;
        for (; i < limit; i += 8, index++) {
            assert vector[i] >= 0 && vector[i] <= 3;
            assert vector[i + 1] >= 0 && vector[i + 1] <= 3;
            assert vector[i + 2] >= 0 && vector[i + 2] <= 3;
            assert vector[i + 3] >= 0 && vector[i + 3] <= 3;
            assert vector[i + 4] >= 0 && vector[i + 4] <= 3;
            assert vector[i + 5] >= 0 && vector[i + 5] <= 3;
            assert vector[i + 6] >= 0 && vector[i + 6] <= 3;
            assert vector[i + 7] >= 0 && vector[i + 7] <= 3;
            int lowerByte = (vector[i] & 1) << 7 | (vector[i + 1] & 1) << 6 | (vector[i + 2] & 1) << 5 | (vector[i + 3] & 1) << 4
                | (vector[i + 4] & 1) << 3 | (vector[i + 5] & 1) << 2 | (vector[i + 6] & 1) << 1 | (vector[i + 7] & 1);
            int upperByte = ((vector[i] >> 1) & 1) << 7 | ((vector[i + 1] >> 1) & 1) << 6 | ((vector[i + 2] >> 1) & 1) << 5 | ((vector[i
                + 3] >> 1) & 1) << 4 | ((vector[i + 4] >> 1) & 1) << 3 | ((vector[i + 5] >> 1) & 1) << 2 | ((vector[i + 6] >> 1) & 1) << 1
                | ((vector[i + 7] >> 1) & 1);
            packed[index] = (byte) lowerByte;
            packed[index + packed.length / 2] = (byte) upperByte;
        }
        if (i == vector.length) {
            return;
        }
        int lowerByte = 0;
        int upperByte = 0;
        for (int j = 7; i < vector.length; j--, i++) {
            assert vector[i] >= 0 && vector[i] <= 3;
            lowerByte |= (vector[i] & 1) << j;
            upperByte |= ((vector[i] >> 1) & 1) << j;
        }
        packed[index] = (byte) lowerByte;
        packed[index + packed.length / 2] = (byte) upperByte;
    }

    public static void packAsBinaryImpl(int[] vector, byte[] packed) {
        int limit = vector.length - 7;
        int i = 0;
        int index = 0;
        for (; i < limit; i += 8, index++) {
            assert vector[i] == 0 || vector[i] == 1;
            assert vector[i + 1] == 0 || vector[i + 1] == 1;
            assert vector[i + 2] == 0 || vector[i + 2] == 1;
            assert vector[i + 3] == 0 || vector[i + 3] == 1;
            assert vector[i + 4] == 0 || vector[i + 4] == 1;
            assert vector[i + 5] == 0 || vector[i + 5] == 1;
            assert vector[i + 6] == 0 || vector[i + 6] == 1;
            assert vector[i + 7] == 0 || vector[i + 7] == 1;
            int result = vector[i] << 7 | (vector[i + 1] << 6) | (vector[i + 2] << 5) | (vector[i + 3] << 4) | (vector[i + 4] << 3)
                | (vector[i + 5] << 2) | (vector[i + 6] << 1) | (vector[i + 7]);
            packed[index] = (byte) result;
        }
        if (i == vector.length) {
            return;
        }
        byte result = 0;
        for (int j = 7; j >= 0 && i < vector.length; i++, j--) {
            assert vector[i] == 0 || vector[i] == 1;
            result |= (byte) ((vector[i] & 1) << j);
        }
        packed[index] = result;
    }

    @Override
    public void transposeHalfByte(int[] q, byte[] quantQueryByte) {
        transposeHalfByteImpl(q, quantQueryByte);
    }

    public static void transposeHalfByteImpl(int[] q, byte[] quantQueryByte) {
        int limit = q.length - 7;
        int i = 0;
        int index = 0;
        for (; i < limit; i += 8, index++) {
            assert q[i] >= 0 && q[i] <= 15;
            assert q[i + 1] >= 0 && q[i + 1] <= 15;
            assert q[i + 2] >= 0 && q[i + 2] <= 15;
            assert q[i + 3] >= 0 && q[i + 3] <= 15;
            assert q[i + 4] >= 0 && q[i + 4] <= 15;
            assert q[i + 5] >= 0 && q[i + 5] <= 15;
            assert q[i + 6] >= 0 && q[i + 6] <= 15;
            assert q[i + 7] >= 0 && q[i + 7] <= 15;
            int lowerByte = (q[i] & 1) << 7 | (q[i + 1] & 1) << 6 | (q[i + 2] & 1) << 5 | (q[i + 3] & 1) << 4 | (q[i + 4] & 1) << 3 | (q[i
                + 5] & 1) << 2 | (q[i + 6] & 1) << 1 | (q[i + 7] & 1);
            int lowerMiddleByte = ((q[i] >> 1) & 1) << 7 | ((q[i + 1] >> 1) & 1) << 6 | ((q[i + 2] >> 1) & 1) << 5 | ((q[i + 3] >> 1) & 1)
                << 4 | ((q[i + 4] >> 1) & 1) << 3 | ((q[i + 5] >> 1) & 1) << 2 | ((q[i + 6] >> 1) & 1) << 1 | ((q[i + 7] >> 1) & 1);
            int upperMiddleByte = ((q[i] >> 2) & 1) << 7 | ((q[i + 1] >> 2) & 1) << 6 | ((q[i + 2] >> 2) & 1) << 5 | ((q[i + 3] >> 2) & 1)
                << 4 | ((q[i + 4] >> 2) & 1) << 3 | ((q[i + 5] >> 2) & 1) << 2 | ((q[i + 6] >> 2) & 1) << 1 | ((q[i + 7] >> 2) & 1);
            int upperByte = ((q[i] >> 3) & 1) << 7 | ((q[i + 1] >> 3) & 1) << 6 | ((q[i + 2] >> 3) & 1) << 5 | ((q[i + 3] >> 3) & 1) << 4
                | ((q[i + 4] >> 3) & 1) << 3 | ((q[i + 5] >> 3) & 1) << 2 | ((q[i + 6] >> 3) & 1) << 1 | ((q[i + 7] >> 3) & 1);
            quantQueryByte[index] = (byte) lowerByte;
            quantQueryByte[index + quantQueryByte.length / 4] = (byte) lowerMiddleByte;
            quantQueryByte[index + quantQueryByte.length / 2] = (byte) upperMiddleByte;
            quantQueryByte[index + 3 * quantQueryByte.length / 4] = (byte) upperByte;
        }
        if (i == q.length) {
            return; // all done
        }
        int lowerByte = 0;
        int lowerMiddleByte = 0;
        int upperMiddleByte = 0;
        int upperByte = 0;
        for (int j = 7; i < q.length; j--, i++) {
            lowerByte |= (q[i] & 1) << j;
            lowerMiddleByte |= ((q[i] >> 1) & 1) << j;
            upperMiddleByte |= ((q[i] >> 2) & 1) << j;
            upperByte |= ((q[i] >> 3) & 1) << j;
        }
        quantQueryByte[index] = (byte) lowerByte;
        quantQueryByte[index + quantQueryByte.length / 4] = (byte) lowerMiddleByte;
        quantQueryByte[index + quantQueryByte.length / 2] = (byte) upperMiddleByte;
        quantQueryByte[index + 3 * quantQueryByte.length / 4] = (byte) upperByte;
    }

    @Override
    public int indexOf(byte[] bytes, int offset, int length, byte marker) {
        return ByteArrayUtils.indexOf(bytes, offset, length, marker);
    }
}
