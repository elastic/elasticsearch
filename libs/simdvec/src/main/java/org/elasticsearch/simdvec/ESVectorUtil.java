/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.simdvec;

import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.BitUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.Constants;
import org.apache.lucene.util.UnicodeUtil;
import org.apache.lucene.util.VectorUtil;
import org.elasticsearch.simdvec.internal.vectorization.ESVectorUtilSupport;

import java.io.IOException;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.nio.ByteOrder;
import java.util.Objects;

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
    private static final VectorScorerFactory SCORERS = ESVectorizationProvider.getInstance().getVectorScorerFactory();

    public static ES91OSQVectorsScorer getES91OSQVectorsScorer(IndexInput input, int dimension, int bulkSize) throws IOException {
        return SCORERS.newES91OSQVectorsScorer(input, dimension, bulkSize);
    }

    public static ES940OSQVectorsScorer getES940OSQVectorsScorer(
        IndexInput input,
        byte queryBits,
        byte indexBits,
        int dimension,
        int dataLength,
        int bulkSize,
        ES940OSQVectorsScorer.BitEncoding bitEncoding
    ) throws IOException {
        return SCORERS.newES940OSQVectorsScorer(input, queryBits, indexBits, dimension, dataLength, bulkSize, bitEncoding);
    }

    public static ES92Int7VectorsScorer getES92Int7VectorsScorer(IndexInput input, int dimension, int bulkSize) throws IOException {
        return SCORERS.newES92Int7VectorsScorer(input, dimension, bulkSize);
    }

    public static ES93BinaryQuantizedVectorScorer getES93BinaryQuantizedVectorScorer(
        IndexInput input,
        int dimension,
        int vectorLengthInBytes
    ) throws IOException {
        return SCORERS.newES93BinaryQuantizedVectorScorer(input, dimension, vectorLengthInBytes);
    }

    public static void bFloat16ToFloat(byte[] bfBytes, int bfOffset, float[] floats, int floatOffset, int floatCount, ByteOrder byteOrder) {
        IMPL.bFloat16ToFloat(bfBytes, bfOffset, floats, floatOffset, floatCount, byteOrder);
    }

    public static void floatToBFloat16(float[] floats, int floatOffset, byte[] bfBytes, int bfOffset, int floatCount, ByteOrder byteOrder) {
        assert floats.length - floatOffset >= floatCount;
        assert (bfBytes.length - bfOffset) >= floatCount * Short.BYTES;
        IMPL.floatToBFloat16(floats, floatOffset, bfBytes, bfOffset, floatCount, byteOrder);
    }

    public static float dotProduct(float[] a, float[] b) {
        if (a.length != b.length) {
            throw new IllegalArgumentException("vector dimensions incompatible: " + a.length + "!= " + b.length);
        }
        return IMPL.dotProduct(a, b);
    }

    /**
     * Dot product of the first {@code length} components of {@code a} and {@code b}.
     */
    public static float dotProduct(float[] a, float[] b, int length) {
        if (a.length != b.length) {
            throw new IllegalArgumentException("vector dimensions incompatible: " + a.length + "!= " + b.length);
        }
        Objects.checkFromIndexSize(0, length, a.length);
        return IMPL.dotProduct(a, b, 0, length);
    }

    /**
     * Dot product over {@code [offset, offset + length)}.
     */
    public static float dotProduct(float[] a, float[] b, int offset, int length) {
        if (a.length != b.length) {
            throw new IllegalArgumentException("vector dimensions incompatible: " + a.length + "!= " + b.length);
        }
        Objects.checkFromIndexSize(offset, length, a.length);
        return IMPL.dotProduct(a, b, offset, length);
    }

    /**
     * L2-normalizes the prefix {@code v[0..length)} in place. Elements at indices {@code length} and
     * beyond are left unchanged. A zero prefix is a no-op; unlike {@link VectorUtil#l2normalize(float[])},
     * this method does not throw on a zero vector.
     */
    public static void l2Normalize(float[] v, int length) {
        l2Normalize(v, 0, length);
    }

    /**
     * L2-normalizes {@code v[offset:offset + length)} in place. Elements outside the range are left
     * unchanged. A zero range is a no-op.
     */
    public static void l2Normalize(float[] v, int offset, int length) {
        if (length <= 0) {
            return;
        }
        Objects.checkFromIndexSize(offset, length, v.length);
        IMPL.l2Normalize(v, offset, length);
    }

    /** L2-normalizes all components of {@code v} in place. */
    public static void l2Normalize(float[] v) {
        l2Normalize(v, 0, v.length);
    }

    public static float squareDistance(float[] a, float[] b) {
        if (a.length != b.length) {
            throw new IllegalArgumentException("vector dimensions incompatible: " + a.length + "!= " + b.length);
        }
        return IMPL.squareDistance(a, b);
    }

    /**
     * Computes the squared Euclidean distance between a byte vector and a float vector.
     * Each byte element is implicitly widened to float for the computation.
     */
    public static float squareDistance(byte[] a, float[] b) {
        if (a.length != b.length) {
            throw new IllegalArgumentException("vector dimensions incompatible: " + a.length + "!= " + b.length);
        }
        return IMPL.squareDistance(a, b);
    }

    public static float squareDistance(float[] a, float[] b, int offset, int length) {
        if (a.length != b.length) {
            throw new IllegalArgumentException("vector dimensions incompatible: " + a.length + "!= " + b.length);
        }
        Objects.checkFromIndexSize(offset, length, a.length);
        return IMPL.squareDistance(a, b, offset, length);
    }

    public static float cosine(byte[] a, byte[] b) {
        if (a.length != b.length) {
            throw new IllegalArgumentException("vector dimensions incompatible: " + a.length + "!= " + b.length);
        }
        return IMPL.cosine(a, b);
    }

    public static float dotProduct(byte[] a, byte[] b) {
        if (a.length != b.length) {
            throw new IllegalArgumentException("vector dimensions incompatible: " + a.length + "!= " + b.length);
        }
        return IMPL.dotProduct(a, b);
    }

    /**
     * Dot product of the first {@code length} components of {@code a} and {@code b}.
     */
    public static float dotProduct(byte[] a, byte[] b, int length) {
        if (a.length != b.length) {
            throw new IllegalArgumentException("vector dimensions incompatible: " + a.length + "!= " + b.length);
        }
        Objects.checkFromIndexSize(0, length, a.length);
        return IMPL.dotProduct(a, b, 0, length);
    }

    /**
     * Dot product over {@code [offset, offset + length)}.
     */
    public static float dotProduct(byte[] a, byte[] b, int offset, int length) {
        if (a.length != b.length) {
            throw new IllegalArgumentException("vector dimensions incompatible: " + a.length + "!= " + b.length);
        }
        Objects.checkFromIndexSize(offset, length, a.length);
        return IMPL.dotProduct(a, b, offset, length);
    }

    /**
     * L2-normalizes the prefix {@code v[0..length)} in place using signed byte values as real
     * components. Elements at indices {@code length} and beyond are left unchanged. A zero prefix
     * is a no-op.
     */
    public static void l2Normalize(byte[] v, int length) {
        l2Normalize(v, 0, length);
    }

    /**
     * L2-normalizes {@code v[offset:offset + length)} in place using signed byte values as real
     * components. Elements outside the range are left unchanged. A zero range is a no-op.
     */
    public static void l2Normalize(byte[] v, int offset, int length) {
        if (length <= 0) {
            return;
        }
        Objects.checkFromIndexSize(offset, length, v.length);
        IMPL.l2Normalize(v, offset, length);
    }

    /** L2-normalizes all components of {@code v} in place. */
    public static void l2Normalize(byte[] v) {
        l2Normalize(v, 0, v.length);
    }

    /**
     * Computes max-sim dot product for float query vectors against a multi-vector source.
     * <p>
     * The provided {@code scoresScratch} buffer is reused as temporary per-document scores for
     * each query vector to avoid per-call allocations. Its length must be at least
     * {@code source.vectorCount()}.
     */
    public static float maxSimDotProduct(MultiFloatVectorsSource source, float[][] query, float[] scoresScratch) {
        ensureScoresScratchCapacity(source, scoresScratch);
        return IMPL.maxSimDotProduct(source, query, scoresScratch);
    }

    /**
     * Computes max-sim dot product for float query vectors against a bfloat16 multi-vector source.
     * <p>
     * The provided {@code scoresScratch} buffer is reused as temporary per-document scores for
     * each query vector to avoid per-call allocations. Its length must be at least
     * {@code source.vectorCount()}.
     */
    public static float maxSimDotProduct(MultiBFloat16VectorsSource source, float[][] query, float[] scoresScratch) {
        ensureScoresScratchCapacity(source, scoresScratch);
        return IMPL.maxSimDotProduct(source, query, scoresScratch);
    }

    /**
     * Computes max-sim dot product for byte query vectors against a multi-vector source.
     * <p>
     * The provided {@code scoresScratch} buffer is reused as temporary per-document scores for
     * each query vector to avoid per-call allocations. Its length must be at least
     * {@code source.vectorCount()}.
     */
    public static float maxSimDotProduct(MultiByteVectorsSource source, byte[][] query, float[] scoresScratch) {
        ensureScoresScratchCapacity(source, scoresScratch);
        return IMPL.maxSimDotProduct(source, query, scoresScratch);
    }

    public static float squareDistance(byte[] a, byte[] b) {
        if (a.length != b.length) {
            throw new IllegalArgumentException("vector dimensions incompatible: " + a.length + "!= " + b.length);
        }
        return IMPL.squareDistance(a, b);
    }

    /** Returns the sum of squared differences of the two byte vectors over a sub-range. */
    public static float squareDistance(byte[] a, byte[] b, int offset, int length) {
        if (a.length != b.length) {
            throw new IllegalArgumentException("vector dimensions incompatible: " + a.length + "!= " + b.length);
        }
        Objects.checkFromIndexSize(offset, length, a.length);
        return IMPL.squareDistance(a, b, offset, length);
    }

    /**
     * Bulk computation of square distances from a byte query vector to four byte candidate vectors.
     */
    public static void squareDistanceBulk(byte[] q, byte[] v0, byte[] v1, byte[] v2, byte[] v3, int distancesOffset, float[] distances) {
        if (q.length != v0.length || q.length != v1.length || q.length != v2.length || q.length != v3.length) {
            throw new IllegalArgumentException("vector dimensions incompatible");
        }
        if (distances.length < 4) {
            throw new IllegalArgumentException("distances array must have length >= 4, but was: " + distances.length);
        }
        if (distancesOffset < 0 || distancesOffset > distances.length - 4) {
            throw new IllegalArgumentException("distancesOffset must be between 0 and distances.length - 4");
        }
        IMPL.squareDistanceBulk(q, 0, v0, v1, v2, v3, distancesOffset, distances, q.length);
    }

    /**
     * Bulk computation of square distances from a sub-range of a byte query vector to four byte candidate vectors.
     */
    public static void squareDistanceBulk(
        byte[] q,
        int qOffset,
        int length,
        byte[] v0,
        byte[] v1,
        byte[] v2,
        byte[] v3,
        float[] distances
    ) {
        if (q.length != v0.length || q.length != v1.length || q.length != v2.length || q.length != v3.length) {
            throw new IllegalArgumentException("vector dimensions incompatible");
        }
        if (distances.length != 4) {
            throw new IllegalArgumentException("distances array must have length 4, but was: " + distances.length);
        }
        Objects.checkFromIndexSize(qOffset, length, q.length);
        IMPL.squareDistanceBulk(q, qOffset, v0, v1, v2, v3, 0, distances, length);
    }

    /**
     * Bulk computation of dot product from a byte query vector to four byte candidate vectors.
     */
    public static void dotProductBulk(byte[] q, byte[] v0, byte[] v1, byte[] v2, byte[] v3, int distancesOffset, float[] distances) {
        if (q.length != v0.length || q.length != v1.length || q.length != v2.length || q.length != v3.length) {
            throw new IllegalArgumentException("vector dimensions incompatible");
        }
        if (distances.length < 4) {
            throw new IllegalArgumentException("distances array must have length >= 4, but was: " + distances.length);
        }
        if (distancesOffset < 0 || distancesOffset > distances.length - 4) {
            throw new IllegalArgumentException("distancesOffset must be between 0 and distances.length - 4");
        }
        IMPL.dotProductBulk(q, v0, v1, v2, v3, distancesOffset, distances);
    }

    /**
     * Bulk computation of cosine similarity from a byte query vector to four byte candidate vectors.
     */
    public static void cosineBulk(byte[] q, byte[] v0, byte[] v1, byte[] v2, byte[] v3, int distancesOffset, float[] distances) {
        if (q.length != v0.length || q.length != v1.length || q.length != v2.length || q.length != v3.length) {
            throw new IllegalArgumentException("vector dimensions incompatible");
        }
        if (distances.length < 4) {
            throw new IllegalArgumentException("distances array must have length >= 4, but was: " + distances.length);
        }
        if (distancesOffset < 0 || distancesOffset > distances.length - 4) {
            throw new IllegalArgumentException("distancesOffset must be between 0 and distances.length - 4");
        }
        IMPL.cosineBulk(q, v0, v1, v2, v3, distancesOffset, distances);
    }

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
     * Compute the inner product of two vectors, where the query vector is a float vector and the document vector is a byte vector.
     * @param q the query vector
     * @param d the document vector
     * @return the inner product of the two vectors
     */
    public static float ipFloatByte(float[] q, byte[] d) {
        if (q.length != d.length) {
            throw new IllegalArgumentException("vector dimensions incompatible: " + q.length + "!= " + d.length);
        }
        return IMPL.ipFloatByte(q, d);
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

    /**
     * Hamming similarity between two equal-length bit vectors, matching Lucene's
     * {@code FlatBitVectorsScorer}: {@code (numBits - xorBitCount) / numBits}.
     */
    public static float hammingScore(byte[] a, byte[] b) {
        return ((a.length * Byte.SIZE) - VectorUtil.xorBitCount(a, b)) / (float) (a.length * Byte.SIZE);
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

    public static float max(float[] values, int length) {
        Objects.checkFromIndexSize(0, length, values.length);
        float max = Float.NEGATIVE_INFINITY;
        for (int i = 0; i < length; i++) {
            max = Math.max(max, values[i]);
        }
        return max;
    }

    public static float sum(float[] values, int length) {
        Objects.checkFromIndexSize(0, length, values.length);
        float sum = 0f;
        for (int i = 0; i < length; i++) {
            sum += values[i];
        }
        return sum;
    }

    private static void ensureScoresScratchCapacity(MultiVectorsSource<?> source, float[] scoresScratch) {
        if (scoresScratch.length < source.vectorCount()) {
            throw new IllegalArgumentException("scores array too small: " + scoresScratch.length + " < " + source.vectorCount());
        }
    }

    /**
     * Calculate the loss for optimized-scalar quantization for the given parameteres
     * @param target The vector being quantized, assumed to be centered
     * @param lowerInterval The lower interval value for which to calculate the loss
     * @param upperInterval The upper interval value for which to calculate the loss
     * @param points the quantization points
     * @param norm2 The norm squared of the target vector
     * @param lambda The lambda parameter for controlling anisotropic loss calculation
     * @param quantize array to store the computed quantize vector.
     *
     * @return The loss for the given parameters
     */
    public static float calculateOSQLoss(
        float[] target,
        float lowerInterval,
        float upperInterval,
        int points,
        float norm2,
        float lambda,
        int[] quantize
    ) {
        assert upperInterval >= lowerInterval
            : "upperInterval must be greater than or equal to lowerInterval, but was: " + upperInterval + " < " + lowerInterval;
        float step = ((upperInterval - lowerInterval) / (points - 1.0F));
        float invStep = 1f / step;
        return IMPL.calculateOSQLoss(target, lowerInterval, upperInterval, step, invStep, norm2, lambda, quantize);
    }

    /**
     * Calculate the grid points for optimized-scalar quantization
     * @param target The vector being quantized, assumed to be centered
     * @param quantize The quantize vector which should have at least the target vector length
     * @param points the quantization points
     * @param pts The array to store the grid points, must be of length 5
     */
    public static void calculateOSQGridPoints(float[] target, int[] quantize, int points, float[] pts) {
        assert target.length <= quantize.length;
        assert pts.length == 5;
        IMPL.calculateOSQGridPoints(target, quantize, points, pts);
    }

    /**
     * Center the target vector and calculate the optimized-scalar quantization statistics
     * @param target The vector being quantized
     * @param centroid The centroid of the target vector
     * @param centered The destination of the centered vector, will be overwritten
     * @param stats The array to store the statistics, must be of length 5
     */
    public static void centerAndCalculateOSQStatsEuclidean(float[] target, float[] centroid, float[] centered, float[] stats) {
        assert target.length == centroid.length;
        assert stats.length == 5;
        if (target.length != centroid.length) {
            throw new IllegalArgumentException("vector dimensions differ: " + target.length + "!=" + centroid.length);
        }
        if (centered.length != target.length) {
            throw new IllegalArgumentException("vector dimensions differ: " + centered.length + "!=" + target.length);
        }
        IMPL.centerAndCalculateOSQStatsEuclidean(target, centroid, centered, stats);
    }

    /**
     * Center the target vector and calculate the optimized-scalar quantization statistics
     * @param target The vector being quantized
     * @param centroid The centroid of the target vector
     * @param centered The destination of the centered vector, will be overwritten
     * @param stats The array to store the statistics, must be of length 6
     */
    public static void centerAndCalculateOSQStatsDp(float[] target, float[] centroid, float[] centered, float[] stats) {
        if (target.length != centroid.length) {
            throw new IllegalArgumentException("vector dimensions differ: " + target.length + "!=" + centroid.length);
        }
        if (centered.length != target.length) {
            throw new IllegalArgumentException("vector dimensions differ: " + centered.length + "!=" + target.length);
        }
        assert stats.length == 6;
        IMPL.centerAndCalculateOSQStatsDp(target, centroid, centered, stats);
    }

    /**
     * Center the byte target vector against a byte centroid and calculate the optimized-scalar quantization statistics
     * for euclidean similarity.
     * @param target The byte vector being quantized
     * @param centroid The byte centroid of the target vector
     * @param centered The destination of the centered vector, will be overwritten
     * @param stats The array to store the statistics, must be of length 5
     */
    public static void centerAndCalculateOSQStatsEuclidean(byte[] target, byte[] centroid, float[] centered, float[] stats) {
        assert stats.length == 5;
        if (target.length != centroid.length) {
            throw new IllegalArgumentException("vector dimensions differ: " + target.length + "!=" + centroid.length);
        }
        if (centered.length != target.length) {
            throw new IllegalArgumentException("vector dimensions differ: " + centered.length + "!=" + target.length);
        }
        IMPL.centerAndCalculateOSQStatsEuclidean(target, centroid, centered, stats);
    }

    /**
     * Center the byte target vector against a byte centroid and calculate the optimized-scalar quantization statistics
     * for dot-product similarity.
     * @param target The byte vector being quantized
     * @param centroid The byte centroid of the target vector
     * @param centered The destination of the centered vector, will be overwritten
     * @param stats The array to store the statistics, must be of length 6
     */
    public static void centerAndCalculateOSQStatsDp(byte[] target, byte[] centroid, float[] centered, float[] stats) {
        if (target.length != centroid.length) {
            throw new IllegalArgumentException("vector dimensions differ: " + target.length + "!=" + centroid.length);
        }
        if (centered.length != target.length) {
            throw new IllegalArgumentException("vector dimensions differ: " + centered.length + "!=" + target.length);
        }
        assert stats.length == 6;
        IMPL.centerAndCalculateOSQStatsDp(target, centroid, centered, stats);
    }

    /** Calculates the difference between two vectors and stores the result in a third vector.
     * @param v1 the first vector
     * @param v2 the second vector
     * @param result the result vector, must be the same length as the input vectors
     */
    public static void subtract(float[] v1, float[] v2, float[] result) {
        if (v1.length != v2.length) {
            throw new IllegalArgumentException("vector dimensions differ: " + v1.length + "!=" + v2.length);
        }
        if (result.length != v1.length) {
            throw new IllegalArgumentException("vector dimensions differ: " + result.length + "!=" + v1.length);
        }
        for (int i = 0; i < v1.length; i++) {
            result[i] = v1[i] - v2[i];
        }
    }

    /**
     * calculates the soar distance for a vector and a centroid
     * @param v1 the vector
     * @param centroid the centroid
     * @param originalResidual the residual with the actually nearest centroid
     * @param soarLambda the lambda parameter
     * @param rnorm distance to the nearest centroid
     * @return the soar distance
     */
    public static float soarDistance(float[] v1, float[] centroid, float[] originalResidual, float soarLambda, float rnorm) {
        if (v1.length != centroid.length) {
            throw new IllegalArgumentException("vector dimensions differ: " + v1.length + "!=" + centroid.length);
        }
        if (originalResidual.length != v1.length) {
            throw new IllegalArgumentException("vector dimensions differ: " + originalResidual.length + "!=" + v1.length);
        }
        return IMPL.soarDistance(v1, centroid, originalResidual, soarLambda, rnorm);
    }

    /**
     * Compute the SOAR distance between a byte vector and a byte centroid.
     * SOAR distance: {@code ||x-c||^2 + lambda * ((x-c1)^T (x-c))^2 / ||x-c1||^2}
     *
     * @param v1 the byte vector
     * @param centroid the byte centroid
     * @param originalResidual the precomputed residual (x - c1) as float
     * @param soarLambda the SOAR lambda parameter
     * @param rnorm the squared norm of the original residual
     * @return the SOAR distance
     */
    public static float soarDistance(byte[] v1, byte[] centroid, float[] originalResidual, float soarLambda, float rnorm) {
        if (v1.length != centroid.length) {
            throw new IllegalArgumentException("vector dimensions differ: " + v1.length + "!=" + centroid.length);
        }
        if (originalResidual.length != v1.length) {
            throw new IllegalArgumentException("vector dimensions differ: " + originalResidual.length + "!=" + v1.length);
        }
        return IMPL.soarDistance(v1, centroid, originalResidual, soarLambda, rnorm);
    }

    /**
     * Optimized-scalar quantization of the provided vector to the provided destination array.
     *
     * @param vector the vector to quantize
     * @param destination the array to store the result
     * @param lowInterval the minimum value, lower values in the original array will be replaced by this value
     * @param upperInterval the maximum value, bigger values in the original array will be replaced by this value
     * @param bit the number of bits to use for quantization, must be between 1 and 8
     *
     * @return return the sum of all the elements of the resulting quantized vector.
     */
    public static int quantizeVectorWithIntervals(float[] vector, int[] destination, float lowInterval, float upperInterval, byte bit) {
        if (vector.length > destination.length) {
            throw new IllegalArgumentException("vector dimensions differ: " + vector.length + "!=" + destination.length);
        }
        if (bit <= 0 || bit > Byte.SIZE) {
            throw new IllegalArgumentException("bit must be between 1 and 8, but was: " + bit);
        }
        return IMPL.quantizeVectorWithIntervals(vector, destination, lowInterval, upperInterval, bit);
    }

    /**
     * Bulk computation of square distances between a query vector and four vectors.Result is stored in the provided distances array.
     *
     * @param q the query vector
     * @param v0 the first vector
     * @param v1 the second vector
     * @param v2 the third vector
     * @param v3 the fourth vector
     * @param distancesOffset offset to the location in the distances array where we want to store the 4 results,
     *                        we require distancesOffset to be between 0 and distances.length - 4
     * @param distances an array to store the computed square distances, must have length >= 4
     *
     * @throws IllegalArgumentException if the dimensions of the vectors do not match or if the distances array does not have length 4
     */
    public static void squareDistanceBulk(
        float[] q,
        float[] v0,
        float[] v1,
        float[] v2,
        float[] v3,
        int distancesOffset,
        float[] distances
    ) {
        if (q.length != v0.length || q.length != v1.length || q.length != v2.length || q.length != v3.length) {
            throw new IllegalArgumentException("vector dimensions incompatible");
        }
        if (distances.length < 4) {
            throw new IllegalArgumentException("distances array must have length >= 4, but was: " + distances.length);
        }
        if (distancesOffset < 0 || distancesOffset > distances.length - 4) {
            throw new IllegalArgumentException("distancesOffset must be between have length 0 and distances.length - 4");
        }
        IMPL.squareDistanceBulk(q, 0, v0, v1, v2, v3, distancesOffset, distances, q.length);
    }

    /**
     * Bulk computation of square distances between a query vector and four vectors.Result is stored in the provided distances array.
     *
     * @param q the query vector
     * @param v0 the first vector
     * @param v1 the second vector
     * @param v2 the third vector
     * @param v3 the fourth vector
     * @param distancesOffset offset to the location in the distances array where we want to store the 4 results,
     *                        we require distancesOffset to be between 0 and distances.length - 4
     * @param distances an array to store the computed square distances, must have length >= 4
     *
     * @throws IllegalArgumentException if the dimensions of the vectors do not match or if the distances array does not have length 4
     */
    public static void dotProductBulk(float[] q, float[] v0, float[] v1, float[] v2, float[] v3, int distancesOffset, float[] distances) {
        if (q.length != v0.length || q.length != v1.length || q.length != v2.length || q.length != v3.length) {
            throw new IllegalArgumentException("vector dimensions incompatible");
        }
        if (distances.length < 4) {
            throw new IllegalArgumentException("distances array must have length >= 4, but was: " + distances.length);
        }
        if (distancesOffset < 0 || distancesOffset > distances.length - 4) {
            throw new IllegalArgumentException("distancesOffset must be between have length 0 and distances.length - 4");
        }
        IMPL.dotProductBulk(q, v0, v1, v2, v3, distancesOffset, distances);
    }

    public static void squareDistanceBulk(
        float[] q,
        int qOffset,
        int length,
        float[] v0,
        float[] v1,
        float[] v2,
        float[] v3,
        float[] distances
    ) {
        if (q.length != v0.length || q.length != v1.length || q.length != v2.length || q.length != v3.length) {
            throw new IllegalArgumentException("vector dimensions incompatible");
        }
        if (distances.length != 4) {
            throw new IllegalArgumentException("distances array must have length 4, but was: " + distances.length);
        }
        Objects.checkFromIndexSize(qOffset, length, q.length);
        IMPL.squareDistanceBulk(q, qOffset, v0, v1, v2, v3, 0, distances, length);
    }

    /**
     * Bulk computation of the soar distance for a vector to four centroids
     * @param v1 the vector
     * @param c0 the first centroid
     * @param c1 the second centroid
     * @param c2 the third centroid
     * @param c3 the fourth centroid
     * @param originalResidual the residual with the actually nearest centroid
     * @param soarLambda the lambda parameter
     * @param rnorm distance to the nearest centroid
     * @param distances an array to store the computed soar distances, must have length 4
     */
    public static void soarDistanceBulk(
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
        if (v1.length != c0.length) {
            throw new IllegalArgumentException("vector dimensions differ: " + v1.length + "!=" + c0.length);
        }
        if (v1.length != c1.length) {
            throw new IllegalArgumentException("vector dimensions differ: " + v1.length + "!=" + c1.length);
        }
        if (v1.length != c2.length) {
            throw new IllegalArgumentException("vector dimensions differ: " + v1.length + "!=" + c2.length);
        }
        if (v1.length != c3.length) {
            throw new IllegalArgumentException("vector dimensions differ: " + v1.length + "!=" + c3.length);
        }
        if (v1.length != originalResidual.length) {
            throw new IllegalArgumentException("vector dimensions differ: " + v1.length + "!=" + originalResidual.length);
        }
        if (distances.length != 4) {
            throw new IllegalArgumentException("distances array must have length 4, but was: " + distances.length);
        }
        IMPL.soarDistanceBulk(v1, c0, c1, c2, c3, originalResidual, soarLambda, rnorm, distances);
    }

    /**
     * Calculates SOAR distances between a byte query vector and 4 byte centroid vectors in bulk.
     * SOAR distance: ||x-c||^2 + lambda * ((x-c)^T * originalResidual)^2 / rnorm
     *
     * @param v1 the query vector
     * @param c0 centroid 0
     * @param c1 centroid 1
     * @param c2 centroid 2
     * @param c3 centroid 3
     * @param originalResidual the precomputed residual (x - globalCentroid) as float
     * @param soarLambda the SOAR lambda parameter
     * @param rnorm the squared norm of the original residual
     * @param distances output array of length 4 for the computed distances
     */
    public static void soarDistanceBulk(
        byte[] v1,
        byte[] c0,
        byte[] c1,
        byte[] c2,
        byte[] c3,
        float[] originalResidual,
        float soarLambda,
        float rnorm,
        float[] distances
    ) {
        if (v1.length != c0.length) {
            throw new IllegalArgumentException("vector dimensions differ: " + v1.length + "!=" + c0.length);
        }
        if (v1.length != c1.length) {
            throw new IllegalArgumentException("vector dimensions differ: " + v1.length + "!=" + c1.length);
        }
        if (v1.length != c2.length) {
            throw new IllegalArgumentException("vector dimensions differ: " + v1.length + "!=" + c2.length);
        }
        if (v1.length != c3.length) {
            throw new IllegalArgumentException("vector dimensions differ: " + v1.length + "!=" + c3.length);
        }
        if (v1.length != originalResidual.length) {
            throw new IllegalArgumentException("vector dimensions differ: " + v1.length + "!=" + originalResidual.length);
        }
        if (distances.length != 4) {
            throw new IllegalArgumentException("distances array must have length 4, but was: " + distances.length);
        }
        IMPL.soarDistanceBulk(v1, c0, c1, c2, c3, originalResidual, soarLambda, rnorm, distances);
    }

    /**
     * Narrows each of the first {@code len} ints to a byte by truncating to the low 8 bits,
     * writing into {@code dst[0..len)}. No bounds check is performed; the caller must ensure
     * {@code dst.length >= len}.
     *
     * @param src int array of quantized values
     * @param dst byte array to receive the narrowed values
     * @param len number of elements to convert
     */
    public static void packAsBytes(int[] src, byte[] dst, int len) {
        for (int i = 0; i < len; i++) {
            dst[i] = (byte) src[i];
        }
    }

    /**
     * Packs the provided int array populated with "0" and "1" values into a byte array.
     *
     * @param vector the int array to pack, must contain only "0" and "1" values.
     * @param packed the byte array to store the packed result, must be large enough to hold the packed data.
     */
    public static void packAsBinary(int[] vector, byte[] packed) {
        if (packed.length * Byte.SIZE < vector.length) {
            throw new IllegalArgumentException("packed array is too small: " + packed.length * Byte.SIZE + " < " + vector.length);
        }
        IMPL.packAsBinary(vector, packed);
    }

    public static void packDibit(int[] vector, byte[] packed) {
        if (packed.length * Byte.SIZE / 2 < vector.length) {
            throw new IllegalArgumentException("packed array is too small: " + packed.length * Byte.SIZE / 2 + " < " + vector.length);
        }
        IMPL.packDibit(vector, packed);
    }

    public static void packDibitQuad(int[] vector, byte[] packed) {
        if (packed.length * Byte.SIZE / 2 < vector.length) {
            throw new IllegalArgumentException("packed array is too small: " + packed.length * Byte.SIZE / 2 + " < " + vector.length);
        }
        IMPL.packDibitQuad(vector, packed);
    }

    /**
     * The idea here is to organize the query vector bits such that the first bit
     * of every dimension is in the first set dimensions bits, or (dimensions/8) bytes. The second,
     * third, and fourth bits are in the second, third, and fourth set of dimensions bits,
     * respectively. This allows for direct bitwise comparisons with the stored index vectors through
     * summing the bitwise results with the relative required bit shifts.
     *
     * @param q the query vector, assumed to be half-byte quantized with values between 0 and 15
     * @param quantQueryByte the byte array to store the transposed query vector.
     *
     **/
    public static void transposeHalfByte(int[] q, byte[] quantQueryByte) {
        if (quantQueryByte.length * Byte.SIZE < 4 * q.length) {
            throw new IllegalArgumentException("packed array is too small: " + quantQueryByte.length * Byte.SIZE + " < " + 4 * q.length);
        }
        IMPL.transposeHalfByte(q, quantQueryByte);
    }

    /**
     * Searches for the first occurrence of the given marker byte in the specified range of the array.
     *
     * <p>The search starts at {@code offset} and examines at most {@code length} bytes. The return
     * value is the relative index of the first occurrence of {@code marker} within this slice,
     * or {@code -1} if not found.
     *
     * @param bytes  the byte array to search
     * @param offset the starting index within the array
     * @param length the number of bytes to examine
     * @param marker the byte to search for
     * @return the relative index (0..length-1) of the first match, or {@code -1} if not found
     */
    public static int indexOf(byte[] bytes, int offset, int length, byte marker) {
        Objects.checkFromIndexSize(offset, length, bytes.length);
        return IMPL.indexOf(bytes, offset, length, marker);
    }

    /**
     * Checks whether the byte sequence {@code term} appears as a contiguous subsequence
     * within {@code value}.
     *
     * @param value       the byte array to search in
     * @param valueOffset the starting index within value
     * @param valueLength the number of bytes to search
     * @param term        the byte array containing the term to search for
     * @param termOffset  the starting index within term
     * @param termLength  the number of bytes in the term
     * @return true if term is found within value
     */
    public static boolean contains(byte[] value, int valueOffset, int valueLength, byte[] term, int termOffset, int termLength) {
        Objects.checkFromIndexSize(valueOffset, valueLength, value.length);
        Objects.checkFromIndexSize(termOffset, termLength, term.length);
        if (termLength == 0) {
            return true;
        }
        if (termLength > valueLength) {
            return false;
        }
        return IMPL.contains(value, valueOffset, valueLength, term, termOffset, termLength);
    }

    /**
     * Count the number of Unicode code points in a utf-8 encoded string. Assumes that the input
     * string is correctly encoded. If the input string is incorrectly encoded, no errors will be
     * thrown, but invalid results will be returned.
     *
     * @param bytesRef bytes reference containing a valid utf-8 encoded string
     * @return the number of code points in the bytes ref
     */
    public static int codePointCount(BytesRef bytesRef) {
        // Scalar logic is faster for lengths below approximately 12
        if (bytesRef.length < 12) {
            return UnicodeUtil.codePointCount(bytesRef);
        }
        Objects.checkFromIndexSize(bytesRef.offset, bytesRef.length, bytesRef.bytes.length);
        return IMPL.codePointCount(bytesRef);
    }

    /**
     * Computes dest = scale * other + scaledDes * dest
     *
     * @param scaleOther a multiplicative factor for other
     * @param other the other vector
     * @param scaleDest a multiplicative factor for dest
     * @param dest the destination vector
     */
    public static void linearCombination(float scaleOther, float[] other, float scaleDest, float[] dest) {
        if (other.length != dest.length) {
            throw new IllegalArgumentException("vector dimensions differ: " + other.length + "!=" + dest.length);
        }
        IMPL.linearCombination(scaleOther, other, scaleDest, dest);
    }

    /**
     * Computes dest = scale * other + dest
     *
     * @param scaleOther a multiplicative factor for other
     * @param other the other vector
     * @param dest the destination vector
     */
    public static void linearCombination(float scaleOther, float[] other, float[] dest) {
        if (other.length != dest.length) {
            throw new IllegalArgumentException("vector dimensions differ: " + other.length + "!=" + dest.length);
        }
        IMPL.linearCombination(scaleOther, other, dest);
    }

    /**
     * Computes dest[d] = scaleSrc * src[d] + scaleDest * dest[d], widening byte src to float.
     *
     * @param scaleOther a multiplicative factor for src
     * @param other the byte source vector (widened to float for computation)
     * @param scaleDest a multiplicative factor for dest
     * @param dest the destination float vector (modified in place)
     */
    public static void linearCombination(float scaleOther, byte[] other, float scaleDest, float[] dest) {
        if (other.length != dest.length) {
            throw new IllegalArgumentException("vector dimensions differ: " + other.length + "!=" + dest.length);
        }
        IMPL.linearCombination(scaleOther, other, scaleDest, dest);
    }

    /**
     * Computes dest = scale * other + dest, widening byte src to float.
     *
     * @param scaleOther a multiplicative factor for src
     * @param other the byte source vector (widened to float for computation)
     * @param dest the destination float vector (modified in place)
     */
    public static void linearCombination(float scaleOther, byte[] other, float[] dest) {
        if (other.length != dest.length) {
            throw new IllegalArgumentException("vector dimensions differ: " + other.length + "!=" + dest.length);
        }
        IMPL.linearCombination(scaleOther, other, dest);
    }

    /**
     * Calculates an approximation of the LogSumExp of the input array in base 2.
     * The formula used is: log2(sum_i(pow(2, x[i]))).
     * This implementation uses the log-sum-exp trick for numerical stability and Not-Quite-Trascendental functions for speed.
     *
     * @param vector The input array of double values (log probabilities/values).
     * @return The log-sum-exp result.
     */
    public static float logSumExpNQT(float[] vector) {
        return IMPL.logSumExpNQT(vector);
    }

    /**
     * Calculates a shifted and scaled LogSumExp of the input arrays in base 2, according to the formula:
     * log2(sum_i(pow(2, (v1[i] - v2[i]) / eps)))
     * This implementation uses the log-sum-exp trick for numerical stability and Not-Quite-Trascendental functions for speed.
     *
     * @param v1 The first input array of double values (log probabilities/values).
     * @param v2 The second input array of double values (log probabilities/values).
     * @param eps The normalization constant (that is, the temperature parameter).
     * @return The log-sum-exp result.
     */
    public static float logSumExpNQTDiff(float[] v1, float[] v2, float eps) {
        return IMPL.logSumExpNQTDiff(v1, v2, eps);
    }

    /**
     * Compute the following operation:
     * result[i] = pow(2, (a + v1[i] - v2[i]) / eps)
     * This implementation uses the log-sum-exp trick for numerical stability.
     *
     * @param v1 The first input array of double values (log probabilities/values).
     * @param v2 The second input array of double values (log probabilities/values).
     * @param eps The normalization constant (that is, the temperature parameter).
     * @param result The output array.
     */
    public static void pow2DiffAndScaleNQT(float[] v1, float[] v2, float a, float eps, float[] result) {
        IMPL.pow2DiffAndScaleNQT(v1, v2, a, eps, result);
    }

    /**
     * For every index {@code i} in {@code [0, values.length)}, sets bit {@code i} in
     * {@code matches} ({@code matches[i>>>6]}, bit position {@code i & 0x3f}) when
     * {@code values[i]} lies in {@code [lowerValue, upperValue]}.
     *
     * <p>Requires {@code values.length} to be a multiple of 8 (the maximum supported SIMD lane
     * count, for AVX-512) and {@code matches.length == values.length / 64}.
     */
    public static void inRangeBitmask(long[] values, long lowerValue, long upperValue, long[] matches) {
        IMPL.inRangeBitmask(values, lowerValue, upperValue, matches);
    }
}
