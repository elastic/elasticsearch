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
import org.elasticsearch.simdvec.internal.vectorization.ESVectorUtilSupport;
import org.elasticsearch.simdvec.internal.vectorization.ESVectorizationProvider;

import java.io.IOException;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
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

    public static ES91OSQVectorsScorer getES91OSQVectorsScorer(IndexInput input, int dimension, int bulkSize) throws IOException {
        return ESVectorizationProvider.getInstance().newES91OSQVectorsScorer(input, dimension, bulkSize);
    }

    public static ESNextOSQVectorsScorer getESNextOSQVectorsScorer(
        IndexInput input,
        byte queryBits,
        byte indexBits,
        int dimension,
        int dataLength,
        int bulkSize
    ) throws IOException {
        return ESVectorizationProvider.getInstance()
            .newESNextOSQVectorsScorer(input, queryBits, indexBits, dimension, dataLength, bulkSize);
    }

    public static ES92Int7VectorsScorer getES92Int7VectorsScorer(IndexInput input, int dimension, int bulkSize) throws IOException {
        return ESVectorizationProvider.getInstance().newES92Int7VectorsScorer(input, dimension, bulkSize);
    }

    public static float dotProduct(float[] a, float[] b) {
        if (a.length != b.length) {
            throw new IllegalArgumentException("vector dimensions incompatible: " + a.length + "!= " + b.length);
        }
        return IMPL.dotProduct(a, b);
    }

    public static float squareDistance(float[] a, float[] b) {
        if (a.length != b.length) {
            throw new IllegalArgumentException("vector dimensions incompatible: " + a.length + "!= " + b.length);
        }
        return IMPL.squareDistance(a, b);
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

    public static float squareDistance(byte[] a, byte[] b) {
        if (a.length != b.length) {
            throw new IllegalArgumentException("vector dimensions incompatible: " + a.length + "!= " + b.length);
        }
        return IMPL.squareDistance(a, b);
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
        assert upperInterval >= lowerInterval;
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
     * Calculates the difference between two vectors and stores the result in a third vector.
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
     * @param distances an array to store the computed square distances, must have length 4
     *
     * @throws IllegalArgumentException if the dimensions of the vectors do not match or if the distances array does not have length 4
     */
    public static void squareDistanceBulk(float[] q, float[] v0, float[] v1, float[] v2, float[] v3, float[] distances) {
        if (q.length != v0.length) {
            throw new IllegalArgumentException("vector dimensions differ: " + q.length + "!=" + v0.length);
        }
        if (q.length != v1.length) {
            throw new IllegalArgumentException("vector dimensions differ: " + q.length + "!=" + v1.length);
        }
        if (q.length != v2.length) {
            throw new IllegalArgumentException("vector dimensions differ: " + q.length + "!=" + v2.length);
        }
        if (q.length != v3.length) {
            throw new IllegalArgumentException("vector dimensions differ: " + q.length + "!=" + v3.length);
        }
        if (distances.length != 4) {
            throw new IllegalArgumentException("distances array must have length 4, but was: " + distances.length);
        }
        IMPL.squareDistanceBulk(q, v0, v1, v2, v3, distances);
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
}
