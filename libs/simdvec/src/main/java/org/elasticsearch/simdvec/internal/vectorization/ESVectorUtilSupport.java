/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.simdvec.internal.vectorization;

import org.apache.lucene.util.BytesRef;

public interface ESVectorUtilSupport {

    /**
     * The number of bits in bit-quantized query vectors
     */
    short B_QUERY = 4;

    /** Calculates the dot product of the given float arrays. */
    float dotProduct(float[] a, float[] b);

    /** Returns the sum of squared differences of the two vectors. */
    float squareDistance(float[] a, float[] b);

    /** Returns the sum of squared differences over {@code [offset, offset + length)}. */
    float squareDistance(float[] a, float[] b, int offset, int length);

    /** Calculates the cosine of the given byte arrays. */
    float cosine(byte[] a, byte[] b);

    /** Calculates the dot product of the given byte arrays. */
    float dotProduct(byte[] a, byte[] b);

    /** Returns the sum of squared differences of the two vectors. */
    float squareDistance(byte[] a, byte[] b);

    /**
     * Compute dot product between {@code q} and {@code d}
     * @param q query vector, {@link #B_QUERY}-bit quantized and striped (see {@code ESVectorUtil.transposeHalfByte})
     * @param d data vector, 1-bit quantized
     */
    long ipByteBinByte(byte[] q, byte[] d);

    int ipByteBit(byte[] q, byte[] d);

    float ipFloatBit(float[] q, byte[] d);

    float ipFloatByte(float[] q, byte[] d);

    float calculateOSQLoss(
        float[] target,
        float lowerInterval,
        float upperInterval,
        float step,
        float invStep,
        float norm2,
        float lambda,
        int[] quantize
    );

    void calculateOSQGridPoints(float[] target, int[] quantize, int points, float[] pts);

    void centerAndCalculateOSQStatsEuclidean(float[] target, float[] centroid, float[] centered, float[] stats);

    void centerAndCalculateOSQStatsDp(float[] target, float[] centroid, float[] centered, float[] stats);

    float soarDistance(float[] v1, float[] centroid, float[] originalResidual, float soarLambda, float rnorm);

    int quantizeVectorWithIntervals(float[] vector, int[] quantize, float lowInterval, float upperInterval, byte bit);

    void squareDistanceBulk(float[] query, float[] v0, float[] v1, float[] v2, float[] v3, int distancesOffset, float[] distances);

    void squareDistanceBulk(
        float[] query,
        int queryOffset,
        int length,
        float[] v0,
        float[] v1,
        float[] v2,
        float[] v3,
        int distancesOffset,
        float[] distances
    );

    void soarDistanceBulk(
        float[] v1,
        float[] c0,
        float[] c1,
        float[] c2,
        float[] c3,
        float[] originalResidual,
        float soarLambda,
        float rnorm,
        float[] distances
    );

    void packAsBinary(int[] vector, byte[] packed);

    void packDibit(int[] vector, byte[] packed);

    void transposeHalfByte(int[] q, byte[] quantQueryByte);

    int indexOf(byte[] bytes, int offset, int length, byte marker);

    int codePointCount(BytesRef bytesRef);

    boolean contains(byte[] value, int valueOffset, int valueLength, byte[] term, int termOffset, int termLength);

    void linearCombination(float scaleOther, float[] other, float scaleDest, float[] dest);

    void linearCombination(float scaleOther, float[] other, float[] dest);

    float logSumExpNQT(float[] vector);

    float logSumExpNQTDiff(float[] v1, float[] v2, float eps);

    void pow2DiffAndScaleNQT(float[] v1, float[] v2, float a, float eps, float[] result);

    /**
     * Decodes {@code count} values packed as {@code bytesPerValue} little-endian bytes each from
     * {@code in} into {@code out}. {@code bytesPerValue} must be 5, 6, or 7.
     * The caller must ensure {@code in} has at least {@code count * bytesPerValue} readable bytes
     * and {@code out} has at least {@code count} elements.
     */
    void decodeMultiByteLongs(byte[] in, int bytesPerValue, long[] out, int count);

    /**
     * Unpacks 16 longs at {@code arr[offset..offset+15]}, each containing 8 packed byte-sized
     * values, into 128 longs at {@code arr[offset..offset+127]}.
     */
    void expandLongs8(long[] arr, int offset);

    /**
     * Unpacks 32 longs at {@code arr[offset..offset+31]}, each containing 4 packed short-sized
     * values, into 128 longs at {@code arr[offset..offset+127]}.
     */
    void expandLongs16(long[] arr, int offset);

    /**
     * Unpacks 64 longs at {@code arr[offset..offset+63]}, each containing 2 packed int-sized
     * values, into 128 longs at {@code arr[offset..offset+127]}.
     */
    void expandLongs32(long[] arr, int offset);

    /**
     * Like {@link #expandLongs8} but leaves pairs of byte-sized values packed into each output long
     * in the decodeTo32 representation used by {@code ForUtil.decodeTo32}.
     * Output occupies {@code arr[offset..offset+63]}.
     */
    void expandLongs8To32(long[] arr, int offset);

    /**
     * Like {@link #expandLongs16} but leaves pairs of short-sized values packed into each output
     * long in the decodeTo32 representation used by {@code ForUtil.decodeTo32}.
     * Output occupies {@code arr[offset..offset+63]}.
     */
    void expandLongs16To32(long[] arr, int offset);

    /**
     * Packs 128 longs at {@code arr[offset..offset+127]} into 16 longs at
     * {@code arr[offset..offset+15]} by OR-ing 8 byte-sized values per output long.
     */
    void collapseLongs8(long[] arr, int offset);

    /**
     * Packs 128 longs at {@code arr[offset..offset+127]} into 32 longs at
     * {@code arr[offset..offset+31]} by OR-ing 4 short-sized values per output long.
     */
    void collapseLongs16(long[] arr, int offset);

    /**
     * Packs 128 longs at {@code arr[offset..offset+127]} into 64 longs at
     * {@code arr[offset..offset+63]} by OR-ing 2 int-sized values per output long.
     */
    void collapseLongs32(long[] arr, int offset);
}
