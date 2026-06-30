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
import org.elasticsearch.simdvec.MultiBFloat16VectorsSource;
import org.elasticsearch.simdvec.MultiByteVectorsSource;
import org.elasticsearch.simdvec.MultiFloatVectorsSource;

import java.nio.ByteOrder;

public interface ESVectorUtilSupport {

    /**
     * The number of bits in bit-quantized query vectors
     */
    short B_QUERY = 4;

    /** Converts bfloat16s to floats */
    void bFloat16ToFloat(byte[] bfBytes, int bfOffset, float[] floats, int floatOffset, int floatCount, ByteOrder byteOrder);

    /** Converts floats to bfloat16s */
    void floatToBFloat16(float[] floats, int floatOffset, byte[] bfBytes, int bfOffset, int floatCount, ByteOrder byteOrder);

    /** Calculates the dot product of the given float arrays. */
    float dotProduct(float[] a, float[] b);

    /** Calculates the dot product over {@code [offset, offset + length)}. */
    float dotProduct(float[] a, float[] b, int offset, int length);

    /** L2-normalizes {@code v[offset:offset + length)} in place. A zero prefix is a no-op. */
    void l2Normalize(float[] v, int offset, int length);

    /** Returns the sum of squared differences of the two vectors. */
    float squareDistance(float[] a, float[] b);

    /** Returns the sum of squared differences over {@code [offset, offset + length)}. */
    float squareDistance(float[] a, float[] b, int offset, int length);

    /** Calculates the cosine of the given byte arrays. */
    float cosine(byte[] a, byte[] b);

    /** Calculates the dot product of the given byte arrays. */
    float dotProduct(byte[] a, byte[] b);

    /** Calculates the dot product over {@code [offset, offset + length)}. */
    float dotProduct(byte[] a, byte[] b, int offset, int length);

    /** L2-normalizes {@code v[offset:offset + length)} in place using signed byte values as real components. */
    void l2Normalize(byte[] v, int offset, int length);

    /** Returns the sum of squared differences of the two vectors. */
    float squareDistance(byte[] a, byte[] b);

    /** Returns the sum of squared differences of the two byte vectors over a sub-range. */
    float squareDistance(byte[] a, byte[] b, int offset, int length);

    float maxSimDotProduct(MultiFloatVectorsSource source, float[][] query, float[] scoresScratch);

    float maxSimDotProduct(MultiBFloat16VectorsSource source, float[][] query, float[] scoresScratch);

    float maxSimDotProduct(MultiByteVectorsSource source, byte[][] query, float[] scoresScratch);

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

    void centerAndCalculateOSQStatsEuclidean(byte[] target, byte[] centroid, float[] centered, float[] stats);

    void centerAndCalculateOSQStatsDp(byte[] target, byte[] centroid, float[] centered, float[] stats);

    float soarDistance(float[] v1, float[] centroid, float[] originalResidual, float soarLambda, float rnorm);

    float soarDistance(byte[] v1, byte[] centroid, float[] originalResidual, float soarLambda, float rnorm);

    int quantizeVectorWithIntervals(float[] vector, int[] quantize, float lowInterval, float upperInterval, byte bit);

    void dotProductBulk(float[] query, float[] v0, float[] v1, float[] v2, float[] v3, int distancesOffset, float[] distances);

    void squareDistanceBulk(
        float[] query,
        int vectorOffset,
        float[] v0,
        float[] v1,
        float[] v2,
        float[] v3,
        int distancesOffset,
        float[] distances,
        int length
    );

    void dotProductBulk(byte[] query, byte[] v0, byte[] v1, byte[] v2, byte[] v3, int distancesOffset, float[] distances);

    void cosineBulk(byte[] query, byte[] v0, byte[] v1, byte[] v2, byte[] v3, int distancesOffset, float[] distances);

    void squareDistanceBulk(
        byte[] query,
        int vectorOffset,
        byte[] v0,
        byte[] v1,
        byte[] v2,
        byte[] v3,
        int distancesOffset,
        float[] distances,
        int length
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

    void soarDistanceBulk(
        byte[] v1,
        byte[] c0,
        byte[] c1,
        byte[] c2,
        byte[] c3,
        float[] originalResidual,
        float soarLambda,
        float rnorm,
        float[] distances
    );

    void packAsBinary(int[] vector, byte[] packed);

    void packDibit(int[] vector, byte[] packed);

    void packDibitQuad(int[] vector, byte[] packed);

    void transposeHalfByte(int[] q, byte[] quantQueryByte);

    int indexOf(byte[] bytes, int offset, int length, byte marker);

    int codePointCount(BytesRef bytesRef);

    boolean contains(byte[] value, int valueOffset, int valueLength, byte[] term, int termOffset, int termLength);

    void inRangeBitmask(long[] values, long lowerValue, long upperValue, long[] matches);

    void linearCombination(float scaleOther, float[] other, float scaleDest, float[] dest);

    void linearCombination(float scaleOther, float[] other, float[] dest);

    void linearCombination(float scaleOther, byte[] other, float scaleDest, float[] dest);

    void linearCombination(float scaleOther, byte[] other, float[] dest);

    float logSumExpNQT(float[] vector);

    float logSumExpNQTDiff(float[] v1, float[] v2, float eps);

    void pow2DiffAndScaleNQT(float[] v1, float[] v2, float a, float eps, float[] result);
}
