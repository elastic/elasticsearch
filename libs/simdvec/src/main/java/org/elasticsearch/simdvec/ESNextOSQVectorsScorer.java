/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.simdvec;

import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.BitUtil;
import org.apache.lucene.util.VectorUtil;

import java.io.IOException;

import static org.apache.lucene.index.VectorSimilarityFunction.EUCLIDEAN;
import static org.apache.lucene.index.VectorSimilarityFunction.MAXIMUM_INNER_PRODUCT;

/** Scorer for quantized vectors stored as an {@link IndexInput}. */
public class ESNextOSQVectorsScorer {

    public static final int BULK_SIZE = 16;

    protected static final float[] BIT_SCALES = new float[] {
        1f,
        1f / ((1 << 2) - 1),
        1f / ((1 << 3) - 1),
        1f / ((1 << 4) - 1),
        1f / ((1 << 5) - 1),
        1f / ((1 << 6) - 1),
        1f / ((1 << 7) - 1),
        1f / ((1 << 8) - 1), };

    /** The wrapper {@link IndexInput}. */
    protected final IndexInput in;

    protected final byte queryBits;
    protected final byte indexBits;
    protected final int length;
    protected final int dimensions;

    protected final float[] lowerIntervals = new float[BULK_SIZE];
    protected final float[] upperIntervals = new float[BULK_SIZE];
    protected final int[] targetComponentSums = new int[BULK_SIZE];
    protected final float[] additionalCorrections = new float[BULK_SIZE];

    /** Sole constructor, called by sub-classes. */
    public ESNextOSQVectorsScorer(IndexInput in, byte queryBits, byte indexBits, int dimensions, int dataLength) {
        if (queryBits != 4 || (indexBits != 1 && indexBits != 2 && indexBits != 4)) {
            throw new IllegalArgumentException("Only asymmetric 4-bit query and 1, 2 or 4-bit index supported");
        }
        this.in = in;
        this.queryBits = queryBits;
        this.indexBits = indexBits;
        this.dimensions = dimensions;
        this.length = dataLength;
    }

    /**
     * compute the quantize distance between the provided quantized query and the quantized vector
     * that is read from the wrapped {@link IndexInput}.
     */
    public long quantizeScore(byte[] q) throws IOException {
        if (indexBits == 1) {
            if (queryBits == 4) {
                return quantized4BitScore(q, length);
            }
            throw new IllegalArgumentException("Only asymmetric 4-bit query supported for 1-bit index");
        }
        if (indexBits == 2) {
            if (queryBits == 4) {
                return quantized4BitScore2BitIndex(q);
            }
        }
        if (indexBits == 4) {
            if (queryBits == 4) {
                return quantized4BitScoreSymmetric(q);
            }
        }
        throw new IllegalArgumentException("Only 1-bit index supported");
    }

    private long quantized4BitScoreSymmetric(byte[] q) throws IOException {
        assert q.length == length : "length mismatch q " + q.length + " vs " + length;
        assert length % 4 == 0 : "length must be multiple of 4 for 4-bit index length: " + length + " dimensions: " + dimensions;
        int stripe0 = (int) quantized4BitScore(q, length / 4);
        int stripe1 = (int) quantized4BitScore(q, length / 4);
        int stripe2 = (int) quantized4BitScore(q, length / 4);
        int stripe3 = (int) quantized4BitScore(q, length / 4);
        return stripe0 + ((long) stripe1 << 1) + ((long) stripe2 << 2) + ((long) stripe3 << 3);
    }

    private long quantized4BitScore2BitIndex(byte[] q) throws IOException {
        assert q.length == length * 2;
        assert length % 2 == 0 : "length must be even for 2-bit index length: " + length + " dimensions: " + dimensions;
        int lower = (int) quantized4BitScore(q, length / 2);
        int upper = (int) quantized4BitScore(q, length / 2);
        return lower + ((long) upper << 1);
    }

    private long quantized4BitScore(byte[] q, int length) throws IOException {
        assert q.length == length * 4;
        final int size = length;
        long subRet0 = 0;
        long subRet1 = 0;
        long subRet2 = 0;
        long subRet3 = 0;
        int r = 0;
        for (final int upperBound = size & -Long.BYTES; r < upperBound; r += Long.BYTES) {
            final long value = in.readLong();
            subRet0 += Long.bitCount((long) BitUtil.VH_LE_LONG.get(q, r) & value);
            subRet1 += Long.bitCount((long) BitUtil.VH_LE_LONG.get(q, r + size) & value);
            subRet2 += Long.bitCount((long) BitUtil.VH_LE_LONG.get(q, r + 2 * size) & value);
            subRet3 += Long.bitCount((long) BitUtil.VH_LE_LONG.get(q, r + 3 * size) & value);
        }
        for (final int upperBound = size & -Integer.BYTES; r < upperBound; r += Integer.BYTES) {
            final int value = in.readInt();
            subRet0 += Integer.bitCount((int) BitUtil.VH_LE_INT.get(q, r) & value);
            subRet1 += Integer.bitCount((int) BitUtil.VH_LE_INT.get(q, r + size) & value);
            subRet2 += Integer.bitCount((int) BitUtil.VH_LE_INT.get(q, r + 2 * size) & value);
            subRet3 += Integer.bitCount((int) BitUtil.VH_LE_INT.get(q, r + 3 * size) & value);
        }
        for (; r < size; r++) {
            final byte value = in.readByte();
            subRet0 += Integer.bitCount((q[r] & value) & 0xFF);
            subRet1 += Integer.bitCount((q[r + size] & value) & 0xFF);
            subRet2 += Integer.bitCount((q[r + 2 * size] & value) & 0xFF);
            subRet3 += Integer.bitCount((q[r + 3 * size] & value) & 0xFF);
        }
        return subRet0 + (subRet1 << 1) + (subRet2 << 2) + (subRet3 << 3);
    }

    /**
     * compute the quantize distance between the provided quantized query and the quantized vectors
     * that are read from the wrapped {@link IndexInput}. The number of quantized vectors to read is
     * determined by {code count} and the results are stored in the provided {@code scores} array.
     */
    public void quantizeScoreBulk(byte[] q, int count, float[] scores) throws IOException {
        if (indexBits == 1) {
            if (queryBits == 4) {
                for (int i = 0; i < count; i++) {
                    scores[i] = quantizeScore(q);
                }
                return;
            }
            throw new IllegalArgumentException("Only asymmetric 4-bit query supported for 1-bit index");
        }
        if (indexBits == 2) {
            if (queryBits == 4) {
                for (int i = 0; i < count; i++) {
                    scores[i] = quantizeScore(q);
                }
                return;
            }
            throw new IllegalArgumentException("Only asymmetric 4-bit query supported for 2-bit index");
        }
        if (indexBits == 4) {
            if (queryBits == 4) {
                for (int i = 0; i < count; i++) {
                    scores[i] = quantizeScore(q);
                }
                return;
            }
            throw new IllegalArgumentException("Only symmetric 4-bit query supported for 4-bit index");
        }
    }

    /**
     * Computes the score by applying the necessary corrections to the provided quantized distance.
     */
    public float score(
        float queryLowerInterval,
        float queryUpperInterval,
        int queryComponentSum,
        float queryAdditionalCorrection,
        VectorSimilarityFunction similarityFunction,
        float centroidDp,
        float lowerInterval,
        float upperInterval,
        int targetComponentSum,
        float additionalCorrection,
        float qcDist
    ) {
        float ax = lowerInterval;
        // Here we assume `lx` is simply bit vectors, so the scaling isn't necessary
        float lx = (upperInterval - ax) * BIT_SCALES[indexBits - 1];
        float ay = queryLowerInterval;
        float ly = (queryUpperInterval - ay) * BIT_SCALES[queryBits - 1];
        float y1 = queryComponentSum;
        float score = ax * ay * dimensions + ay * lx * (float) targetComponentSum + ax * ly * y1 + lx * ly * qcDist;
        // For euclidean, we need to invert the score and apply the additional correction, which is
        // assumed to be the squared l2norm of the centroid centered vectors.
        if (similarityFunction == EUCLIDEAN) {
            score = queryAdditionalCorrection + additionalCorrection - 2 * score;
            return Math.max(1 / (1f + score), 0);
        } else {
            // For cosine and max inner product, we need to apply the additional correction, which is
            // assumed to be the non-centered dot-product between the vector and the centroid
            score += queryAdditionalCorrection + additionalCorrection - centroidDp;
            if (similarityFunction == MAXIMUM_INNER_PRODUCT) {
                return VectorUtil.scaleMaxInnerProductScore(score);
            }
            return Math.max((1f + score) / 2f, 0);
        }
    }

    /**
     * compute the distance between the provided quantized query and the quantized vectors that are
     * read from the wrapped {@link IndexInput}.
     *
     * <p>The number of vectors to score is defined by {@link #BULK_SIZE}. The expected format of the
     * input is as follows: First the quantized vectors are read from the input,then all the lower
     * intervals as floats, then all the upper intervals as floats, then all the target component sums
     * as shorts, and finally all the additional corrections as floats.
     *
     * <p>The results are stored in the provided scores array.
     */
    public float scoreBulk(
        byte[] q,
        float queryLowerInterval,
        float queryUpperInterval,
        int queryComponentSum,
        float queryAdditionalCorrection,
        VectorSimilarityFunction similarityFunction,
        float centroidDp,
        float[] scores
    ) throws IOException {
        quantizeScoreBulk(q, BULK_SIZE, scores);
        in.readFloats(lowerIntervals, 0, BULK_SIZE);
        in.readFloats(upperIntervals, 0, BULK_SIZE);
        for (int i = 0; i < BULK_SIZE; i++) {
            targetComponentSums[i] = Short.toUnsignedInt(in.readShort());
        }
        in.readFloats(additionalCorrections, 0, BULK_SIZE);
        float maxScore = Float.NEGATIVE_INFINITY;
        for (int i = 0; i < BULK_SIZE; i++) {
            scores[i] = score(
                queryLowerInterval,
                queryUpperInterval,
                queryComponentSum,
                queryAdditionalCorrection,
                similarityFunction,
                centroidDp,
                lowerIntervals[i],
                upperIntervals[i],
                targetComponentSums[i],
                additionalCorrections[i],
                scores[i]
            );
            if (scores[i] > maxScore) {
                maxScore = scores[i];
            }
        }
        return maxScore;
    }
}
