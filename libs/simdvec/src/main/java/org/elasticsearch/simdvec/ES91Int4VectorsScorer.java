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
import org.apache.lucene.util.VectorUtil;

import java.io.IOException;

import static org.apache.lucene.index.VectorSimilarityFunction.EUCLIDEAN;
import static org.apache.lucene.index.VectorSimilarityFunction.MAXIMUM_INNER_PRODUCT;

/** Scorer for quantized vectors stored as an {@link IndexInput}.
 * <p>
 * Similar to {@link org.apache.lucene.util.VectorUtil#int4DotProduct(byte[], byte[])} but
 * one value is read directly from an {@link IndexInput}.
 *
 * */
public class ES91Int4VectorsScorer {

    public static final int BULK_SIZE = 16;
    protected static final float FOUR_BIT_SCALE = 1f / ((1 << 4) - 1);

    /** The wrapper {@link IndexInput}. */
    protected final IndexInput in;
    protected final int dimensions;
    protected byte[] scratch;

    protected final float[] lowerIntervals = new float[BULK_SIZE];
    protected final float[] upperIntervals = new float[BULK_SIZE];
    protected final int[] targetComponentSums = new int[BULK_SIZE];
    protected final float[] additionalCorrections = new float[BULK_SIZE];

    /** Sole constructor, called by sub-classes. */
    public ES91Int4VectorsScorer(IndexInput in, int dimensions) {
        this.in = in;
        this.dimensions = dimensions;
        scratch = new byte[dimensions];
    }

    /**
     * compute the quantize distance between the provided quantized query and the quantized vector
     * that is read from the wrapped {@link IndexInput}.
     */
    public long int4DotProduct(byte[] b) throws IOException {
        in.readBytes(scratch, 0, dimensions);
        int total = 0;
        for (int i = 0; i < dimensions; i++) {
            total += scratch[i] * b[i];
        }
        return total;
    }

    /**
     * compute the quantize distance between the provided quantized query and the quantized vectors
     * that are read from the wrapped {@link IndexInput}. The number of quantized vectors to read is
     * determined by {code count} and the results are stored in the provided {@code scores} array.
     */
    public void int4DotProductBulk(byte[] b, int count, float[] scores) throws IOException {
        for (int i = 0; i < count; i++) {
            scores[i] = int4DotProduct(b);
        }
    }

    /**
     * Computes the score by applying the necessary corrections to the provided quantized distance.
     */
    public float score(
        byte[] q,
        float queryLowerInterval,
        float queryUpperInterval,
        int queryComponentSum,
        float queryAdditionalCorrection,
        VectorSimilarityFunction similarityFunction,
        float centroidDp
    ) throws IOException {
        float score = int4DotProduct(q);
        in.readFloats(lowerIntervals, 0, 3);
        int addition = Short.toUnsignedInt(in.readShort());
        return applyCorrections(
            queryLowerInterval,
            queryUpperInterval,
            queryComponentSum,
            queryAdditionalCorrection,
            similarityFunction,
            centroidDp,
            lowerIntervals[0],
            lowerIntervals[1],
            addition,
            lowerIntervals[2],
            score
        );
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
    public void scoreBulk(
        byte[] q,
        float queryLowerInterval,
        float queryUpperInterval,
        int queryComponentSum,
        float queryAdditionalCorrection,
        VectorSimilarityFunction similarityFunction,
        float centroidDp,
        float[] scores
    ) throws IOException {
        int4DotProductBulk(q, BULK_SIZE, scores);
        in.readFloats(lowerIntervals, 0, BULK_SIZE);
        in.readFloats(upperIntervals, 0, BULK_SIZE);
        for (int i = 0; i < BULK_SIZE; i++) {
            targetComponentSums[i] = Short.toUnsignedInt(in.readShort());
        }
        in.readFloats(additionalCorrections, 0, BULK_SIZE);
        for (int i = 0; i < BULK_SIZE; i++) {
            scores[i] = applyCorrections(
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
        }
    }

    /**
     * Computes the score by applying the necessary corrections to the provided quantized distance.
     */
    public float applyCorrections(
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
        float lx = upperInterval - ax;
        float ay = queryLowerInterval;
        float ly = (queryUpperInterval - ay) * FOUR_BIT_SCALE;
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
}
