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
import org.apache.lucene.util.VectorUtil;

import java.io.IOException;

import static org.apache.lucene.index.VectorSimilarityFunction.EUCLIDEAN;
import static org.apache.lucene.index.VectorSimilarityFunction.MAXIMUM_INNER_PRODUCT;

public abstract class ES93BinaryQuantizedVectorScorer {

    protected static final float FOUR_BIT_SCALE = 1f / ((1 << 4) - 1);
    protected final int dimensions;
    protected final int numBytes;
    protected final int byteSize;

    public ES93BinaryQuantizedVectorScorer(int dimensions, int numBytes) {
        this.dimensions = dimensions;
        this.numBytes = numBytes;
        this.byteSize = numBytes + (Float.BYTES * 3) + Short.BYTES;
    }

    public abstract float score(
        byte[] q,
        float queryLowerInterval,
        float queryUpperInterval,
        int queryQuantizedComponentSum,
        float queryAdditionalCorrection,
        VectorSimilarityFunction similarityFunction,
        float centroidDp,
        int targetOrd
    ) throws IOException;

    public float scoreBulk(
        byte[] q,
        float queryLowerInterval,
        float queryUpperInterval,
        int queryQuantizedComponentSum,
        float queryAdditionalCorrection,
        VectorSimilarityFunction similarityFunction,
        float centroidDp,
        int[] nodes,
        float[] scores,
        int bulkSize
    ) throws IOException {
        float maxScore = Float.NEGATIVE_INFINITY;
        for (var i = 0; i < bulkSize; i++) {
            scores[i] = score(
                q,
                queryLowerInterval,
                queryUpperInterval,
                queryQuantizedComponentSum,
                queryAdditionalCorrection,
                similarityFunction,
                centroidDp,
                nodes[i]
            );
            maxScore = Math.max(maxScore, scores[i]);
        }
        return maxScore;
    }

    protected static float applyCorrections(
        int dims,
        VectorSimilarityFunction similarityFunction,
        float centroidDp,
        float qcDist,
        float queryLowerInterval,
        float queryUpperInterval,
        float queryAdditionalCorrection,
        int queryQuantizedComponentSum,
        float indexLowerInterval,
        float indexUpperInterval,
        float indexAdditionalCorrection,
        int indexQuantizedComponentSum
    ) {
        float x1 = indexQuantizedComponentSum;
        float ax = indexLowerInterval;
        // Here we assume `lx` is simply bit vectors, so the scaling isn't necessary
        float lx = indexUpperInterval - ax;
        float ay = queryLowerInterval;
        float ly = (queryUpperInterval - ay) * FOUR_BIT_SCALE;
        float y1 = queryQuantizedComponentSum;
        float score = ax * ay * dims + ay * lx * x1 + ax * ly * y1 + lx * ly * qcDist;
        // For euclidean, we need to invert the score and apply the additional correction, which is
        // assumed to be the squared l2norm of the centroid centered vectors.
        if (similarityFunction == EUCLIDEAN) {
            score = queryAdditionalCorrection + indexAdditionalCorrection - 2 * score;
            return Math.max(VectorUtil.normalizeDistanceToUnitInterval(score), 0);
        } else {
            // For dot product/cosine and max inner product, we need to apply the additional correction, which is
            // assumed to be the non-centered dot-product between the vector and the centroid
            score += queryAdditionalCorrection + indexAdditionalCorrection - centroidDp;
            if (similarityFunction == MAXIMUM_INNER_PRODUCT) {
                return VectorUtil.scaleMaxInnerProductScore(score);
            }
            return VectorUtil.normalizeToUnitInterval(score);
        }
    }
}
