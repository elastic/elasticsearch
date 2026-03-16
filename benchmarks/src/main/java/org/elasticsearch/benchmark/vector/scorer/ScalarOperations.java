/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.benchmark.vector.scorer;

import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.util.VectorUtil;
import org.apache.lucene.util.quantization.OptimizedScalarQuantizer;

import static org.apache.lucene.index.VectorSimilarityFunction.EUCLIDEAN;
import static org.apache.lucene.index.VectorSimilarityFunction.MAXIMUM_INNER_PRODUCT;

/**
 * Basic scalar implementations of similarity operations.
 * <p>
 * It is tricky to get specifically the scalar implementations from Lucene,
 * as it tries to push into Panama implementations. So just re-implement them here.
 */
class ScalarOperations {

    private static final float FOUR_BIT_SCALE = 1f / ((1 << 4) - 1);

    static float cosine(byte[] a, byte[] b) {
        int sum = 0;
        int norm1 = 0;
        int norm2 = 0;

        for (int i = 0; i < a.length; i++) {
            byte elem1 = a[i];
            byte elem2 = b[i];
            sum += elem1 * elem2;
            norm1 += elem1 * elem1;
            norm2 += elem2 * elem2;
        }
        return (float) (sum / Math.sqrt((double) norm1 * (double) norm2));
    }

    static float dotProduct(float[] a, float[] b) {
        float res = 0;
        for (int i = 0; i < a.length; i++) {
            res += a[i] * b[i];
        }
        return res;
    }

    static int dotProduct(byte[] a, byte[] b) {
        int res = 0;
        for (int i = 0; i < a.length; i++) {
            res += a[i] * b[i];
        }
        return res;
    }

    static float squareDistance(float[] a, float[] b) {
        float res = 0;
        for (int i = 0; i < a.length; i++) {
            float diff = a[i] - b[i];
            res += diff * diff;
        }
        return res;
    }

    static int squareDistance(byte[] a, byte[] b) {
        int res = 0;
        for (int i = 0; i < a.length; i++) {
            int diff = a[i] - b[i];
            res += diff * diff;
        }
        return res;
    }

    static int dotProductI4SinglePacked(byte[] unpacked, byte[] packed) {
        int total = 0;
        for (int i = 0; i < packed.length; i++) {
            byte packedByte = packed[i];
            byte unpacked1 = unpacked[i];
            byte unpacked2 = unpacked[i + packed.length];
            total += (packedByte & 0x0F) * unpacked2;
            total += ((packedByte & 0xFF) >> 4) * unpacked1;
        }
        return total;
    }

    public static float applyI4Corrections(
        int rawDot,
        int dims,
        OptimizedScalarQuantizer.QuantizationResult nodeCorrections,
        OptimizedScalarQuantizer.QuantizationResult queryCorrections,
        float centroidDP,
        VectorSimilarityFunction similarityFunction
    ) {
        float ax = nodeCorrections.lowerInterval();
        float lx = (nodeCorrections.upperInterval() - ax) * FOUR_BIT_SCALE;
        float ay = queryCorrections.lowerInterval();
        float ly = (queryCorrections.upperInterval() - ay) * FOUR_BIT_SCALE;
        float x1 = nodeCorrections.quantizedComponentSum();
        float y1 = queryCorrections.quantizedComponentSum();

        float score = ax * ay * dims + ay * lx * x1 + ax * ly * y1 + lx * ly * rawDot;

        // For euclidean, we need to invert the score and apply the additional correction, which is
        // assumed to be the squared l2norm of the centroid centered vectors.
        if (similarityFunction == EUCLIDEAN) {
            score = queryCorrections.additionalCorrection() + nodeCorrections.additionalCorrection() - 2 * score;
            // Ensure that 'score' (the squared euclidean distance) is non-negative. The computed value
            // may be negative as a result of quantization loss.
            return VectorUtil.normalizeDistanceToUnitInterval(Math.max(score, 0f));
        } else {
            // For cosine and max inner product, we need to apply the additional correction, which is
            // assumed to be the non-centered dot-product between the vector and the centroid
            score += queryCorrections.additionalCorrection() + nodeCorrections.additionalCorrection() - centroidDP;
            if (similarityFunction == MAXIMUM_INNER_PRODUCT) {
                return VectorUtil.scaleMaxInnerProductScore(score);
            }
            // Ensure that 'score' (a normalized dot product) is in [-1,1]. The computed value may be out
            // of bounds as a result of quantization loss.
            score = Math.clamp(score, -1, 1);
            return VectorUtil.normalizeToUnitInterval(score);
        }
    }
}
