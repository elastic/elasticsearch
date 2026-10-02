/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.simdvec.internal;

import org.apache.lucene.codecs.lucene104.Lucene104ScalarQuantizedVectorScorer;
import org.apache.lucene.util.VectorUtil;
import org.elasticsearch.simdvec.VectorSimilarityType;

/**
 * Shared correction formulas for int4 packed-nibble scoring. Used by both the
 * scorer supplier (ordinal-vs-ordinal) and the query-time scorer paths.
 * Correction formulas are the same as in {@link Lucene104ScalarQuantizedVectorScorer}, specialized for the INT4 case.
 */
final class Int4Corrections {

    static final float LIMIT_SCALE = 1f / ((1 << 4) - 1);

    @FunctionalInterface
    interface SingleCorrection {
        float apply(
            int dims,
            float rawScore,
            float docLower,
            float docUpper,
            float docAdditional,
            int docComponentSum,
            float centroidDP,
            float qLower,
            float qUpper,
            float qAdditional,
            int qComponentSum
        );
    }

    static SingleCorrection singleCorrectionFor(VectorSimilarityType type) {
        return switch (type) {
            case COSINE, DOT_PRODUCT -> Int4Corrections::dotProduct;
            case EUCLIDEAN -> Int4Corrections::euclidean;
            case MAXIMUM_INNER_PRODUCT -> Int4Corrections::maxInnerProduct;
        };
    }

    private Int4Corrections() {}

    static float dotProduct(
        int dims,
        float rawScore,
        float docLower,
        float docUpper,
        float docAdditional,
        int docComponentSum,
        float centroidDP,
        float queryLower,
        float queryUpper,
        float queryAdditional,
        int queryComponentSum
    ) {
        float ax = docLower;
        float lx = (docUpper - ax) * LIMIT_SCALE;
        float ay = queryLower;
        float ly = (queryUpper - ay) * LIMIT_SCALE;
        float score = ax * ay * dims + ay * lx * docComponentSum + ax * ly * queryComponentSum + lx * ly * rawScore;
        score += queryAdditional + docAdditional - centroidDP;
        return VectorUtil.normalizeToUnitInterval(Math.clamp(score, -1, 1));
    }

    static float euclidean(
        int dims,
        float rawScore,
        float docLower,
        float docUpper,
        float docAdditional,
        int docComponentSum,
        float centroidDP,
        float queryLower,
        float queryUpper,
        float queryAdditional,
        int queryComponentSum
    ) {
        float ax = docLower;
        float lx = (docUpper - ax) * LIMIT_SCALE;
        float ay = queryLower;
        float ly = (queryUpper - ay) * LIMIT_SCALE;
        float score = ax * ay * dims + ay * lx * docComponentSum + ax * ly * queryComponentSum + lx * ly * rawScore;
        score = queryAdditional + docAdditional - 2 * score;
        return VectorUtil.normalizeDistanceToUnitInterval(Math.max(score, 0f));
    }

    static float maxInnerProduct(
        int dims,
        float rawScore,
        float docLower,
        float docUpper,
        float docAdditional,
        int docComponentSum,
        float centroidDP,
        float queryLower,
        float queryUpper,
        float queryAdditional,
        int queryComponentSum
    ) {
        float ax = docLower;
        float lx = (docUpper - ax) * LIMIT_SCALE;
        float ay = queryLower;
        float ly = (queryUpper - ay) * LIMIT_SCALE;
        float score = ax * ay * dims + ay * lx * docComponentSum + ax * ly * queryComponentSum + lx * ly * rawScore;
        score += queryAdditional + docAdditional - centroidDP;
        return VectorUtil.scaleMaxInnerProductScore(score);
    }
}
