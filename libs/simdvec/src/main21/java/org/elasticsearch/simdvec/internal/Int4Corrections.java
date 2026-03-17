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
import org.apache.lucene.codecs.lucene104.QuantizedByteVectorValues;
import org.apache.lucene.util.VectorUtil;
import org.elasticsearch.simdvec.VectorSimilarityType;

import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;

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
            QuantizedByteVectorValues values,
            int dims,
            float rawScore,
            int ord,
            float qLower,
            float qUpper,
            float qAdditional,
            int qComponentSum
        ) throws IOException;
    }

    @FunctionalInterface
    interface BulkCorrection {
        float apply(
            QuantizedByteVectorValues values,
            int dims,
            MemorySegment scores,
            MemorySegment ordinals,
            int numNodes,
            float qLower,
            float qUpper,
            float qAdditional,
            int qComponentSum
        ) throws IOException;
    }

    static SingleCorrection singleCorrectionFor(VectorSimilarityType type) {
        return switch (type) {
            case COSINE, DOT_PRODUCT -> Int4Corrections::dotProduct;
            case EUCLIDEAN -> Int4Corrections::euclidean;
            case MAXIMUM_INNER_PRODUCT -> Int4Corrections::maxInnerProduct;
        };
    }

    static BulkCorrection bulkCorrectionFor(VectorSimilarityType type) {
        return switch (type) {
            case COSINE, DOT_PRODUCT -> Int4Corrections::dotProductBulk;
            case EUCLIDEAN -> Int4Corrections::euclideanBulk;
            case MAXIMUM_INNER_PRODUCT -> Int4Corrections::maxInnerProductBulk;
        };
    }

    private Int4Corrections() {}

    static float dotProduct(
        QuantizedByteVectorValues values,
        int dims,
        float rawScore,
        int ord,
        float queryLower,
        float queryUpper,
        float queryAdditional,
        int queryComponentSum
    ) throws IOException {
        var ct = values.getCorrectiveTerms(ord);
        float ax = ct.lowerInterval();
        float lx = (ct.upperInterval() - ax) * LIMIT_SCALE;
        float ay = queryLower;
        float ly = (queryUpper - ay) * LIMIT_SCALE;
        float score = ax * ay * dims + ay * lx * ct.quantizedComponentSum() + ax * ly * queryComponentSum + lx * ly * rawScore;
        score += queryAdditional + ct.additionalCorrection() - values.getCentroidDP();
        return VectorUtil.normalizeToUnitInterval(Math.clamp(score, -1, 1));
    }

    static float dotProductBulk(
        QuantizedByteVectorValues values,
        int dims,
        MemorySegment scoreSeg,
        MemorySegment ordinalsSeg,
        int numNodes,
        float queryLower,
        float queryUpper,
        float queryAdditional,
        int queryComponentSum
    ) throws IOException {
        float ay = queryLower;
        float ly = (queryUpper - ay) * LIMIT_SCALE;
        float max = Float.NEGATIVE_INFINITY;
        for (int i = 0; i < numNodes; i++) {
            float raw = scoreSeg.getAtIndex(ValueLayout.JAVA_FLOAT, i);
            int nodeOrd = ordinalsSeg.getAtIndex(ValueLayout.JAVA_INT, i);
            var ct = values.getCorrectiveTerms(nodeOrd);
            float ax = ct.lowerInterval();
            float lx = (ct.upperInterval() - ax) * LIMIT_SCALE;
            float score = ax * ay * dims + ay * lx * ct.quantizedComponentSum() + ax * ly * queryComponentSum + lx * ly * raw;
            score += queryAdditional + ct.additionalCorrection() - values.getCentroidDP();
            float normalized = VectorUtil.normalizeToUnitInterval(Math.clamp(score, -1, 1));
            scoreSeg.setAtIndex(ValueLayout.JAVA_FLOAT, i, normalized);
            max = Math.max(max, normalized);
        }
        return max;
    }

    static float euclidean(
        QuantizedByteVectorValues values,
        int dims,
        float rawScore,
        int ord,
        float queryLower,
        float queryUpper,
        float queryAdditional,
        int queryComponentSum
    ) throws IOException {
        var ct = values.getCorrectiveTerms(ord);
        float ax = ct.lowerInterval();
        float lx = (ct.upperInterval() - ax) * LIMIT_SCALE;
        float ay = queryLower;
        float ly = (queryUpper - ay) * LIMIT_SCALE;
        float score = ax * ay * dims + ay * lx * ct.quantizedComponentSum() + ax * ly * queryComponentSum + lx * ly * rawScore;
        score = queryAdditional + ct.additionalCorrection() - 2 * score;
        return VectorUtil.normalizeDistanceToUnitInterval(Math.max(score, 0f));
    }

    static float euclideanBulk(
        QuantizedByteVectorValues values,
        int dims,
        MemorySegment scoreSeg,
        MemorySegment ordinalsSeg,
        int numNodes,
        float queryLower,
        float queryUpper,
        float queryAdditional,
        int queryComponentSum
    ) throws IOException {
        float ay = queryLower;
        float ly = (queryUpper - ay) * LIMIT_SCALE;
        float max = Float.NEGATIVE_INFINITY;
        for (int i = 0; i < numNodes; i++) {
            float raw = scoreSeg.getAtIndex(ValueLayout.JAVA_FLOAT, i);
            int nodeOrd = ordinalsSeg.getAtIndex(ValueLayout.JAVA_INT, i);
            var ct = values.getCorrectiveTerms(nodeOrd);
            float ax = ct.lowerInterval();
            float lx = (ct.upperInterval() - ax) * LIMIT_SCALE;
            float score = ax * ay * dims + ay * lx * ct.quantizedComponentSum() + ax * ly * queryComponentSum + lx * ly * raw;
            score = queryAdditional + ct.additionalCorrection() - 2 * score;
            float normalized = VectorUtil.normalizeDistanceToUnitInterval(Math.max(score, 0f));
            scoreSeg.setAtIndex(ValueLayout.JAVA_FLOAT, i, normalized);
            max = Math.max(max, normalized);
        }
        return max;
    }

    static float maxInnerProduct(
        QuantizedByteVectorValues values,
        int dims,
        float rawScore,
        int ord,
        float queryLower,
        float queryUpper,
        float queryAdditional,
        int queryComponentSum
    ) throws IOException {
        var ct = values.getCorrectiveTerms(ord);
        float ax = ct.lowerInterval();
        float lx = (ct.upperInterval() - ax) * LIMIT_SCALE;
        float ay = queryLower;
        float ly = (queryUpper - ay) * LIMIT_SCALE;
        float score = ax * ay * dims + ay * lx * ct.quantizedComponentSum() + ax * ly * queryComponentSum + lx * ly * rawScore;
        score += queryAdditional + ct.additionalCorrection() - values.getCentroidDP();
        return VectorUtil.scaleMaxInnerProductScore(score);
    }

    static float maxInnerProductBulk(
        QuantizedByteVectorValues values,
        int dims,
        MemorySegment scoreSeg,
        MemorySegment ordinalsSeg,
        int numNodes,
        float queryLower,
        float queryUpper,
        float queryAdditional,
        int queryComponentSum
    ) throws IOException {
        float ay = queryLower;
        float ly = (queryUpper - ay) * LIMIT_SCALE;
        float max = Float.NEGATIVE_INFINITY;
        for (int i = 0; i < numNodes; i++) {
            float raw = scoreSeg.getAtIndex(ValueLayout.JAVA_FLOAT, i);
            int nodeOrd = ordinalsSeg.getAtIndex(ValueLayout.JAVA_INT, i);
            var ct = values.getCorrectiveTerms(nodeOrd);
            float ax = ct.lowerInterval();
            float lx = (ct.upperInterval() - ax) * LIMIT_SCALE;
            float score = ax * ay * dims + ay * lx * ct.quantizedComponentSum() + ax * ly * queryComponentSum + lx * ly * raw;
            score += queryAdditional + ct.additionalCorrection() - values.getCentroidDP();
            float normalizedScore = VectorUtil.scaleMaxInnerProductScore(score);
            scoreSeg.setAtIndex(ValueLayout.JAVA_FLOAT, i, normalizedScore);
            max = Math.max(max, normalizedScore);
        }
        return max;
    }
}
