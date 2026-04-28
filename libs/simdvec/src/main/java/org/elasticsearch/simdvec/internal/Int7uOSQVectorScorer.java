/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.simdvec.internal;

import org.apache.lucene.codecs.lucene104.QuantizedByteVectorValues;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.store.FilterIndexInput;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.VectorUtil;
import org.apache.lucene.util.hnsw.RandomVectorScorer;
import org.elasticsearch.simdvec.MemorySegmentAccessInputAccess;

import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.util.Optional;

import static org.elasticsearch.simdvec.internal.Similarities.dotProductI7u;
import static org.elasticsearch.simdvec.internal.Similarities.dotProductI7uBulkSparse;
import static org.elasticsearch.simdvec.internal.vectorization.JdkFeatures.SUPPORTS_HEAP_SEGMENTS;

/**
 * JDK-22+ implementation for Int7 OSQ query-time scorers.
 */
public abstract sealed class Int7uOSQVectorScorer extends RandomVectorScorer.AbstractRandomVectorScorer permits
    Int7uOSQVectorScorer.DotProductScorer, Int7uOSQVectorScorer.EuclideanScorer, Int7uOSQVectorScorer.MaxInnerProductScorer {

    private static final float LIMIT_SCALE = 1f / ((1 << 7) - 1);

    public static Optional<RandomVectorScorer> create(
        VectorSimilarityFunction sim,
        QuantizedByteVectorValues values,
        byte[] quantizedQuery,
        float lowerInterval,
        float upperInterval,
        float additionalCorrection,
        int quantizedComponentSum
    ) {
        if (SUPPORTS_HEAP_SEGMENTS == false) {
            return Optional.empty();
        }
        if (quantizedQuery.length != values.getVectorByteLength()) {
            throw new IllegalArgumentException(
                "quantized query length " + quantizedQuery.length + " differs from vector byte length " + values.getVectorByteLength()
            );
        }

        var input = values.getSlice();
        if (input == null) {
            return Optional.empty();
        }
        input = FilterIndexInput.unwrapOnlyTest(input);
        input = MemorySegmentAccessInputAccess.unwrap(input);
        checkInvariants(values.size(), values.getVectorByteLength(), input);
        return switch (sim) {
            case COSINE, DOT_PRODUCT -> Optional.of(
                new DotProductScorer(
                    input,
                    values,
                    quantizedQuery,
                    lowerInterval,
                    upperInterval,
                    additionalCorrection,
                    quantizedComponentSum
                )
            );
            case EUCLIDEAN -> Optional.of(
                new EuclideanScorer(
                    input,
                    values,
                    quantizedQuery,
                    lowerInterval,
                    upperInterval,
                    additionalCorrection,
                    quantizedComponentSum
                )
            );
            case MAXIMUM_INNER_PRODUCT -> Optional.of(
                new MaxInnerProductScorer(
                    input,
                    values,
                    quantizedQuery,
                    lowerInterval,
                    upperInterval,
                    additionalCorrection,
                    quantizedComponentSum
                )
            );
        };
    }

    final QuantizedByteVectorValues values;
    final IndexInput input;
    final int vectorByteSize;
    final long vectorPitch;
    final MemorySegment query;
    final float lowerInterval;
    final float upperInterval;
    final float additionalCorrection;
    final int quantizedComponentSum;
    final FixedSizeScratch scratch;

    Int7uOSQVectorScorer(
        IndexInput input,
        QuantizedByteVectorValues values,
        byte[] quantizedQuery,
        float lowerInterval,
        float upperInterval,
        float additionalCorrection,
        int quantizedComponentSum
    ) {
        super(values);
        this.values = values;
        this.input = input;
        this.vectorByteSize = values.getVectorByteLength();
        this.vectorPitch = vectorByteSize + 3L * Float.BYTES + Integer.BYTES;
        this.query = MemorySegment.ofArray(quantizedQuery);
        this.lowerInterval = lowerInterval;
        this.upperInterval = upperInterval;
        this.additionalCorrection = additionalCorrection;
        this.quantizedComponentSum = quantizedComponentSum;
        this.scratch = new FixedSizeScratch(vectorByteSize);
    }

    abstract float applyCorrections(float rawScore, int ord) throws IOException;

    abstract float applyCorrectionsBulk(float[] scores, int[] ords, int numNodes) throws IOException;

    @Override
    public float score(int node) throws IOException {
        checkOrdinal(node);
        long vectorOffset = (long) node * vectorPitch;
        input.seek(vectorOffset);
        return IndexInputUtils.withSlice(input, vectorByteSize, scratch::getScratch, secondSeg -> {
            int dotProduct = dotProductI7u(query, secondSeg, vectorByteSize);
            return applyCorrections(dotProduct, node);
        });
    }

    @Override
    public float bulkScore(int[] nodes, float[] scores, int numNodes) throws IOException {
        if (numNodes == 0) {
            return Float.NEGATIVE_INFINITY;
        }

        long[] offsets = new long[numNodes];
        for (int i = 0; i < numNodes; i++) {
            offsets[i] = (long) nodes[i] * vectorPitch;
        }

        float[] maxScore = new float[] { Float.NEGATIVE_INFINITY };
        boolean resolved = IndexInputUtils.withSliceAddresses(input, offsets, vectorByteSize, numNodes, addrs -> {
            var scoresSeg = MemorySegment.ofArray(scores);
            dotProductI7uBulkSparse(addrs, query, vectorByteSize, numNodes, scoresSeg);
            maxScore[0] = applyCorrectionsBulk(scores, nodes, numNodes);
        });
        if (resolved == false) {
            // fallback to per-vector scorer
            for (int i = 0; i < numNodes; i++) {
                input.seek(offsets[i]);
                var documentOrdinal = nodes[i];
                scores[i] = IndexInputUtils.withSlice(input, vectorByteSize, scratch::getScratch, documentSeg -> {
                    int rawScore = dotProductI7u(query, documentSeg, vectorByteSize);
                    float adjustedScore = applyCorrections(rawScore, documentOrdinal);
                    maxScore[0] = Math.max(maxScore[0], adjustedScore);
                    return adjustedScore;
                });
            }
        }
        return maxScore[0];
    }

    final void checkOrdinal(int ord) {
        if (ord < 0 || ord >= maxOrd()) {
            throw new IllegalArgumentException("illegal ordinal: " + ord);
        }
    }

    public static final class DotProductScorer extends Int7uOSQVectorScorer {
        public DotProductScorer(
            IndexInput input,
            QuantizedByteVectorValues values,
            byte[] quantizedQuery,
            float lowerInterval,
            float upperInterval,
            float additionalCorrection,
            int quantizedComponentSum
        ) {
            super(input, values, quantizedQuery, lowerInterval, upperInterval, additionalCorrection, quantizedComponentSum);
        }

        @Override
        float applyCorrections(float rawScore, int ord) throws IOException {
            var correctiveTerms = values.getCorrectiveTerms(ord);
            float x1 = correctiveTerms.quantizedComponentSum();
            float ax = correctiveTerms.lowerInterval();
            float lx = (correctiveTerms.upperInterval() - ax) * LIMIT_SCALE;
            float ay = lowerInterval;
            float ly = (upperInterval - ay) * LIMIT_SCALE;
            float y1 = quantizedComponentSum;
            float score = ax * ay * values.dimension() + ay * lx * x1 + ax * ly * y1 + lx * ly * rawScore;
            score += additionalCorrection + correctiveTerms.additionalCorrection() - values.getCentroidDP();
            score = Math.clamp(score, -1, 1);
            return VectorUtil.normalizeToUnitInterval(score);
        }

        @Override
        float applyCorrectionsBulk(float[] scores, int[] ords, int numNodes) throws IOException {
            float ay = lowerInterval;
            float ly = (upperInterval - ay) * LIMIT_SCALE;
            float y1 = quantizedComponentSum;
            float maxScore = Float.NEGATIVE_INFINITY;
            for (int i = 0; i < numNodes; i++) {
                int ord = ords[i];
                var correctiveTerms = values.getCorrectiveTerms(ord);
                float ax = correctiveTerms.lowerInterval();
                float lx = (correctiveTerms.upperInterval() - ax) * LIMIT_SCALE;
                float x1 = correctiveTerms.quantizedComponentSum();
                float score = ax * ay * values.dimension() + ay * lx * x1 + ax * ly * y1 + lx * ly * scores[i];
                score += additionalCorrection + correctiveTerms.additionalCorrection() - values.getCentroidDP();
                score = Math.clamp(score, -1, 1);
                scores[i] = VectorUtil.normalizeToUnitInterval(score);
                if (scores[i] > maxScore) {
                    maxScore = scores[i];
                }
            }
            return maxScore;
        }
    }

    public static final class EuclideanScorer extends Int7uOSQVectorScorer {
        public EuclideanScorer(
            IndexInput input,
            QuantizedByteVectorValues values,
            byte[] quantizedQuery,
            float lowerInterval,
            float upperInterval,
            float additionalCorrection,
            int quantizedComponentSum
        ) {
            super(input, values, quantizedQuery, lowerInterval, upperInterval, additionalCorrection, quantizedComponentSum);
        }

        @Override
        float applyCorrections(float rawScore, int ord) throws IOException {
            var correctiveTerms = values.getCorrectiveTerms(ord);
            float x1 = correctiveTerms.quantizedComponentSum();
            float ax = correctiveTerms.lowerInterval();
            float lx = (correctiveTerms.upperInterval() - ax) * LIMIT_SCALE;
            float ay = lowerInterval;
            float ly = (upperInterval - ay) * LIMIT_SCALE;
            float y1 = quantizedComponentSum;
            float score = ax * ay * values.dimension() + ay * lx * x1 + ax * ly * y1 + lx * ly * rawScore;
            score = additionalCorrection + correctiveTerms.additionalCorrection() - 2 * score;
            return VectorUtil.normalizeDistanceToUnitInterval(Math.max(score, 0f));
        }

        @Override
        float applyCorrectionsBulk(float[] scores, int[] ords, int numNodes) throws IOException {
            float ay = lowerInterval;
            float ly = (upperInterval - ay) * LIMIT_SCALE;
            float y1 = quantizedComponentSum;
            float maxScore = Float.NEGATIVE_INFINITY;
            for (int i = 0; i < numNodes; i++) {
                int ord = ords[i];
                var correctiveTerms = values.getCorrectiveTerms(ord);
                float ax = correctiveTerms.lowerInterval();
                float lx = (correctiveTerms.upperInterval() - ax) * LIMIT_SCALE;
                float x1 = correctiveTerms.quantizedComponentSum();
                float score = ax * ay * values.dimension() + ay * lx * x1 + ax * ly * y1 + lx * ly * scores[i];
                score = additionalCorrection + correctiveTerms.additionalCorrection() - 2 * score;
                scores[i] = VectorUtil.normalizeDistanceToUnitInterval(Math.max(score, 0f));
                if (scores[i] > maxScore) {
                    maxScore = scores[i];
                }
            }
            return maxScore;
        }
    }

    public static final class MaxInnerProductScorer extends Int7uOSQVectorScorer {
        public MaxInnerProductScorer(
            IndexInput input,
            QuantizedByteVectorValues values,
            byte[] quantizedQuery,
            float lowerInterval,
            float upperInterval,
            float additionalCorrection,
            int quantizedComponentSum
        ) {
            super(input, values, quantizedQuery, lowerInterval, upperInterval, additionalCorrection, quantizedComponentSum);
        }

        @Override
        float applyCorrections(float rawScore, int ord) throws IOException {
            var correctiveTerms = values.getCorrectiveTerms(ord);
            float x1 = correctiveTerms.quantizedComponentSum();
            float ax = correctiveTerms.lowerInterval();
            float lx = (correctiveTerms.upperInterval() - ax) * LIMIT_SCALE;
            float ay = lowerInterval;
            float ly = (upperInterval - ay) * LIMIT_SCALE;
            float y1 = quantizedComponentSum;
            float score = ax * ay * values.dimension() + ay * lx * x1 + ax * ly * y1 + lx * ly * rawScore;
            score += additionalCorrection + correctiveTerms.additionalCorrection() - values.getCentroidDP();
            return VectorUtil.scaleMaxInnerProductScore(score);
        }

        @Override
        float applyCorrectionsBulk(float[] scores, int[] ords, int numNodes) throws IOException {
            float ay = lowerInterval;
            float ly = (upperInterval - ay) * LIMIT_SCALE;
            float y1 = quantizedComponentSum;
            float maxScore = Float.NEGATIVE_INFINITY;
            for (int i = 0; i < numNodes; i++) {
                int ord = ords[i];
                var correctiveTerms = values.getCorrectiveTerms(ord);
                float ax = correctiveTerms.lowerInterval();
                float lx = (correctiveTerms.upperInterval() - ax) * LIMIT_SCALE;
                float x1 = correctiveTerms.quantizedComponentSum();
                float score = ax * ay * values.dimension() + ay * lx * x1 + ax * ly * y1 + lx * ly * scores[i];
                score += additionalCorrection + correctiveTerms.additionalCorrection() - values.getCentroidDP();
                scores[i] = VectorUtil.scaleMaxInnerProductScore(score);
                if (scores[i] > maxScore) {
                    maxScore = scores[i];
                }
            }
            return maxScore;
        }
    }

    static void checkInvariants(int maxOrd, int vectorByteLength, IndexInput input) {
        long vectorPitch = vectorByteLength + 3L * Float.BYTES + Integer.BYTES;
        if (input.length() < vectorPitch * maxOrd) {
            throw new IllegalArgumentException("input length is less than expected vector data");
        }
    }
}
