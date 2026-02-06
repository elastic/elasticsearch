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
import org.apache.lucene.store.MemorySegmentAccessInput;
import org.apache.lucene.util.VectorUtil;
import org.apache.lucene.util.hnsw.RandomVectorScorer;

import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.util.Optional;

import static org.elasticsearch.simdvec.internal.Similarities.dotProduct7u;
import static org.elasticsearch.simdvec.internal.Similarities.dotProduct7uBulkWithOffsets;

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
        if ((input instanceof MemorySegmentAccessInput) == false) {
            return Optional.empty();
        }
        MemorySegmentAccessInput msInput = (MemorySegmentAccessInput) input;
        checkInvariants(values.size(), values.getVectorByteLength(), input);

        return switch (sim) {
            case COSINE, DOT_PRODUCT -> Optional.of(
                new DotProductScorer(
                    msInput,
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
                    msInput,
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
                    msInput,
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
    final MemorySegmentAccessInput input;
    final int vectorByteSize;
    final MemorySegment query;
    final float lowerInterval;
    final float upperInterval;
    final float additionalCorrection;
    final int quantizedComponentSum;
    byte[] scratch;

    Int7uOSQVectorScorer(
        MemorySegmentAccessInput input,
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
        this.query = MemorySegment.ofArray(quantizedQuery);
        this.lowerInterval = lowerInterval;
        this.upperInterval = upperInterval;
        this.additionalCorrection = additionalCorrection;
        this.quantizedComponentSum = quantizedComponentSum;
    }

    abstract float applyCorrections(float rawScore, int ord) throws IOException;

    abstract float applyCorrectionsBulk(float[] scores, int[] ords, int numNodes) throws IOException;

    @Override
    public float score(int node) throws IOException {
        checkOrdinal(node);
        int dotProduct = dotProduct7u(query, getSegment(node), vectorByteSize);
        return applyCorrections(dotProduct, node);
    }

    @Override
    public float bulkScore(int[] nodes, float[] scores, int numNodes) throws IOException {
        MemorySegment vectorsSeg = input.segmentSliceOrNull(0, input.length());
        if (vectorsSeg == null) {
            return super.bulkScore(nodes, scores, numNodes);
        } else {
            var ordinalsSeg = MemorySegment.ofArray(nodes);
            var scoresSeg = MemorySegment.ofArray(scores);

            var vectorPitch = vectorByteSize + 3 * Float.BYTES + Integer.BYTES;
            dotProduct7uBulkWithOffsets(vectorsSeg, query, vectorByteSize, vectorPitch, ordinalsSeg, numNodes, scoresSeg);
            return applyCorrectionsBulk(scores, nodes, numNodes);
        }
    }

    final MemorySegment getSegment(int ord) throws IOException {
        checkOrdinal(ord);
        long byteOffset = (long) ord * (vectorByteSize + 3 * Float.BYTES + Integer.BYTES);
        MemorySegment seg = input.segmentSliceOrNull(byteOffset, vectorByteSize);
        if (seg == null) {
            if (scratch == null) {
                scratch = new byte[vectorByteSize];
            }
            input.readBytes(byteOffset, scratch, 0, vectorByteSize);
            seg = MemorySegment.ofArray(scratch);
        }
        return seg;
    }

    final void checkOrdinal(int ord) {
        if (ord < 0 || ord >= maxOrd()) {
            throw new IllegalArgumentException("illegal ordinal: " + ord);
        }
    }

    public static final class DotProductScorer extends Int7uOSQVectorScorer {
        public DotProductScorer(
            MemorySegmentAccessInput input,
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
            MemorySegmentAccessInput input,
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
            MemorySegmentAccessInput input,
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
