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
import org.apache.lucene.store.MemorySegmentAccessInput;
import org.apache.lucene.util.VectorUtil;
import org.apache.lucene.util.hnsw.RandomVectorScorerSupplier;
import org.apache.lucene.util.hnsw.UpdateableRandomVectorScorer;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;

import static org.elasticsearch.simdvec.internal.Int7SQVectorScorerSupplier.SUPPORTS_HEAP_SEGMENTS;
import static org.elasticsearch.simdvec.internal.Similarities.dotProduct7u;
import static org.elasticsearch.simdvec.internal.Similarities.dotProduct7uBulkWithOffsets;

/**
 * Int7 OSQ scorer supplier backed by {@link MemorySegmentAccessInput} storage.
 */
public abstract sealed class Int7OSQVectorScorerSupplier implements RandomVectorScorerSupplier permits
    Int7OSQVectorScorerSupplier.DotProductSupplier, Int7OSQVectorScorerSupplier.EuclideanSupplier,
    Int7OSQVectorScorerSupplier.MaxInnerProductSupplier {

    private static final float LIMIT_SCALE = 1f / ((1 << 7) - 1);

    protected final MemorySegmentAccessInput input;
    protected final QuantizedByteVectorValues values;
    protected final int dims;
    protected final int maxOrd;

    Int7OSQVectorScorerSupplier(MemorySegmentAccessInput input, QuantizedByteVectorValues values) {
        this.input = input;
        this.values = values;
        this.dims = values.dimension();
        this.maxOrd = values.size();
    }

    protected abstract float applyCorrections(float rawScore, int ord, QueryContext query) throws IOException;

    protected abstract float applyCorrections(MemorySegment scores, MemorySegment ordinals, int numNodes, QueryContext query)
        throws IOException;

    protected static final class QueryContext {
        final int ord;
        final float lowerInterval;
        final float upperInterval;
        final float additionalCorrection;
        final int quantizedComponentSum;

        QueryContext(int ord, float lowerInterval, float upperInterval, float additionalCorrection, int quantizedComponentSum) {
            this.ord = ord;
            this.lowerInterval = lowerInterval;
            this.upperInterval = upperInterval;
            this.additionalCorrection = additionalCorrection;
            this.quantizedComponentSum = quantizedComponentSum;
        }
    }

    protected QueryContext createQueryContext(int ord) throws IOException {
        var correctiveTerms = values.getCorrectiveTerms(ord);
        return new QueryContext(
            ord,
            correctiveTerms.lowerInterval(),
            correctiveTerms.upperInterval(),
            correctiveTerms.additionalCorrection(),
            correctiveTerms.quantizedComponentSum()
        );
    }

    protected final void checkOrdinal(int ord) {
        if (ord < 0 || ord >= maxOrd) {
            throw new IllegalArgumentException("illegal ordinal: " + ord);
        }
    }

    protected final float scoreFromOrds(QueryContext query, int secondOrd) throws IOException {
        int firstOrd = query.ord;
        checkOrdinal(firstOrd);
        checkOrdinal(secondOrd);
        long vectorPitch = getVectorPitch();
        long firstVectorOffset = firstOrd * vectorPitch;
        long secondVectorOffset = secondOrd * vectorPitch;

        MemorySegment first = input.segmentSliceOrNull(firstVectorOffset, dims);
        MemorySegment second = input.segmentSliceOrNull(secondVectorOffset, dims);
        if (first == null || second == null) {
            return scoreViaFallback(query, secondOrd, firstVectorOffset, secondVectorOffset);
        }
        int rawScore = dotProduct7u(first, second, dims);
        return applyCorrections(rawScore, secondOrd, query);
    }

    protected final float bulkScoreFromOrds(QueryContext query, int[] ordinals, float[] scores, int numNodes) throws IOException {
        checkOrdinal(query.ord);
        MemorySegment vectors = input.segmentSliceOrNull(0, input.length());
        if (vectors == null) {
            float max = Float.NEGATIVE_INFINITY;
            for (int i = 0; i < numNodes; i++) {
                scores[i] = scoreFromOrds(query, ordinals[i]);
                max = Math.max(max, scores[i]);
            }
            return max;
        }
        if (SUPPORTS_HEAP_SEGMENTS) {
            var ordinalsSeg = MemorySegment.ofArray(ordinals);
            var scoresSeg = MemorySegment.ofArray(scores);
            computeBulkForQuery(query, vectors, ordinalsSeg, scoresSeg, numNodes);
            return applyCorrections(scoresSeg, ordinalsSeg, numNodes, query);
        } else {
            try (Arena arena = Arena.ofConfined()) {
                MemorySegment ordinalsSeg = arena.allocate((long) numNodes * Integer.BYTES, Integer.BYTES);
                MemorySegment scoresSeg = arena.allocate((long) numNodes * Float.BYTES, Float.BYTES);
                MemorySegment.copy(ordinals, 0, ordinalsSeg, ValueLayout.JAVA_INT, 0, numNodes);
                computeBulkForQuery(query, vectors, ordinalsSeg, scoresSeg, numNodes);
                float max = applyCorrections(scoresSeg, ordinalsSeg, numNodes, query);
                MemorySegment.copy(scoresSeg, ValueLayout.JAVA_FLOAT, 0, scores, 0, numNodes);
                return max;
            }
        }
    }

    private void computeBulkForQuery(QueryContext query, MemorySegment vectors, MemorySegment ordinals, MemorySegment scores, int numNodes)
        throws IOException {
        long firstByteOffset = query.ord * getVectorPitch();
        MemorySegment firstVector = vectors.asSlice(firstByteOffset, getVectorPitch());
        computeBulk(firstVector, vectors, ordinals, scores, numNodes);
    }

    private float scoreViaFallback(QueryContext query, int secondOrd, long firstVectorOffset, long secondVectorOffset) throws IOException {
        byte[] a = new byte[dims];
        byte[] b = new byte[dims];
        input.readBytes(firstVectorOffset, a, 0, dims);
        input.readBytes(secondVectorOffset, b, 0, dims);
        // Just fall back to regular dot-product and apply corrections
        int raw = VectorUtil.dotProduct(a, b);
        return applyCorrections(raw, secondOrd, query);
    }

    protected final void computeBulk(
        MemorySegment firstVector,
        MemorySegment vectors,
        MemorySegment ordinals,
        MemorySegment scores,
        int numNodes
    ) throws IOException {
        dotProduct7uBulkWithOffsets(vectors, firstVector, dims, (int) getVectorPitch(), ordinals, numNodes, scores);
    }

    protected final long getVectorPitch() {
        return dims + 3L * Float.BYTES + Integer.BYTES;
    }

    @Override
    public UpdateableRandomVectorScorer scorer() {
        return new UpdateableRandomVectorScorer.AbstractUpdateableRandomVectorScorer(values) {
            private int ord = -1;
            private QueryContext query;

            @Override
            public float score(int node) throws IOException {
                if (query == null) {
                    throw new IllegalStateException("scoring ordinal is not set");
                }
                return scoreFromOrds(query, node);
            }

            @Override
            public float bulkScore(int[] nodes, float[] scores, int numNodes) throws IOException {
                if (query == null) {
                    throw new IllegalStateException("scoring ordinal is not set");
                }
                return bulkScoreFromOrds(query, nodes, scores, numNodes);
            }

            @Override
            public void setScoringOrdinal(int node) throws IOException {
                checkOrdinal(node);
                ord = node;
                query = createQueryContext(node);
            }
        };
    }

    public QuantizedByteVectorValues get() {
        return values;
    }

    public static final class DotProductSupplier extends Int7OSQVectorScorerSupplier {
        public DotProductSupplier(MemorySegmentAccessInput input, QuantizedByteVectorValues values) {
            super(input, values);
        }

        @Override
        public RandomVectorScorerSupplier copy() throws IOException {
            return new DotProductSupplier(input.clone(), values.copy());
        }

        @Override
        protected float applyCorrections(float rawScore, int ord, QueryContext query) throws IOException {
            var correctiveTerms = values.getCorrectiveTerms(ord);
            float ax = correctiveTerms.lowerInterval();
            float lx = (correctiveTerms.upperInterval() - ax) * LIMIT_SCALE;
            float ay = query.lowerInterval;
            float ly = (query.upperInterval - ay) * LIMIT_SCALE;
            float y1 = query.quantizedComponentSum;
            float x1 = correctiveTerms.quantizedComponentSum();
            float score = ax * ay * dims + ay * lx * x1 + ax * ly * y1 + lx * ly * rawScore;
            score += query.additionalCorrection + correctiveTerms.additionalCorrection() - values.getCentroidDP();
            return VectorUtil.normalizeToUnitInterval(Math.clamp(score, -1, 1));
        }

        @Override
        protected float applyCorrections(MemorySegment scoreSeg, MemorySegment ordinalsSeg, int numNodes, QueryContext query)
            throws IOException {
            float ay = query.lowerInterval;
            float ly = (query.upperInterval - ay) * LIMIT_SCALE;
            float y1 = query.quantizedComponentSum;
            float max = Float.NEGATIVE_INFINITY;
            for (int i = 0; i < numNodes; i++) {
                float raw = scoreSeg.getAtIndex(ValueLayout.JAVA_FLOAT, i);
                int ord = ordinalsSeg.getAtIndex(ValueLayout.JAVA_INT, i);
                var correctiveTerms = values.getCorrectiveTerms(ord);
                float ax = correctiveTerms.lowerInterval();
                float lx = (correctiveTerms.upperInterval() - ax) * LIMIT_SCALE;
                float x1 = correctiveTerms.quantizedComponentSum();
                float adjustedScore = ax * ay * dims + ay * lx * x1 + ax * ly * y1 + lx * ly * raw;
                adjustedScore += query.additionalCorrection + correctiveTerms.additionalCorrection() - values.getCentroidDP();
                float normalized = VectorUtil.normalizeToUnitInterval(Math.clamp(adjustedScore, -1, 1));
                scoreSeg.setAtIndex(ValueLayout.JAVA_FLOAT, i, normalized);
                max = Math.max(max, normalized);
            }
            return max;
        }

    }

    public static final class EuclideanSupplier extends Int7OSQVectorScorerSupplier {
        public EuclideanSupplier(MemorySegmentAccessInput input, QuantizedByteVectorValues values) {
            super(input, values);
        }

        @Override
        public RandomVectorScorerSupplier copy() throws IOException {
            return new EuclideanSupplier(input.clone(), values.copy());
        }

        @Override
        protected float applyCorrections(float rawScore, int ord, QueryContext query) throws IOException {
            var correctiveTerms = values.getCorrectiveTerms(ord);
            float ax = correctiveTerms.lowerInterval();
            float lx = (correctiveTerms.upperInterval() - ax) * LIMIT_SCALE;
            float ay = query.lowerInterval;
            float ly = (query.upperInterval - ay) * LIMIT_SCALE;
            float y1 = query.quantizedComponentSum;
            float x1 = correctiveTerms.quantizedComponentSum();
            float score = ax * ay * dims + ay * lx * x1 + ax * ly * y1 + lx * ly * rawScore;
            score = query.additionalCorrection + correctiveTerms.additionalCorrection() - 2 * score;
            return VectorUtil.normalizeDistanceToUnitInterval(Math.max(score, 0f));
        }

        @Override
        protected float applyCorrections(MemorySegment scoreSeg, MemorySegment ordinalsSeg, int numNodes, QueryContext query)
            throws IOException {
            float ay = query.lowerInterval;
            float ly = (query.upperInterval - ay) * LIMIT_SCALE;
            float y1 = query.quantizedComponentSum;
            float max = Float.NEGATIVE_INFINITY;
            for (int i = 0; i < numNodes; i++) {
                float raw = scoreSeg.getAtIndex(ValueLayout.JAVA_FLOAT, i);
                int ord = ordinalsSeg.getAtIndex(ValueLayout.JAVA_INT, i);
                var correctiveTerms = values.getCorrectiveTerms(ord);
                float ax = correctiveTerms.lowerInterval();
                float lx = (correctiveTerms.upperInterval() - ax) * LIMIT_SCALE;
                float x1 = correctiveTerms.quantizedComponentSum();
                float score = ax * ay * dims + ay * lx * x1 + ax * ly * y1 + lx * ly * raw;
                score = query.additionalCorrection + correctiveTerms.additionalCorrection() - 2 * score;
                float normalized = VectorUtil.normalizeDistanceToUnitInterval(Math.max(score, 0f));
                scoreSeg.setAtIndex(ValueLayout.JAVA_FLOAT, i, normalized);
                max = Math.max(max, normalized);
            }
            return max;
        }
    }

    public static final class MaxInnerProductSupplier extends Int7OSQVectorScorerSupplier {
        public MaxInnerProductSupplier(MemorySegmentAccessInput input, QuantizedByteVectorValues values) {
            super(input, values);
        }

        @Override
        public RandomVectorScorerSupplier copy() throws IOException {
            return new MaxInnerProductSupplier(input.clone(), values.copy());
        }

        @Override
        protected float applyCorrections(float rawScore, int ord, QueryContext query) throws IOException {
            var correctiveTerms = values.getCorrectiveTerms(ord);
            float ax = correctiveTerms.lowerInterval();
            float lx = (correctiveTerms.upperInterval() - ax) * LIMIT_SCALE;
            float ay = query.lowerInterval;
            float ly = (query.upperInterval - ay) * LIMIT_SCALE;
            float y1 = query.quantizedComponentSum;
            float x1 = correctiveTerms.quantizedComponentSum();
            float score = ax * ay * dims + ay * lx * x1 + ax * ly * y1 + lx * ly * rawScore;
            score += query.additionalCorrection + correctiveTerms.additionalCorrection() - values.getCentroidDP();
            return VectorUtil.scaleMaxInnerProductScore(score);
        }

        @Override
        protected float applyCorrections(MemorySegment scoreSeg, MemorySegment ordinalsSeg, int numNodes, QueryContext query)
            throws IOException {
            float ay = query.lowerInterval;
            float ly = (query.upperInterval - ay) * LIMIT_SCALE;
            float y1 = query.quantizedComponentSum;
            float max = Float.NEGATIVE_INFINITY;
            for (int i = 0; i < numNodes; i++) {
                float raw = scoreSeg.getAtIndex(ValueLayout.JAVA_FLOAT, i);
                int ord = ordinalsSeg.getAtIndex(ValueLayout.JAVA_INT, i);
                var correctiveTerms = values.getCorrectiveTerms(ord);
                float ax = correctiveTerms.lowerInterval();
                float lx = (correctiveTerms.upperInterval() - ax) * LIMIT_SCALE;
                float x1 = correctiveTerms.quantizedComponentSum();
                float score = ax * ay * dims + ay * lx * x1 + ax * ly * y1 + lx * ly * raw;
                score += query.additionalCorrection + correctiveTerms.additionalCorrection() - values.getCentroidDP();
                float normalizedScore = VectorUtil.scaleMaxInnerProductScore(score);
                scoreSeg.setAtIndex(ValueLayout.JAVA_FLOAT, i, normalizedScore);
                max = Math.max(max, normalizedScore);
            }
            return max;
        }
    }
}
