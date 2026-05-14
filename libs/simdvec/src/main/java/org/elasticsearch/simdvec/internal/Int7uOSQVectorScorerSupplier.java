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
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.MemorySegmentAccessInput;
import org.apache.lucene.util.VectorUtil;
import org.apache.lucene.util.hnsw.RandomVectorScorerSupplier;
import org.apache.lucene.util.hnsw.UpdateableRandomVectorScorer;

import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;

import static org.elasticsearch.simdvec.internal.Similarities.dotProductI7u;
import static org.elasticsearch.simdvec.internal.Similarities.dotProductI7uBulkSparse;

/**
 * Int7 OSQ scorer supplier backed by {@link MemorySegmentAccessInput} storage.
 */
public abstract sealed class Int7uOSQVectorScorerSupplier implements RandomVectorScorerSupplier permits
    Int7uOSQVectorScorerSupplier.DotProductSupplier, Int7uOSQVectorScorerSupplier.EuclideanSupplier,
    Int7uOSQVectorScorerSupplier.MaxInnerProductSupplier {

    private static final float LIMIT_SCALE = 1f / ((1 << 7) - 1);

    protected final IndexInput input;
    protected final QuantizedByteVectorValues values;
    protected final int dims;
    protected final int maxOrd;
    protected final long vectorPitch;
    final FixedSizeScratch firstScratch;
    final FixedSizeScratch secondScratch;
    final AddressesScratch addrsScratch = new AddressesScratch();
    final OffsetsScratch offsetsScratch = new OffsetsScratch();

    Int7uOSQVectorScorerSupplier(IndexInput input, QuantizedByteVectorValues values) {
        this.input = input;
        this.values = values;
        this.dims = values.dimension();
        this.maxOrd = values.size();
        this.vectorPitch = dims + 3L * Float.BYTES + Integer.BYTES;
        this.firstScratch = new FixedSizeScratch(dims);
        this.secondScratch = new FixedSizeScratch(dims);
    }

    protected abstract float applyCorrections(float rawScore, int ord, QueryContext query) throws IOException;

    protected abstract float applyCorrectionsBulk(MemorySegment scores, int[] ordinals, int numNodes, QueryContext query)
        throws IOException;

    protected record QueryContext(
        int ord,
        float lowerInterval,
        float upperInterval,
        float additionalCorrection,
        int quantizedComponentSum
    ) {}

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

        long firstVectorOffset = (long) firstOrd * vectorPitch;
        long secondVectorOffset = (long) secondOrd * vectorPitch;

        input.seek(firstVectorOffset);
        return IndexInputUtils.withSlice(input, dims, firstScratch::getScratch, firstSeg -> {
            input.seek(secondVectorOffset);
            return IndexInputUtils.withSlice(input, dims, secondScratch::getScratch, secondSeg -> {

                int rawScore = dotProductI7u(firstSeg, secondSeg, dims);
                return applyCorrections(rawScore, secondOrd, query);
            });
        });
    }

    protected final float bulkScoreFromOrds(QueryContext query, int[] ordinals, float[] scores, int numNodes) throws IOException {
        checkOrdinal(query.ord);

        if (numNodes == 0) {
            return Float.NEGATIVE_INFINITY;
        }

        long queryByteOffset = (long) query.ord * vectorPitch;
        input.seek(queryByteOffset);
        return IndexInputUtils.withSlice(input, dims, firstScratch::getScratch, querySeg -> {
            long[] offsets = offsetsScratch.get(numNodes);
            for (int i = 0; i < numNodes; i++) {
                offsets[i] = (long) ordinals[i] * vectorPitch;
            }

            float[] maxScore = new float[] { Float.NEGATIVE_INFINITY };
            boolean resolved = IndexInputUtils.withSliceAddresses(input, offsets, dims, numNodes, addrsScratch::get, addrs -> {
                var scoresSeg = MemorySegment.ofArray(scores);
                dotProductI7uBulkSparse(addrs, querySeg, dims, numNodes, scoresSeg);
                maxScore[0] = applyCorrectionsBulk(scoresSeg, ordinals, numNodes, query);
            });
            if (resolved == false) {
                // fallback to per-vector scorer
                for (int i = 0; i < numNodes; i++) {
                    input.seek(offsets[i]);
                    var documentOrdinal = ordinals[i];
                    scores[i] = IndexInputUtils.withSlice(input, dims, secondScratch::getScratch, documentSeg -> {
                        int rawScore = dotProductI7u(querySeg, documentSeg, dims);
                        float adjustedScore = applyCorrections(rawScore, documentOrdinal, query);
                        maxScore[0] = Math.max(maxScore[0], adjustedScore);
                        return adjustedScore;
                    });
                }
            }
            return maxScore[0];
        });
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
                if (ord != node) {
                    ord = node;
                    query = createQueryContext(node);
                }
            }
        };
    }

    public QuantizedByteVectorValues get() {
        return values;
    }

    public static final class DotProductSupplier extends Int7uOSQVectorScorerSupplier {
        public DotProductSupplier(IndexInput input, QuantizedByteVectorValues values) {
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
        protected float applyCorrectionsBulk(MemorySegment scoreSeg, int[] ordinals, int numNodes, QueryContext query) throws IOException {
            float ay = query.lowerInterval;
            float ly = (query.upperInterval - ay) * LIMIT_SCALE;
            float y1 = query.quantizedComponentSum;
            float max = Float.NEGATIVE_INFINITY;
            for (int i = 0; i < numNodes; i++) {
                float raw = scoreSeg.getAtIndex(ValueLayout.JAVA_FLOAT, i);
                var correctiveTerms = values.getCorrectiveTerms(ordinals[i]);
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

    public static final class EuclideanSupplier extends Int7uOSQVectorScorerSupplier {
        public EuclideanSupplier(IndexInput input, QuantizedByteVectorValues values) {
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
        protected float applyCorrectionsBulk(MemorySegment scoreSeg, int[] ordinals, int numNodes, QueryContext query) throws IOException {
            float ay = query.lowerInterval;
            float ly = (query.upperInterval - ay) * LIMIT_SCALE;
            float y1 = query.quantizedComponentSum;
            float max = Float.NEGATIVE_INFINITY;
            for (int i = 0; i < numNodes; i++) {
                float raw = scoreSeg.getAtIndex(ValueLayout.JAVA_FLOAT, i);
                var correctiveTerms = values.getCorrectiveTerms(ordinals[i]);
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

    public static final class MaxInnerProductSupplier extends Int7uOSQVectorScorerSupplier {
        public MaxInnerProductSupplier(IndexInput input, QuantizedByteVectorValues values) {
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
        protected float applyCorrectionsBulk(MemorySegment scoreSeg, int[] ordinals, int numNodes, QueryContext query) throws IOException {
            float ay = query.lowerInterval;
            float ly = (query.upperInterval - ay) * LIMIT_SCALE;
            float y1 = query.quantizedComponentSum;
            float max = Float.NEGATIVE_INFINITY;
            for (int i = 0; i < numNodes; i++) {
                float raw = scoreSeg.getAtIndex(ValueLayout.JAVA_FLOAT, i);
                var correctiveTerms = values.getCorrectiveTerms(ordinals[i]);
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
