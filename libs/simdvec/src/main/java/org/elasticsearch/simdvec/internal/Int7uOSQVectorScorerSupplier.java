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
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.MemorySegmentAccessInput;
import org.apache.lucene.util.VectorUtil;
import org.apache.lucene.util.hnsw.RandomVectorScorerSupplier;
import org.apache.lucene.util.hnsw.UpdateableRandomVectorScorer;
import org.elasticsearch.simdvec.internal.vectorization.ScoreCorrections;

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
    // Size of the corrections trailer that follows each quantized vector in the codec's per-vector
    // record: 3 floats (lowerInterval, upperInterval, additionalCorrection) + 1 int (quantizedComponentSum).
    private static final int CORRECTIONS_BYTES = 3 * Float.BYTES + Integer.BYTES;

    protected final IndexInput input;
    protected final QuantizedByteVectorValues values;
    protected final int dims;
    protected final int maxOrd;
    protected final int vectorPitch;
    final FixedSizeScratch firstScratch;
    final FixedSizeScratch secondScratch;
    final AddressesScratch addrsScratch = new AddressesScratch();
    final OffsetsScratch offsetsScratch = new OffsetsScratch();

    Int7uOSQVectorScorerSupplier(IndexInput input, QuantizedByteVectorValues values) {
        this.input = input;
        this.values = values;
        this.dims = values.dimension();
        this.maxOrd = values.size();
        this.vectorPitch = dims + CORRECTIONS_BYTES;
        // Scratches are sized to the full per-vector record (vector + corrections), so that the same
        // backing slice can be used for both the dot product (first dims bytes) and the corrections
        // (trailing {@code CORRECTIONS_BYTES} bytes).
        this.firstScratch = new FixedSizeScratch(vectorPitch);
        this.secondScratch = new FixedSizeScratch(vectorPitch);
    }

    protected abstract float applyCorrections(float rawScore, MemorySegment correctionsSlice, QueryContext query) throws IOException;

    protected abstract float applyCorrectionsBulk(MemorySegment scores, MemorySegment addrs, int numNodes, QueryContext query)
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
        return IndexInputUtils.withSlice(input, vectorPitch, firstScratch::getScratch, firstSeg -> {
            input.seek(secondVectorOffset);
            return IndexInputUtils.withSlice(input, vectorPitch, secondScratch::getScratch, secondSeg -> {
                int rawScore = dotProductI7u(firstSeg, secondSeg, dims);
                return applyCorrections(rawScore, secondSeg.asSlice(dims, CORRECTIONS_BYTES), query);
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
        // Request vectorPitch bytes per slice (not dims): the doc-side corrections sit at offset
        // [dims, dims+CORRECTIONS_BYTES) of the same record, and we read them in
        // applyCorrectionsBulk via MemorySegment reinterpret.
        return IndexInputUtils.withSlice(input, vectorPitch, firstScratch::getScratch, querySeg -> {
            long[] offsets = offsetsScratch.get(numNodes);
            for (int i = 0; i < numNodes; i++) {
                offsets[i] = (long) ordinals[i] * vectorPitch;
            }

            float[] maxScore = new float[] { Float.NEGATIVE_INFINITY };
            boolean resolved = IndexInputUtils.withSliceAddresses(input, offsets, vectorPitch, numNodes, addrsScratch::get, addrs -> {
                var scoresSeg = MemorySegment.ofArray(scores);
                dotProductI7uBulkSparse(addrs, querySeg, dims, numNodes, scoresSeg);
                maxScore[0] = applyCorrectionsBulk(scoresSeg, addrs, numNodes, query);
            });
            if (resolved == false) {
                // fallback to per-vector scorer
                for (int i = 0; i < numNodes; i++) {
                    input.seek(offsets[i]);
                    scores[i] = IndexInputUtils.withSlice(input, vectorPitch, secondScratch::getScratch, documentSeg -> {
                        int rawScore = dotProductI7u(querySeg, documentSeg, dims);
                        float adjustedScore = applyCorrections(rawScore, documentSeg.asSlice(dims, CORRECTIONS_BYTES), query);
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
        protected float applyCorrections(float rawScore, MemorySegment correctionsSlice, QueryContext query) throws IOException {
            float ax = correctionsSlice.get(ValueLayout.JAVA_FLOAT_UNALIGNED, 0);
            float ux = correctionsSlice.get(ValueLayout.JAVA_FLOAT_UNALIGNED, Float.BYTES);
            float xAdditionalCorrection = correctionsSlice.get(ValueLayout.JAVA_FLOAT_UNALIGNED, 2L * Float.BYTES);
            int x1 = correctionsSlice.get(ValueLayout.JAVA_INT_UNALIGNED, 3L * Float.BYTES);
            float lx = (ux - ax) * LIMIT_SCALE;
            float ay = query.lowerInterval;
            float ly = (query.upperInterval - ay) * LIMIT_SCALE;
            float y1 = query.quantizedComponentSum;
            float score = ax * ay * dims + ay * lx * x1 + ax * ly * y1 + lx * ly * rawScore;
            score += query.additionalCorrection + xAdditionalCorrection - values.getCentroidDP();
            return VectorUtil.normalizeToUnitInterval(Math.clamp(score, -1, 1));
        }

        @Override
        protected float applyCorrectionsBulk(MemorySegment scoreSeg, MemorySegment addrs, int numNodes, QueryContext query)
            throws IOException {
            return ScoreCorrections.nativeBbqApplyCorrectionsBulk(
                VectorSimilarityFunction.DOT_PRODUCT,
                addrs,
                numNodes,
                dims,
                vectorPitch,
                dims,
                query.lowerInterval,
                query.upperInterval,
                query.quantizedComponentSum,
                query.additionalCorrection,
                LIMIT_SCALE,
                LIMIT_SCALE,
                values.getCentroidDP(),
                true,
                scoreSeg
            );
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
        protected float applyCorrections(float rawScore, MemorySegment correctionsSlice, QueryContext query) {
            float ax = correctionsSlice.get(ValueLayout.JAVA_FLOAT_UNALIGNED, 0);
            float ux = correctionsSlice.get(ValueLayout.JAVA_FLOAT_UNALIGNED, Float.BYTES);
            float xAdditionalCorrection = correctionsSlice.get(ValueLayout.JAVA_FLOAT_UNALIGNED, 2L * Float.BYTES);
            int x1 = correctionsSlice.get(ValueLayout.JAVA_INT_UNALIGNED, 3L * Float.BYTES);
            float lx = (ux - ax) * LIMIT_SCALE;
            float ay = query.lowerInterval;
            float ly = (query.upperInterval - ay) * LIMIT_SCALE;
            float y1 = query.quantizedComponentSum;
            float score = ax * ay * dims + ay * lx * x1 + ax * ly * y1 + lx * ly * rawScore;
            score = query.additionalCorrection + xAdditionalCorrection - 2 * score;
            return VectorUtil.normalizeDistanceToUnitInterval(Math.max(score, 0f));
        }

        @Override
        protected float applyCorrectionsBulk(MemorySegment scoreSeg, MemorySegment addrs, int numNodes, QueryContext query)
            throws IOException {
            return ScoreCorrections.nativeBbqApplyCorrectionsBulk(
                VectorSimilarityFunction.EUCLIDEAN,
                addrs,
                numNodes,
                dims,
                vectorPitch,
                dims,
                query.lowerInterval,
                query.upperInterval,
                query.quantizedComponentSum,
                query.additionalCorrection,
                LIMIT_SCALE,
                LIMIT_SCALE,
                values.getCentroidDP(),
                true,
                scoreSeg
            );
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
        protected float applyCorrections(float rawScore, MemorySegment correctionsSlice, QueryContext query) throws IOException {
            float ax = correctionsSlice.get(ValueLayout.JAVA_FLOAT_UNALIGNED, 0);
            float ux = correctionsSlice.get(ValueLayout.JAVA_FLOAT_UNALIGNED, Float.BYTES);
            float xAdditionalCorrection = correctionsSlice.get(ValueLayout.JAVA_FLOAT_UNALIGNED, 2L * Float.BYTES);
            int x1 = correctionsSlice.get(ValueLayout.JAVA_INT_UNALIGNED, 3L * Float.BYTES);
            float lx = (ux - ax) * LIMIT_SCALE;
            float ay = query.lowerInterval;
            float ly = (query.upperInterval - ay) * LIMIT_SCALE;
            float y1 = query.quantizedComponentSum;
            float score = ax * ay * dims + ay * lx * x1 + ax * ly * y1 + lx * ly * rawScore;
            score += query.additionalCorrection + xAdditionalCorrection - values.getCentroidDP();
            return VectorUtil.scaleMaxInnerProductScore(score);
        }

        @Override
        protected float applyCorrectionsBulk(MemorySegment scoreSeg, MemorySegment addrs, int numNodes, QueryContext query)
            throws IOException {
            return ScoreCorrections.nativeBbqApplyCorrectionsBulk(
                VectorSimilarityFunction.MAXIMUM_INNER_PRODUCT,
                addrs,
                numNodes,
                dims,
                vectorPitch,
                dims,
                query.lowerInterval,
                query.upperInterval,
                query.quantizedComponentSum,
                query.additionalCorrection,
                LIMIT_SCALE,
                LIMIT_SCALE,
                values.getCentroidDP(),
                true,
                scoreSeg
            );
        }
    }
}
