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
import org.apache.lucene.util.VectorUtil;
import org.apache.lucene.util.hnsw.RandomVectorScorerSupplier;
import org.apache.lucene.util.hnsw.UpdateableRandomVectorScorer;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;

import static org.elasticsearch.simdvec.internal.Similarities.dotProductI4;
import static org.elasticsearch.simdvec.internal.Similarities.dotProductI4BulkWithOffsets;

/**
 * Int4 packed-nibble scorer supplier.
 * Each stored vector is {@code dims/2} packed bytes (two 4-bit values per byte), followed by
 * corrective terms (3 floats + 1 int). The query is unpacked to {@code dims} bytes before scoring.
 */
public abstract sealed class Int4VectorScorerSupplier implements RandomVectorScorerSupplier permits
    Int4VectorScorerSupplier.DotProductSupplier, Int4VectorScorerSupplier.EuclideanSupplier,
    Int4VectorScorerSupplier.MaxInnerProductSupplier {

    private static final float LIMIT_SCALE = 1f / ((1 << 4) - 1);
    private static final boolean SUPPORTS_HEAP_SEGMENTS = Runtime.version().feature() >= 22;

    protected final IndexInput input;
    protected final QuantizedByteVectorValues values;
    protected final int dims;
    private final int packedDims;
    private final int maxOrd;
    private final long vectorPitch;

    private byte[] scratch;

    Int4VectorScorerSupplier(IndexInput input, QuantizedByteVectorValues values) {
        IndexInputUtils.checkInputType(input);
        this.input = input;
        this.values = values;
        this.dims = values.dimension();
        this.packedDims = dims / 2;
        this.maxOrd = values.size();
        this.vectorPitch = packedDims + 3L * Float.BYTES + Integer.BYTES;
    }

    protected abstract float applyCorrections(float rawScore, int ord, QueryContext query) throws IOException;

    protected abstract float applyCorrectionsBulk(MemorySegment scores, MemorySegment ordinals, int numNodes, QueryContext query)
        throws IOException;

    protected record QueryContext(
        int ord,
        float lowerInterval,
        float upperInterval,
        float additionalCorrection,
        int quantizedComponentSum,
        MemorySegment unpackedQuery
    ) {}

    private QueryContext createQueryContext(int ord) throws IOException {
        var correctiveTerms = values.getCorrectiveTerms(ord);
        long offset = (long) ord * vectorPitch;
        input.seek(offset);
        byte[] packed = new byte[packedDims];
        input.readBytes(packed, 0, packedDims);
        byte[] unpacked = unpackNibbles(packed);
        return new QueryContext(
            ord,
            correctiveTerms.lowerInterval(),
            correctiveTerms.upperInterval(),
            correctiveTerms.additionalCorrection(),
            correctiveTerms.quantizedComponentSum(),
            MemorySegment.ofArray(unpacked)
        );
    }

    private void checkOrdinal(int ord) {
        if (ord < 0 || ord >= maxOrd) {
            throw new IllegalArgumentException("illegal ordinal: " + ord);
        }
    }

    private byte[] getScratch(int len) {
        if (scratch == null || scratch.length < len) {
            scratch = new byte[len];
        }
        return scratch;
    }

    private float scoreFromOrds(QueryContext query, int secondOrd) throws IOException {
        checkOrdinal(query.ord);
        checkOrdinal(secondOrd);
        long secondVectorOffset = secondOrd * vectorPitch;
        input.seek(secondVectorOffset);
        return IndexInputUtils.withSlice(input, packedDims, this::getScratch, packedTarget -> {
            int rawScore = dotProductI4(query.unpackedQuery, packedTarget, packedDims);
            return applyCorrections(rawScore, secondOrd, query);
        });
    }

    private float bulkScoreFromOrds(QueryContext query, int[] ordinals, float[] scores, int numNodes) throws IOException {
        checkOrdinal(query.ord);
        input.seek(0);
        return IndexInputUtils.withSlice(input, input.length(), this::getScratch, vectors -> {
            if (SUPPORTS_HEAP_SEGMENTS) {
                var ordinalsSeg = MemorySegment.ofArray(ordinals);
                var scoresSeg = MemorySegment.ofArray(scores);
                computeBulk(query.unpackedQuery, vectors, ordinalsSeg, scoresSeg, numNodes);
                return applyCorrectionsBulk(scoresSeg, ordinalsSeg, numNodes, query);
            } else {
                try (Arena arena = Arena.ofConfined()) {
                    MemorySegment ordinalsSeg = arena.allocate((long) numNodes * Integer.BYTES, Integer.BYTES);
                    MemorySegment scoresSeg = arena.allocate((long) numNodes * Float.BYTES, Float.BYTES);
                    MemorySegment.copy(ordinals, 0, ordinalsSeg, ValueLayout.JAVA_INT, 0, numNodes);
                    computeBulk(query.unpackedQuery, vectors, ordinalsSeg, scoresSeg, numNodes);
                    float max = applyCorrectionsBulk(scoresSeg, ordinalsSeg, numNodes, query);
                    MemorySegment.copy(scoresSeg, ValueLayout.JAVA_FLOAT, 0, scores, 0, numNodes);
                    return max;
                }
            }
        });
    }

    private void computeBulk(
        MemorySegment unpackedQuery,
        MemorySegment vectors,
        MemorySegment ordinals,
        MemorySegment scores,
        int numNodes
    ) {
        dotProductI4BulkWithOffsets(vectors, unpackedQuery, packedDims, (int) vectorPitch, ordinals, numNodes, scores);
    }

    @Override
    public UpdateableRandomVectorScorer scorer() {
        return new UpdateableRandomVectorScorer.AbstractUpdateableRandomVectorScorer(values) {
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
                query = createQueryContext(node);
            }
        };
    }

    public QuantizedByteVectorValues get() {
        return values;
    }

    static byte[] unpackNibbles(byte[] packed) {
        int packedLen = packed.length;
        byte[] unpacked = new byte[packedLen * 2];
        for (int i = 0; i < packedLen; i++) {
            unpacked[i] = (byte) ((packed[i] & 0xFF) >>> 4);
            unpacked[i + packedLen] = (byte) (packed[i] & 0x0F);
        }
        return unpacked;
    }

    public static final class DotProductSupplier extends Int4VectorScorerSupplier {
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
        protected float applyCorrectionsBulk(MemorySegment scoreSeg, MemorySegment ordinalsSeg, int numNodes, QueryContext query)
            throws IOException {
            float ay = query.lowerInterval;
            float ly = (query.upperInterval - ay) * LIMIT_SCALE;
            float y1 = query.quantizedComponentSum;
            float max = Float.NEGATIVE_INFINITY;
            for (int i = 0; i < numNodes; i++) {
                float raw = scoreSeg.getAtIndex(ValueLayout.JAVA_FLOAT, i);
                int nodeOrd = ordinalsSeg.getAtIndex(ValueLayout.JAVA_INT, i);
                var correctiveTerms = values.getCorrectiveTerms(nodeOrd);
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

    public static final class EuclideanSupplier extends Int4VectorScorerSupplier {
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
        protected float applyCorrectionsBulk(MemorySegment scoreSeg, MemorySegment ordinalsSeg, int numNodes, QueryContext query)
            throws IOException {
            float ay = query.lowerInterval;
            float ly = (query.upperInterval - ay) * LIMIT_SCALE;
            float y1 = query.quantizedComponentSum;
            float max = Float.NEGATIVE_INFINITY;
            for (int i = 0; i < numNodes; i++) {
                float raw = scoreSeg.getAtIndex(ValueLayout.JAVA_FLOAT, i);
                int nodeOrd = ordinalsSeg.getAtIndex(ValueLayout.JAVA_INT, i);
                var correctiveTerms = values.getCorrectiveTerms(nodeOrd);
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

    public static final class MaxInnerProductSupplier extends Int4VectorScorerSupplier {
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
        protected float applyCorrectionsBulk(MemorySegment scoreSeg, MemorySegment ordinalsSeg, int numNodes, QueryContext query)
            throws IOException {
            float ay = query.lowerInterval;
            float ly = (query.upperInterval - ay) * LIMIT_SCALE;
            float y1 = query.quantizedComponentSum;
            float max = Float.NEGATIVE_INFINITY;
            for (int i = 0; i < numNodes; i++) {
                float raw = scoreSeg.getAtIndex(ValueLayout.JAVA_FLOAT, i);
                int nodeOrd = ordinalsSeg.getAtIndex(ValueLayout.JAVA_INT, i);
                var correctiveTerms = values.getCorrectiveTerms(nodeOrd);
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
