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
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.Optional;

import static org.elasticsearch.simdvec.internal.Similarities.dotProductI4;
import static org.elasticsearch.simdvec.internal.Similarities.dotProductI4BulkWithOffsets;

/**
 * Int4 packed-nibble query-time scorer. The float query is quantized externally
 * and passed in as unpacked bytes (one byte per dimension, 0-15 range) along
 * with corrective terms. Each stored vector is {@code dims/2} packed bytes
 * followed by corrective terms (3 floats + 1 int).
 */
public abstract sealed class Int4VectorScorer extends RandomVectorScorer.AbstractRandomVectorScorer permits
    Int4VectorScorer.DotProductScorer, Int4VectorScorer.EuclideanScorer, Int4VectorScorer.MaxInnerProductScorer {

    private static final float LIMIT_SCALE = 1f / ((1 << 4) - 1);
    private static final boolean SUPPORTS_HEAP_SEGMENTS = Runtime.version().feature() >= 22;

    private final IndexInput input;
    private final QuantizedByteVectorValues values;
    private final int dims;
    private final int packedDims;
    private final long vectorPitch;
    private final MemorySegment unpackedQuery;
    private final float queryLowerInterval;
    private final float queryUpperInterval;
    private final float queryAdditionalCorrection;
    private final int queryQuantizedComponentSum;
    private byte[] scratch;

    /**
     * Creates an int4 query-time scorer if the input supports efficient access.
     *
     * @param sim                    the similarity function
     * @param values                 the quantized vector values
     * @param unpackedQuery          the quantized query (dims bytes, one per dimension, 0-15)
     * @param lowerInterval          query corrective term
     * @param upperInterval          query corrective term
     * @param additionalCorrection   query corrective term
     * @param quantizedComponentSum  query corrective term
     * @return an optional scorer, or empty if the input doesn't support native access
     */
    public static Optional<RandomVectorScorer> create(
        VectorSimilarityFunction sim,
        QuantizedByteVectorValues values,
        byte[] unpackedQuery,
        float lowerInterval,
        float upperInterval,
        float additionalCorrection,
        int quantizedComponentSum
    ) {
        IndexInput input = values.getSlice();
        if (input == null) {
            return Optional.empty();
        }
        input = FilterIndexInput.unwrapOnlyTest(input);
        input = MemorySegmentAccessInputAccess.unwrap(input);
        return switch (sim) {
            case COSINE, DOT_PRODUCT -> Optional.of(
                new DotProductScorer(
                    input,
                    values,
                    unpackedQuery,
                    lowerInterval,
                    upperInterval,
                    additionalCorrection,
                    quantizedComponentSum
                )
            );
            case EUCLIDEAN -> Optional.of(
                new EuclideanScorer(input, values, unpackedQuery, lowerInterval, upperInterval, additionalCorrection, quantizedComponentSum)
            );
            case MAXIMUM_INNER_PRODUCT -> Optional.of(
                new MaxInnerProductScorer(
                    input,
                    values,
                    unpackedQuery,
                    lowerInterval,
                    upperInterval,
                    additionalCorrection,
                    quantizedComponentSum
                )
            );
        };
    }

    Int4VectorScorer(
        IndexInput input,
        QuantizedByteVectorValues values,
        byte[] unpackedQuery,
        float lowerInterval,
        float upperInterval,
        float additionalCorrection,
        int quantizedComponentSum
    ) {
        super(values);
        IndexInputUtils.checkInputType(input);
        this.input = input;
        this.values = values;
        this.dims = values.dimension();
        this.packedDims = dims / 2;
        this.vectorPitch = packedDims + 3L * Float.BYTES + Integer.BYTES;
        this.unpackedQuery = MemorySegment.ofArray(unpackedQuery);
        this.queryLowerInterval = lowerInterval;
        this.queryUpperInterval = upperInterval;
        this.queryAdditionalCorrection = additionalCorrection;
        this.queryQuantizedComponentSum = quantizedComponentSum;
    }

    protected abstract float applyCorrections(float rawScore, int ord) throws IOException;

    protected abstract float applyCorrectionsBulk(MemorySegment scores, MemorySegment ordinals, int numNodes) throws IOException;

    private byte[] getScratch(int len) {
        if (scratch == null || scratch.length < len) {
            scratch = new byte[len];
        }
        return scratch;
    }

    private void checkOrdinal(int ord) {
        if (ord < 0 || ord >= maxOrd()) {
            throw new IllegalArgumentException("illegal ordinal: " + ord);
        }
    }

    @Override
    public float score(int node) throws IOException {
        checkOrdinal(node);
        long nodeOffset = (long) node * vectorPitch;
        input.seek(nodeOffset);
        return IndexInputUtils.withSlice(input, packedDims, this::getScratch, packedTarget -> {
            int rawScore = dotProductI4(unpackedQuery, packedTarget, packedDims);
            return applyCorrections(rawScore, node);
        });
    }

    @Override
    public float bulkScore(int[] nodes, float[] scores, int numNodes) throws IOException {
        input.seek(0);
        return IndexInputUtils.withSlice(input, input.length(), this::getScratch, vectors -> {
            if (SUPPORTS_HEAP_SEGMENTS) {
                var ordinalsSeg = MemorySegment.ofArray(nodes);
                var scoresSeg = MemorySegment.ofArray(scores);
                dotProductI4BulkWithOffsets(vectors, unpackedQuery, packedDims, (int) vectorPitch, ordinalsSeg, numNodes, scoresSeg);
                return applyCorrectionsBulk(scoresSeg, ordinalsSeg, numNodes);
            } else {
                try (Arena arena = Arena.ofConfined()) {
                    MemorySegment ordinalsSeg = arena.allocate((long) numNodes * Integer.BYTES, Integer.BYTES);
                    MemorySegment scoresSeg = arena.allocate((long) numNodes * Float.BYTES, Float.BYTES);
                    MemorySegment.copy(nodes, 0, ordinalsSeg, ValueLayout.JAVA_INT, 0, numNodes);
                    dotProductI4BulkWithOffsets(vectors, unpackedQuery, packedDims, (int) vectorPitch, ordinalsSeg, numNodes, scoresSeg);
                    float max = applyCorrectionsBulk(scoresSeg, ordinalsSeg, numNodes);
                    MemorySegment.copy(scoresSeg, ValueLayout.JAVA_FLOAT, 0, scores, 0, numNodes);
                    return max;
                }
            }
        });
    }

    public static final class DotProductScorer extends Int4VectorScorer {
        DotProductScorer(
            IndexInput input,
            QuantizedByteVectorValues values,
            byte[] unpackedQuery,
            float lowerInterval,
            float upperInterval,
            float additionalCorrection,
            int quantizedComponentSum
        ) {
            super(input, values, unpackedQuery, lowerInterval, upperInterval, additionalCorrection, quantizedComponentSum);
        }

        @Override
        protected float applyCorrections(float rawScore, int ord) throws IOException {
            var ct = values.getCorrectiveTerms(ord);
            float ax = ct.lowerInterval();
            float lx = (ct.upperInterval() - ax) * LIMIT_SCALE;
            float ay = queryLowerInterval;
            float ly = (queryUpperInterval - ay) * LIMIT_SCALE;
            float y1 = queryQuantizedComponentSum;
            float x1 = ct.quantizedComponentSum();
            float score = ax * ay * dims + ay * lx * x1 + ax * ly * y1 + lx * ly * rawScore;
            score += queryAdditionalCorrection + ct.additionalCorrection() - values.getCentroidDP();
            return VectorUtil.normalizeToUnitInterval(Math.clamp(score, -1, 1));
        }

        @Override
        protected float applyCorrectionsBulk(MemorySegment scoreSeg, MemorySegment ordinalsSeg, int numNodes) throws IOException {
            float ay = queryLowerInterval;
            float ly = (queryUpperInterval - ay) * LIMIT_SCALE;
            float y1 = queryQuantizedComponentSum;
            float max = Float.NEGATIVE_INFINITY;
            for (int i = 0; i < numNodes; i++) {
                float raw = scoreSeg.getAtIndex(ValueLayout.JAVA_FLOAT, i);
                int nodeOrd = ordinalsSeg.getAtIndex(ValueLayout.JAVA_INT, i);
                var ct = values.getCorrectiveTerms(nodeOrd);
                float ax = ct.lowerInterval();
                float lx = (ct.upperInterval() - ax) * LIMIT_SCALE;
                float x1 = ct.quantizedComponentSum();
                float adjusted = ax * ay * dims + ay * lx * x1 + ax * ly * y1 + lx * ly * raw;
                adjusted += queryAdditionalCorrection + ct.additionalCorrection() - values.getCentroidDP();
                float normalized = VectorUtil.normalizeToUnitInterval(Math.clamp(adjusted, -1, 1));
                scoreSeg.setAtIndex(ValueLayout.JAVA_FLOAT, i, normalized);
                max = Math.max(max, normalized);
            }
            return max;
        }
    }

    public static final class EuclideanScorer extends Int4VectorScorer {
        EuclideanScorer(
            IndexInput input,
            QuantizedByteVectorValues values,
            byte[] unpackedQuery,
            float lowerInterval,
            float upperInterval,
            float additionalCorrection,
            int quantizedComponentSum
        ) {
            super(input, values, unpackedQuery, lowerInterval, upperInterval, additionalCorrection, quantizedComponentSum);
        }

        @Override
        protected float applyCorrections(float rawScore, int ord) throws IOException {
            var ct = values.getCorrectiveTerms(ord);
            float ax = ct.lowerInterval();
            float lx = (ct.upperInterval() - ax) * LIMIT_SCALE;
            float ay = queryLowerInterval;
            float ly = (queryUpperInterval - ay) * LIMIT_SCALE;
            float y1 = queryQuantizedComponentSum;
            float x1 = ct.quantizedComponentSum();
            float score = ax * ay * dims + ay * lx * x1 + ax * ly * y1 + lx * ly * rawScore;
            score = queryAdditionalCorrection + ct.additionalCorrection() - 2 * score;
            return VectorUtil.normalizeDistanceToUnitInterval(Math.max(score, 0f));
        }

        @Override
        protected float applyCorrectionsBulk(MemorySegment scoreSeg, MemorySegment ordinalsSeg, int numNodes) throws IOException {
            float ay = queryLowerInterval;
            float ly = (queryUpperInterval - ay) * LIMIT_SCALE;
            float y1 = queryQuantizedComponentSum;
            float max = Float.NEGATIVE_INFINITY;
            for (int i = 0; i < numNodes; i++) {
                float raw = scoreSeg.getAtIndex(ValueLayout.JAVA_FLOAT, i);
                int nodeOrd = ordinalsSeg.getAtIndex(ValueLayout.JAVA_INT, i);
                var ct = values.getCorrectiveTerms(nodeOrd);
                float ax = ct.lowerInterval();
                float lx = (ct.upperInterval() - ax) * LIMIT_SCALE;
                float x1 = ct.quantizedComponentSum();
                float score = ax * ay * dims + ay * lx * x1 + ax * ly * y1 + lx * ly * raw;
                score = queryAdditionalCorrection + ct.additionalCorrection() - 2 * score;
                float normalized = VectorUtil.normalizeDistanceToUnitInterval(Math.max(score, 0f));
                scoreSeg.setAtIndex(ValueLayout.JAVA_FLOAT, i, normalized);
                max = Math.max(max, normalized);
            }
            return max;
        }
    }

    public static final class MaxInnerProductScorer extends Int4VectorScorer {
        MaxInnerProductScorer(
            IndexInput input,
            QuantizedByteVectorValues values,
            byte[] unpackedQuery,
            float lowerInterval,
            float upperInterval,
            float additionalCorrection,
            int quantizedComponentSum
        ) {
            super(input, values, unpackedQuery, lowerInterval, upperInterval, additionalCorrection, quantizedComponentSum);
        }

        @Override
        protected float applyCorrections(float rawScore, int ord) throws IOException {
            var ct = values.getCorrectiveTerms(ord);
            float ax = ct.lowerInterval();
            float lx = (ct.upperInterval() - ax) * LIMIT_SCALE;
            float ay = queryLowerInterval;
            float ly = (queryUpperInterval - ay) * LIMIT_SCALE;
            float y1 = queryQuantizedComponentSum;
            float x1 = ct.quantizedComponentSum();
            float score = ax * ay * dims + ay * lx * x1 + ax * ly * y1 + lx * ly * rawScore;
            score += queryAdditionalCorrection + ct.additionalCorrection() - values.getCentroidDP();
            return VectorUtil.scaleMaxInnerProductScore(score);
        }

        @Override
        protected float applyCorrectionsBulk(MemorySegment scoreSeg, MemorySegment ordinalsSeg, int numNodes) throws IOException {
            float ay = queryLowerInterval;
            float ly = (queryUpperInterval - ay) * LIMIT_SCALE;
            float y1 = queryQuantizedComponentSum;
            float max = Float.NEGATIVE_INFINITY;
            for (int i = 0; i < numNodes; i++) {
                float raw = scoreSeg.getAtIndex(ValueLayout.JAVA_FLOAT, i);
                int nodeOrd = ordinalsSeg.getAtIndex(ValueLayout.JAVA_INT, i);
                var ct = values.getCorrectiveTerms(nodeOrd);
                float ax = ct.lowerInterval();
                float lx = (ct.upperInterval() - ax) * LIMIT_SCALE;
                float x1 = ct.quantizedComponentSum();
                float score = ax * ay * dims + ay * lx * x1 + ax * ly * y1 + lx * ly * raw;
                score += queryAdditionalCorrection + ct.additionalCorrection() - values.getCentroidDP();
                float normalizedScore = VectorUtil.scaleMaxInnerProductScore(score);
                scoreSeg.setAtIndex(ValueLayout.JAVA_FLOAT, i, normalizedScore);
                max = Math.max(max, normalizedScore);
            }
            return max;
        }
    }
}
