/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.simdvec.internal;

import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.store.FilterIndexInput;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.VectorUtil;
import org.apache.lucene.util.hnsw.RandomVectorScorer;
import org.apache.lucene.util.quantization.QuantizedByteVectorValues;
import org.elasticsearch.nativeaccess.NativeAccess;
import org.elasticsearch.nativeaccess.VectorSimilarityFunctions;
import org.elasticsearch.simdvec.IndexInputUtils;
import org.elasticsearch.simdvec.MemorySegmentAccessInputAccess;
import org.elasticsearch.simdvec.internal.vectorization.ScoreCorrections;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.Optional;

/**
 * JDK-22+ implementation for Int7 OSQ query-time scorers.
 */
public abstract sealed class Int7uOSQVectorScorer extends RandomVectorScorer.AbstractRandomVectorScorer permits
    Int7uOSQVectorScorer.DotProductScorer, Int7uOSQVectorScorer.EuclideanScorer, Int7uOSQVectorScorer.MaxInnerProductScorer {

    private static final VectorSimilarityFunctions DISTANCE_FUNCS = NativeAccess.instance()
        .getVectorSimilarityFunctions()
        .orElseThrow(AssertionError::new);

    private static final float LIMIT_SCALE = 1f / ((1 << 7) - 1);
    // Size of the corrections trailer that follows each quantized vector in the codec's per-vector
    // record: 3 floats (lowerInterval, upperInterval, additionalCorrection) + 1 int (quantizedComponentSum).
    private static final int CORRECTIONS_BYTES = 3 * Float.BYTES + Integer.BYTES;

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
    final int dims;
    final long vectorPitch;
    final float centroidDP;
    final MemorySegment query;
    final float lowerInterval;
    final float upperInterval;
    final float additionalCorrection;
    final int quantizedComponentSum;
    final FixedSizeScratch scratch;
    final AddressesScratch addrsScratch = new AddressesScratch();
    final OffsetsScratch offsetsScratch = new OffsetsScratch();

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
        this.dims = values.dimension();
        this.vectorPitch = vectorByteSize + 3L * Float.BYTES + Integer.BYTES;
        try {
            this.centroidDP = values.getCentroidDP();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        this.query = MemorySegment.ofArray(quantizedQuery);
        this.lowerInterval = lowerInterval;
        this.upperInterval = upperInterval;
        this.additionalCorrection = additionalCorrection;
        this.quantizedComponentSum = quantizedComponentSum;
        // Scratch sized to the full per-vector record (vector + corrections), so that the same
        // backing slice can be used for both the dot product (first vectorByteSize bytes) and the
        // corrections (trailing CORRECTIONS_BYTES bytes).
        this.scratch = new FixedSizeScratch((int) vectorPitch);
    }

    abstract float applyCorrections(float rawScore, MemorySegment correctionsSlice);

    abstract float applyCorrectionsBulk(MemorySegment scores, MemorySegment addrs, int numNodes) throws IOException;

    @Override
    public float score(int node) throws IOException {
        checkOrdinal(node);
        long vectorOffset = (long) node * vectorPitch;
        input.seek(vectorOffset);
        return IndexInputUtils.withSlice(input, (int) vectorPitch, scratch::getScratch, seg -> {
            int dotProduct = DISTANCE_FUNCS.dotProductI7u(query, seg, vectorByteSize);
            return applyCorrections(dotProduct, seg.asSlice(vectorByteSize, CORRECTIONS_BYTES));
        });
    }

    @Override
    public float bulkScore(int[] nodes, float[] scores, int numNodes) throws IOException {
        if (numNodes == 0) {
            return Float.NEGATIVE_INFINITY;
        }

        long[] offsets = offsetsScratch.get(numNodes);
        for (int i = 0; i < numNodes; i++) {
            offsets[i] = (long) nodes[i] * vectorPitch;
        }

        float[] maxScore = new float[] { Float.NEGATIVE_INFINITY };
        boolean resolved = IndexInputUtils.withSliceAddresses(input, offsets, (int) vectorPitch, numNodes, addrsScratch::get, addrs -> {
            var scoresSeg = MemorySegment.ofArray(scores);
            DISTANCE_FUNCS.dotProductI7uBulkSparse(addrs, query, vectorByteSize, numNodes, scoresSeg);
            maxScore[0] = applyCorrectionsBulk(scoresSeg, addrs, numNodes);
        });
        if (resolved == false) {
            // fallback to per-vector scorer
            for (int i = 0; i < numNodes; i++) {
                input.seek(offsets[i]);
                scores[i] = IndexInputUtils.withSlice(input, (int) vectorPitch, scratch::getScratch, seg -> {
                    int rawScore = DISTANCE_FUNCS.dotProductI7u(query, seg, vectorByteSize);
                    float adjustedScore = applyCorrections(rawScore, seg.asSlice(vectorByteSize, CORRECTIONS_BYTES));
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
        float applyCorrections(float rawScore, MemorySegment correctionsSlice) {
            float ax = correctionsSlice.get(ValueLayout.JAVA_FLOAT_UNALIGNED, 0);
            float ux = correctionsSlice.get(ValueLayout.JAVA_FLOAT_UNALIGNED, Float.BYTES);
            float xAdditionalCorrection = correctionsSlice.get(ValueLayout.JAVA_FLOAT_UNALIGNED, 2L * Float.BYTES);
            int x1 = correctionsSlice.get(ValueLayout.JAVA_INT_UNALIGNED, 3L * Float.BYTES);
            float lx = (ux - ax) * LIMIT_SCALE;
            float ay = lowerInterval;
            float ly = (upperInterval - ay) * LIMIT_SCALE;
            float y1 = quantizedComponentSum;
            float score = ax * ay * dims + ay * lx * x1 + ax * ly * y1 + lx * ly * rawScore;
            score += additionalCorrection + xAdditionalCorrection - centroidDP;
            return VectorUtil.normalizeToUnitInterval(Math.clamp(score, -1, 1));
        }

        @Override
        float applyCorrectionsBulk(MemorySegment scoreSeg, MemorySegment addrs, int numNodes) throws IOException {
            return ScoreCorrections.nativeBbqApplyCorrectionsBulk(
                VectorSimilarityFunction.DOT_PRODUCT,
                addrs,
                numNodes,
                vectorByteSize,
                (int) vectorPitch,
                dims,
                lowerInterval,
                upperInterval,
                quantizedComponentSum,
                additionalCorrection,
                LIMIT_SCALE,
                LIMIT_SCALE,
                centroidDP,
                true,
                scoreSeg
            );
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
        float applyCorrections(float rawScore, MemorySegment correctionsSlice) {
            float ax = correctionsSlice.get(ValueLayout.JAVA_FLOAT_UNALIGNED, 0);
            float ux = correctionsSlice.get(ValueLayout.JAVA_FLOAT_UNALIGNED, Float.BYTES);
            float xAdditionalCorrection = correctionsSlice.get(ValueLayout.JAVA_FLOAT_UNALIGNED, 2L * Float.BYTES);
            int x1 = correctionsSlice.get(ValueLayout.JAVA_INT_UNALIGNED, 3L * Float.BYTES);
            float lx = (ux - ax) * LIMIT_SCALE;
            float ay = lowerInterval;
            float ly = (upperInterval - ay) * LIMIT_SCALE;
            float y1 = quantizedComponentSum;
            float score = ax * ay * dims + ay * lx * x1 + ax * ly * y1 + lx * ly * rawScore;
            score = additionalCorrection + xAdditionalCorrection - 2 * score;
            return VectorUtil.normalizeDistanceToUnitInterval(Math.max(score, 0f));
        }

        @Override
        float applyCorrectionsBulk(MemorySegment scoreSeg, MemorySegment addrs, int numNodes) throws IOException {
            return ScoreCorrections.nativeBbqApplyCorrectionsBulk(
                VectorSimilarityFunction.EUCLIDEAN,
                addrs,
                numNodes,
                vectorByteSize,
                (int) vectorPitch,
                dims,
                lowerInterval,
                upperInterval,
                quantizedComponentSum,
                additionalCorrection,
                LIMIT_SCALE,
                LIMIT_SCALE,
                centroidDP,
                true,
                scoreSeg
            );
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
        float applyCorrections(float rawScore, MemorySegment correctionsSlice) {
            float ax = correctionsSlice.get(ValueLayout.JAVA_FLOAT_UNALIGNED, 0);
            float ux = correctionsSlice.get(ValueLayout.JAVA_FLOAT_UNALIGNED, Float.BYTES);
            float xAdditionalCorrection = correctionsSlice.get(ValueLayout.JAVA_FLOAT_UNALIGNED, 2L * Float.BYTES);
            int x1 = correctionsSlice.get(ValueLayout.JAVA_INT_UNALIGNED, 3L * Float.BYTES);
            float lx = (ux - ax) * LIMIT_SCALE;
            float ay = lowerInterval;
            float ly = (upperInterval - ay) * LIMIT_SCALE;
            float y1 = quantizedComponentSum;
            float score = ax * ay * dims + ay * lx * x1 + ax * ly * y1 + lx * ly * rawScore;
            score += additionalCorrection + xAdditionalCorrection - centroidDP;
            return VectorUtil.scaleMaxInnerProductScore(score);
        }

        @Override
        float applyCorrectionsBulk(MemorySegment scoreSeg, MemorySegment addrs, int numNodes) throws IOException {
            return ScoreCorrections.nativeBbqApplyCorrectionsBulk(
                VectorSimilarityFunction.MAXIMUM_INNER_PRODUCT,
                addrs,
                numNodes,
                vectorByteSize,
                (int) vectorPitch,
                dims,
                lowerInterval,
                upperInterval,
                quantizedComponentSum,
                additionalCorrection,
                LIMIT_SCALE,
                LIMIT_SCALE,
                centroidDP,
                true,
                scoreSeg
            );
        }
    }

    static void checkInvariants(int maxOrd, int vectorByteLength, IndexInput input) {
        long vectorPitch = vectorByteLength + 3L * Float.BYTES + Integer.BYTES;
        if (input.length() < vectorPitch * maxOrd) {
            throw new IllegalArgumentException("input length is less than expected vector data");
        }
    }
}
