/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.simdvec.internal;

import org.apache.lucene.store.MemorySegmentAccessInput;
import org.apache.lucene.util.hnsw.RandomVectorScorerSupplier;
import org.apache.lucene.util.hnsw.UpdateableRandomVectorScorer;
import org.apache.lucene.util.quantization.QuantizedByteVectorValues;
import org.apache.lucene.util.quantization.ScalarQuantizedVectorSimilarity;
import org.elasticsearch.simdvec.QuantizedByteVectorValuesAccess;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;

import static org.apache.lucene.index.VectorSimilarityFunction.DOT_PRODUCT;
import static org.apache.lucene.index.VectorSimilarityFunction.EUCLIDEAN;
import static org.apache.lucene.index.VectorSimilarityFunction.MAXIMUM_INNER_PRODUCT;
import static org.apache.lucene.util.quantization.ScalarQuantizedVectorSimilarity.fromVectorSimilarity;

public abstract sealed class Int7SQVectorScorerSupplier implements RandomVectorScorerSupplier, QuantizedByteVectorValuesAccess {

    static final byte BITS = 7;

    final int dims;
    final int maxOrd;
    final float scoreCorrectionConstant;
    final MemorySegmentAccessInput input;
    final QuantizedByteVectorValues values; // to support ordToDoc/getAcceptOrds
    final ScalarQuantizedVectorSimilarity fallbackScorer;

    static final boolean SUPPORTS_HEAP_SEGMENTS = Runtime.version().feature() >= 22;

    protected Int7SQVectorScorerSupplier(
        MemorySegmentAccessInput input,
        QuantizedByteVectorValues values,
        float scoreCorrectionConstant,
        ScalarQuantizedVectorSimilarity fallbackScorer
    ) {
        this.input = input;
        this.values = values;
        this.dims = values.dimension();
        this.maxOrd = values.size();
        this.scoreCorrectionConstant = scoreCorrectionConstant;
        this.fallbackScorer = fallbackScorer;
    }

    protected final void checkOrdinal(int ord) {
        if (ord < 0 || ord >= maxOrd) {
            throw new IllegalArgumentException("illegal ordinal: " + ord);
        }
    }

    final void bulkScoreFromOrds(int firstOrd, int[] ordinals, float[] scores, int numNodes) throws IOException {
        MemorySegment vectorsSeg = input.segmentSliceOrNull(0, input.length());
        if (vectorsSeg == null) {
            for (int i = 0; i < numNodes; i++) {
                scores[i] = scoreFromOrds(firstOrd, ordinals[i]);
            }
        } else {
            final int vectorLength = dims;
            final int vectorPitch = vectorLength + Float.BYTES;
            if (SUPPORTS_HEAP_SEGMENTS) {
                var ordinalsSeg = MemorySegment.ofArray(ordinals);
                var scoresSeg = MemorySegment.ofArray(scores);
                bulkScoreFromSegment(vectorsSeg, vectorLength, vectorPitch, firstOrd, ordinalsSeg, scoresSeg, numNodes);
            } else {
                try (var arena = Arena.ofConfined()) {
                    var ordinalsMemorySegment = arena.allocate((long) numNodes * Integer.BYTES, 32);
                    var scoresMemorySegment = arena.allocate((long) numNodes * Float.BYTES, 32);
                    MemorySegment.copy(ordinals, 0, ordinalsMemorySegment, ValueLayout.JAVA_INT, 0, numNodes);

                    bulkScoreFromSegment(
                        vectorsSeg,
                        vectorLength,
                        vectorPitch,
                        firstOrd,
                        ordinalsMemorySegment,
                        scoresMemorySegment,
                        numNodes
                    );

                    MemorySegment.copy(scoresMemorySegment, ValueLayout.JAVA_FLOAT, 0, scores, 0, numNodes);
                }
            }
        }
    }

    final float scoreFromOrds(int firstOrd, int secondOrd) throws IOException {
        final int length = dims;
        long firstByteOffset = (long) firstOrd * (length + Float.BYTES);
        long secondByteOffset = (long) secondOrd * (length + Float.BYTES);

        MemorySegment firstSeg = input.segmentSliceOrNull(firstByteOffset, length);
        if (firstSeg == null) {
            return fallbackScore(firstByteOffset, secondByteOffset);
        }
        float firstOffset = Float.intBitsToFloat(input.readInt(firstByteOffset + length));

        MemorySegment secondSeg = input.segmentSliceOrNull(secondByteOffset, length);
        if (secondSeg == null) {
            return fallbackScore(firstByteOffset, secondByteOffset);
        }
        float secondOffset = Float.intBitsToFloat(input.readInt(secondByteOffset + length));

        return scoreFromSegments(firstSeg, firstOffset, secondSeg, secondOffset);
    }

    abstract float scoreFromSegments(MemorySegment a, float aOffset, MemorySegment b, float bOffset);

    protected abstract void bulkScoreFromSegment(
        MemorySegment vectors,
        int vectorLength,
        int vectorPitch,
        int firstOrd,
        MemorySegment ordinals,
        MemorySegment scores,
        int numNodes
    );

    private float fallbackScore(long firstByteOffset, long secondByteOffset) throws IOException {
        byte[] a = new byte[dims];
        input.readBytes(firstByteOffset, a, 0, a.length);
        float aOffsetValue = Float.intBitsToFloat(input.readInt(firstByteOffset + dims));

        byte[] b = new byte[dims];
        input.readBytes(secondByteOffset, b, 0, a.length);
        float bOffsetValue = Float.intBitsToFloat(input.readInt(secondByteOffset + dims));

        return fallbackScorer.score(a, aOffsetValue, b, bOffsetValue);
    }

    @Override
    public UpdateableRandomVectorScorer scorer() {
        return new UpdateableRandomVectorScorer.AbstractUpdateableRandomVectorScorer(values) {
            private int ord = -1;

            @Override
            public float score(int node) throws IOException {
                checkOrdinal(node);
                return scoreFromOrds(ord, node);
            }

            @Override
            public void bulkScore(int[] nodes, float[] scores, int numNodes) throws IOException {
                bulkScoreFromOrds(ord, nodes, scores, numNodes);
            }

            @Override
            public void setScoringOrdinal(int node) throws IOException {
                checkOrdinal(node);
                this.ord = node;
            }
        };
    }

    @Override
    public QuantizedByteVectorValues get() {
        return values;
    }

    public static final class EuclideanSupplier extends Int7SQVectorScorerSupplier {

        public EuclideanSupplier(MemorySegmentAccessInput input, QuantizedByteVectorValues values, float scoreCorrectionConstant) {
            super(input, values, scoreCorrectionConstant, fromVectorSimilarity(EUCLIDEAN, scoreCorrectionConstant, BITS));
        }

        @Override
        float scoreFromSegments(MemorySegment a, float aOffset, MemorySegment b, float bOffset) {
            int squareDistance = Similarities.squareDistanceI7u(a, b, dims);
            float adjustedDistance = squareDistance * scoreCorrectionConstant;
            return 1 / (1f + adjustedDistance);
        }

        @Override
        protected void bulkScoreFromSegment(
            MemorySegment vectors,
            int vectorLength,
            int vectorPitch,
            int firstOrd,
            MemorySegment ordinals,
            MemorySegment scores,
            int numNodes
        ) {
            long firstByteOffset = (long) firstOrd * vectorPitch;
            var firstVector = vectors.asSlice(firstByteOffset, vectorPitch);
            Similarities.squareDistanceI7uBulkWithOffsets(vectors, firstVector, dims, vectorPitch, ordinals, numNodes, scores);

            for (int i = 0; i < numNodes; ++i) {
                var squareDistance = scores.getAtIndex(ValueLayout.JAVA_FLOAT, i);
                float adjustedDistance = squareDistance * scoreCorrectionConstant;
                scores.setAtIndex(ValueLayout.JAVA_FLOAT, i, 1 / (1f + adjustedDistance));
            }
        }

        @Override
        public EuclideanSupplier copy() {
            return new EuclideanSupplier(input.clone(), values, scoreCorrectionConstant);
        }
    }

    public static final class DotProductSupplier extends Int7SQVectorScorerSupplier {

        public DotProductSupplier(MemorySegmentAccessInput input, QuantizedByteVectorValues values, float scoreCorrectionConstant) {
            super(input, values, scoreCorrectionConstant, fromVectorSimilarity(DOT_PRODUCT, scoreCorrectionConstant, BITS));
        }

        @Override
        float scoreFromSegments(MemorySegment a, float aOffset, MemorySegment b, float bOffset) {
            int dotProduct = Similarities.dotProductI7u(a, b, dims);
            assert dotProduct >= 0;
            float adjustedDistance = dotProduct * scoreCorrectionConstant + aOffset + bOffset;
            return Math.max((1 + adjustedDistance) / 2, 0f);
        }

        @Override
        protected void bulkScoreFromSegment(
            MemorySegment vectors,
            int vectorLength,
            int vectorPitch,
            int firstOrd,
            MemorySegment ordinals,
            MemorySegment scores,
            int numNodes
        ) {
            long firstByteOffset = (long) firstOrd * vectorPitch;
            var firstVector = vectors.asSlice(firstByteOffset, vectorPitch);
            Similarities.dotProductI7uBulkWithOffsets(vectors, firstVector, dims, vectorPitch, ordinals, numNodes, scores);

            // Java-side adjustment
            var aOffset = Float.intBitsToFloat(
                vectors.asSlice(firstByteOffset + vectorLength, Float.BYTES).getAtIndex(ValueLayout.JAVA_INT_UNALIGNED, 0)
            );
            for (int i = 0; i < numNodes; ++i) {
                var dotProduct = scores.getAtIndex(ValueLayout.JAVA_FLOAT, i);
                var secondOrd = ordinals.getAtIndex(ValueLayout.JAVA_INT, i);
                long secondByteOffset = (long) secondOrd * vectorPitch;
                var bOffset = Float.intBitsToFloat(
                    vectors.asSlice(secondByteOffset + vectorLength, Float.BYTES).getAtIndex(ValueLayout.JAVA_INT_UNALIGNED, 0)
                );
                float adjustedDistance = dotProduct * scoreCorrectionConstant + aOffset + bOffset;
                scores.setAtIndex(ValueLayout.JAVA_FLOAT, i, Math.max((1 + adjustedDistance) / 2, 0f));
            }
        }

        @Override
        public DotProductSupplier copy() {
            return new DotProductSupplier(input.clone(), values, scoreCorrectionConstant);
        }
    }

    public static final class MaxInnerProductSupplier extends Int7SQVectorScorerSupplier {

        public MaxInnerProductSupplier(MemorySegmentAccessInput input, QuantizedByteVectorValues values, float scoreCorrectionConstant) {
            super(input, values, scoreCorrectionConstant, fromVectorSimilarity(MAXIMUM_INNER_PRODUCT, scoreCorrectionConstant, BITS));
        }

        @Override
        float scoreFromSegments(MemorySegment a, float aOffset, MemorySegment b, float bOffset) {
            int dotProduct = Similarities.dotProductI7u(a, b, dims);
            assert dotProduct >= 0;
            float adjustedDistance = dotProduct * scoreCorrectionConstant + aOffset + bOffset;
            if (adjustedDistance < 0) {
                return 1 / (1 + -1 * adjustedDistance);
            }
            return adjustedDistance + 1;
        }

        @Override
        protected void bulkScoreFromSegment(
            MemorySegment vectors,
            int vectorLength,
            int vectorPitch,
            int firstOrd,
            MemorySegment ordinals,
            MemorySegment scores,
            int numNodes
        ) {
            long firstByteOffset = (long) firstOrd * vectorPitch;
            var firstVector = vectors.asSlice(firstByteOffset, vectorPitch);
            Similarities.dotProductI7uBulkWithOffsets(vectors, firstVector, dims, vectorPitch, ordinals, numNodes, scores);

            // Java-side adjustment
            var aOffset = Float.intBitsToFloat(
                vectors.asSlice(firstByteOffset + vectorLength, Float.BYTES).getAtIndex(ValueLayout.JAVA_INT_UNALIGNED, 0)
            );
            for (int i = 0; i < numNodes; ++i) {
                var dotProduct = scores.getAtIndex(ValueLayout.JAVA_FLOAT, i);
                var secondOrd = ordinals.getAtIndex(ValueLayout.JAVA_INT, i);
                long secondByteOffset = (long) secondOrd * vectorPitch;
                var bOffset = Float.intBitsToFloat(
                    vectors.asSlice(secondByteOffset + vectorLength, Float.BYTES).getAtIndex(ValueLayout.JAVA_INT_UNALIGNED, 0)
                );
                float adjustedDistance = dotProduct * scoreCorrectionConstant + aOffset + bOffset;
                adjustedDistance = adjustedDistance < 0 ? 1 / (1 + -1 * adjustedDistance) : adjustedDistance + 1;
                scores.setAtIndex(ValueLayout.JAVA_FLOAT, i, adjustedDistance);
            }
        }

        @Override
        public MaxInnerProductSupplier copy() {
            return new MaxInnerProductSupplier(input.clone(), values, scoreCorrectionConstant);
        }
    }

    static boolean checkIndex(long index, long length) {
        return index >= 0 && index < length;
    }
}
