/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.simdvec.internal;

import org.apache.lucene.index.ByteVectorValues;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.store.MemorySegmentAccessInput;
import org.apache.lucene.util.VectorUtil;
import org.apache.lucene.util.hnsw.RandomVectorScorerSupplier;
import org.apache.lucene.util.hnsw.UpdateableRandomVectorScorer;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;

import static org.apache.lucene.index.VectorSimilarityFunction.COSINE;
import static org.apache.lucene.index.VectorSimilarityFunction.DOT_PRODUCT;
import static org.apache.lucene.index.VectorSimilarityFunction.EUCLIDEAN;
import static org.apache.lucene.index.VectorSimilarityFunction.MAXIMUM_INNER_PRODUCT;

public abstract sealed class ByteVectorScorerSupplier implements RandomVectorScorerSupplier {

    final int dims;
    final int maxOrd;
    final MemorySegmentAccessInput input;
    final ByteVectorValues values;
    final VectorSimilarityFunction fallbackScorer;

    static final boolean SUPPORTS_HEAP_SEGMENTS = Runtime.version().feature() >= 22;

    protected ByteVectorScorerSupplier(MemorySegmentAccessInput input, ByteVectorValues values, VectorSimilarityFunction fallbackScorer) {
        this.input = input;
        this.values = values;
        this.dims = values.dimension();
        this.maxOrd = values.size();
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
            // we might be able to get segments for individual vectors, so try separately
            scoreSeparately(firstOrd, ordinals, scores, numNodes);
        } else {
            final int vectorPitch = dims;

            if (SUPPORTS_HEAP_SEGMENTS) {
                var ordinalsSeg = MemorySegment.ofArray(ordinals);
                var scoresSeg = MemorySegment.ofArray(scores);
                bulkScoreFromSegment(vectorsSeg, dims, vectorPitch, firstOrd, ordinalsSeg, scoresSeg, numNodes);
            } else {
                try (var arena = Arena.ofConfined()) {
                    var ordinalsMemorySegment = arena.allocate((long) numNodes * Integer.BYTES, 32);
                    var scoresMemorySegment = arena.allocate((long) numNodes * Float.BYTES, 32);
                    MemorySegment.copy(ordinals, 0, ordinalsMemorySegment, ValueLayout.JAVA_INT, 0, numNodes);

                    bulkScoreFromSegment(vectorsSeg, dims, vectorPitch, firstOrd, ordinalsMemorySegment, scoresMemorySegment, numNodes);

                    MemorySegment.copy(scoresMemorySegment, ValueLayout.JAVA_FLOAT, 0, scores, 0, numNodes);
                }
            }
        }
    }

    private void scoreSeparately(int firstOrd, int[] ordinals, float[] scores, int numNodes) throws IOException {
        long firstByteOffset = (long) firstOrd * dims;
        byte[] firstVector = null;

        MemorySegment firstSeg = input.segmentSliceOrNull(firstByteOffset, dims);
        if (firstSeg == null) {
            firstVector = values.vectorValue(firstOrd).clone();
            for (int i = 0; i < numNodes; i++) {
                scores[i] = fallbackScorer.compare(firstVector, values.vectorValue(ordinals[i]));
            }
        } else {
            for (int i = 0; i < numNodes; i++) {
                long secondByteOffset = (long) ordinals[i] * dims;
                MemorySegment secondSeg = input.segmentSliceOrNull(secondByteOffset, dims);
                if (secondSeg == null) {
                    if (firstVector == null) {
                        firstVector = values.vectorValue(firstOrd).clone();
                    }
                    scores[i] = fallbackScorer.compare(firstVector, values.vectorValue(ordinals[i]));
                } else {
                    scores[i] = scoreFromSegments(firstSeg, secondSeg);
                }
            }
        }
    }

    final float scoreFromOrds(int firstOrd, int secondOrd) throws IOException {
        long firstByteOffset = (long) firstOrd * dims;
        long secondByteOffset = (long) secondOrd * dims;

        MemorySegment firstSeg = input.segmentSliceOrNull(firstByteOffset, dims);
        if (firstSeg == null) {
            return fallbackScore(firstOrd, secondOrd);
        }

        MemorySegment secondSeg = input.segmentSliceOrNull(secondByteOffset, dims);
        if (secondSeg == null) {
            return fallbackScore(firstOrd, secondOrd);
        }

        return scoreFromSegments(firstSeg, secondSeg);
    }

    abstract float scoreFromSegments(MemorySegment a, MemorySegment b);

    abstract void bulkScoreFromSegment(
        MemorySegment vectors,
        int vectorLength,
        int vectorPitch,
        int firstOrd,
        MemorySegment ordinals,
        MemorySegment scores,
        int numNodes
    );

    private float fallbackScore(int firstOrd, int secondOrd) throws IOException {
        byte[] a = values.vectorValue(firstOrd).clone();
        byte[] b = values.vectorValue(secondOrd);
        return fallbackScorer.compare(a, b);
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

    public static final class CosineSupplier extends ByteVectorScorerSupplier {

        public CosineSupplier(MemorySegmentAccessInput input, ByteVectorValues values) {
            super(input, values, COSINE);
        }

        private static float normalize(float cosine) {
            return (1 + cosine) / 2;
        }

        @Override
        float scoreFromSegments(MemorySegment a, MemorySegment b) {
            return normalize(Similarities.cosineI8(a, b, dims));
        }

        @Override
        void bulkScoreFromSegment(
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
            Similarities.cosineI8BulkWithOffsets(vectors, firstVector, dims, vectorPitch, ordinals, numNodes, scores);

            for (int i = 0; i < numNodes; ++i) {
                float squareDistance = scores.getAtIndex(ValueLayout.JAVA_FLOAT, i);
                scores.setAtIndex(ValueLayout.JAVA_FLOAT, i, normalize(squareDistance));
            }
        }

        @Override
        public CosineSupplier copy() {
            return new CosineSupplier(input.clone(), values);
        }
    }

    public static final class EuclideanSupplier extends ByteVectorScorerSupplier {

        public EuclideanSupplier(MemorySegmentAccessInput input, ByteVectorValues values) {
            super(input, values, EUCLIDEAN);
        }

        @Override
        float scoreFromSegments(MemorySegment a, MemorySegment b) {
            return VectorUtil.normalizeDistanceToUnitInterval(Similarities.squareDistanceI8(a, b, dims));
        }

        @Override
        void bulkScoreFromSegment(
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
            Similarities.squareDistanceI8BulkWithOffsets(vectors, firstVector, dims, vectorPitch, ordinals, numNodes, scores);

            for (int i = 0; i < numNodes; ++i) {
                float squareDistance = scores.getAtIndex(ValueLayout.JAVA_FLOAT, i);
                scores.setAtIndex(ValueLayout.JAVA_FLOAT, i, VectorUtil.normalizeDistanceToUnitInterval(squareDistance));
            }
        }

        @Override
        public EuclideanSupplier copy() {
            return new EuclideanSupplier(input.clone(), values);
        }
    }

    public static final class DotProductSupplier extends ByteVectorScorerSupplier {

        private final float denom = (float) (dims * (1 << 15));

        public DotProductSupplier(MemorySegmentAccessInput input, ByteVectorValues values) {
            super(input, values, DOT_PRODUCT);
        }

        private float normalize(float dotProduct) {
            return 0.5f + dotProduct / denom;
        }

        @Override
        float scoreFromSegments(MemorySegment a, MemorySegment b) {
            return normalize(Similarities.dotProductI8(a, b, dims));
        }

        @Override
        void bulkScoreFromSegment(
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
            Similarities.dotProductI8BulkWithOffsets(vectors, firstVector, dims, vectorPitch, ordinals, numNodes, scores);

            for (int i = 0; i < numNodes; ++i) {
                float dotProduct = scores.getAtIndex(ValueLayout.JAVA_FLOAT, i);
                scores.setAtIndex(ValueLayout.JAVA_FLOAT, i, normalize(dotProduct));
            }
        }

        @Override
        public DotProductSupplier copy() {
            return new DotProductSupplier(input.clone(), values);
        }
    }

    public static final class MaxInnerProductSupplier extends ByteVectorScorerSupplier {

        public MaxInnerProductSupplier(MemorySegmentAccessInput input, ByteVectorValues values) {
            super(input, values, MAXIMUM_INNER_PRODUCT);
        }

        @Override
        float scoreFromSegments(MemorySegment a, MemorySegment b) {
            return VectorUtil.scaleMaxInnerProductScore(Similarities.dotProductI8(a, b, dims));
        }

        @Override
        void bulkScoreFromSegment(
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
            Similarities.dotProductI8BulkWithOffsets(vectors, firstVector, dims, vectorPitch, ordinals, numNodes, scores);

            for (int i = 0; i < numNodes; ++i) {
                float dotProduct = scores.getAtIndex(ValueLayout.JAVA_FLOAT, i);
                scores.setAtIndex(ValueLayout.JAVA_FLOAT, i, VectorUtil.scaleMaxInnerProductScore(dotProduct));
            }
        }

        @Override
        public MaxInnerProductSupplier copy() {
            return new MaxInnerProductSupplier(input.clone(), values);
        }
    }
}
