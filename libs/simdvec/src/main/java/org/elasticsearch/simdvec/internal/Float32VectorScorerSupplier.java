/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.simdvec.internal;

import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.VectorUtil;
import org.apache.lucene.util.hnsw.RandomVectorScorerSupplier;
import org.apache.lucene.util.hnsw.UpdateableRandomVectorScorer;
import org.elasticsearch.lucene.store.IndexInputUtils;
import org.elasticsearch.simdvec.SimdVecLibrary;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;

public abstract sealed class Float32VectorScorerSupplier implements RandomVectorScorerSupplier {

    private static final SimdVecLibrary DISTANCE_FUNCS = SimdVecLibrary.instance().orElseThrow(AssertionError::new);

    final int dims;
    final int vectorByteSize;
    final IndexInput input;
    final FloatVectorValues values;
    final FixedSizeScratch firstScratch;
    final FixedSizeScratch secondScratch;
    final AddressesScratch addrsScratch = new AddressesScratch();
    final OffsetsScratch offsetsScratch = new OffsetsScratch();
    final float[] maxScore = new float[] { Float.NEGATIVE_INFINITY };

    protected Float32VectorScorerSupplier(IndexInput input, FloatVectorValues values) {
        this.input = input;
        this.values = values;
        this.dims = values.dimension();
        this.vectorByteSize = dims * Float.BYTES;
        this.firstScratch = new FixedSizeScratch(vectorByteSize);
        this.secondScratch = new FixedSizeScratch(vectorByteSize);
    }

    protected final void checkOrdinal(int ord) {
        if (ord < 0 || ord >= values.size()) {
            throw new IllegalArgumentException("illegal ordinal: " + ord);
        }
    }

    final float bulkScoreFromOrds(int firstOrd, int[] ordinals, float[] scores, int numNodes) throws IOException {
        if (numNodes == 0) {
            return Float.NEGATIVE_INFINITY;
        }

        long queryByteOffset = (long) firstOrd * vectorByteSize;
        input.seek(queryByteOffset);
        return IndexInputUtils.withFloatSlice(input, vectorByteSize, firstScratch, query -> {
            long[] offsets = offsetsScratch.get(numNodes);
            for (int i = 0; i < numNodes; i++) {
                offsets[i] = (long) ordinals[i] * vectorByteSize;
            }

            maxScore[0] = Float.NEGATIVE_INFINITY;
            boolean resolved = IndexInputUtils.withSliceAddresses(
                input,
                offsets,
                vectorByteSize,
                numNodes,
                addrsScratch,
                addrs -> maxScore[0] = bulkScoreFromSegment(addrs, query, scores, numNodes)
            );
            if (resolved == false) {
                maxScore[0] = scorePerVectorFallback(query, scores, numNodes, offsets);
            }
            return maxScore[0];
        });
    }

    private float scorePerVectorFallback(MemorySegment query, float[] scores, int numNodes, long[] offsets) throws IOException {
        float maxScore = Float.NEGATIVE_INFINITY;
        for (int i = 0; i < numNodes; i++) {
            final int idx = i;
            input.seek(offsets[idx]);
            IndexInputUtils.withVoidSlice(input, vectorByteSize, secondScratch, vector -> scores[idx] = scoreFromSegments(query, vector));
            maxScore = Math.max(maxScore, scores[idx]);
        }
        return maxScore;
    }

    final float scoreFromOrds(int firstOrd, int secondOrd) throws IOException {
        long firstByteOffset = (long) firstOrd * vectorByteSize;
        long secondByteOffset = (long) secondOrd * vectorByteSize;

        input.seek(firstByteOffset);
        return IndexInputUtils.withFloatSlice(input, vectorByteSize, firstScratch, firstSeg -> {
            input.seek(secondByteOffset);
            return IndexInputUtils.withFloatSlice(
                input,
                vectorByteSize,
                secondScratch,
                secondSeg -> scoreFromSegments(firstSeg, secondSeg)
            );
        });
    }

    abstract float scoreFromSegments(MemorySegment a, MemorySegment b);

    /**
     * Scores {@code numNodes} candidates in bulk, writing normalized results into {@code scores}.
     *
     * <p>Uses the plain (non-{@code @Critical}) binding, scoring into a confined arena opened and
     * closed within the call, whenever {@code query} is native -- the common merge/HNSW-build case.
     * See {@link SimdVecLibrary#dotProductF32BulkSparseOffHeap} for why. Falls back to the
     * {@code @Critical} binding, writing straight into {@code scores}, only when {@code query} is
     * heap-backed (the {@link IndexInputUtils} last-resort heap-copy case).
     */
    abstract float bulkScoreFromSegment(MemorySegment addresses, MemorySegment query, float[] scores, int numNodes);

    /** Normalizes {@code numNodes} values from {@code segment} into {@code scores}, returning the max. */
    private static float readScores(MemorySegment segment, float[] scores, int numNodes, FloatUnaryOperator normalize) {
        float max = Float.NEGATIVE_INFINITY;
        for (int i = 0; i < numNodes; ++i) {
            float normalized = normalize.apply(segment.getAtIndex(ValueLayout.JAVA_FLOAT, i));
            scores[i] = normalized;
            max = Math.max(max, normalized);
        }
        return max;
    }

    @FunctionalInterface
    private interface FloatUnaryOperator {
        float apply(float v);
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
            public float bulkScore(int[] nodes, float[] scores, int numNodes) throws IOException {
                return bulkScoreFromOrds(ord, nodes, scores, numNodes);
            }

            @Override
            public void setScoringOrdinal(int node) {
                checkOrdinal(node);
                this.ord = node;
            }
        };
    }

    public static final class EuclideanSupplier extends Float32VectorScorerSupplier {

        public EuclideanSupplier(IndexInput input, FloatVectorValues values) {
            super(input, values);
        }

        @Override
        float scoreFromSegments(MemorySegment a, MemorySegment b) {
            return VectorUtil.normalizeDistanceToUnitInterval(DISTANCE_FUNCS.squareDistanceF32(a, b, dims));
        }

        @Override
        protected float bulkScoreFromSegment(MemorySegment addresses, MemorySegment query, float[] scores, int numNodes) {
            if (query.isNative()) {
                try (Arena arena = Arena.ofConfined()) {
                    MemorySegment segment = arena.allocate((long) numNodes * Float.BYTES, ValueLayout.JAVA_FLOAT.byteAlignment());
                    DISTANCE_FUNCS.squareDistanceF32BulkSparseOffHeap(addresses, query, dims, numNodes, segment);
                    return readScores(segment, scores, numNodes, VectorUtil::normalizeDistanceToUnitInterval);
                }
            }
            MemorySegment segment = MemorySegment.ofArray(scores);
            DISTANCE_FUNCS.squareDistanceF32BulkSparse(addresses, query, dims, numNodes, segment);
            return readScores(segment, scores, numNodes, VectorUtil::normalizeDistanceToUnitInterval);
        }

        @Override
        public EuclideanSupplier copy() {
            return new EuclideanSupplier(input.clone(), values);
        }
    }

    public static final class DotProductSupplier extends Float32VectorScorerSupplier {

        public DotProductSupplier(IndexInput input, FloatVectorValues values) {
            super(input, values);
        }

        @Override
        float scoreFromSegments(MemorySegment a, MemorySegment b) {
            return VectorUtil.normalizeToUnitInterval(DISTANCE_FUNCS.dotProductF32(a, b, dims));
        }

        @Override
        protected float bulkScoreFromSegment(MemorySegment addresses, MemorySegment query, float[] scores, int numNodes) {
            if (query.isNative()) {
                try (Arena arena = Arena.ofConfined()) {
                    MemorySegment segment = arena.allocate((long) numNodes * Float.BYTES, ValueLayout.JAVA_FLOAT.byteAlignment());
                    DISTANCE_FUNCS.dotProductF32BulkSparseOffHeap(addresses, query, dims, numNodes, segment);
                    return readScores(segment, scores, numNodes, VectorUtil::normalizeToUnitInterval);
                }
            }
            MemorySegment segment = MemorySegment.ofArray(scores);
            DISTANCE_FUNCS.dotProductF32BulkSparse(addresses, query, dims, numNodes, segment);
            return readScores(segment, scores, numNodes, VectorUtil::normalizeToUnitInterval);
        }

        @Override
        public DotProductSupplier copy() {
            return new DotProductSupplier(input.clone(), values);
        }
    }

    public static final class MaxInnerProductSupplier extends Float32VectorScorerSupplier {

        public MaxInnerProductSupplier(IndexInput input, FloatVectorValues values) {
            super(input, values);
        }

        @Override
        float scoreFromSegments(MemorySegment a, MemorySegment b) {
            return VectorUtil.scaleMaxInnerProductScore(DISTANCE_FUNCS.dotProductF32(a, b, dims));
        }

        @Override
        protected float bulkScoreFromSegment(MemorySegment addresses, MemorySegment query, float[] scores, int numNodes) {
            if (query.isNative()) {
                try (Arena arena = Arena.ofConfined()) {
                    MemorySegment segment = arena.allocate((long) numNodes * Float.BYTES, ValueLayout.JAVA_FLOAT.byteAlignment());
                    DISTANCE_FUNCS.dotProductF32BulkSparseOffHeap(addresses, query, dims, numNodes, segment);
                    return readScores(segment, scores, numNodes, VectorUtil::scaleMaxInnerProductScore);
                }
            }
            MemorySegment segment = MemorySegment.ofArray(scores);
            DISTANCE_FUNCS.dotProductF32BulkSparse(addresses, query, dims, numNodes, segment);
            return readScores(segment, scores, numNodes, VectorUtil::scaleMaxInnerProductScore);
        }

        @Override
        public MaxInnerProductSupplier copy() {
            return new MaxInnerProductSupplier(input.clone(), values);
        }
    }
}
