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
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.VectorUtil;
import org.apache.lucene.util.hnsw.RandomVectorScorerSupplier;
import org.apache.lucene.util.hnsw.UpdateableRandomVectorScorer;
import org.elasticsearch.lucene.store.IndexInputUtils;
import org.elasticsearch.lucene.store.SegmentBufferScratch;
import org.elasticsearch.simdvec.SimdVecLibrary;

import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;

// Scores pairs of indexed vectors (ordinal vs ordinal) for graph construction and segment merging.
public abstract sealed class Int8VectorScorerSupplier implements RandomVectorScorerSupplier {

    private static final SimdVecLibrary DISTANCE_FUNCS = SimdVecLibrary.instance().orElseThrow(AssertionError::new);

    final int dims;
    /**
     * The length in bytes of one vector. For this scorer, it matches the pitch (the distance in memory between 2 vectors, when laid
     * out consecutively) and the total vector size (no padding, no extra fields).
     */
    final int vectorByteSize;
    final IndexInput input;
    final ByteVectorValues values;
    final FixedSizeSegmentScratch queryScratch;
    final FixedSizeSegmentScratch documentScratch;
    final AddressesScratch addrsScratch = new AddressesScratch();
    final OffsetsScratch offsetsScratch = new OffsetsScratch();
    final SegmentBufferScratch scoresScratch = new SegmentBufferScratch();
    final float[] maxScore = new float[] { Float.NEGATIVE_INFINITY };

    protected Int8VectorScorerSupplier(IndexInput input, ByteVectorValues values) {
        this.input = input;
        this.values = values;
        this.dims = values.dimension();
        this.vectorByteSize = values.getVectorByteLength();
        this.queryScratch = new FixedSizeSegmentScratch(vectorByteSize);
        this.documentScratch = new FixedSizeSegmentScratch(vectorByteSize);
    }

    protected final void checkOrdinal(int ord) {
        if (ord < 0 || ord >= values.size()) {
            throw new IllegalArgumentException("illegal ordinal: " + ord);
        }
    }

    final float bulkScoreFromOrds(int queryOrd, int[] ordinals, float[] scores, int numNodes) throws IOException {
        if (numNodes == 0) {
            return Float.NEGATIVE_INFINITY;
        }

        long queryByteOffset = (long) queryOrd * vectorByteSize;
        input.seek(queryByteOffset);
        return IndexInputUtils.withFloatSlice(input, vectorByteSize, queryScratch, query -> {
            long[] offsets = offsetsScratch.get(numNodes);
            for (int i = 0; i < numNodes; i++) {
                offsets[i] = (long) ordinals[i] * vectorByteSize;
            }

            float[] maxScore = new float[] { Float.NEGATIVE_INFINITY };
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
            IndexInputUtils.withVoidSlice(input, vectorByteSize, documentScratch, vector -> scores[idx] = scoreFromSegments(vector, query));
            maxScore = Math.max(maxScore, scores[idx]);
        }
        return maxScore;
    }

    final float scoreFromOrds(int queryOrd, int documentOrd) throws IOException {
        long queryByteOffset = (long) queryOrd * vectorByteSize;
        long documentByteOffset = (long) documentOrd * vectorByteSize;

        input.seek(queryByteOffset);
        return IndexInputUtils.withFloatSlice(input, vectorByteSize, queryScratch, querySeg -> {
            input.seek(documentByteOffset);
            return IndexInputUtils.withFloatSlice(
                input,
                vectorByteSize,
                documentScratch,
                documentSeg -> scoreFromSegments(documentSeg, querySeg)
            );
        });
    }

    abstract float scoreFromSegments(MemorySegment document, MemorySegment query);

    abstract float bulkScoreFromSegment(MemorySegment addresses, MemorySegment query, float[] scores, int numNodes);

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

    public static final class CosineSupplier extends Int8VectorScorerSupplier {

        public CosineSupplier(IndexInput input, ByteVectorValues values) {
            super(input, values);
        }

        private static float normalize(float cosine) {
            return (1 + cosine) / 2;
        }

        @Override
        float scoreFromSegments(MemorySegment document, MemorySegment query) {
            return normalize(DISTANCE_FUNCS.cosineI8(document, query, dims));
        }

        @Override
        float bulkScoreFromSegment(MemorySegment addresses, MemorySegment query, float[] scores, int numNodes) {
            MemorySegment segment = scoresScratch.apply(numNodes * Float.BYTES);
            DISTANCE_FUNCS.cosineI8BulkSparse(addresses, query, dims, numNodes, segment);

            float max = Float.NEGATIVE_INFINITY;
            for (int i = 0; i < numNodes; ++i) {
                float squareDistance = segment.getAtIndex(ValueLayout.JAVA_FLOAT, i);
                float normalized = normalize(squareDistance);
                scores[i] = normalized;
                max = Math.max(max, normalized);
            }
            return max;
        }

        @Override
        public CosineSupplier copy() {
            return new CosineSupplier(input.clone(), values);
        }
    }

    public static final class EuclideanSupplier extends Int8VectorScorerSupplier {

        public EuclideanSupplier(IndexInput input, ByteVectorValues values) {
            super(input, values);
        }

        @Override
        float scoreFromSegments(MemorySegment document, MemorySegment query) {
            return VectorUtil.normalizeDistanceToUnitInterval(DISTANCE_FUNCS.squareDistanceI8(document, query, dims));
        }

        @Override
        float bulkScoreFromSegment(MemorySegment addresses, MemorySegment query, float[] scores, int numNodes) {
            MemorySegment segment = scoresScratch.apply(numNodes * Float.BYTES);
            DISTANCE_FUNCS.squareDistanceI8BulkSparse(addresses, query, dims, numNodes, segment);

            float max = Float.NEGATIVE_INFINITY;
            for (int i = 0; i < numNodes; ++i) {
                float squareDistance = segment.getAtIndex(ValueLayout.JAVA_FLOAT, i);
                float normalized = VectorUtil.normalizeDistanceToUnitInterval(squareDistance);
                scores[i] = normalized;
                max = Math.max(max, normalized);
            }
            return max;
        }

        @Override
        public EuclideanSupplier copy() {
            return new EuclideanSupplier(input.clone(), values);
        }
    }

    public static final class DotProductSupplier extends Int8VectorScorerSupplier {

        private final float denom = (float) (dims * (1 << 15));

        public DotProductSupplier(IndexInput input, ByteVectorValues values) {
            super(input, values);
        }

        private float normalize(float dotProduct) {
            return 0.5f + dotProduct / denom;
        }

        @Override
        float scoreFromSegments(MemorySegment document, MemorySegment query) {
            return normalize(DISTANCE_FUNCS.dotProductI8(document, query, dims));
        }

        @Override
        float bulkScoreFromSegment(MemorySegment addresses, MemorySegment query, float[] scores, int numNodes) {
            MemorySegment segment = scoresScratch.apply(numNodes * Float.BYTES);
            DISTANCE_FUNCS.dotProductI8BulkSparse(addresses, query, dims, numNodes, segment);

            float max = Float.NEGATIVE_INFINITY;
            for (int i = 0; i < numNodes; ++i) {
                float dotProduct = segment.getAtIndex(ValueLayout.JAVA_FLOAT, i);
                float normalized = normalize(dotProduct);
                scores[i] = normalized;
                max = Math.max(max, normalized);
            }
            return max;
        }

        @Override
        public DotProductSupplier copy() {
            return new DotProductSupplier(input.clone(), values);
        }
    }

    public static final class MaxInnerProductSupplier extends Int8VectorScorerSupplier {

        public MaxInnerProductSupplier(IndexInput input, ByteVectorValues values) {
            super(input, values);
        }

        @Override
        float scoreFromSegments(MemorySegment document, MemorySegment query) {
            return VectorUtil.scaleMaxInnerProductScore(DISTANCE_FUNCS.dotProductI8(document, query, dims));
        }

        @Override
        float bulkScoreFromSegment(MemorySegment addresses, MemorySegment query, float[] scores, int numNodes) {
            MemorySegment segment = scoresScratch.apply(numNodes * Float.BYTES);
            DISTANCE_FUNCS.dotProductI8BulkSparse(addresses, query, dims, numNodes, segment);

            float max = Float.NEGATIVE_INFINITY;
            for (int i = 0; i < numNodes; ++i) {
                float dotProduct = segment.getAtIndex(ValueLayout.JAVA_FLOAT, i);
                float normalized = VectorUtil.scaleMaxInnerProductScore(dotProduct);
                scores[i] = normalized;
                max = Math.max(max, normalized);
            }
            return max;
        }

        @Override
        public MaxInnerProductSupplier copy() {
            return new MaxInnerProductSupplier(input.clone(), values);
        }
    }
}
