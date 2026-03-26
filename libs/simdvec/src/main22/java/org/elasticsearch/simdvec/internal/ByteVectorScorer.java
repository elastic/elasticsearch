/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.simdvec.internal;

import org.apache.lucene.codecs.lucene95.HasIndexSlice;
import org.apache.lucene.index.ByteVectorValues;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.store.FilterIndexInput;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.MemorySegmentAccessInput;
import org.apache.lucene.util.VectorUtil;
import org.apache.lucene.util.hnsw.RandomVectorScorer;
import org.elasticsearch.core.DirectAccessInput;
import org.elasticsearch.simdvec.MemorySegmentAccessInputAccess;

import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.util.Optional;

import static org.elasticsearch.simdvec.internal.Similarities.cosineI8;
import static org.elasticsearch.simdvec.internal.Similarities.dotProductI8;
import static org.elasticsearch.simdvec.internal.Similarities.squareDistanceI8;

public abstract sealed class ByteVectorScorer extends RandomVectorScorer.AbstractRandomVectorScorer {

    final int dimensions;
    final int vectorByteSize;
    final IndexInput input;
    final MemorySegment query;
    byte[] scratch;

    public static Optional<RandomVectorScorer> create(VectorSimilarityFunction sim, ByteVectorValues values, byte[] queryVector) {
        checkDimensions(queryVector.length, values.dimension());
        IndexInput input = values instanceof HasIndexSlice slice ? slice.getSlice() : null;
        if (input == null) {
            return Optional.empty();
        }
        input = FilterIndexInput.unwrapOnlyTest(input);
        input = MemorySegmentAccessInputAccess.unwrap(input);
        if (input instanceof MemorySegmentAccessInput || input instanceof DirectAccessInput) {
            IndexInputUtils.checkInputType(input);
            checkInvariants(values.size(), values.getVectorByteLength(), input);

            return switch (sim) {
                case COSINE -> Optional.of(new CosineScorer(input, values, queryVector));
                case DOT_PRODUCT -> Optional.of(new DotProductScorer(input, values, queryVector));
                case EUCLIDEAN -> Optional.of(new EuclideanScorer(input, values, queryVector));
                case MAXIMUM_INNER_PRODUCT -> Optional.of(new MaxInnerProductScorer(input, values, queryVector));
            };
        }
        return Optional.empty();
    }

    ByteVectorScorer(IndexInput input, ByteVectorValues values, byte[] queryVector) {
        super(values);
        this.input = input;
        assert queryVector.length == values.dimension();
        this.dimensions = values.dimension();
        this.vectorByteSize = values.getVectorByteLength();
        this.query = MemorySegment.ofArray(queryVector);
    }

    byte[] getScratch(int length) {
        if (scratch == null || scratch.length < length) {
            scratch = new byte[length];
        }
        return scratch;
    }

    static void checkInvariants(int maxOrd, int vectorByteLength, IndexInput input) {
        if (input.length() < (long) vectorByteLength * maxOrd) {
            throw new IllegalArgumentException("input length is less than expected vector data");
        }
    }

    final void checkOrdinal(int ord) {
        if (ord < 0 || ord >= maxOrd()) {
            throw new IllegalArgumentException("illegal ordinal: " + ord);
        }
    }

    /**
     * Resolves native memory addresses for the given node ordinals and calls
     * the sparse scoring function. Returns true if addresses were resolved
     * (via mmap or DirectAccessInput), false if fallback scoring is needed.
     */
    final boolean bulkScoreWithSparse(int[] nodes, float[] scores, int numNodes, SparseScorer sparseScorer) throws IOException {
        if (numNodes == 0) {
            return false;
        }
        long[] offsets = new long[numNodes];
        for (int i = 0; i < numNodes; i++) {
            offsets[i] = (long) nodes[i] * vectorByteSize;
        }
        return IndexInputUtils.withSliceAddresses(input, offsets, vectorByteSize, numNodes, a -> {
            sparseScorer.score(a, query, dimensions, numNodes, MemorySegment.ofArray(scores));
        });
    }

    public static final class DotProductScorer extends ByteVectorScorer {
        private final float denom = (float) (dimensions * (1 << 15));

        public DotProductScorer(IndexInput in, ByteVectorValues values, byte[] query) {
            super(in, values, query);
        }

        private float normalize(float dotProduct) {
            return 0.5f + dotProduct / denom;
        }

        @Override
        public float score(int node) throws IOException {
            checkOrdinal(node);
            long byteOffset = (long) node * vectorByteSize;
            input.seek(byteOffset);
            return IndexInputUtils.withSlice(input, vectorByteSize, this::getScratch, seg -> {
                float dp = dotProductI8(query, seg, dimensions);
                return normalize(dp);
            });
        }

        @Override
        public float bulkScore(int[] nodes, float[] scores, int numNodes) throws IOException {
            if (bulkScoreWithSparse(nodes, scores, numNodes, Similarities::dotProductI8BulkSparse)) {
                float max = Float.NEGATIVE_INFINITY;
                for (int i = 0; i < numNodes; ++i) {
                    scores[i] = normalize(scores[i]);
                    max = Math.max(max, scores[i]);
                }
                return max;
            } else {
                return super.bulkScore(nodes, scores, numNodes);
            }
        }
    }

    public static final class CosineScorer extends ByteVectorScorer {
        public CosineScorer(IndexInput in, ByteVectorValues values, byte[] query) {
            super(in, values, query);
        }

        private static float normalize(float cosine) {
            return (1 + cosine) / 2;
        }

        @Override
        public float score(int node) throws IOException {
            checkOrdinal(node);
            long byteOffset = (long) node * vectorByteSize;
            input.seek(byteOffset);
            return IndexInputUtils.withSlice(input, vectorByteSize, this::getScratch, seg -> {
                float cos = cosineI8(query, seg, dimensions);
                return normalize(cos);
            });
        }

        @Override
        public float bulkScore(int[] nodes, float[] scores, int numNodes) throws IOException {
            if (bulkScoreWithSparse(nodes, scores, numNodes, Similarities::cosineI8BulkSparse)) {
                float max = Float.NEGATIVE_INFINITY;
                for (int i = 0; i < numNodes; ++i) {
                    scores[i] = normalize(scores[i]);
                    max = Math.max(max, scores[i]);
                }
                return max;
            } else {
                return super.bulkScore(nodes, scores, numNodes);
            }
        }
    }

    public static final class EuclideanScorer extends ByteVectorScorer {
        public EuclideanScorer(IndexInput in, ByteVectorValues values, byte[] query) {
            super(in, values, query);
        }

        @Override
        public float score(int node) throws IOException {
            checkOrdinal(node);
            long byteOffset = (long) node * vectorByteSize;
            input.seek(byteOffset);
            return IndexInputUtils.withSlice(input, vectorByteSize, this::getScratch, seg -> {
                float sqDist = squareDistanceI8(query, seg, dimensions);
                return VectorUtil.normalizeDistanceToUnitInterval(sqDist);
            });
        }

        @Override
        public float bulkScore(int[] nodes, float[] scores, int numNodes) throws IOException {
            if (bulkScoreWithSparse(nodes, scores, numNodes, Similarities::squareDistanceI8BulkSparse)) {
                float max = Float.NEGATIVE_INFINITY;
                for (int i = 0; i < numNodes; ++i) {
                    scores[i] = VectorUtil.normalizeDistanceToUnitInterval(scores[i]);
                    max = Math.max(max, scores[i]);
                }
                return max;
            } else {
                return super.bulkScore(nodes, scores, numNodes);
            }
        }
    }

    public static final class MaxInnerProductScorer extends ByteVectorScorer {
        public MaxInnerProductScorer(IndexInput in, ByteVectorValues values, byte[] query) {
            super(in, values, query);
        }

        @Override
        public float score(int node) throws IOException {
            checkOrdinal(node);
            long byteOffset = (long) node * vectorByteSize;
            input.seek(byteOffset);
            return IndexInputUtils.withSlice(input, vectorByteSize, this::getScratch, seg -> {
                float dp = dotProductI8(query, seg, dimensions);
                return VectorUtil.scaleMaxInnerProductScore(dp);
            });
        }

        @Override
        public float bulkScore(int[] nodes, float[] scores, int numNodes) throws IOException {
            if (bulkScoreWithSparse(nodes, scores, numNodes, Similarities::dotProductI8BulkSparse)) {
                float max = Float.NEGATIVE_INFINITY;
                for (int i = 0; i < numNodes; ++i) {
                    scores[i] = VectorUtil.scaleMaxInnerProductScore(scores[i]);
                    max = Math.max(max, scores[i]);
                }
                return max;
            } else {
                return super.bulkScore(nodes, scores, numNodes);
            }
        }
    }

    static void checkDimensions(int queryLen, int fieldLen) {
        if (queryLen != fieldLen) {
            throw new IllegalArgumentException("vector query dimension: " + queryLen + " differs from field dimension: " + fieldLen);
        }
    }
}
