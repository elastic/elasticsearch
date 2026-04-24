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
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.store.FilterIndexInput;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.VectorUtil;
import org.apache.lucene.util.hnsw.RandomVectorScorer;
import org.elasticsearch.simdvec.MemorySegmentAccessInputAccess;

import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.util.Optional;

import static org.elasticsearch.simdvec.internal.Similarities.dotProductDBF16QF32;
import static org.elasticsearch.simdvec.internal.Similarities.dotProductDBF16QF32BulkSparse;
import static org.elasticsearch.simdvec.internal.Similarities.squareDistanceDBF16QF32;
import static org.elasticsearch.simdvec.internal.Similarities.squareDistanceDBF16QF32BulkSparse;
import static org.elasticsearch.simdvec.internal.vectorization.JdkFeatures.SUPPORTS_HEAP_SEGMENTS;

public abstract sealed class BFloat16VectorScorer extends RandomVectorScorer.AbstractRandomVectorScorer {

    final int dimensions;
    final int vectorByteSize;
    final IndexInput input;
    final MemorySegment query;
    final FixedSizeScratch scratch;

    public static Optional<RandomVectorScorer> create(VectorSimilarityFunction sim, FloatVectorValues values, float[] queryVector) {
        if (SUPPORTS_HEAP_SEGMENTS == false) {
            return Optional.empty();
        }
        checkDimensions(queryVector.length, values.dimension());
        IndexInput input = values instanceof HasIndexSlice slice ? slice.getSlice() : null;
        if (input == null) {
            return Optional.empty();
        }
        input = FilterIndexInput.unwrapOnlyTest(input);
        input = MemorySegmentAccessInputAccess.unwrap(input);
        checkInvariants(values.size(), values.dimension(), input);

        return switch (sim) {
            case COSINE, DOT_PRODUCT -> Optional.of(new DotProductScorer(input, values, queryVector));
            case EUCLIDEAN -> Optional.of(new EuclideanScorer(input, values, queryVector));
            case MAXIMUM_INNER_PRODUCT -> Optional.of(new MaxInnerProductScorer(input, values, queryVector));
        };
    }

    BFloat16VectorScorer(IndexInput input, FloatVectorValues values, float[] queryVector) {
        super(values);
        this.input = input;
        assert queryVector.length == values.dimension();
        this.dimensions = values.dimension();
        this.vectorByteSize = values.getVectorByteLength();
        this.query = MemorySegment.ofArray(queryVector);
        this.scratch = new FixedSizeScratch(vectorByteSize);
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

    public static final class DotProductScorer extends BFloat16VectorScorer {
        public DotProductScorer(IndexInput in, FloatVectorValues values, float[] query) {
            super(in, values, query);
        }

        @Override
        public float score(int node) throws IOException {
            checkOrdinal(node);

            long byteOffset = (long) node * vectorByteSize;
            input.seek(byteOffset);
            float dotProduct = IndexInputUtils.withSlice(
                input,
                vectorByteSize,
                scratch::getScratch,
                memorySegment -> dotProductDBF16QF32(memorySegment, query, dimensions)
            );
            return VectorUtil.normalizeToUnitInterval(dotProduct);
        }

        @Override
        public float bulkScore(int[] nodes, float[] scores, int numNodes) throws IOException {
            if (numNodes == 0) {
                return Float.NEGATIVE_INFINITY;
            }

            long[] offsets = new long[numNodes];
            for (int i = 0; i < numNodes; i++) {
                offsets[i] = (long) nodes[i] * vectorByteSize;
            }
            boolean resolved = IndexInputUtils.withSliceAddresses(
                input,
                offsets,
                vectorByteSize,
                numNodes,
                addrs -> dotProductDBF16QF32BulkSparse(addrs, query, dimensions, numNodes, MemorySegment.ofArray(scores))
            );
            if (resolved) {
                float max = Float.NEGATIVE_INFINITY;
                for (int i = 0; i < numNodes; ++i) {
                    scores[i] = VectorUtil.normalizeToUnitInterval(scores[i]);
                    max = Math.max(max, scores[i]);
                }
                return max;
            }
            return super.bulkScore(nodes, scores, numNodes);
        }
    }

    public static final class EuclideanScorer extends BFloat16VectorScorer {
        public EuclideanScorer(IndexInput in, FloatVectorValues values, float[] query) {
            super(in, values, query);
        }

        @Override
        public float score(int node) throws IOException {
            checkOrdinal(node);

            long byteOffset = (long) node * vectorByteSize;
            input.seek(byteOffset);
            float sqDist = IndexInputUtils.withSlice(
                input,
                vectorByteSize,
                scratch::getScratch,
                memorySegment -> squareDistanceDBF16QF32(memorySegment, query, dimensions)
            );
            return VectorUtil.normalizeDistanceToUnitInterval(sqDist);
        }

        @Override
        public float bulkScore(int[] nodes, float[] scores, int numNodes) throws IOException {
            if (numNodes == 0) {
                return Float.NEGATIVE_INFINITY;
            }

            long[] offsets = new long[numNodes];
            for (int i = 0; i < numNodes; i++) {
                offsets[i] = (long) nodes[i] * vectorByteSize;
            }
            boolean resolved = IndexInputUtils.withSliceAddresses(
                input,
                offsets,
                vectorByteSize,
                numNodes,
                addrs -> squareDistanceDBF16QF32BulkSparse(addrs, query, dimensions, numNodes, MemorySegment.ofArray(scores))
            );
            if (resolved) {
                float max = Float.NEGATIVE_INFINITY;
                for (int i = 0; i < numNodes; ++i) {
                    scores[i] = VectorUtil.normalizeDistanceToUnitInterval(scores[i]);
                    max = Math.max(max, scores[i]);
                }
                return max;
            }
            return super.bulkScore(nodes, scores, numNodes);
        }
    }

    public static final class MaxInnerProductScorer extends BFloat16VectorScorer {
        public MaxInnerProductScorer(IndexInput in, FloatVectorValues values, float[] query) {
            super(in, values, query);
        }

        @Override
        public float score(int node) throws IOException {
            checkOrdinal(node);

            long byteOffset = (long) node * vectorByteSize;
            input.seek(byteOffset);
            float dotProduct = IndexInputUtils.withSlice(
                input,
                vectorByteSize,
                scratch::getScratch,
                memorySegment -> dotProductDBF16QF32(memorySegment, query, dimensions)
            );
            return VectorUtil.scaleMaxInnerProductScore(dotProduct);
        }

        @Override
        public float bulkScore(int[] nodes, float[] scores, int numNodes) throws IOException {
            if (numNodes == 0) {
                return Float.NEGATIVE_INFINITY;
            }

            long[] offsets = new long[numNodes];
            for (int i = 0; i < numNodes; i++) {
                offsets[i] = (long) nodes[i] * vectorByteSize;
            }
            boolean resolved = IndexInputUtils.withSliceAddresses(
                input,
                offsets,
                vectorByteSize,
                numNodes,
                addrs -> dotProductDBF16QF32BulkSparse(addrs, query, dimensions, numNodes, MemorySegment.ofArray(scores))
            );
            if (resolved) {
                float max = Float.NEGATIVE_INFINITY;
                for (int i = 0; i < numNodes; ++i) {
                    scores[i] = VectorUtil.scaleMaxInnerProductScore(scores[i]);
                    max = Math.max(max, scores[i]);
                }
                return max;
            }
            return super.bulkScore(nodes, scores, numNodes);
        }
    }

    static void checkDimensions(int queryLen, int fieldLen) {
        if (queryLen != fieldLen) {
            throw new IllegalArgumentException("vector query dimension: " + queryLen + " differs from field dimension: " + fieldLen);
        }
    }
}
