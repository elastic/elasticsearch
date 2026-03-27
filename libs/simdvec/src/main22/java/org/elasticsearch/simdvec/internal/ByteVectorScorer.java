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
import org.elasticsearch.simdvec.MemorySegmentAccessInputAccess;

import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.util.Optional;

import static org.elasticsearch.simdvec.internal.Similarities.cosineI8;
import static org.elasticsearch.simdvec.internal.Similarities.cosineI8BulkWithOffsets;
import static org.elasticsearch.simdvec.internal.Similarities.dotProductI8;
import static org.elasticsearch.simdvec.internal.Similarities.dotProductI8BulkWithOffsets;
import static org.elasticsearch.simdvec.internal.Similarities.squareDistanceI8;
import static org.elasticsearch.simdvec.internal.Similarities.squareDistanceI8BulkWithOffsets;

public abstract sealed class ByteVectorScorer extends RandomVectorScorer.AbstractRandomVectorScorer {

    final int dimensions;
    final int vectorByteSize;
    final MemorySegmentAccessInput input;
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
        if (input instanceof MemorySegmentAccessInput msInput) {
            checkInvariants(values.size(), values.getVectorByteLength(), input);

            return switch (sim) {
                case COSINE -> Optional.of(new CosineScorer(msInput, values, queryVector));
                case DOT_PRODUCT -> Optional.of(new DotProductScorer(msInput, values, queryVector));
                case EUCLIDEAN -> Optional.of(new EuclideanScorer(msInput, values, queryVector));
                case MAXIMUM_INNER_PRODUCT -> Optional.of(new MaxInnerProductScorer(msInput, values, queryVector));
            };
        }
        return Optional.empty();
    }

    ByteVectorScorer(MemorySegmentAccessInput input, ByteVectorValues values, byte[] queryVector) {
        super(values);
        this.input = input;
        assert queryVector.length == values.dimension();
        this.dimensions = values.dimension();
        this.vectorByteSize = values.getVectorByteLength();
        this.query = MemorySegment.ofArray(queryVector);
    }

    final MemorySegment getSegment(int ord) throws IOException {
        checkOrdinal(ord);
        long byteOffset = (long) ord * vectorByteSize;
        MemorySegment seg = input.segmentSliceOrNull(byteOffset, vectorByteSize);
        if (seg == null) {
            if (scratch == null) {
                scratch = new byte[vectorByteSize];
            }
            input.readBytes(byteOffset, scratch, 0, vectorByteSize);
            seg = MemorySegment.ofArray(scratch);
        }
        return seg;
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

    public static final class DotProductScorer extends ByteVectorScorer {
        private final float denom = (float) (dimensions * (1 << 15));

        public DotProductScorer(MemorySegmentAccessInput in, ByteVectorValues values, byte[] query) {
            super(in, values, query);
        }

        private float normalize(float dotProduct) {
            return 0.5f + dotProduct / denom;
        }

        @Override
        public float score(int node) throws IOException {
            checkOrdinal(node);
            float dotProduct = dotProductI8(query, getSegment(node), dimensions);
            return normalize(dotProduct);
        }

        @Override
        public void bulkScore(int[] nodes, float[] scores, int numNodes) throws IOException {
            MemorySegment vectorsSeg = input.segmentSliceOrNull(0, input.length());
            if (vectorsSeg == null) {
                super.bulkScore(nodes, scores, numNodes);
            } else {
                var ordinalsSeg = MemorySegment.ofArray(nodes);
                var scoresSeg = MemorySegment.ofArray(scores);

                dotProductI8BulkWithOffsets(vectorsSeg, query, dimensions, vectorByteSize, ordinalsSeg, numNodes, scoresSeg);

                for (int i = 0; i < numNodes; ++i) {
                    scores[i] = normalize(scores[i]);
                }
            }
        }
    }

    public static final class CosineScorer extends ByteVectorScorer {
        public CosineScorer(MemorySegmentAccessInput in, ByteVectorValues values, byte[] query) {
            super(in, values, query);
        }

        private static float normalize(float cosine) {
            return (1 + cosine) / 2;
        }

        @Override
        public float score(int node) throws IOException {
            checkOrdinal(node);
            float sqDist = cosineI8(query, getSegment(node), dimensions);
            return normalize(sqDist);
        }

        @Override
        public void bulkScore(int[] nodes, float[] scores, int numNodes) throws IOException {
            MemorySegment vectorsSeg = input.segmentSliceOrNull(0, input.length());
            if (vectorsSeg == null) {
                super.bulkScore(nodes, scores, numNodes);
            } else {
                var ordinalsSeg = MemorySegment.ofArray(nodes);
                var scoresSeg = MemorySegment.ofArray(scores);

                cosineI8BulkWithOffsets(vectorsSeg, query, dimensions, vectorByteSize, ordinalsSeg, numNodes, scoresSeg);

                for (int i = 0; i < numNodes; ++i) {
                    scores[i] = normalize(scores[i]);
                }
            }
        }
    }

    public static final class EuclideanScorer extends ByteVectorScorer {
        public EuclideanScorer(MemorySegmentAccessInput in, ByteVectorValues values, byte[] query) {
            super(in, values, query);
        }

        @Override
        public float score(int node) throws IOException {
            checkOrdinal(node);
            float sqDist = squareDistanceI8(query, getSegment(node), dimensions);
            return VectorUtil.normalizeDistanceToUnitInterval(sqDist);
        }

        @Override
        public void bulkScore(int[] nodes, float[] scores, int numNodes) throws IOException {
            MemorySegment vectorsSeg = input.segmentSliceOrNull(0, input.length());
            if (vectorsSeg == null) {
                super.bulkScore(nodes, scores, numNodes);
            } else {
                var ordinalsSeg = MemorySegment.ofArray(nodes);
                var scoresSeg = MemorySegment.ofArray(scores);

                squareDistanceI8BulkWithOffsets(vectorsSeg, query, dimensions, vectorByteSize, ordinalsSeg, numNodes, scoresSeg);

                for (int i = 0; i < numNodes; ++i) {
                    scores[i] = VectorUtil.normalizeDistanceToUnitInterval(scores[i]);
                }
            }
        }
    }

    public static final class MaxInnerProductScorer extends ByteVectorScorer {
        public MaxInnerProductScorer(MemorySegmentAccessInput in, ByteVectorValues values, byte[] query) {
            super(in, values, query);
        }

        @Override
        public float score(int node) throws IOException {
            checkOrdinal(node);
            float dotProduct = dotProductI8(query, getSegment(node), dimensions);
            return VectorUtil.scaleMaxInnerProductScore(dotProduct);
        }

        @Override
        public void bulkScore(int[] nodes, float[] scores, int numNodes) throws IOException {
            MemorySegment vectorsSeg = input.segmentSliceOrNull(0, input.length());
            if (vectorsSeg == null) {
                super.bulkScore(nodes, scores, numNodes);
            } else {
                var ordinalsSeg = MemorySegment.ofArray(nodes);
                var scoresSeg = MemorySegment.ofArray(scores);

                dotProductI8BulkWithOffsets(vectorsSeg, query, dimensions, vectorByteSize, ordinalsSeg, numNodes, scoresSeg);

                for (int i = 0; i < numNodes; ++i) {
                    scores[i] = VectorUtil.scaleMaxInnerProductScore(scores[i]);
                }
            }
        }
    }

    static void checkDimensions(int queryLen, int fieldLen) {
        if (queryLen != fieldLen) {
            throw new IllegalArgumentException("vector query dimension: " + queryLen + " differs from field dimension: " + fieldLen);
        }
    }
}
