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
import org.apache.lucene.store.MemorySegmentAccessInput;
import org.apache.lucene.util.hnsw.RandomVectorScorer;

import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.util.Optional;

import static org.elasticsearch.simdvec.internal.Similarities.dotProductF32;
import static org.elasticsearch.simdvec.internal.Similarities.dotProductF32BulkWithOffsets;
import static org.elasticsearch.simdvec.internal.Similarities.squareDistanceF32;
import static org.elasticsearch.simdvec.internal.Similarities.squareDistanceF32BulkWithOffsets;

public abstract sealed class FloatVectorScorer extends RandomVectorScorer.AbstractRandomVectorScorer {

    final int dimensions;
    final int vectorByteSize;
    final MemorySegmentAccessInput input;
    final MemorySegment query;
    byte[] scratch;

    public static Optional<RandomVectorScorer> create(VectorSimilarityFunction sim, FloatVectorValues values, float[] queryVector) {
        checkDimensions(queryVector.length, values.dimension());
        IndexInput input = values instanceof HasIndexSlice slice ? slice.getSlice() : null;
        if (input == null) {
            return Optional.empty();
        }
        input = FilterIndexInput.unwrapOnlyTest(input);
        if (input instanceof MemorySegmentAccessInput msInput) {
            checkInvariants(values.size(), values.dimension(), input);

            return switch (sim) {
                case COSINE, DOT_PRODUCT -> Optional.of(new DotProductScorer(msInput, values, queryVector));
                case EUCLIDEAN -> Optional.of(new EuclideanScorer(msInput, values, queryVector));
                case MAXIMUM_INNER_PRODUCT -> Optional.of(new MaxInnerProductScorer(msInput, values, queryVector));
            };
        }
        return Optional.empty();
    }

    FloatVectorScorer(MemorySegmentAccessInput input, FloatVectorValues values, float[] queryVector) {
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

    public static final class DotProductScorer extends FloatVectorScorer {
        public DotProductScorer(MemorySegmentAccessInput in, FloatVectorValues values, float[] query) {
            super(in, values, query);
        }

        @Override
        public float score(int node) throws IOException {
            checkOrdinal(node);
            float dotProduct = dotProductF32(query, getSegment(node), dimensions);
            return Math.max((1 + dotProduct) / 2, 0f);
        }

        @Override
        public void bulkScore(int[] nodes, float[] scores, int numNodes) throws IOException {
            MemorySegment vectorsSeg = input.segmentSliceOrNull(0, input.length());
            if (vectorsSeg == null) {
                super.bulkScore(nodes, scores, numNodes);
            } else {
                var ordinalsSeg = MemorySegment.ofArray(nodes);
                var scoresSeg = MemorySegment.ofArray(scores);

                dotProductF32BulkWithOffsets(vectorsSeg, query, dimensions, vectorByteSize, ordinalsSeg, numNodes, scoresSeg);

                for (int i = 0; i < numNodes; ++i) {
                    scores[i] = Math.max((1 + scores[i]) / 2, 0f);
                }
            }
        }
    }

    public static final class EuclideanScorer extends FloatVectorScorer {
        public EuclideanScorer(MemorySegmentAccessInput in, FloatVectorValues values, float[] query) {
            super(in, values, query);
        }

        @Override
        public float score(int node) throws IOException {
            checkOrdinal(node);
            float sqDist = squareDistanceF32(query, getSegment(node), dimensions);
            return 1 / (1f + sqDist);
        }

        @Override
        public void bulkScore(int[] nodes, float[] scores, int numNodes) throws IOException {
            MemorySegment vectorsSeg = input.segmentSliceOrNull(0, input.length());
            if (vectorsSeg == null) {
                super.bulkScore(nodes, scores, numNodes);
            } else {
                var ordinalsSeg = MemorySegment.ofArray(nodes);
                var scoresSeg = MemorySegment.ofArray(scores);

                squareDistanceF32BulkWithOffsets(vectorsSeg, query, dimensions, vectorByteSize, ordinalsSeg, numNodes, scoresSeg);

                for (int i = 0; i < numNodes; ++i) {
                    scores[i] = 1 / (1f + scores[i]);
                }
            }
        }
    }

    public static final class MaxInnerProductScorer extends FloatVectorScorer {
        public MaxInnerProductScorer(MemorySegmentAccessInput in, FloatVectorValues values, float[] query) {
            super(in, values, query);
        }

        @Override
        public float score(int node) throws IOException {
            checkOrdinal(node);
            float dotProduct = dotProductF32(query, getSegment(node), dimensions);
            if (dotProduct < 0) {
                return 1 / (1 + -1 * dotProduct);
            }
            return dotProduct + 1;
        }

        @Override
        public void bulkScore(int[] nodes, float[] scores, int numNodes) throws IOException {
            MemorySegment vectorsSeg = input.segmentSliceOrNull(0, input.length());
            if (vectorsSeg == null) {
                super.bulkScore(nodes, scores, numNodes);
            } else {
                var ordinalsSeg = MemorySegment.ofArray(nodes);
                var scoresSeg = MemorySegment.ofArray(scores);

                dotProductF32BulkWithOffsets(vectorsSeg, query, dimensions, vectorByteSize, ordinalsSeg, numNodes, scoresSeg);

                for (int i = 0; i < numNodes; ++i) {
                    float dotProduct = scores[i];
                    float adjustedDistance = dotProduct < 0 ? 1 / (1 + -1 * dotProduct) : dotProduct + 1;
                    scores[i] = adjustedDistance;
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
