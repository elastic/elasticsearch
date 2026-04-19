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
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.VectorUtil;
import org.apache.lucene.util.hnsw.RandomVectorScorer;

import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.util.Optional;

import static org.elasticsearch.simdvec.internal.Similarities.cosineI8;
import static org.elasticsearch.simdvec.internal.Similarities.dotProductI8;
import static org.elasticsearch.simdvec.internal.Similarities.squareDistanceI8;

public abstract sealed class ByteVectorScorer extends NativeVectorScorer {

    public static Optional<RandomVectorScorer> create(VectorSimilarityFunction sim, ByteVectorValues values, byte[] queryVector) {
        checkDimensions(queryVector.length, values.dimension());

        return getNativeAccessibleSegment(values).map(input -> switch (sim) {
            case COSINE -> new CosineScorer(input, values, queryVector);
            case DOT_PRODUCT -> new DotProductScorer(input, values, queryVector);
            case EUCLIDEAN -> new EuclideanScorer(input, values, queryVector);
            case MAXIMUM_INNER_PRODUCT -> new MaxInnerProductScorer(input, values, queryVector);
        });
    }

    ByteVectorScorer(IndexInput input, ByteVectorValues values, byte[] queryVector) {
        super(input, values, MemorySegment.ofArray(queryVector));
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
}
