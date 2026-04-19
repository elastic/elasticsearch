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
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.VectorUtil;
import org.apache.lucene.util.hnsw.RandomVectorScorer;

import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.util.Optional;

import static org.elasticsearch.simdvec.internal.Similarities.dotProductF32;
import static org.elasticsearch.simdvec.internal.Similarities.squareDistanceF32;

public abstract sealed class FloatVectorScorer extends NativeVectorScorer {

    public static Optional<RandomVectorScorer> create(VectorSimilarityFunction sim, FloatVectorValues values, float[] queryVector) {
        checkDimensions(queryVector.length, values.dimension());
        return getNativeAccessibleSegment(values).map(input -> switch (sim) {
            case COSINE, DOT_PRODUCT -> new DotProductScorer(input, values, queryVector);
            case EUCLIDEAN -> new EuclideanScorer(input, values, queryVector);
            case MAXIMUM_INNER_PRODUCT -> new MaxInnerProductScorer(input, values, queryVector);
        });
    }

    FloatVectorScorer(IndexInput input, FloatVectorValues values, float[] queryVector) {
        super(input, values, MemorySegment.ofArray(queryVector));
    }

    public static final class DotProductScorer extends FloatVectorScorer {
        public DotProductScorer(IndexInput in, FloatVectorValues values, float[] query) {
            super(in, values, query);
        }

        @Override
        public float score(int node) throws IOException {
            checkOrdinal(node);
            long byteOffset = (long) node * vectorByteSize;
            input.seek(byteOffset);
            return IndexInputUtils.withSlice(input, vectorByteSize, this::getScratch, seg -> {
                float dp = dotProductF32(query, seg, dimensions);
                return VectorUtil.normalizeToUnitInterval(dp);
            });
        }

        @Override
        public float bulkScore(int[] nodes, float[] scores, int numNodes) throws IOException {
            if (numNodes == 0) {
                return Float.NEGATIVE_INFINITY;
            }

            if (bulkScore(nodes, scores, numNodes, Similarities::dotProductF32BulkWithOffsets)) {
                float max = Float.NEGATIVE_INFINITY;
                for (int i = 0; i < numNodes; ++i) {
                    scores[i] = VectorUtil.normalizeToUnitInterval(scores[i]);
                    max = Math.max(max, scores[i]);
                }
                return max;
            } else {
                return super.bulkScore(nodes, scores, numNodes);
            }
        }
    }

    public static final class EuclideanScorer extends FloatVectorScorer {
        public EuclideanScorer(IndexInput in, FloatVectorValues values, float[] query) {
            super(in, values, query);
        }

        @Override
        public float score(int node) throws IOException {
            checkOrdinal(node);
            long byteOffset = (long) node * vectorByteSize;
            input.seek(byteOffset);
            return IndexInputUtils.withSlice(input, vectorByteSize, this::getScratch, seg -> {
                float dp = squareDistanceF32(query, seg, dimensions);
                return VectorUtil.normalizeDistanceToUnitInterval(dp);
            });
        }

        @Override
        public float bulkScore(int[] nodes, float[] scores, int numNodes) throws IOException {
            if (numNodes == 0) {
                return Float.NEGATIVE_INFINITY;
            }

            if (bulkScore(nodes, scores, numNodes, Similarities::squareDistanceF32BulkWithOffsets)) {
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

    public static final class MaxInnerProductScorer extends FloatVectorScorer {
        public MaxInnerProductScorer(IndexInput in, FloatVectorValues values, float[] query) {
            super(in, values, query);
        }

        @Override
        public float score(int node) throws IOException {
            checkOrdinal(node);
            long byteOffset = (long) node * vectorByteSize;
            input.seek(byteOffset);
            return IndexInputUtils.withSlice(input, vectorByteSize, this::getScratch, seg -> {
                float dp = dotProductF32(query, seg, dimensions);
                return VectorUtil.scaleMaxInnerProductScore(dp);
            });
        }

        @Override
        public float bulkScore(int[] nodes, float[] scores, int numNodes) throws IOException {
            if (numNodes == 0) {
                return Float.NEGATIVE_INFINITY;
            }

            if (bulkScore(nodes, scores, numNodes, Similarities::dotProductF32BulkWithOffsets)) {
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
