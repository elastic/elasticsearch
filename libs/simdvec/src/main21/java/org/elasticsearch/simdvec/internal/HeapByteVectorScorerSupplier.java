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
import org.apache.lucene.util.VectorUtil;
import org.apache.lucene.util.hnsw.RandomVectorScorerSupplier;
import org.apache.lucene.util.hnsw.UpdateableRandomVectorScorer;
import org.elasticsearch.simdvec.VectorSimilarityType;

import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.util.Optional;

/**
 * Array-backed scorer supplier used when vectors do not expose an index slice.
 * This is only enabled on JDK22+ where passing heap-backed memory segments to native code is supported.
 */
public abstract class HeapByteVectorScorerSupplier implements RandomVectorScorerSupplier {

    private static final boolean SUPPORTS_HEAP_SEGMENTS = Runtime.version().feature() >= 22;

    protected final ByteVectorValues values;
    protected final int dims;

    HeapByteVectorScorerSupplier(ByteVectorValues values) {
        this.values = values;
        this.dims = values.dimension();
    }

    public static Optional<RandomVectorScorerSupplier> create(VectorSimilarityType similarityType, ByteVectorValues values) {
        if (SUPPORTS_HEAP_SEGMENTS == false) {
            return Optional.empty();
        }
        return switch (similarityType) {
            case COSINE -> Optional.of(new CosineSupplier(values));
            case DOT_PRODUCT -> Optional.of(new DotProductSupplier(values));
            case EUCLIDEAN -> Optional.of(new EuclideanSupplier(values));
            case MAXIMUM_INNER_PRODUCT -> Optional.of(new MaxInnerProductSupplier(values));
        };
    }

    @Override
    public UpdateableRandomVectorScorer scorer() {
        return new UpdateableRandomVectorScorer.AbstractUpdateableRandomVectorScorer(values) {
            private MemorySegment firstSegment;
            private byte[] packedVectors = new byte[0];
            private MemorySegment packedVectorsSegment = MemorySegment.ofArray(packedVectors);

            @Override
            public float score(int node) throws IOException {
                checkOrdinal(node);
                final byte[] secondVector = values.vectorValue(node);
                return scoreFromSegments(firstSegment, MemorySegment.ofArray(secondVector));
            }

            @Override
            public void bulkScore(int[] nodes, float[] scores, int numNodes) throws IOException {
                if (numNodes == 0) {
                    return;
                }
                final MemorySegment vectorsSegment = packVectors(nodes, numNodes);
                final MemorySegment scoresSegment = MemorySegment.ofArray(scores);
                bulkScoreFromSegments(vectorsSegment, firstSegment, numNodes, scoresSegment);
                normalizeBulkScores(scores, numNodes);
            }

            @Override
            public void setScoringOrdinal(int node) throws IOException {
                checkOrdinal(node);
                this.firstSegment = MemorySegment.ofArray(values.vectorValue(node));
            }

            private MemorySegment packVectors(int[] nodes, int numNodes) throws IOException {
                final int requiredValues = Math.multiplyExact(numNodes, dims);
                if (packedVectors.length < requiredValues) {
                    packedVectors = new byte[requiredValues];
                    packedVectorsSegment = MemorySegment.ofArray(packedVectors);
                }

                for (int i = 0; i < numNodes; i++) {
                    final int node = nodes[i];
                    checkOrdinal(node);
                    final byte[] vector = values.vectorValue(node);
                    System.arraycopy(vector, 0, packedVectors, i * dims, dims);
                }
                return packedVectorsSegment;
            }
        };
    }

    private void checkOrdinal(int ord) {
        final int maxOrd = values.size();
        if (ord < 0 || ord >= maxOrd) {
            throw new IllegalArgumentException("illegal ordinal: " + ord);
        }
    }

    abstract float scoreFromSegments(MemorySegment a, MemorySegment b);

    abstract void bulkScoreFromSegments(MemorySegment a, MemorySegment b, int count, MemorySegment scores);

    abstract void normalizeBulkScores(float[] scores, int count);

    static final class CosineSupplier extends HeapByteVectorScorerSupplier {
        CosineSupplier(ByteVectorValues values) {
            super(values);
        }

        @Override
        float scoreFromSegments(MemorySegment a, MemorySegment b) {
            return (1 + Similarities.cosineI8(a, b, dims)) / 2;
        }

        @Override
        void bulkScoreFromSegments(MemorySegment a, MemorySegment b, int count, MemorySegment scores) {
            Similarities.cosineI8Bulk(a, b, dims, count, scores);
        }

        @Override
        void normalizeBulkScores(float[] scores, int count) {
            for (int i = 0; i < count; i++) {
                scores[i] = (1 + scores[i]) / 2;
            }
        }

        @Override
        public HeapByteVectorScorerSupplier copy() {
            return new CosineSupplier(values);
        }
    }

    static final class DotProductSupplier extends HeapByteVectorScorerSupplier {
        private final float denom = (float) (dims * (1 << 15));

        DotProductSupplier(ByteVectorValues values) {
            super(values);
        }

        @Override
        float scoreFromSegments(MemorySegment a, MemorySegment b) {
            return 0.5f + Similarities.dotProductI8(a, b, dims) / denom;
        }

        @Override
        void bulkScoreFromSegments(MemorySegment a, MemorySegment b, int count, MemorySegment scores) {
            Similarities.dotProductI8Bulk(a, b, dims, count, scores);
        }

        @Override
        void normalizeBulkScores(float[] scores, int count) {
            for (int i = 0; i < count; i++) {
                scores[i] = 0.5f + scores[i] / denom;
            }
        }

        @Override
        public HeapByteVectorScorerSupplier copy() {
            return new DotProductSupplier(values);
        }
    }

    static final class EuclideanSupplier extends HeapByteVectorScorerSupplier {
        EuclideanSupplier(ByteVectorValues values) {
            super(values);
        }

        @Override
        float scoreFromSegments(MemorySegment a, MemorySegment b) {
            return VectorUtil.normalizeDistanceToUnitInterval(Similarities.squareDistanceI8(a, b, dims));
        }

        @Override
        void bulkScoreFromSegments(MemorySegment a, MemorySegment b, int count, MemorySegment scores) {
            Similarities.squareDistanceI8Bulk(a, b, dims, count, scores);
        }

        @Override
        void normalizeBulkScores(float[] scores, int count) {
            for (int i = 0; i < count; i++) {
                scores[i] = VectorUtil.normalizeDistanceToUnitInterval(scores[i]);
            }
        }

        @Override
        public HeapByteVectorScorerSupplier copy() {
            return new EuclideanSupplier(values);
        }
    }

    static final class MaxInnerProductSupplier extends HeapByteVectorScorerSupplier {
        MaxInnerProductSupplier(ByteVectorValues values) {
            super(values);
        }

        @Override
        float scoreFromSegments(MemorySegment a, MemorySegment b) {
            return VectorUtil.scaleMaxInnerProductScore(Similarities.dotProductI8(a, b, dims));
        }

        @Override
        void bulkScoreFromSegments(MemorySegment a, MemorySegment b, int count, MemorySegment scores) {
            Similarities.dotProductI8Bulk(a, b, dims, count, scores);
        }

        @Override
        void normalizeBulkScores(float[] scores, int count) {
            for (int i = 0; i < count; i++) {
                scores[i] = VectorUtil.scaleMaxInnerProductScore(scores[i]);
            }
        }

        @Override
        public HeapByteVectorScorerSupplier copy() {
            return new MaxInnerProductSupplier(values);
        }
    }
}
