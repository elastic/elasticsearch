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
public abstract class HeapFloatVectorScorerSupplier implements RandomVectorScorerSupplier {

    private static final boolean SUPPORTS_HEAP_SEGMENTS = Runtime.version().feature() >= 22;

    protected final FloatVectorValues values;
    protected final int maxOrd;
    protected final int dims;

    HeapFloatVectorScorerSupplier(FloatVectorValues values) {
        this.values = values;
        this.maxOrd = values.size();
        this.dims = values.dimension();
    }

    public static Optional<RandomVectorScorerSupplier> create(VectorSimilarityType similarityType, FloatVectorValues values) {
        if (SUPPORTS_HEAP_SEGMENTS == false) {
            return Optional.empty();
        }
        return switch (similarityType) {
            case COSINE, DOT_PRODUCT -> Optional.of(new DotProductSupplier(values));
            case EUCLIDEAN -> Optional.of(new EuclideanSupplier(values));
            case MAXIMUM_INNER_PRODUCT -> Optional.of(new MaxInnerProductSupplier(values));
        };
    }

    @Override
    public UpdateableRandomVectorScorer scorer() {
        return new UpdateableRandomVectorScorer.AbstractUpdateableRandomVectorScorer(values) {
            private int firstOrd = -1;
            private MemorySegment firstSegment;

            @Override
            public float score(int node) throws IOException {
                checkOrdinal(node);
                final float[] secondVector = values.vectorValue(node);
                return scoreFromSegments(firstSegment, MemorySegment.ofArray(secondVector));
            }

            @Override
            public void bulkScore(int[] nodes, float[] scores, int numNodes) throws IOException {
                for (int i = 0; i < numNodes; i++) {
                    scores[i] = score(nodes[i]);
                }
            }

            @Override
            public void setScoringOrdinal(int node) throws IOException {
                checkOrdinal(node);
                this.firstOrd = node;
                this.firstSegment = MemorySegment.ofArray(values.vectorValue(firstOrd));
            }
        };
    }

    private void checkOrdinal(int ord) {
        if (ord < 0 || ord >= maxOrd) {
            throw new IllegalArgumentException("illegal ordinal: " + ord);
        }
    }

    abstract float scoreFromSegments(MemorySegment a, MemorySegment b);

    abstract HeapFloatVectorScorerSupplier copyInternal();

    @Override
    public HeapFloatVectorScorerSupplier copy() {
        return copyInternal();
    }

    static final class DotProductSupplier extends HeapFloatVectorScorerSupplier {
        DotProductSupplier(FloatVectorValues values) {
            super(values);
        }

        @Override
        float scoreFromSegments(MemorySegment a, MemorySegment b) {
            return VectorUtil.normalizeToUnitInterval(Similarities.dotProductF32(a, b, dims));
        }

        @Override
        HeapFloatVectorScorerSupplier copyInternal() {
            return new DotProductSupplier(values);
        }
    }

    static final class EuclideanSupplier extends HeapFloatVectorScorerSupplier {
        EuclideanSupplier(FloatVectorValues values) {
            super(values);
        }

        @Override
        float scoreFromSegments(MemorySegment a, MemorySegment b) {
            return VectorUtil.normalizeDistanceToUnitInterval(Similarities.squareDistanceF32(a, b, dims));
        }

        @Override
        HeapFloatVectorScorerSupplier copyInternal() {
            return new EuclideanSupplier(values);
        }
    }

    static final class MaxInnerProductSupplier extends HeapFloatVectorScorerSupplier {
        MaxInnerProductSupplier(FloatVectorValues values) {
            super(values);
        }

        @Override
        float scoreFromSegments(MemorySegment a, MemorySegment b) {
            return VectorUtil.scaleMaxInnerProductScore(Similarities.dotProductF32(a, b, dims));
        }

        @Override
        HeapFloatVectorScorerSupplier copyInternal() {
            return new MaxInnerProductSupplier(values);
        }
    }
}
