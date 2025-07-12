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
import org.apache.lucene.store.FilterIndexInput;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.MemorySegmentAccessInput;
import org.apache.lucene.util.VectorUtil;
import org.apache.lucene.util.hnsw.RandomVectorScorerSupplier;
import org.apache.lucene.util.hnsw.UpdateableRandomVectorScorer;
import org.elasticsearch.simdvec.VectorSimilarityType;

import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.util.Optional;

public abstract sealed class Float32VectorScorerSupplier implements RandomVectorScorerSupplier {

    public static Optional<RandomVectorScorerSupplier> create(
        VectorSimilarityType similarityType,
        IndexInput input,
        FloatVectorValues values
    ) {
        input = FilterIndexInput.unwrapOnlyTest(input);
        if (input instanceof MemorySegmentAccessInput == false) {
            return Optional.empty();
        }
        MemorySegmentAccessInput msInput = (MemorySegmentAccessInput) input;
        checkInvariants(values.size(), values.dimension(), input);
        return switch (similarityType) {
            case COSINE -> Optional.of(new CosineSupplier(msInput, values));
            case DOT_PRODUCT -> Optional.of(new DotProductSupplier(msInput, values));
            case EUCLIDEAN -> Optional.of(new EuclideanSupplier(msInput, values));
            case MAXIMUM_INNER_PRODUCT -> Optional.of(new MaxInnerProductSupplier(msInput, values));
        };
    }

    final int dims;
    final int vectorByteSize;
    final int maxOrd;
    final MemorySegmentAccessInput input;
    final FloatVectorValues values; // to support ordToDoc/getAcceptOrds
    final VectorSimilarityFunction fallbackScorer;

    protected Float32VectorScorerSupplier(
        MemorySegmentAccessInput input,
        FloatVectorValues values,
        VectorSimilarityFunction fallbackScorer
    ) {
        this.input = input;
        this.values = values;
        this.dims = values.dimension();
        this.vectorByteSize = values.getVectorByteLength();
        this.maxOrd = values.size();
        this.fallbackScorer = fallbackScorer;
    }

    protected final void checkOrdinal(int ord) {
        if (ord < 0 || ord > maxOrd) {
            throw new IllegalArgumentException("illegal ordinal: " + ord);
        }
    }

    final float scoreFromOrds(int firstOrd, int secondOrd) throws IOException {
        long firstByteOffset = (long) firstOrd * vectorByteSize;
        long secondByteOffset = (long) secondOrd * vectorByteSize;

        MemorySegment firstSeg = input.segmentSliceOrNull(firstByteOffset, vectorByteSize);
        if (firstSeg == null) {
            return fallbackScore(firstByteOffset, secondByteOffset);
        }

        MemorySegment secondSeg = input.segmentSliceOrNull(secondByteOffset, vectorByteSize);
        if (secondSeg == null) {
            return fallbackScore(firstByteOffset, secondByteOffset);
        }

        return scoreFromSegments(firstSeg, secondSeg);
    }

    abstract float scoreFromSegments(MemorySegment a, MemorySegment b);

    protected final float fallbackScore(long firstByteOffset, long secondByteOffset) throws IOException {
        float[] a = new float[dims];
        readFloats(a, firstByteOffset, dims);

        float[] b = new float[dims];
        readFloats(b, secondByteOffset, dims);

        return fallbackScorer.compare(a, b);
    }

    final void readFloats(float[] floats, long byteOffset, int len) throws IOException {
        for (int i = 0; i < len; i++) {
            floats[i] = Float.intBitsToFloat(input.readInt(byteOffset));
            byteOffset += Float.BYTES;
        }
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
            public void setScoringOrdinal(int node) throws IOException {
                checkOrdinal(node);
                this.ord = node;
            }
        };
    }

    public static final class CosineSupplier extends Float32VectorScorerSupplier {

        public CosineSupplier(MemorySegmentAccessInput input, FloatVectorValues values) {
            super(input, values, VectorSimilarityFunction.COSINE);
        }

        @Override
        float scoreFromSegments(MemorySegment a, MemorySegment b) {
            float v = Similarities.cosineFloat32(a, b, dims);
            return Math.max((1 + v) / 2, 0);
        }

        @Override
        public CosineSupplier copy() {
            return new CosineSupplier(input.clone(), values);
        }
    }

    public static final class EuclideanSupplier extends Float32VectorScorerSupplier {

        public EuclideanSupplier(MemorySegmentAccessInput input, FloatVectorValues values) {
            super(input, values, VectorSimilarityFunction.EUCLIDEAN);
        }

        @Override
        float scoreFromSegments(MemorySegment a, MemorySegment b) {
            float v = Similarities.squareDistanceFloat32(a, b, dims);
            return 1 / (1 + v);
        }

        @Override
        public EuclideanSupplier copy() {
            return new EuclideanSupplier(input.clone(), values);
        }
    }

    public static final class DotProductSupplier extends Float32VectorScorerSupplier {

        public DotProductSupplier(MemorySegmentAccessInput input, FloatVectorValues values) {
            super(input, values, VectorSimilarityFunction.DOT_PRODUCT);
        }

        @Override
        float scoreFromSegments(MemorySegment a, MemorySegment b) {
            float dotProduct = Similarities.dotProductFloat32(a, b, dims);
            return Math.max((1 + dotProduct) / 2, 0);
        }

        @Override
        public DotProductSupplier copy() {
            return new DotProductSupplier(input.clone(), values);
        }
    }

    public static final class MaxInnerProductSupplier extends Float32VectorScorerSupplier {

        public MaxInnerProductSupplier(MemorySegmentAccessInput input, FloatVectorValues values) {
            super(input, values, VectorSimilarityFunction.MAXIMUM_INNER_PRODUCT);
        }

        @Override
        float scoreFromSegments(MemorySegment a, MemorySegment b) {
            float v = Similarities.dotProductFloat32(a, b, dims);
            return VectorUtil.scaleMaxInnerProductScore(v);
        }

        @Override
        public MaxInnerProductSupplier copy() {
            return new MaxInnerProductSupplier(input.clone(), values);
        }
    }

    static boolean checkIndex(long index, long length) {
        return index >= 0 && index < length;
    }

    static void checkInvariants(int maxOrd, int vectorByteLength, IndexInput input) {
        if (input.length() < (long) vectorByteLength * maxOrd) {
            throw new IllegalArgumentException("input length is less than expected vector data");
        }
    }
}
