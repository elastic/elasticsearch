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
import org.apache.lucene.util.VectorUtil;
import org.apache.lucene.util.hnsw.RandomVectorScorer;

import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.util.Optional;

public abstract sealed class Float32VectorScorer extends RandomVectorScorer.AbstractRandomVectorScorer {

    final int dims;
    final int vectorByteSize;
    final MemorySegmentAccessInput input;
    final MemorySegment query;
    byte[] scratch;

    /** Return an optional whose value, if present, is the scorer. Otherwise, an empty optional is returned. */
    public static Optional<RandomVectorScorer> create(VectorSimilarityFunction sim, FloatVectorValues values, float[] queryVector) {
        checkDimensions(queryVector.length, values.dimension());
        IndexInput input = null;
        if (values instanceof HasIndexSlice hasIndexSlice) {
            input = hasIndexSlice.getSlice();
        }
        if (input == null) {
            return Optional.empty();
        }
        input = FilterIndexInput.unwrapOnlyTest(input);
        if (input instanceof MemorySegmentAccessInput == false) {
            return Optional.empty();
        }
        MemorySegmentAccessInput msInput = (MemorySegmentAccessInput) input;
        checkInvariants(values.size(), values.getVectorByteLength(), input);

        return switch (sim) {
            case COSINE -> Optional.of(new CosineScorer(msInput, values, queryVector));
            case DOT_PRODUCT -> Optional.of(new DotProductScorer(msInput, values, queryVector));
            case EUCLIDEAN -> Optional.of(new EuclideanScorer(msInput, values, queryVector));
            case MAXIMUM_INNER_PRODUCT -> Optional.of(new MaxInnerProductScorer(msInput, values, queryVector));
        };
    }

    Float32VectorScorer(MemorySegmentAccessInput input, FloatVectorValues values, float[] queryVector) {
        super(values);
        this.input = input;
        assert queryVector.length * Float.BYTES == values.getVectorByteLength() && queryVector.length == values.dimension();
        this.vectorByteSize = values.getVectorByteLength();
        this.dims = values.dimension();
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

    public static final class CosineScorer extends Float32VectorScorer {
        public CosineScorer(MemorySegmentAccessInput in, FloatVectorValues values, float[] query) {
            super(in, values, query);
        }

        @Override
        public float score(int node) throws IOException {
            checkOrdinal(node);
            float dotProduct = Similarities.cosineFloat32(query, getSegment(node), dims);
            return Math.max((1 + dotProduct) / 2, 0);
        }
    }

    public static final class DotProductScorer extends Float32VectorScorer {
        public DotProductScorer(MemorySegmentAccessInput in, FloatVectorValues values, float[] query) {
            super(in, values, query);
        }

        @Override
        public float score(int node) throws IOException {
            checkOrdinal(node);
            float dotProduct = Similarities.dotProductFloat32(query, getSegment(node), dims);
            return Math.max((1 + dotProduct) / 2, 0);
        }
    }

    public static final class EuclideanScorer extends Float32VectorScorer {
        public EuclideanScorer(MemorySegmentAccessInput in, FloatVectorValues values, float[] query) {
            super(in, values, query);
        }

        @Override
        public float score(int node) throws IOException {
            checkOrdinal(node);
            float v = Similarities.squareDistanceFloat32(query, getSegment(node), dims);
            return 1 / (1 + v);
        }
    }

    public static final class MaxInnerProductScorer extends Float32VectorScorer {
        public MaxInnerProductScorer(MemorySegmentAccessInput in, FloatVectorValues values, float[] query) {
            super(in, values, query);
        }

        @Override
        public float score(int node) throws IOException {
            checkOrdinal(node);
            float v = Similarities.dotProductFloat32(query, getSegment(node), dims);
            return VectorUtil.scaleMaxInnerProductScore(v);
        }
    }

    static void checkDimensions(int queryLen, int fieldLen) {
        if (queryLen != fieldLen) {
            throw new IllegalArgumentException("vector query dimension: " + queryLen + " differs from field dimension: " + fieldLen);
        }
    }
}
