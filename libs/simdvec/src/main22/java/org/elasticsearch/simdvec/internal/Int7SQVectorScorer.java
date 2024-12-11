/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.simdvec.internal;

import org.apache.lucene.codecs.hnsw.ScalarQuantizedVectorScorer;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.store.FilterIndexInput;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.MemorySegmentAccessInput;
import org.apache.lucene.util.hnsw.RandomVectorScorer;
import org.apache.lucene.util.quantization.QuantizedByteVectorValues;
import org.apache.lucene.util.quantization.ScalarQuantizer;

import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.util.Optional;

import static org.elasticsearch.simdvec.internal.Similarities.dotProduct7u;
import static org.elasticsearch.simdvec.internal.Similarities.squareDistance7u;

public abstract sealed class Int7SQVectorScorer extends RandomVectorScorer.AbstractRandomVectorScorer {

    final int vectorByteSize;
    final MemorySegmentAccessInput input;
    final MemorySegment query;
    final float scoreCorrectionConstant;
    final float queryCorrection;
    byte[] scratch;

    /** Return an optional whose value, if present, is the scorer. Otherwise, an empty optional is returned. */
    public static Optional<RandomVectorScorer> create(VectorSimilarityFunction sim, QuantizedByteVectorValues values, float[] queryVector) {
        checkDimensions(queryVector.length, values.dimension());
        var input = values.getSlice();
        if (input == null) {
            return Optional.empty();
        }
        input = FilterIndexInput.unwrapOnlyTest(input);
        if (input instanceof MemorySegmentAccessInput == false) {
            return Optional.empty();
        }
        MemorySegmentAccessInput msInput = (MemorySegmentAccessInput) input;
        checkInvariants(values.size(), values.dimension(), input);

        ScalarQuantizer scalarQuantizer = values.getScalarQuantizer();
        // TODO assert scalarQuantizer.getBits() == 7 or 8 ?
        byte[] quantizedQuery = new byte[queryVector.length];
        float queryCorrection = ScalarQuantizedVectorScorer.quantizeQuery(queryVector, quantizedQuery, sim, scalarQuantizer);
        return switch (sim) {
            case COSINE, DOT_PRODUCT -> Optional.of(new DotProductScorer(msInput, values, quantizedQuery, queryCorrection));
            case EUCLIDEAN -> Optional.of(new EuclideanScorer(msInput, values, quantizedQuery, queryCorrection));
            case MAXIMUM_INNER_PRODUCT -> Optional.of(new MaxInnerProductScorer(msInput, values, quantizedQuery, queryCorrection));
        };
    }

    Int7SQVectorScorer(MemorySegmentAccessInput input, QuantizedByteVectorValues values, byte[] queryVector, float queryCorrection) {
        super(values);
        this.input = input;
        assert queryVector.length == values.getVectorByteLength();
        this.vectorByteSize = values.getVectorByteLength();
        this.query = MemorySegment.ofArray(queryVector);
        this.queryCorrection = queryCorrection;
        this.scoreCorrectionConstant = values.getScalarQuantizer().getConstantMultiplier();
    }

    final MemorySegment getSegment(int ord) throws IOException {
        checkOrdinal(ord);
        long byteOffset = (long) ord * (vectorByteSize + Float.BYTES);
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

    public static final class DotProductScorer extends Int7SQVectorScorer {
        public DotProductScorer(MemorySegmentAccessInput in, QuantizedByteVectorValues values, byte[] query, float correction) {
            super(in, values, query, correction);
        }

        @Override
        public float score(int node) throws IOException {
            checkOrdinal(node);
            int dotProduct = dotProduct7u(query, getSegment(node), vectorByteSize);
            assert dotProduct >= 0;
            long byteOffset = (long) node * (vectorByteSize + Float.BYTES);
            float nodeCorrection = Float.intBitsToFloat(input.readInt(byteOffset + vectorByteSize));
            float adjustedDistance = dotProduct * scoreCorrectionConstant + queryCorrection + nodeCorrection;
            return Math.max((1 + adjustedDistance) / 2, 0f);
        }
    }

    public static final class EuclideanScorer extends Int7SQVectorScorer {
        public EuclideanScorer(MemorySegmentAccessInput in, QuantizedByteVectorValues values, byte[] query, float correction) {
            super(in, values, query, correction);
        }

        @Override
        public float score(int node) throws IOException {
            checkOrdinal(node);
            int sqDist = squareDistance7u(query, getSegment(node), vectorByteSize);
            float adjustedDistance = sqDist * scoreCorrectionConstant;
            return 1 / (1f + adjustedDistance);
        }
    }

    public static final class MaxInnerProductScorer extends Int7SQVectorScorer {
        public MaxInnerProductScorer(MemorySegmentAccessInput in, QuantizedByteVectorValues values, byte[] query, float corr) {
            super(in, values, query, corr);
        }

        @Override
        public float score(int node) throws IOException {
            checkOrdinal(node);
            int dotProduct = dotProduct7u(query, getSegment(node), vectorByteSize);
            assert dotProduct >= 0;
            long byteOffset = (long) node * (vectorByteSize + Float.BYTES);
            float nodeCorrection = Float.intBitsToFloat(input.readInt(byteOffset + vectorByteSize));
            float adjustedDistance = dotProduct * scoreCorrectionConstant + queryCorrection + nodeCorrection;
            if (adjustedDistance < 0) {
                return 1 / (1 + -1 * adjustedDistance);
            }
            return adjustedDistance + 1;
        }
    }

    static void checkDimensions(int queryLen, int fieldLen) {
        if (queryLen != fieldLen) {
            throw new IllegalArgumentException("vector query dimension: " + queryLen + " differs from field dimension: " + fieldLen);
        }
    }
}
