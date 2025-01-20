/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.simdvec.internal;

import org.apache.lucene.store.MemorySegmentAccessInput;
import org.apache.lucene.util.hnsw.RandomVectorScorer;
import org.apache.lucene.util.hnsw.RandomVectorScorerSupplier;
import org.apache.lucene.util.quantization.QuantizedByteVectorValues;
import org.apache.lucene.util.quantization.ScalarQuantizedVectorSimilarity;

import java.io.IOException;
import java.lang.foreign.MemorySegment;

import static org.apache.lucene.index.VectorSimilarityFunction.DOT_PRODUCT;
import static org.apache.lucene.index.VectorSimilarityFunction.EUCLIDEAN;
import static org.apache.lucene.index.VectorSimilarityFunction.MAXIMUM_INNER_PRODUCT;
import static org.apache.lucene.util.quantization.ScalarQuantizedVectorSimilarity.fromVectorSimilarity;

public abstract sealed class Int7SQVectorScorerSupplier implements RandomVectorScorerSupplier {

    static final byte BITS = 7;

    final int dims;
    final int maxOrd;
    final float scoreCorrectionConstant;
    final MemorySegmentAccessInput input;
    final QuantizedByteVectorValues values; // to support ordToDoc/getAcceptOrds
    final ScalarQuantizedVectorSimilarity fallbackScorer;

    protected Int7SQVectorScorerSupplier(
        MemorySegmentAccessInput input,
        QuantizedByteVectorValues values,
        float scoreCorrectionConstant,
        ScalarQuantizedVectorSimilarity fallbackScorer
    ) {
        this.input = input;
        this.values = values;
        this.dims = values.dimension();
        this.maxOrd = values.size();
        this.scoreCorrectionConstant = scoreCorrectionConstant;
        this.fallbackScorer = fallbackScorer;
    }

    protected final void checkOrdinal(int ord) {
        if (ord < 0 || ord > maxOrd) {
            throw new IllegalArgumentException("illegal ordinal: " + ord);
        }
    }

    final float scoreFromOrds(int firstOrd, int secondOrd) throws IOException {
        checkOrdinal(firstOrd);
        checkOrdinal(secondOrd);

        final int length = dims;
        long firstByteOffset = (long) firstOrd * (length + Float.BYTES);
        long secondByteOffset = (long) secondOrd * (length + Float.BYTES);

        MemorySegment firstSeg = input.segmentSliceOrNull(firstByteOffset, length);
        if (firstSeg == null) {
            return fallbackScore(firstByteOffset, secondByteOffset);
        }
        float firstOffset = Float.intBitsToFloat(input.readInt(firstByteOffset + length));

        MemorySegment secondSeg = input.segmentSliceOrNull(secondByteOffset, length);
        if (secondSeg == null) {
            return fallbackScore(firstByteOffset, secondByteOffset);
        }
        float secondOffset = Float.intBitsToFloat(input.readInt(secondByteOffset + length));

        return scoreFromSegments(firstSeg, firstOffset, secondSeg, secondOffset);
    }

    abstract float scoreFromSegments(MemorySegment a, float aOffset, MemorySegment b, float bOffset);

    protected final float fallbackScore(long firstByteOffset, long secondByteOffset) throws IOException {
        byte[] a = new byte[dims];
        input.readBytes(firstByteOffset, a, 0, a.length);
        float aOffsetValue = Float.intBitsToFloat(input.readInt(firstByteOffset + dims));

        byte[] b = new byte[dims];
        input.readBytes(secondByteOffset, b, 0, a.length);
        float bOffsetValue = Float.intBitsToFloat(input.readInt(secondByteOffset + dims));

        return fallbackScorer.score(a, aOffsetValue, b, bOffsetValue);
    }

    @Override
    public RandomVectorScorer scorer(int ord) {
        checkOrdinal(ord);
        return new RandomVectorScorer.AbstractRandomVectorScorer(values) {
            @Override
            public float score(int node) throws IOException {
                return scoreFromOrds(ord, node);
            }
        };
    }

    public static final class EuclideanSupplier extends Int7SQVectorScorerSupplier {

        public EuclideanSupplier(MemorySegmentAccessInput input, QuantizedByteVectorValues values, float scoreCorrectionConstant) {
            super(input, values, scoreCorrectionConstant, fromVectorSimilarity(EUCLIDEAN, scoreCorrectionConstant, BITS));
        }

        @Override
        float scoreFromSegments(MemorySegment a, float aOffset, MemorySegment b, float bOffset) {
            int squareDistance = Similarities.squareDistance7u(a, b, dims);
            float adjustedDistance = squareDistance * scoreCorrectionConstant;
            return 1 / (1f + adjustedDistance);
        }

        @Override
        public EuclideanSupplier copy() {
            return new EuclideanSupplier(input.clone(), values, scoreCorrectionConstant);
        }
    }

    public static final class DotProductSupplier extends Int7SQVectorScorerSupplier {

        public DotProductSupplier(MemorySegmentAccessInput input, QuantizedByteVectorValues values, float scoreCorrectionConstant) {
            super(input, values, scoreCorrectionConstant, fromVectorSimilarity(DOT_PRODUCT, scoreCorrectionConstant, BITS));
        }

        @Override
        float scoreFromSegments(MemorySegment a, float aOffset, MemorySegment b, float bOffset) {
            int dotProduct = Similarities.dotProduct7u(a, b, dims);
            assert dotProduct >= 0;
            float adjustedDistance = dotProduct * scoreCorrectionConstant + aOffset + bOffset;
            return Math.max((1 + adjustedDistance) / 2, 0f);
        }

        @Override
        public DotProductSupplier copy() {
            return new DotProductSupplier(input.clone(), values, scoreCorrectionConstant);
        }
    }

    public static final class MaxInnerProductSupplier extends Int7SQVectorScorerSupplier {

        public MaxInnerProductSupplier(MemorySegmentAccessInput input, QuantizedByteVectorValues values, float scoreCorrectionConstant) {
            super(input, values, scoreCorrectionConstant, fromVectorSimilarity(MAXIMUM_INNER_PRODUCT, scoreCorrectionConstant, BITS));
        }

        @Override
        float scoreFromSegments(MemorySegment a, float aOffset, MemorySegment b, float bOffset) {
            int dotProduct = Similarities.dotProduct7u(a, b, dims);
            assert dotProduct >= 0;
            float adjustedDistance = dotProduct * scoreCorrectionConstant + aOffset + bOffset;
            if (adjustedDistance < 0) {
                return 1 / (1 + -1 * adjustedDistance);
            }
            return adjustedDistance + 1;
        }

        @Override
        public MaxInnerProductSupplier copy() {
            return new MaxInnerProductSupplier(input.clone(), values, scoreCorrectionConstant);
        }
    }

    static boolean checkIndex(long index, long length) {
        return index >= 0 && index < length;
    }
}
