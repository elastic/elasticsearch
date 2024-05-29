/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.vec.internal;

import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.hnsw.RandomVectorScorer;
import org.apache.lucene.util.hnsw.RandomVectorScorerSupplier;
import org.apache.lucene.util.quantization.RandomAccessQuantizedByteVectorValues;
import org.apache.lucene.util.quantization.ScalarQuantizedVectorSimilarity;

import java.io.IOException;
import java.lang.foreign.MemorySegment;

import static org.apache.lucene.index.VectorSimilarityFunction.DOT_PRODUCT;
import static org.apache.lucene.index.VectorSimilarityFunction.EUCLIDEAN;
import static org.apache.lucene.index.VectorSimilarityFunction.MAXIMUM_INNER_PRODUCT;
import static org.apache.lucene.util.quantization.ScalarQuantizedVectorSimilarity.fromVectorSimilarity;

public abstract sealed class Int7SQVectorScorerSupplier implements RandomVectorScorerSupplier {

    final int dims;
    final int maxOrd;
    final float scoreCorrectionConstant;
    final IndexInput input;
    final RandomAccessQuantizedByteVectorValues values; // to support ordToDoc/getAcceptOrds
    final ScalarQuantizedVectorSimilarity fallbackScorer;

    final MemorySegment segment;
    final MemorySegment[] segments;
    final long offset;
    final int chunkSizePower;
    final long chunkSizeMask;

    protected Int7SQVectorScorerSupplier(
        IndexInput input,
        RandomAccessQuantizedByteVectorValues values,
        float scoreCorrectionConstant,
        ScalarQuantizedVectorSimilarity fallbackScorer
    ) {
        this.input = input;
        this.values = values;
        this.dims = values.dimension();
        this.maxOrd = values.size();
        this.scoreCorrectionConstant = scoreCorrectionConstant;
        this.fallbackScorer = fallbackScorer;

        this.segments = IndexInputUtils.segmentArray(input);
        if (segments.length == 1) {
            segment = segments[0];
            offset = 0L;
        } else {
            segment = null;
            offset = IndexInputUtils.offset(input);
        }
        this.chunkSizePower = IndexInputUtils.chunkSizePower(input);
        this.chunkSizeMask = IndexInputUtils.chunkSizeMask(input);
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

        MemorySegment firstSeg = segmentSlice(firstByteOffset, length);
        if (firstSeg == null) {
            return fallbackScore(firstByteOffset, secondByteOffset);
        }
        input.seek(firstByteOffset + length);
        float firstOffset = Float.intBitsToFloat(input.readInt());

        MemorySegment secondSeg = segmentSlice(secondByteOffset, length);
        if (secondSeg == null) {
            return fallbackScore(firstByteOffset, secondByteOffset);
        }
        input.seek(secondByteOffset + length);
        float secondOffset = Float.intBitsToFloat(input.readInt());

        return scoreFromSegments(firstSeg, firstOffset, secondSeg, secondOffset);
    }

    abstract float scoreFromSegments(MemorySegment a, float aOffset, MemorySegment b, float bOffset);

    protected final float fallbackScore(long firstByteOffset, long secondByteOffset) throws IOException {
        input.seek(firstByteOffset);
        byte[] a = new byte[dims];
        input.readBytes(a, 0, a.length);
        float aOffsetValue = Float.intBitsToFloat(input.readInt());

        input.seek(secondByteOffset);
        byte[] b = new byte[dims];
        input.readBytes(b, 0, a.length);
        float bOffsetValue = Float.intBitsToFloat(input.readInt());

        return fallbackScorer.score(a, aOffsetValue, b, bOffsetValue);
    }

    @Override
    public RandomVectorScorer scorer(int ord) {
        checkOrdinal(ord);
        return new RandomVectorScorer.AbstractRandomVectorScorer<>(values) {
            @Override
            public float score(int node) throws IOException {
                return scoreFromOrds(ord, node);
            }
        };
    }

    protected final MemorySegment segmentSlice(long pos, int length) {
        if (segment != null) {
            // single
            if (checkIndex(pos, segment.byteSize() + 1)) {
                return segment.asSlice(pos, length);
            }
        } else {
            // multi
            pos = pos + this.offset;
            final int si = (int) (pos >> chunkSizePower);
            final MemorySegment seg = segments[si];
            long offset = pos & chunkSizeMask;
            if (checkIndex(offset + length, seg.byteSize() + 1)) {
                return seg.asSlice(offset, length);
            }
        }
        return null;
    }

    public static final class EuclideanSupplier extends Int7SQVectorScorerSupplier {

        public EuclideanSupplier(IndexInput input, RandomAccessQuantizedByteVectorValues values, float scoreCorrectionConstant) {
            super(input, values, scoreCorrectionConstant, fromVectorSimilarity(EUCLIDEAN, scoreCorrectionConstant));
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

    // This will be removed when we upgrade to 9.11, see https://github.com/apache/lucene/pull/13356
    static final class DelegateDotScorer implements ScalarQuantizedVectorSimilarity {
        final ScalarQuantizedVectorSimilarity delegate;

        DelegateDotScorer(float scoreCorrectionConstant) {
            delegate = fromVectorSimilarity(DOT_PRODUCT, scoreCorrectionConstant);
        }

        @Override
        public float score(byte[] queryVector, float queryVectorOffset, byte[] storedVector, float vectorOffset) {
            return Math.max(delegate.score(queryVector, queryVectorOffset, storedVector, vectorOffset), 0f);
        }
    }

    public static final class DotProductSupplier extends Int7SQVectorScorerSupplier {

        public DotProductSupplier(IndexInput input, RandomAccessQuantizedByteVectorValues values, float scoreCorrectionConstant) {
            super(input, values, scoreCorrectionConstant, new DelegateDotScorer(scoreCorrectionConstant));
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

        public MaxInnerProductSupplier(IndexInput input, RandomAccessQuantizedByteVectorValues values, float scoreCorrectionConstant) {
            super(input, values, scoreCorrectionConstant, fromVectorSimilarity(MAXIMUM_INNER_PRODUCT, scoreCorrectionConstant));
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
