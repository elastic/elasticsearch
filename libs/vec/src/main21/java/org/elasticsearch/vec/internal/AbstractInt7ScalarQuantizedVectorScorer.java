/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.vec.internal;

import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.quantization.ScalarQuantizedVectorSimilarity;
import org.elasticsearch.nativeaccess.NativeAccess;
import org.elasticsearch.nativeaccess.VectorSimilarityFunctions;
import org.elasticsearch.vec.VectorScorer;

import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.lang.invoke.MethodHandle;

abstract sealed class AbstractInt7ScalarQuantizedVectorScorer implements VectorScorer permits Int7DotProduct, Int7Euclidean,
    Int7MaximumInnerProduct {

    static final VectorSimilarityFunctions DISTANCE_FUNCS = NativeAccess.instance()
        .getVectorSimilarityFunctions()
        .orElseThrow(AssertionError::new);

    protected final int dims;
    protected final int maxOrd;
    protected final float scoreCorrectionConstant;
    protected final IndexInput input;
    protected final MemorySegment segment;
    protected final MemorySegment[] segments;
    protected final long offset;
    protected final int chunkSizePower;
    protected final long chunkSizeMask;

    private final ScalarQuantizedVectorSimilarity fallbackScorer;

    protected AbstractInt7ScalarQuantizedVectorScorer(
        int dims,
        int maxOrd,
        float scoreCorrectionConstant,
        IndexInput input,
        ScalarQuantizedVectorSimilarity fallbackScorer
    ) {
        this.dims = dims;
        this.maxOrd = maxOrd;
        this.scoreCorrectionConstant = scoreCorrectionConstant;
        this.input = input;
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

    @Override
    public final int dims() {
        return dims;
    }

    @Override
    public final int maxOrd() {
        return maxOrd;
    }

    protected final void checkOrdinal(int ord) {
        if (ord < 0 || ord > maxOrd) {
            throw new IllegalArgumentException("illegal ordinal: " + ord);
        }
    }

    protected final float fallbackScore(int firstByteOffset, int secondByteOffset) throws IOException {
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

    static boolean checkIndex(long index, long length) {
        return index >= 0 && index < length;
    }

    static final MethodHandle DOT_PRODUCT_7U = DISTANCE_FUNCS.dotProductHandle7u();
    static final MethodHandle SQUARE_DISTANCE_7U = DISTANCE_FUNCS.squareDistanceHandle7u();

    static int dotProduct7u(MemorySegment a, MemorySegment b, int length) {
        // assert assertSegments(a, b, length);
        try {
            return (int) DOT_PRODUCT_7U.invokeExact(a, b, length);
        } catch (Throwable e) {
            if (e instanceof Error err) {
                throw err;
            } else if (e instanceof RuntimeException re) {
                throw re;
            } else {
                throw new RuntimeException(e);
            }
        }
    }

    static int squareDistance7u(MemorySegment a, MemorySegment b, int length) {
        // assert assertSegments(a, b, length);
        try {
            return (int) SQUARE_DISTANCE_7U.invokeExact(a, b, length);
        } catch (Throwable e) {
            if (e instanceof Error err) {
                throw err;
            } else if (e instanceof RuntimeException re) {
                throw re;
            } else {
                throw new RuntimeException(e);
            }
        }
    }

    static boolean assertSegments(MemorySegment a, MemorySegment b, int length) {
        return a.isNative() && a.byteSize() >= length && b.isNative() && b.byteSize() >= length;
    }
}
