/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.simdvec.internal.vectorization;

import org.apache.lucene.store.IndexInput;
import org.elasticsearch.lucene.store.IndexInputUtils;
import org.elasticsearch.simdvec.SimdVecLibrary;

import java.io.IOException;
import java.lang.foreign.MemorySegment;

/**
 * Native {@link BBQDotProduct}, using methods in {@link SimdVecLibrary}
 */
public final class NativeBBQDotProduct extends BBQDotProduct {

    private static final SimdVecLibrary DISTANCE_FUNCS = SimdVecLibrary.instance().orElse(null);

    private enum NativeMethod {
        D1Q1,
        D1Q4,
        D2Q2,
        D2Q4,
        D4Q4
    }

    private static NativeMethod nativeMethod(int docBits, int queryBits) {
        return switch ((docBits << 8) | queryBits) {
            case (1 << 8) | 1 -> NativeMethod.D1Q1;
            case (1 << 8) | 4 -> NativeMethod.D1Q4;
            case (2 << 8) | 2 -> NativeMethod.D2Q2;
            case (2 << 8) | 4 -> NativeMethod.D2Q4;
            case (4 << 8) | 4 -> NativeMethod.D4Q4;
            default -> null;
        };
    }

    /**
     * The query and score arrays are wrapped as heap segments, which native calls only accept
     * from JDK 22 onwards.
     */
    public static boolean supports(IndexInput in, int docBits, int queryBits) {
        return DISTANCE_FUNCS != null
            && JdkFeatures.SUPPORTS_HEAP_SEGMENTS
            && nativeMethod(docBits, queryBits) != null
            && IndexInputUtils.canUseSegmentSlices(in);
    }

    /**
     * Factory method for a native-code dot-product implementation where possible.
     *
     * @param in         input positioned at the first data vector to score
     * @param nDims      number of dimensions
     * @param docBits    bits per dimension of the data vector, in {@code [1, MAX_BITS]}
     * @param queryBits  bits per dimension of the query vector, in {@code [1, MAX_BITS]}
     */
    public static BBQDotProduct create(IndexInput in, int nDims, int docBits, int queryBits) {
        if (!supports(in, docBits, queryBits)) {
            return PanamaBBQDotProduct.create(in, nDims, docBits, queryBits);
        }
        return new NativeBBQDotProduct(in, docBits, queryBits, planeBytes(nDims));
    }

    private final NativeMethod nativeMethod;

    // Heap segments wrapping the caller's arrays. The caller reuses the same query and scores arrays
    // across a whole posting list, so the wrappers are cached against array identity.
    private byte[] cachedQueryArray;
    private MemorySegment cachedQuerySegment;
    private float[] cachedScoresArray;
    private MemorySegment cachedScoresSegment;

    private NativeBBQDotProduct(IndexInput in, int docBits, int queryBits, int planeBytes) {
        super(in, docBits, queryBits, planeBytes);
        this.nativeMethod = nativeMethod(docBits, queryBits);
    }

    private MemorySegment querySegment(byte[] query) {
        if (query != cachedQueryArray) {
            cachedQueryArray = query;
            cachedQuerySegment = MemorySegment.ofArray(query);
        }
        return cachedQuerySegment;
    }

    private MemorySegment scoresSegment(float[] scores) {
        if (scores != cachedScoresArray) {
            cachedScoresArray = scores;
            cachedScoresSegment = MemorySegment.ofArray(scores);
        }
        return cachedScoresSegment;
    }

    @Override
    public long dotProduct(byte[] query) throws IOException {
        assert query.length == queryBytes : "query length " + query.length + " != " + queryBytes;
        MemorySegment querySegment = querySegment(query);
        return IndexInputUtils.withSlice(in, docBytes, scratch, dataSegment -> switch (nativeMethod) {
            case D1Q1 -> DISTANCE_FUNCS.dotProductD1Q1(dataSegment, querySegment, docBytes);
            case D1Q4 -> DISTANCE_FUNCS.dotProductD1Q4(dataSegment, querySegment, docBytes);
            case D2Q2 -> DISTANCE_FUNCS.dotProductD2Q2(dataSegment, querySegment, docBytes);
            case D2Q4 -> DISTANCE_FUNCS.dotProductD2Q4(dataSegment, querySegment, docBytes);
            case D4Q4 -> DISTANCE_FUNCS.dotProductD4Q4(dataSegment, querySegment, docBytes);
        });
    }

    @Override
    public void dotProductBulk(byte[] query, int count, float[] scores) throws IOException {
        assert query.length == queryBytes : "query length " + query.length + " != " + queryBytes;
        MemorySegment querySegment = querySegment(query);
        MemorySegment scoresSegment = scoresSegment(scores);
        IndexInputUtils.withVoidSlice(in, (long) docBytes * count, scratch, dataSegment -> {
            switch (nativeMethod) {
                case D1Q1 -> DISTANCE_FUNCS.dotProductD1Q1Bulk(dataSegment, querySegment, docBytes, count, scoresSegment);
                case D1Q4 -> DISTANCE_FUNCS.dotProductD1Q4Bulk(dataSegment, querySegment, docBytes, count, scoresSegment);
                case D2Q2 -> DISTANCE_FUNCS.dotProductD2Q2Bulk(dataSegment, querySegment, docBytes, count, scoresSegment);
                case D2Q4 -> DISTANCE_FUNCS.dotProductD2Q4Bulk(dataSegment, querySegment, docBytes, count, scoresSegment);
                case D4Q4 -> DISTANCE_FUNCS.dotProductD4Q4Bulk(dataSegment, querySegment, docBytes, count, scoresSegment);
            }
        });
    }

    @Override
    public void dotProductBulkOffsets(byte[] query, int[] offsets, int offsetsCount, float[] scores, int count) throws IOException {
        assert query.length == queryBytes : "query length " + query.length + " != " + queryBytes;
        MemorySegment querySegment = querySegment(query);
        MemorySegment scoresSegment = scoresSegment(scores);
        MemorySegment offsetsSegment = MemorySegment.ofArray(offsets);
        IndexInputUtils.withVoidSlice(in, (long) docBytes * count, scratch, dataSegment -> {
            switch (nativeMethod) {
                case D1Q1 -> DISTANCE_FUNCS.dotProductD1Q1BulkWithOffsets(
                    dataSegment,
                    querySegment,
                    docBytes,
                    docBytes,
                    offsetsSegment,
                    offsetsCount,
                    scoresSegment
                );
                case D1Q4 -> DISTANCE_FUNCS.dotProductD1Q4BulkWithOffsets(
                    dataSegment,
                    querySegment,
                    docBytes,
                    docBytes,
                    offsetsSegment,
                    offsetsCount,
                    scoresSegment
                );
                case D2Q2 -> DISTANCE_FUNCS.dotProductD2Q2BulkWithOffsets(
                    dataSegment,
                    querySegment,
                    docBytes,
                    docBytes,
                    offsetsSegment,
                    offsetsCount,
                    scoresSegment
                );
                case D2Q4 -> DISTANCE_FUNCS.dotProductD2Q4BulkWithOffsets(
                    dataSegment,
                    querySegment,
                    docBytes,
                    docBytes,
                    offsetsSegment,
                    offsetsCount,
                    scoresSegment
                );
                case D4Q4 -> DISTANCE_FUNCS.dotProductD4Q4BulkWithOffsets(
                    dataSegment,
                    querySegment,
                    docBytes,
                    docBytes,
                    offsetsSegment,
                    offsetsCount,
                    scoresSegment
                );
            }
        });
        repositionScoresMatchingOffsets(offsets, offsetsCount, scores);
    }

    /**
     * The native methods put their scores all at the start of {@code scores}.
     * So we need to move each score to the correct position in {@code scores}.
     */
    private static void repositionScoresMatchingOffsets(int[] offsets, int offsetsCount, float[] scores) {
        for (int i = offsetsCount - 1; i >= 0; i--) {
            int finalScoreIndex = offsets[i];
            if (i < finalScoreIndex) {
                scores[finalScoreIndex] = scores[i];
                scores[i] = 0;
            }
        }
    }
}
