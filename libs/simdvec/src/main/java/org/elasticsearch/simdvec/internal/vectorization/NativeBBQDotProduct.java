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
import org.elasticsearch.lucene.store.SegmentBufferScratch;
import org.elasticsearch.simdvec.BBQEncoding;
import org.elasticsearch.simdvec.ScoringQuery;
import org.elasticsearch.simdvec.SimdVecLibrary;

import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;

import static org.elasticsearch.simdvec.BBQEncoding.D1Q1;
import static org.elasticsearch.simdvec.BBQEncoding.D1Q4;
import static org.elasticsearch.simdvec.BBQEncoding.D2Q2;
import static org.elasticsearch.simdvec.BBQEncoding.D2Q4;
import static org.elasticsearch.simdvec.BBQEncoding.D4Q4;

/**
 * Native {@link BBQDotProduct}, using methods in {@link SimdVecLibrary}
 */
public final class NativeBBQDotProduct extends BBQDotProduct {

    private static final SimdVecLibrary DISTANCE_FUNCS = SimdVecLibrary.instance().orElse(null);

    private static boolean supportedEncoding(BBQEncoding bbqEncoding) {
        return switch (bbqEncoding.toSwitchValue()) {
            case D1Q1, D1Q4, D2Q2, D2Q4, D4Q4 -> true;
            default -> false;
        };
    }

    /**
     * The score array is wrapped as a heap segment, which native calls only accept
     * from JDK 22 onwards.
     */
    public static boolean supports(IndexInput in) {
        return DISTANCE_FUNCS != null && JdkFeatures.SUPPORTS_HEAP_SEGMENTS && IndexInputUtils.canUseSegmentSlices(in);
    }

    /**
     * Factory method for a native-code dot-product implementation where possible.
     *
     * @param in          input positioned at the first data vector to score
     * @param nDims       number of dimensions
     * @param bbqEncoding BBQ encoding sizes
     */
    public static BBQDotProduct create(IndexInput in, int nDims, BBQEncoding bbqEncoding) {
        if (!supports(in) || !supportedEncoding(bbqEncoding)) {
            return PanamaBBQDotProduct.create(in, nDims, bbqEncoding);
        }
        return new NativeBBQDotProduct(in, bbqEncoding, planeBytes(nDims));
    }

    private final ScoringQuery.OffHeapCopy queryCopy = new ScoringQuery.OffHeapCopy();
    private float[] cachedScoresArray;
    private MemorySegment cachedScoresSegment;
    private final SegmentBufferScratch scoresScratch = new SegmentBufferScratch();

    private NativeBBQDotProduct(IndexInput in, BBQEncoding bbqEncoding, int planeBytes) {
        super(in, bbqEncoding, planeBytes);
    }

    private MemorySegment scoresSegment(float[] scores) {
        if (scores != cachedScoresArray) {
            cachedScoresArray = scores;
            cachedScoresSegment = MemorySegment.ofArray(scores);
        }
        return cachedScoresSegment;
    }

    @Override
    public long dotProduct(ScoringQuery query) throws IOException {
        assert query.bytes().length == queryBytes : "query length " + query.bytes().length + " != " + queryBytes;
        MemorySegment querySegment = queryCopy.segmentFor(query);
        return IndexInputUtils.withSlice(in, docBytes, scratch, dataSegment -> switch (encoding.toSwitchValue()) {
            case D1Q1 -> DISTANCE_FUNCS.dotProductD1Q1(dataSegment, querySegment, docBytes);
            case D1Q4 -> DISTANCE_FUNCS.dotProductD1Q4(dataSegment, querySegment, docBytes);
            case D2Q2 -> DISTANCE_FUNCS.dotProductD2Q2(dataSegment, querySegment, docBytes);
            case D2Q4 -> DISTANCE_FUNCS.dotProductD2Q4(dataSegment, querySegment, docBytes);
            case D4Q4 -> DISTANCE_FUNCS.dotProductD4Q4(dataSegment, querySegment, docBytes);
            default -> throw new AssertionError("Unsupported encoding: " + encoding);
        });
    }

    @Override
    public void dotProductBulk(ScoringQuery query, int count, float[] scores) throws IOException {
        assert query.bytes().length == queryBytes : "query length " + query.bytes().length + " != " + queryBytes;
        MemorySegment querySegment = queryCopy.segmentFor(query);
        MemorySegment scoresSegment = scoresSegment(scores);
        IndexInputUtils.withVoidSlice(in, (long) docBytes * count, scratch, dataSegment -> {
            switch (encoding.toSwitchValue()) {
                case D1Q1 -> DISTANCE_FUNCS.dotProductD1Q1Bulk(dataSegment, querySegment, docBytes, count, scoresSegment);
                case D1Q4 -> DISTANCE_FUNCS.dotProductD1Q4Bulk(dataSegment, querySegment, docBytes, count, scoresSegment);
                case D2Q2 -> DISTANCE_FUNCS.dotProductD2Q2Bulk(dataSegment, querySegment, docBytes, count, scoresSegment);
                case D2Q4 -> DISTANCE_FUNCS.dotProductD2Q4Bulk(dataSegment, querySegment, docBytes, count, scoresSegment);
                case D4Q4 -> DISTANCE_FUNCS.dotProductD4Q4Bulk(dataSegment, querySegment, docBytes, count, scoresSegment);
                default -> throw new AssertionError("Unsupported encoding: " + encoding);
            }
        });
    }

    @Override
    public void dotProductBulkMasked(ScoringQuery query, int mask, float[] scores, int count) throws IOException {
        assert query.bytes().length == queryBytes : "query length " + query.bytes().length + " != " + queryBytes;
        MemorySegment querySegment = queryCopy.segmentFor(query);
        MemorySegment scoresSegment = scoresScratch.apply(count * Float.BYTES);
        IndexInputUtils.withVoidSlice(in, (long) docBytes * count, scratch, dataSegment -> {
            switch (encoding.toSwitchValue()) {
                case D1Q1 -> DISTANCE_FUNCS.dotProductD1Q1BulkMasked(
                    dataSegment,
                    querySegment,
                    docBytes,
                    docBytes,
                    mask,
                    count,
                    scoresSegment
                );
                case D1Q4 -> DISTANCE_FUNCS.dotProductD1Q4BulkMasked(
                    dataSegment,
                    querySegment,
                    docBytes,
                    docBytes,
                    mask,
                    count,
                    scoresSegment
                );
                case D2Q2 -> DISTANCE_FUNCS.dotProductD2Q2BulkMasked(
                    dataSegment,
                    querySegment,
                    docBytes,
                    docBytes,
                    mask,
                    count,
                    scoresSegment
                );
                case D2Q4 -> DISTANCE_FUNCS.dotProductD2Q4BulkMasked(
                    dataSegment,
                    querySegment,
                    docBytes,
                    docBytes,
                    mask,
                    count,
                    scoresSegment
                );
                case D4Q4 -> DISTANCE_FUNCS.dotProductD4Q4BulkMasked(
                    dataSegment,
                    querySegment,
                    docBytes,
                    docBytes,
                    mask,
                    count,
                    scoresSegment
                );
                default -> throw new AssertionError("Unsupported encoding: " + encoding);
            }
        });
        MemorySegment.copy(scoresSegment, ValueLayout.JAVA_FLOAT, 0L, scores, 0, count);
    }
}
