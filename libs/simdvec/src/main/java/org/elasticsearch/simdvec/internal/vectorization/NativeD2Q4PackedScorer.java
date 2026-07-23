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

import java.lang.foreign.MemorySegment;

/**
 * Packed D2Q4 scorer that uses existing native dot-product ops.
 * Returns sentinel values when native support is unavailable so callers can fallback.
 */
final class NativeD2Q4PackedScorer extends NativeMemorySegmentScorer {

    NativeD2Q4PackedScorer(IndexInput in, int dimensions, int dataLength, int bulkSize) {
        super(in, dimensions, dataLength, bulkSize);
    }

    @Override
    long dotProduct(MemorySegment dataset, MemorySegment query, int length) {
        return DISTANCE_FUNCS.dotProductD2Q4Packed(dataset, query, length);
    }

    @Override
    void dotProductBulk(MemorySegment dataset, MemorySegment query, int length, int count, MemorySegment scores) {
        DISTANCE_FUNCS.dotProductD2Q4PackedBulk(dataset, query, length, count, scores);
    }

    @Override
    void dotProductBulkWithOffsets(
        MemorySegment dataset,
        MemorySegment query,
        int dataLength,
        int dataStride,
        MemorySegment offsets,
        int offsetsCount,
        MemorySegment scores
    ) {
        DISTANCE_FUNCS.dotProductD2Q4PackedBulkWithOffsets(dataset, query, dataLength, dataStride, offsets, offsetsCount, scores);
    }

    @Override
    float queryBitScale() {
        return FOUR_BIT_SCALE;
    }

    @Override
    float indexBitScale() {
        return TWO_BIT_SCALE;
    }
}
