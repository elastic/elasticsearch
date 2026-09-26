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
 * Packed-nibble int4 scorer that uses existing native dot-product ops.
 * Returns sentinel values when native support is unavailable so callers can fallback.
 */
final class NativeD4Q4PackedScorer extends NativeMemorySegmentScorer {

    NativeD4Q4PackedScorer(IndexInput in, int dimensions, int dataLength, int bulkSize) {
        super(in, dimensions, dataLength, bulkSize);
    }

    @Override
    long dotProduct(MemorySegment dataset, MemorySegment query, int length) {
        return DISTANCE_FUNCS.dotProductI4(query, dataset, length);
    }

    @Override
    void dotProductBulk(MemorySegment dataset, MemorySegment query, int length, int count, MemorySegment scores) {
        DISTANCE_FUNCS.dotProductI4Bulk(dataset, query, length, count, scores);
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
        DISTANCE_FUNCS.dotProductI4BulkWithOffsets(dataset, query, dataLength, dataStride, offsets, offsetsCount, scores);
    }

    @Override
    float queryBitScale() {
        return FOUR_BIT_SCALE;
    }

    @Override
    float indexBitScale() {
        return FOUR_BIT_SCALE;
    }
}
