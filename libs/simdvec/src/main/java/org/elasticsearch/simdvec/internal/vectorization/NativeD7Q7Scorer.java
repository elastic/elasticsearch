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

/** Vectorized scorer for 7-bit symmetric quantized vectors stored as a {@link MemorySegment}. */
final class NativeD7Q7Scorer extends NativeMemorySegmentScorer {

    NativeD7Q7Scorer(IndexInput in, int dimensions, int dataLength, int bulkSize) {
        super(in, dimensions, dataLength, bulkSize);
    }

    @Override
    long dotProduct(MemorySegment dataset, MemorySegment query, int length) {
        return DISTANCE_FUNCS.dotProductI7u(dataset, query, length);
    }

    @Override
    void dotProductBulk(MemorySegment dataset, MemorySegment query, int length, int count, MemorySegment scores) {
        DISTANCE_FUNCS.dotProductI7uBulk(dataset, query, length, count, scores);
    }

    @Override
    void dotProductBulkMasked(
        MemorySegment dataset,
        MemorySegment query,
        int dataLength,
        int dataStride,
        int mask,
        int count,
        MemorySegment scores
    ) {
        DISTANCE_FUNCS.dotProductI7uBulkMasked(dataset, query, dataLength, dataStride, mask, count, scores);
    }

    @Override
    float queryBitScale() {
        return SEVEN_BIT_SCALE;
    }

    @Override
    float indexBitScale() {
        return SEVEN_BIT_SCALE;
    }
}
