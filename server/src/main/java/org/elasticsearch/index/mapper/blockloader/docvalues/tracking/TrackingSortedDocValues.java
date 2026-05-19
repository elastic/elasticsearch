/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper.blockloader.docvalues.tracking;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedDocValues;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Releasable;

import java.io.IOException;

/**
 * Wraps a {@link SortedDocValues}, reserving some space in a {@link CircuitBreaker}
 * while it is live.
 */
public record TrackingSortedDocValues(CircuitBreaker breaker, ByteSizeValue size, SortedDocValues docValues) implements Releasable {

    /**
     * Circuit breaker space reserved for each reader. Measured in heap dumps
     * around 500 bytes and this is an intention overestimate.
     */
    public static final ByteSizeValue DEFAULT_SIZE = ByteSizeValue.ofKb(2);

    @Nullable
    public static TrackingSortedDocValues get(CircuitBreaker breaker, LeafReaderContext context, String fieldName) throws IOException {
        breaker.addEstimateBytesAndMaybeBreak(DEFAULT_SIZE.getBytes(), "load blocks");
        TrackingSortedDocValues result = null;
        try {
            SortedDocValues sdv = context.reader().getSortedDocValues(fieldName);
            if (sdv == null) {
                return null;
            }
            result = new TrackingSortedDocValues(breaker, DEFAULT_SIZE, sdv);
            return result;
        } finally {
            if (result == null) {
                breaker.addWithoutBreaking(-DEFAULT_SIZE.getBytes());
            }
        }
    }

    @Override
    public void close() {
        breaker.addWithoutBreaking(-size.getBytes());
    }
}
