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
import org.apache.lucene.index.NumericDocValues;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.Releasable;

import java.io.IOException;

/**
 * Wraps a {@link NumericDocValues}, reserving some space in a {@link CircuitBreaker}
 * while it is live.
 */
public record TrackingNumericDocValues(CircuitBreaker breaker, NumericDocValues docValues) implements Releasable {
    /**
     * Circuit breaker space reserved for each reader. Measured in heap dumps
     * around 500 bytes and this is an intention overestimate.
     */
    public static final long ESTIMATED_SIZE = ByteSizeValue.ofKb(2).getBytes();

    public static TrackingNumericDocValues get(CircuitBreaker breaker, LeafReaderContext context, String fieldName) throws IOException {
        breaker.addEstimateBytesAndMaybeBreak(ESTIMATED_SIZE, "load blocks");
        TrackingNumericDocValues result = null;
        try {
            NumericDocValues docValues = context.reader().getNumericDocValues(fieldName);
            if (docValues == null) {
                return null;
            }
            result = new TrackingNumericDocValues(breaker, docValues);
            return result;
        } finally {
            if (result == null) {
                breaker.addWithoutBreaking(-ESTIMATED_SIZE);
            }
        }
    }

    @Override
    public void close() {
        breaker.addWithoutBreaking(-ESTIMATED_SIZE);
    }
}
