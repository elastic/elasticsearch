/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper.blockloader.docvalues.tracking;

import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.LeafReaderContext;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.index.mapper.blockloader.docvalues.BytesRefsFromBinaryBlockLoader;

import java.io.IOException;

public class TrackingBinaryDocValues implements Releasable {
    /**
     * Circuit breaker space reserved for each reader. Measured in heap dumps
     * around from 1.5kb. This is an intentional overestimate.
     */
    private static final long ESTIMATED_SIZE = ByteSizeValue.ofKb(3).getBytes();

    public static TrackingBinaryDocValues get(CircuitBreaker breaker, LeafReaderContext context, String fieldName) throws IOException {
        breaker.addEstimateBytesAndMaybeBreak(ESTIMATED_SIZE, "load blocks");
        TrackingBinaryDocValues result = null;
        try {
            BinaryDocValues docValues = context.reader().getBinaryDocValues(fieldName);
            if (docValues == null) {
                return null;
            }
            result = new TrackingBinaryDocValues(breaker, docValues);
            return null;
        } finally {
            if (result == null) {
                breaker.addWithoutBreaking(-BytesRefsFromBinaryBlockLoader.ESTIMATED_SIZE);
            }
        }
    }

    private final CircuitBreaker breaker;
    private final BinaryDocValues docValues;

    TrackingBinaryDocValues(CircuitBreaker breaker, BinaryDocValues docValues) {
        this.breaker = breaker;
        this.docValues = docValues;
    }

    public BinaryDocValues docValues() {
        return docValues;
    }

    @Override
    public void close() {
        breaker.addWithoutBreaking(-ESTIMATED_SIZE);
    }
}
