/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper.blockloader.docvalues.tracking;

import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.core.Nullable;

import java.io.IOException;

import static org.elasticsearch.index.mapper.blockloader.docvalues.tracking.TrackingNumericDocValues.ESTIMATED_SIZE;

/**
 * {@link TrackingNumericDocValues Singleton} or {@link TrackingSortedNumericDocValues sorted} doc values.
 * One of the two will always be {@code null}.
 */
public record NumericDvSingletonOrSorted(@Nullable TrackingNumericDocValues singleton, @Nullable TrackingSortedNumericDocValues sorted) {
    /**
     * Atomically load the {@link NumericDvSingletonOrSorted}.
     */
    public static NumericDvSingletonOrSorted get(CircuitBreaker breaker, LeafReaderContext context, String fieldName) throws IOException {
        breaker.addEstimateBytesAndMaybeBreak(ESTIMATED_SIZE, "load blocks");
        NumericDvSingletonOrSorted result = null;
        try {
            SortedNumericDocValues sorted = context.reader().getSortedNumericDocValues(fieldName);
            if (sorted != null) {
                NumericDocValues singleton = DocValues.unwrapSingleton(sorted);
                if (singleton != null) {
                    result = new NumericDvSingletonOrSorted(new TrackingNumericDocValues(breaker, singleton), null);
                    return result;
                }
                result = new NumericDvSingletonOrSorted(null, new TrackingSortedNumericDocValues(breaker, sorted));
                return result;
            }
            NumericDocValues singleton = context.reader().getNumericDocValues(fieldName);
            if (singleton != null) {
                result = new NumericDvSingletonOrSorted(new TrackingNumericDocValues(breaker, singleton), null);
                return result;
            }
            return null;
        } finally {
            if (result == null) {
                breaker.addWithoutBreaking(-ESTIMATED_SIZE);
            }
        }
    }

    /**
     * Get the sorted variant, wrapping singleton back to sorted if needed.
     */
    public TrackingSortedNumericDocValues forceSorted() {
        if (sorted != null) {
            return sorted;
        }
        return new TrackingSortedNumericDocValues(singleton.breaker(), DocValues.singleton(singleton.docValues()));
    }
}
