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
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.FieldInfo;
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
     * Atomically load the {@link NumericDvSingletonOrSorted}, or {@code null} if the field has no numeric doc values in this segment.
     *
     * @throws IllegalStateException if the field has binary doc values, which this can't read
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
            // Both getters return null for a field with binary doc values, which would read as all nulls. A number field with
            // binary doc values has a block loader of its own, so reaching here with one is a bug.
            FieldInfo fieldInfo = context.reader().getFieldInfos().fieldInfo(fieldName);
            if (fieldInfo != null && fieldInfo.getDocValuesType() == DocValuesType.BINARY) {
                throw new IllegalStateException(
                    "field [" + fieldName + "] has binary doc values, which a numeric doc values block loader can't read"
                );
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
