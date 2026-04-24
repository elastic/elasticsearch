/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper.blockloader.docvalues.tracking;

import org.apache.lucene.index.DocValuesSkipper;
import org.apache.lucene.index.LeafReaderContext;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Releasables;

import java.io.IOException;

import static org.elasticsearch.index.mapper.MultiValuedBinaryDocValuesField.SeparateCount.COUNT_FIELD_SUFFIX;

/**
 * A {@link TrackingBinaryDocValues} containing encoded values and an associated
 * {@link TrackingNumericDocValues} containing the counts of encoded values.
 */
public record BinaryAndCounts(TrackingBinaryDocValues binary, @Nullable TrackingNumericDocValues counts) {
    /**
     * Atomically load the {@link BinaryAndCounts}. If the last parameter is {@code true}
     * then {@link #counts()} is nullable. When it is null then all counts are {@code 1}.
     */
    public static BinaryAndCounts get(CircuitBreaker breaker, LeafReaderContext context, String fieldName, boolean skipCountsIfOne)
        throws IOException {
        BinaryAndCounts result = null;
        TrackingBinaryDocValues binary = null;
        TrackingNumericDocValues counts = null;
        try {
            binary = TrackingBinaryDocValues.get(breaker, context, fieldName);
            if (binary == null) {
                return null;
            }
            String countsFieldName = fieldName + COUNT_FIELD_SUFFIX;

            if (skipCountsIfOne) {
                DocValuesSkipper countsSkipper = context.reader().getDocValuesSkipper(countsFieldName);
                assert countsSkipper != null : "no skipper for counts field [" + countsFieldName + "]";
                if (countsSkipper.maxValue() == 1) {
                    result = new BinaryAndCounts(binary, null);
                    return result;
                }
            }

            counts = TrackingNumericDocValues.get(breaker, context, countsFieldName);
            if (counts == null) {
                throw new IllegalStateException("couldn't find counts");
            }
            result = new BinaryAndCounts(binary, counts);
            return result;
        } finally {
            if (result == null) {
                Releasables.close(binary, counts);
            }
        }

    }
}
