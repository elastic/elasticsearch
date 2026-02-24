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
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.Nullable;

import java.io.IOException;

/**
 * {@link TrackingSortedDocValues Singleton} or {@link TrackingSortedSetDocValues set} doc values.
 * One of the two will always be {@code null}.
 */
public record SortedDvSingletonOrSet(@Nullable TrackingSortedDocValues singleton, @Nullable TrackingSortedSetDocValues set) {
    /**
     * Atomically load the {@link SortedDvSingletonOrSet}.
     */
    public static SortedDvSingletonOrSet get(CircuitBreaker breaker, ByteSizeValue size, LeafReaderContext context, String fieldName)
        throws IOException {
        breaker.addEstimateBytesAndMaybeBreak(size.getBytes(), "load blocks");
        SortedDvSingletonOrSet result = null;
        try {
            SortedSetDocValues set = context.reader().getSortedSetDocValues(fieldName);
            if (set != null) {
                SortedDocValues singleton = DocValues.unwrapSingleton(set);
                if (singleton != null) {
                    result = new SortedDvSingletonOrSet(new TrackingSortedDocValues(breaker, size, singleton), null);
                    return result;
                }
                result = new SortedDvSingletonOrSet(null, new TrackingSortedSetDocValues(breaker, size, set));
                return result;
            }
            SortedDocValues singleton = context.reader().getSortedDocValues(fieldName);
            if (singleton != null) {
                result = new SortedDvSingletonOrSet(new TrackingSortedDocValues(breaker, size, singleton), null);
                return result;
            }
            return null;
        } finally {
            if (result == null) {
                breaker.addWithoutBreaking(-size.getBytes());
            }
        }
    }

    public TrackingSortedSetDocValues forceSet() {
        if (set != null) {
            return set;
        }
        return new TrackingSortedSetDocValues(singleton.breaker(), singleton.size(), DocValues.singleton(singleton.docValues()));
    }
}
