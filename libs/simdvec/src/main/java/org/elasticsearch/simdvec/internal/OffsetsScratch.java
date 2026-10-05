/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.simdvec.internal;

/**
 * Reusable, lazily-grown {@code long[]} scratch buffer. Holding
 * one instance per scorer avoids allocating a fresh {@code long[]} on
 * every bulk-score call.
 *
 * <p>Not thread-safe; instances must not be shared across threads.
 */
public final class OffsetsScratch {

    private long[] offsets;

    /**
     * Returns a {@code long[]} of at least {@code count} slots. The buffer
     * may be larger than requested (it is grown lazily and never shrunk
     * across calls); callers must respect their own {@code count} when
     * reading or writing.
     */
    public long[] get(int count) {
        if (offsets == null || offsets.length < count) {
            offsets = new long[count];
        }
        return offsets;
    }
}
