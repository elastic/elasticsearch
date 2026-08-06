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
 * Reusable, lazily-grown {@code byte[]} scratch buffer. Holding
 * one instance per scorer avoids allocating a fresh {@code byte[]} on
 * every bulk-score call.
 *
 * <p>Not thread-safe; instances must not be shared across threads.
 */
public final class BufferScratch {

    private byte[] bytes;

    /**
     * Returns a {@code byte[]} of at least {@code count} slots. The buffer
     * may be larger than requested (it is grown lazily and never shrunk
     * across calls); callers must respect their own {@code count} when
     * reading or writing.
     */
    public byte[] get(int count) {
        if (bytes == null || bytes.length < count) {
            bytes = new byte[count];
        }
        return bytes;
    }
}
