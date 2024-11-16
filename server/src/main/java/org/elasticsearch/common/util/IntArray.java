/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.util;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.Writeable;

import java.io.IOException;

/**
 * Abstraction of an array of integer values.
 */
public interface IntArray extends BigArray, Writeable {
    static IntArray readFrom(StreamInput in) throws IOException {
        return new ReleasableIntArray(in);
    }

    /**
     * Get an element given its index.
     */
    int get(long index);

    /**
     * Set a value at the given index and return the previous value.
     */
    int getAndSet(long index, int value);

    /**
     * Set a value at the given index
     */
    void set(long index, int value);

    /**
     * Increment value at the given index by <code>inc</code> and return the value.
     */
    int increment(long index, int inc);

    /**
     * Fill slots between <code>fromIndex</code> inclusive to <code>toIndex</code> exclusive with <code>value</code>.
     */
    void fill(long fromIndex, long toIndex, int value);

    /**
     * Alternative of {@link IntArray#readFrom(StreamInput)} where the written bytes are loaded into an existing {@link IntArray}
     */
    void fillWith(StreamInput in) throws IOException;

    /**
     * Bulk set.
     */
    void set(long index, byte[] buf, int offset, int len);

}
