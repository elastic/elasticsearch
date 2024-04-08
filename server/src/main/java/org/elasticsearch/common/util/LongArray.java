/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.util;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.Writeable;

import java.io.IOException;

/**
 * Abstraction of an array of long values.
 */
public interface LongArray extends BigArray, Writeable {

    static LongArray readFrom(StreamInput in) throws IOException {
        return new ReleasableLongArray(in);
    }

    /**
     * Get an element given its index.
     */
    long get(long index);

    /**
     * Set a value at the given index and return the previous value.
     */
    long set(long index, long value);

    /**
     * Increment value at the given index by <code>inc</code> and return the value.
     */
    long increment(long index, long inc);

    /**
     * Fill slots between <code>fromIndex</code> inclusive to <code>toIndex</code> exclusive with <code>value</code>.
     */
    void fill(long fromIndex, long toIndex, long value);

    /**
     * Alternative of {@link #readFrom(StreamInput)} where the written bytes are loaded into an existing {@link LongArray}
     */
    void fillWith(StreamInput in) throws IOException;

    /**
     * Bulk set.
     */
    void set(long index, byte[] buf, int offset, int len);

}
