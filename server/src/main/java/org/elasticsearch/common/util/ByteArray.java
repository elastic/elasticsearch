/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.util;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.Writeable;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Abstraction of an array of byte values.
 */
public interface ByteArray extends BigArray, Writeable {

    static ByteArray readFrom(StreamInput in) throws IOException {
        return new ReleasableByteArray(in);
    }

    /**
     * Get an element given its index.
     */
    byte get(long index);

    /**
     * Set a value at the given index and return the previous value.
     */
    byte set(long index, byte value);

    /**
     * Get a reference to a slice.
     *
     * @return <code>true</code> when a byte[] was materialized, <code>false</code> otherwise.
     */
    boolean get(long index, int len, BytesRef ref);

    /**
     * Bulk set.
     */
    void set(long index, byte[] buf, int offset, int len);

    /**
     * Fill slots between <code>fromIndex</code> inclusive to <code>toIndex</code> exclusive with <code>value</code>.
     */
    void fill(long fromIndex, long toIndex, byte value);

    /**
     * Checks if this instance is backed by a single byte array analogous to {@link ByteBuffer#hasArray()}.
     */
    boolean hasArray();

    /**
     * Get backing byte array analogous to {@link ByteBuffer#array()}.
     */
    byte[] array();
}
