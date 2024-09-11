/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.tdigest.arrays;

/**
 * Minimal interface for IntArray-like classes used within TDigest.
 */
public interface TDigestIntArray extends AutoCloseable {
    int size();

    int get(int index);

    void set(int index, int value);

    /**
     * Copies {@code len} elements from {@code buf} to this array.
     * <p>
     *     Copy must be made in reverse order. That is, starting from offset+len-1 to offset.
     *     This is, because it will be used to copy an array to itself.
     * </p>
     */
    default void set(int index, TDigestIntArray buf, int offset, int len) {
        assert index >= 0 && index + len <= this.size();
        for (int i = len - 1; i >= 0; i--) {
            this.set(index + i, buf.get(offset + i));
        }
    }

    /**
     * Overriding close to remove the exception from the signature.
     */
    @Override
    void close();
}
