/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.core.Releasable;

/**
 * Builder for bytes arrays.
 */
public class BreakingBytesRefBuilder implements Accountable, Releasable {
    private static final long SHALLOW_SIZE = RamUsageEstimator.shallowSizeOfInstance(BreakingBytesRefBuilder.class) + RamUsageEstimator
        .shallowSizeOfInstance(BytesRef.class);

    private final BytesRef bytes = new BytesRef();

    private final CircuitBreaker breaker;
    private final String label;

    public BreakingBytesRefBuilder(CircuitBreaker breaker, String label) {
        this.breaker = breaker;
        this.label = label;
    }

    /**
     * Make sure that the underlying bytes has a capacity of at least
     * {@code capacity}.
     */
    public void grow(int capacity) {
        int oldSize = bytes.bytes.length;
        if (oldSize > capacity) {
            return;
        }
        int newSize = ArrayUtil.oversize(capacity, Byte.BYTES);
        breaker.addEstimateBytesAndMaybeBreak(newSize, label);
        bytes.bytes = ArrayUtil.growExact(bytes.bytes, newSize);
        breaker.addWithoutBreaking(-oldSize);
    }

    /**
     * Return the underlying bytes being built for direct manipulation.
     * Callers will typically use this in combination with {@link #grow},
     * {@link #length} and {@link #setLength} like this:
     * {@code<pre>
     *     bytesRefBuilder.grow(bytesRefBuilder.length() + Long.BYTES);
     *     LONG.set(bytesRefBuilder.bytes(), bytesRefBuilder.length(), value);
     *     bytesRefBuilder.setLength(bytesRefBuilder.length() + Long.BYTES);
     * </pre>}
     */
    public byte[] bytes() {
        return bytes.bytes;
    }

    /**
     * The number of bytes in to this buffer. It is <strong>not></strong>
     * the capacity of the buffer.
     */
    public int length() {
        return bytes.length;
    }

    /**
     * Set the number of bytes in this buffer. Does not deallocate the
     * underlying buffer, only the length once built.
     */
    public void setLength(int length) {
        bytes.length = length;
    }

    /**
     * Append a byte.
     */
    public void append(byte b) {
        grow(bytes.length + 1);
        bytes.bytes[bytes.length++] = b;
    }

    /**
     * Append bytes.
     */
    public void append(byte[] b, int off, int len) {
        grow(bytes.length + len);
        System.arraycopy(b, off, bytes.bytes, bytes.length, len);
        bytes.length += len;
    }

    /**
     * Append bytes.
     */
    public void append(BytesRef bytes) {
        append(bytes.bytes, bytes.offset, bytes.length);
    }

    /**
     * Reset the builder to an empty bytes array. Doesn't deallocate any memory.
     */
    public void clear() {
        bytes.length = 0;
    }

    /**
     * Returns a view of the data added as a {@link BytesRef}. Importantly, this does not
     * copy the bytes and any further modification to the {@link BreakingBytesRefBuilder}
     * will modify the returned {@link BytesRef}. The called must copy the bytes
     * if they wish to keep them.
     */
    public BytesRef bytesRefView() {
        assert bytes.offset == 0;
        return bytes;
    }

    @Override
    public long ramBytesUsed() {
        return SHALLOW_SIZE + RamUsageEstimator.sizeOf(bytes.bytes);
    }

    @Override
    public void close() {
        breaker.addWithoutBreaking(-bytes.bytes.length);
    }
}
