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
 * Builder for bytes arrays that checks its size against a {@link CircuitBreaker}.
 */
public class BreakingBytesRefBuilder implements Accountable, Releasable {
    static final long SHALLOW_SIZE = RamUsageEstimator.shallowSizeOfInstance(BreakingBytesRefBuilder.class) + RamUsageEstimator
        .shallowSizeOfInstance(BytesRef.class);

    private final BytesRef bytes;
    private final CircuitBreaker breaker;
    private final String label;

    /**
     * Build.
     * @param breaker the {@link CircuitBreaker} to check on resize
     * @param label the label reported by the breaker when it breaks
     */
    public BreakingBytesRefBuilder(CircuitBreaker breaker, String label) {
        this(breaker, label, 0);
    }

    /**
     * Build.
     * @param breaker the {@link CircuitBreaker} to check on resize
     * @param label the label reported by the breaker when it breaks
     * @param initialCapacity the number of bytes initially allocated
     */
    public BreakingBytesRefBuilder(CircuitBreaker breaker, String label, int initialCapacity) {
        /*
         * We initialize BytesRef to a shared empty bytes array as is tradition.
         * It's a good tradition. We don't know how big the thing will ultimately
         * get, so we may as well let it grow.
         *
         * But! The ramBytesUsed mechanism doesn't know that this empty bytes is
         * shared. So it overcounts. Which is *fine*. Empty bytes refs usually don't
         * last long so it isn't worth making the accounting more complex to get it
         * perfect. And overcounting in general isn't too bad compared to undercounting.
         */
        breaker.addEstimateBytesAndMaybeBreak(SHALLOW_SIZE + bytesArrayRamBytesUsed(initialCapacity), label);
        if (initialCapacity == 0) {
            this.bytes = new BytesRef();
        } else {
            this.bytes = new BytesRef(initialCapacity);
        }
        this.breaker = breaker;
        this.label = label;
    }

    /**
     * Make sure that the underlying bytes has a capacity of at least
     * {@code capacity}.
     */
    public void grow(int capacity) {
        int oldCapacity = bytes.bytes.length;
        if (oldCapacity > capacity) {
            return;
        }
        int oversizedCapacity = ArrayUtil.oversize(capacity, Byte.BYTES);
        breaker.addEstimateBytesAndMaybeBreak(bytesArrayRamBytesUsed(oversizedCapacity), label);

        bytes.bytes = ArrayUtil.growExact(bytes.bytes, oversizedCapacity);
        breaker.addWithoutBreaking(-bytesArrayRamBytesUsed(oldCapacity));
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
     * The number of bytes in this buffer. It is <strong>not></strong>
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
     * Set the content of the builder to the given bytes.
     */
    public void copyBytes(BytesRef newBytes) {
        clear();
        append(newBytes);
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
     * will modify the returned {@link BytesRef}. The caller must copy the bytes
     * if they wish to keep them.
     */
    public BytesRef bytesRefView() {
        assert bytes.offset == 0;
        return bytes;
    }

    @Override
    public long ramBytesUsed() {
        return SHALLOW_SIZE + bytesArrayRamBytesUsed(bytes.bytes.length);
    }

    private static long bytesArrayRamBytesUsed(long capacity) {
        return RamUsageEstimator.alignObjectSize(RamUsageEstimator.NUM_BYTES_ARRAY_HEADER + capacity);
    }

    @Override
    public void close() {
        breaker.addWithoutBreaking(-ramBytesUsed());
    }
}
