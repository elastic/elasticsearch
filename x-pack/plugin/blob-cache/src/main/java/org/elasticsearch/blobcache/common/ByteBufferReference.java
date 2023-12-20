/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.blobcache.common;

import org.elasticsearch.core.Nullable;

import java.nio.ByteBuffer;
import java.util.concurrent.Semaphore;

/**
 * Wrapper around a {@link ByteBuffer} that allows making slices of it available for writing to multiple threads concurrently in a safe
 * manner. Used to populate a read buffer from Lucene concurrently across multiple threads.
 */
public final class ByteBufferReference {

    private final Semaphore permits = new Semaphore(Integer.MAX_VALUE);

    private ByteBuffer buffer;

    /**
     * @param buffer to guard against concurrent manipulations
     */
    public ByteBufferReference(ByteBuffer buffer) {
        this.buffer = buffer;
    }

    /**
     * Acquires a permit that must be released via {@link #release()} and returns a {@link ByteBuffer} slice of {@link #buffer} that may be
     * written to, or {@code null} if {@link #finish(int)} has already been called on this instance and no more writing to the buffer is
     * allowed.
     * @param position position relative to the position of {@link #buffer} to start slice at
     * @param length slice length
     * @return slice of {@link #buffer} to write to or {@code null} if already closed
     */
    @Nullable
    public ByteBuffer tryAcquire(int position, int length) {
        if (permits.tryAcquire() == false) {
            return null;
        }
        return buffer.slice(buffer.position() + position, length);
    }

    /**
     * Release a permit acquired through {@link #tryAcquire(int, int)}.
     */
    public void release() {
        permits.release();
    }

    /**
     * Acquire all permits and then safely advance the position of {@link #buffer} by the given number of bytes.
     *
     * @param bytesRead number of bytes to advance the current position of {@link #buffer} by.
     * @throws Exception on failure
     */
    public void finish(int bytesRead) throws Exception {
        if (buffer != null) {
            assert bytesRead == 0 || permits.availablePermits() == Integer.MAX_VALUE
                : "Try to finish [" + bytesRead + "] but only had [" + permits.availablePermits() + "] permits available.";
            permits.acquire(Integer.MAX_VALUE);
            buffer.position(buffer.position() + bytesRead); // mark all bytes as accounted for
            buffer = null;
        }
    }
}
