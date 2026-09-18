/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.xpack.esql.datasources.spi.DirectReadBuffer;
import org.elasticsearch.xpack.esql.datasources.spi.ExternalUnavailableException;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;

import java.nio.ByteBuffer;

/**
 * Fills a pre-sized destination from streaming body chunks and types a length mismatch as
 * {@link ExternalUnavailableException}.
 * <p>
 * HTTP 206 and S3 already fail a truncated or over-long body as a non-throttling 503 so the
 * operator names the object instead of a generic "transient read failure". HTTP 200 skip-then-fill
 * has to raise the same exception or the mapper can only wrap {@code IOException}. This helper
 * owns those messages so the three callers cannot drift.
 * <p>
 * Not thread-safe: callers already serialize copies under {@code destinationLock}. Does not
 * allocate a destination, close buffers, cancel subscriptions, or complete futures — those stay
 * with the subscriber so overflow can still close inside the lock and cancel outside it.
 */
public final class KnownLengthBodyFill {

    private final String store;
    private final StoragePath path;
    private final int expectedLength;
    private int offset;

    public KnownLengthBodyFill(String store, StoragePath path, int expectedLength) {
        if (expectedLength < 0) {
            throw new IllegalArgumentException("expectedLength must be non-negative, got: " + expectedLength);
        }
        if (store == null) {
            throw new IllegalArgumentException("store must not be null");
        }
        if (path == null) {
            throw new IllegalArgumentException("path must not be null");
        }
        this.store = store;
        this.path = path;
        this.expectedLength = expectedLength;
    }

    public ExternalUnavailableException copyOrOverflow(DirectReadBuffer dest, ByteBuffer chunk) {
        int remaining = chunk.remaining();
        if (remaining > expectedLength - offset) {
            return new ExternalUnavailableException(
                "{} response body exceeded expected length reading [{}]: cumulative={}, expected={}",
                store,
                path,
                (long) offset + remaining,
                expectedLength
            );
        }
        DirectByteBufferCopies.copyChunkIntoDestination(dest.buffer(), offset, chunk);
        offset += remaining;
        return null;
    }

    public int copyBounded(DirectReadBuffer dest, ByteBuffer chunk) {
        int toCopy = Math.min(chunk.remaining(), expectedLength - offset);
        if (toCopy == 0) {
            return 0;
        }
        ByteBuffer slice = chunk.slice();
        slice.limit(toCopy);
        DirectByteBufferCopies.copyChunkIntoDestination(dest.buffer(), offset, slice);
        chunk.position(chunk.position() + toCopy);
        offset += toCopy;
        return toCopy;
    }

    public ExternalUnavailableException shortReadOrNull() {
        if (offset == expectedLength) {
            return null;
        }
        return new ExternalUnavailableException(
            "{} response body shorter than expected reading [{}]: received={}, expected={}",
            store,
            path,
            offset,
            expectedLength
        );
    }

    public int offset() {
        return offset;
    }
}
