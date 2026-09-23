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
 * owns those messages — overflow, short fill, and skip-past-EOF — so the callers cannot drift.
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

    /**
     * @param store first token of mismatch messages ({@code "HTTP"} or {@code "S3"})
     * @param path named in every mismatch message
     * @param expectedLength fill-window size in bytes
     */
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

    /**
     * Copies {@code chunk} in full, or returns overflow without touching the destination when
     * extra bytes would pass the window. Does not close {@code dest}.
     */
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

    /**
     * Copies at most the remaining window. Extra bytes stay in {@code chunk} and are never
     * overflow — a 200 that ignored {@code Range} still sends the rest of the object. Does not
     * close {@code dest}.
     */
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

    /**
     * Short-window failure when the body ended before {@code expectedLength}. Distinct from
     * {@link #beyondContentLength(long)}: skip finished, fill did not.
     */
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

    /**
     * Skip landed past EOF. Caller still owns skip accounting; this only types the message so
     * it stays next to the other mismatch EUEs.
     */
    public ExternalUnavailableException beyondContentLength(long skip) {
        return new ExternalUnavailableException("Position {} is beyond content length reading [{}]", skip, path);
    }

    /** Bytes copied so far. Callers set {@code dest.buffer().position(0).limit(offset())} after a successful fill. */
    public int offset() {
        return offset;
    }
}
