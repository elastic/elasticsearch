/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.reindex.remote;

import org.apache.http.HttpEntity;
import org.apache.http.entity.ContentType;
import org.elasticsearch.client.HeapBufferedAsyncResponseConsumer;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;

import java.io.IOException;
import java.util.Objects;

/**
 * A {@link HeapBufferedAsyncResponseConsumer} that registers the raw HTTP response buffer against
 * the {@link CircuitBreaker#REQUEST} circuit breaker for the lifetime of the buffer allocation.
 *
 * <p>Apache HC's default consumer buffers the entire response body in heap before the application
 * can read it. Without accounting, multiple concurrent reindex-from-remote operations can each
 * hold up to {@code bufferLimitBytes} of raw response data simultaneously, exhausting heap before
 * any circuit breaker trips. This class fixes that gap by reserving bytes against the REQUEST
 * breaker in {@link #onEntityEnclosed} and releasing them in {@link #releaseResources} (which
 * Apache HC calls unconditionally on request completion, regardless of success or failure).
 *
 * <p>When the response uses chunked transfer encoding ({@code Content-Length} is unknown),
 * no buffer-level reservation is made. The per-hit {@link RemoteParseContext} accounting
 * that runs during parsing provides protection in that case.
 *
 * <p>This accounting is independent of the per-hit {@link RemoteParseContext} accounting: both
 * are charged to the same REQUEST breaker but at different lifecycle points (buffer allocation vs.
 * incremental parse). When the response has a known {@code Content-Length}, both reservations are
 * held simultaneously at peak — the buffer bytes while the response body is being parsed, and the
 * hit bytes for as long as the batch is live.
 */
final class BreakerAwareHeapBufferedAsyncResponseConsumer extends HeapBufferedAsyncResponseConsumer {

    /** Label used when charging the REQUEST circuit breaker for the raw HTTP response buffer. */
    static final String REMOTE_RESPONSE_BUFFER_BREAKER_LABEL = "reindex_remote_response_buffer";

    private final CircuitBreaker breaker;
    private final int bufferLimitBytes;
    // Bytes currently reserved against the breaker; reset to 0 after release.
    // Apache HC invokes onEntityEnclosed and releaseResources on the same I/O reactor thread
    // for a given request, so no explicit synchronization is needed.
    private long reservedBytes;

    BreakerAwareHeapBufferedAsyncResponseConsumer(CircuitBreaker breaker, int bufferLimitBytes) {
        super(bufferLimitBytes);
        this.breaker = Objects.requireNonNull(breaker, "breaker");
        this.bufferLimitBytes = bufferLimitBytes;
    }

    @Override
    protected void onEntityEnclosed(HttpEntity entity, ContentType contentType) throws IOException {
        long contentLength = entity.getContentLength();
        if (contentLength > bufferLimitBytes) {
            // Let super throw ContentTooLongException; nothing reserved yet.
            super.onEntityEnclosed(entity, contentType);
            return; // unreachable — super always throws when contentLength > bufferLimitBytes
        }
        if (contentLength <= 0) {
            // Chunked transfer encoding: Content-Length is unknown. Reserving the full buffer cap
            // (100MB) as a worst case would break concurrent operations that each have a response
            // in flight but whose actual response is much smaller. Skip buffer-level accounting for
            // chunked responses; the per-hit RemoteParseContext accounting provides protection.
            super.onEntityEnclosed(entity, contentType);
            return;
        }
        try {
            breaker.addEstimateBytesAndMaybeBreak(contentLength, REMOTE_RESPONSE_BUFFER_BREAKER_LABEL);
        } catch (CircuitBreakingException cbe) {
            // Apache HC routes IOException from onEntityEnclosed to ResponseListener.onFailure.
            throw new IOException(cbe);
        }
        reservedBytes = contentLength;
        try {
            super.onEntityEnclosed(entity, contentType);
        } catch (IOException | RuntimeException e) {
            // super failed after we reserved; refund now so releaseResources is idempotent.
            breaker.addWithoutBreaking(-reservedBytes);
            reservedBytes = 0;
            throw e;
        }
    }

    @Override
    protected void releaseResources() {
        try {
            if (reservedBytes > 0) {
                breaker.addWithoutBreaking(-reservedBytes);
                reservedBytes = 0;
            }
        } finally {
            super.releaseResources();
        }
    }

    /** Visible for testing. */
    long currentReservation() {
        return reservedBytes;
    }
}
