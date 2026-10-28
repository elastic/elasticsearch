/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.reindex.remote;

import org.apache.http.ContentTooLongException;
import org.apache.http.HttpEntity;
import org.apache.http.HttpException;
import org.apache.http.HttpResponse;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.HttpEntityWrapper;
import org.apache.http.nio.ContentDecoder;
import org.apache.http.nio.IOControl;
import org.apache.http.nio.entity.ContentBufferEntity;
import org.apache.http.nio.protocol.AbstractAsyncResponseConsumer;
import org.apache.http.nio.util.ByteBufferAllocator;
import org.apache.http.nio.util.ContentInputBuffer;
import org.apache.http.nio.util.SimpleInputBuffer;
import org.apache.http.protocol.HttpContext;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.core.Releasable;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

/// Heap-buffered async response consumer that charges the raw Apache HTTP response buffer to the
/// [CircuitBreaker#REQUEST] circuit breaker.
///
/// The low-level REST client buffers each async response in a [SimpleInputBuffer] before
/// invoking the application callback, so heap can be exhausted before the caller sees any bytes. This
/// consumer accounts the actual [ByteBuffer] allocations used by that Apache buffer, including
/// growth for responses without a `Content-Length` header. Responses are not subject to a fixed size
/// cap, whether the length is known or chunked; they grow under the control of the circuit breaker,
/// which is expected to trip before heap is exhausted, up to a hard `MAX_BUFFER_CAPACITY` ceiling that
/// only guards against overflowing the `int`-indexed buffer.
///
/// On a successful response, Apache calls [#releaseResources()] before the caller reads the
/// returned entity. For that reason the breaker reservation is attached to the response entity and
/// must be released after the entity content has been consumed. Failed or cancelled requests release
/// directly from [#releaseResources()].
final class BreakerAwareHeapBufferedAsyncResponseConsumer extends AbstractAsyncResponseConsumer<HttpResponse> {

    static final String REMOTE_RESPONSE_BUFFER_BREAKER_LABEL = "reindex_remote_response_buffer";
    private static final int INITIAL_BUFFER_SIZE = 4096;
    /**
     * Hard upper bound on the capacity we grow an unknown-length (chunked) response buffer to. This is
     * the JVM's maximum safe array size, not a policy limit — the REQUEST breaker is expected to trip
     * long before this. It exists only so growth fails cleanly with a {@link ContentTooLongException}
     * rather than overflowing the {@code int}-indexed buffer.
     */
    private static final int MAX_BUFFER_CAPACITY = Integer.MAX_VALUE - 8;

    private final AccountingByteBufferAllocator allocator;

    private volatile HttpResponse response;
    private volatile ContentInputBuffer contentBuffer;
    private volatile boolean responseDelivered;

    BreakerAwareHeapBufferedAsyncResponseConsumer(CircuitBreaker breaker) {
        this.allocator = new AccountingByteBufferAllocator(Objects.requireNonNull(breaker, "breaker"));
    }

    // Visible for testing
    long currentReservation() {
        return allocator.currentReservation();
    }

    @Override
    protected void onResponseReceived(HttpResponse httpResponse) throws HttpException, IOException {
        this.response = httpResponse;
    }

    @Override
    protected void onEntityEnclosed(HttpEntity entity, ContentType contentType) throws IOException {
        long len = entity.getContentLength();
        if (len > MAX_BUFFER_CAPACITY) {
            throw new ContentTooLongException(
                "entity content is too long [" + len + "] for the maximum buffer capacity [" + MAX_BUFFER_CAPACITY + "]"
            );
        }
        int initialBufferSize = len < 0 ? INITIAL_BUFFER_SIZE : Math.toIntExact(len);
        contentBuffer = new AccountingSimpleInputBuffer(initialBufferSize, allocator);
        this.response.setEntity(new ReleasableContentBufferEntity(new ContentBufferEntity(entity, contentBuffer), allocator));
    }

    @Override
    protected void onContentReceived(ContentDecoder decoder, IOControl ioctrl) throws IOException {
        try {
            contentBuffer.consumeContent(decoder);
        } catch (ContentTooLongRuntimeException e) {
            throw e.unwrap();
        }
    }

    @Override
    protected HttpResponse buildResult(HttpContext context) {
        responseDelivered = true;
        return response;
    }

    @Override
    protected void releaseResources() {
        // AbstractAsyncResponseConsumer calls releaseResources() after building the
        // successful HttpResponse, before the caller has consumed the response entity.
        // On that path, ownership of the buffer reservation has moved to the
        // ReleasableContentBufferEntity, which releases it when the entity content is
        // closed. If no response was delivered because the request failed or was
        // cancelled, there is no caller-owned entity, so the consumer must release the
        // reservation here.
        try {
            if (responseDelivered == false) {
                allocator.close();
            }
        } finally {
            response = null;
            contentBuffer = null;
        }
    }

    private final class AccountingSimpleInputBuffer extends SimpleInputBuffer {
        private final AccountingByteBufferAllocator allocator;

        private AccountingSimpleInputBuffer(int bufferSize, AccountingByteBufferAllocator allocator) {
            super(bufferSize, allocator);
            this.allocator = allocator;
        }

        @Override
        protected void expand() {
            // Growth is bounded by the REQUEST breaker, charged in allocator.allocate() below, up to a
            // hard MAX_BUFFER_CAPACITY ceiling. We override expand() here to ensure the
            // previous buffer's reserved bytes against the circuit breaker are released.
            int oldCapacity = buffer.capacity();
            if (oldCapacity >= MAX_BUFFER_CAPACITY) {
                throw new ContentTooLongRuntimeException(
                    new ContentTooLongException("response buffer exceeded maximum capacity [" + MAX_BUFFER_CAPACITY + "] bytes")
                );
            }
            long doubledCapacity = ((long) oldCapacity + 1) << 1;
            int newCapacity = Math.toIntExact(Math.min(doubledCapacity, MAX_BUFFER_CAPACITY));

            ByteBuffer oldBuffer = buffer;
            ByteBuffer newBuffer = allocator.allocate(newCapacity);
            boolean success = false;
            try {
                oldBuffer.flip();
                newBuffer.put(oldBuffer);
                success = true;
            } finally {
                if (success) {
                    buffer = newBuffer;
                    allocator.release(oldCapacity);
                } else {
                    allocator.release(newCapacity);
                }
            }
        }
    }

    // package-private for testing so the allocator's thread-safety contract can be unit-tested directly
    static final class AccountingByteBufferAllocator implements ByteBufferAllocator, Releasable {
        private final CircuitBreaker breaker;
        private final Object mutex = new Object();
        private boolean closed; // guarded by mutex
        private long reservedBytes; // guarded by mutex

        AccountingByteBufferAllocator(CircuitBreaker breaker) {
            this.breaker = breaker;
        }

        @Override
        public ByteBuffer allocate(int size) {
            if (size < 0) {
                throw new IllegalArgumentException("size must be >= 0, was " + size);
            }
            synchronized (mutex) {
                if (closed) {
                    throw new IllegalStateException("allocator is closed");
                }
                if (size > 0) {
                    breaker.addEstimateBytesAndMaybeBreak(size, REMOTE_RESPONSE_BUFFER_BREAKER_LABEL);
                    reservedBytes += size;
                }
                try {
                    return ByteBuffer.allocate(size);
                } catch (RuntimeException | Error e) {
                    if (size > 0) {
                        reservedBytes -= size;
                        breaker.addWithoutBreaking(-size);
                    }
                    throw e;
                }
            }
        }

        void release(long bytes) {
            if (bytes > 0) {
                synchronized (mutex) {
                    if (closed == false) {
                        reservedBytes -= bytes;
                        breaker.addWithoutBreaking(-bytes);
                    }
                }
            }
        }

        long currentReservation() {
            synchronized (mutex) {
                return reservedBytes;
            }
        }

        @Override
        public void close() {
            synchronized (mutex) {
                if (closed) {
                    return;
                }
                closed = true;
                if (reservedBytes > 0) {
                    breaker.addWithoutBreaking(-reservedBytes);
                }
                reservedBytes = 0L;
            }
        }
    }

    private static final class ReleasableContentBufferEntity extends HttpEntityWrapper implements Releasable {
        private final Releasable releasable;
        private final AtomicBoolean closed = new AtomicBoolean();

        private ReleasableContentBufferEntity(HttpEntity wrappedEntity, Releasable releasable) {
            super(wrappedEntity);
            this.releasable = releasable;
        }

        @Override
        public InputStream getContent() throws IOException {
            return new FilterInputStream(super.getContent()) {
                @Override
                public void close() throws IOException {
                    try {
                        super.close();
                    } finally {
                        ReleasableContentBufferEntity.this.close();
                    }
                }
            };
        }

        @Override
        public void writeTo(OutputStream outStream) throws IOException {
            try {
                super.writeTo(outStream);
            } finally {
                close();
            }
        }

        @Override
        public void close() {
            if (closed.compareAndSet(false, true)) {
                releasable.close();
            }
        }
    }

    private static final class ContentTooLongRuntimeException extends RuntimeException {
        private final ContentTooLongException contentTooLongException;

        private ContentTooLongRuntimeException(ContentTooLongException cause) {
            super(cause);
            this.contentTooLongException = cause;
        }

        private ContentTooLongException unwrap() {
            return contentTooLongException;
        }
    }
}
