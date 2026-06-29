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
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.core.Releasable;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Heap-buffered async response consumer that charges the raw Apache HTTP response buffer to the
 * {@link CircuitBreaker#REQUEST} circuit breaker.
 *
 * <p>The low-level REST client buffers each async response in a {@link SimpleInputBuffer} before
 * invoking the application callback. For remote reindex search responses that means heap can be
 * exhausted before {@link RemoteParseContext} sees any bytes. This consumer accounts the actual
 * {@link ByteBuffer} allocations used by that Apache buffer, including growth for responses without
 * a {@code Content-Length} header.
 *
 * <p>On a successful response, Apache calls {@link #releaseResources()} before the reindex callback
 * reads the returned entity. For that reason the breaker reservation is attached to the response
 * entity and must be released after the entity content has been consumed. Failed or cancelled
 * requests release directly from {@link #releaseResources()}.
 */
final class BreakerAwareHeapBufferedAsyncResponseConsumer extends AbstractAsyncResponseConsumer<HttpResponse> {

    /** Label used when charging the REQUEST breaker for the raw HTTP response buffer. */
    static final String REMOTE_RESPONSE_BUFFER_BREAKER_LABEL = "reindex_remote_response_buffer";

    private final int bufferLimitBytes;
    private final AccountingByteBufferAllocator allocator;

    private volatile HttpResponse response;
    private volatile ContentInputBuffer contentBuffer;
    private volatile boolean responseDelivered;

    BreakerAwareHeapBufferedAsyncResponseConsumer(CircuitBreaker breaker, int bufferLimitBytes) {
        if (bufferLimitBytes <= 0) {
            throw new IllegalArgumentException("bufferLimitBytes must be > 0, was " + bufferLimitBytes);
        }
        this.bufferLimitBytes = bufferLimitBytes;
        this.allocator = new AccountingByteBufferAllocator(Objects.requireNonNull(breaker, "breaker"));
    }

    int getBufferLimit() {
        return bufferLimitBytes;
    }

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
        if (len > bufferLimitBytes) {
            throw contentTooLong(len);
        }
        int initialBufferSize = len < 0 ? 4096 : Math.toIntExact(len);
        try {
            contentBuffer = new AccountingSimpleInputBuffer(initialBufferSize, bufferLimitBytes, allocator);
            this.response.setEntity(new ReleasableContentBufferEntity(new ContentBufferEntity(entity, contentBuffer), allocator));
        } catch (CircuitBreakingException cbe) {
            throw new IOException(cbe);
        }
    }

    @Override
    protected void onContentReceived(ContentDecoder decoder, IOControl ioctrl) throws IOException {
        try {
            contentBuffer.consumeContent(decoder);
        } catch (ContentTooLongRuntimeException e) {
            throw e.asIOException();
        } catch (CircuitBreakingException cbe) {
            throw new IOException(cbe);
        }
    }

    @Override
    protected HttpResponse buildResult(HttpContext context) {
        responseDelivered = true;
        return response;
    }

    @Override
    protected void releaseResources() {
        try {
            if (responseDelivered == false) {
                allocator.close();
            }
        } finally {
            response = null;
            contentBuffer = null;
        }
    }

    private ContentTooLongException contentTooLong(long contentLength) {
        return new ContentTooLongException(
            "entity content is too long [" + contentLength + "] for the configured buffer limit [" + bufferLimitBytes + "]"
        );
    }

    private final class AccountingSimpleInputBuffer extends SimpleInputBuffer {
        private final int bufferLimitBytes;
        private final AccountingByteBufferAllocator allocator;

        private AccountingSimpleInputBuffer(int bufferSize, int bufferLimitBytes, AccountingByteBufferAllocator allocator) {
            super(bufferSize, allocator);
            this.bufferLimitBytes = bufferLimitBytes;
            this.allocator = allocator;
        }

        @Override
        protected void expand() {
            int oldCapacity = buffer.capacity();
            if (oldCapacity >= bufferLimitBytes) {
                throw new ContentTooLongRuntimeException(
                    new ContentTooLongException("response buffer exceeded limit [" + bufferLimitBytes + "] bytes")
                );
            }
            long doubledCapacity = ((long) oldCapacity + 1) << 1;
            int newCapacity = Math.toIntExact(Math.min(doubledCapacity, (long) bufferLimitBytes));

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

    private static final class AccountingByteBufferAllocator implements ByteBufferAllocator, Releasable {
        private final CircuitBreaker breaker;
        private final AtomicBoolean closed = new AtomicBoolean();
        private final AtomicLong reservedBytes = new AtomicLong();

        private AccountingByteBufferAllocator(CircuitBreaker breaker) {
            this.breaker = breaker;
        }

        @Override
        public ByteBuffer allocate(int size) {
            if (size < 0) {
                throw new IllegalArgumentException("size must be >= 0, was " + size);
            }
            if (size > 0) {
                breaker.addEstimateBytesAndMaybeBreak(size, REMOTE_RESPONSE_BUFFER_BREAKER_LABEL);
                reservedBytes.addAndGet(size);
            }
            try {
                return ByteBuffer.allocate(size);
            } catch (RuntimeException | Error e) {
                release(size);
                throw e;
            }
        }

        void release(long bytes) {
            if (bytes > 0) {
                reservedBytes.addAndGet(-bytes);
                breaker.addWithoutBreaking(-bytes);
            }
        }

        long currentReservation() {
            return reservedBytes.get();
        }

        @Override
        public void close() {
            if (closed.compareAndSet(false, true)) {
                long bytes = reservedBytes.getAndSet(0L);
                if (bytes > 0) {
                    breaker.addWithoutBreaking(-bytes);
                }
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

        private ContentTooLongException asIOException() {
            return contentTooLongException;
        }
    }
}
