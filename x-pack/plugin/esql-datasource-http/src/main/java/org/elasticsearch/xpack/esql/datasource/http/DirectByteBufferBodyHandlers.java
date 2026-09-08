/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.http;

import org.apache.http.HttpStatus;
import org.elasticsearch.xpack.esql.datasources.DirectByteBufferCopies;
import org.elasticsearch.xpack.esql.datasources.spi.DirectBufferFactory;
import org.elasticsearch.xpack.esql.datasources.spi.DirectReadBuffer;

import java.io.IOException;
import java.net.http.HttpResponse;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Flow;

/**
 * {@link HttpResponse.BodyHandler} implementations that accumulate HTTP response bodies into a
 * pre-allocated destination {@link ByteBuffer}, avoiding {@code BodyHandlers.ofByteArray()}.
 *
 * <p>Destination buffers are obtained from a caller-supplied {@link DirectBufferFactory} so the
 * allocation is breaker-accounted. On success the {@link DirectReadBuffer} is handed to the
 * caller; the caller releases it when the bytes have been consumed. On failure paths the
 * subscriber releases the buffer itself so the charge does not outlive the failed request.
 */
final class DirectByteBufferBodyHandlers {

    private DirectByteBufferBodyHandlers() {}

    /**
     * Returns a body handler for range reads. When the server responds with {@code 206 Partial Content},
     * the body is accumulated into a buffer of {@code length}. When the server ignores the
     * {@code Range} header and responds with {@code 200 OK}, the first {@code skip} bytes are
     * discarded and the next {@code length} bytes are accumulated into a destination buffer.
     *
     * @param factory factory used to produce the destination buffer on the 200/206 paths
     */
    static HttpResponse.BodyHandler<DirectReadBuffer> ofRangeRead(long skip, int length, DirectBufferFactory factory) {
        return responseInfo -> {
            int status = responseInfo.statusCode();
            if (status == HttpStatus.SC_PARTIAL_CONTENT) {
                return new FixedLengthDirectSubscriber(length, factory);
            } else if (status == HttpStatus.SC_OK) {
                return new SkipThenFillDirectSubscriber(skip, length, factory);
            } else {
                return new DiscardingSubscriber();
            }
        };
    }

    /**
     * Accumulates exactly {@code expectedLength} bytes into a destination buffer. Used for {@code 206} responses.
     */
    static final class FixedLengthDirectSubscriber implements HttpResponse.BodySubscriber<DirectReadBuffer> {
        private final int expectedLength;
        private final DirectBufferFactory factory;
        private final CompletableFuture<DirectReadBuffer> body = new CompletableFuture<>();
        // Subscriber signals are serialized, but cancellation of body can arrive from another
        // thread and must not close the destination while onNext is copying into it.
        private final Object destinationLock = new Object();
        private DirectReadBuffer destinationBuf;
        private int offset;
        private volatile Flow.Subscription subscription;
        private boolean failed;

        FixedLengthDirectSubscriber(int expectedLength, DirectBufferFactory factory) {
            if (expectedLength < 0) {
                throw new IllegalArgumentException("expectedLength must be non-negative, got: " + expectedLength);
            }
            this.expectedLength = expectedLength;
            this.factory = factory;
            body.whenComplete((ignored, error) -> {
                if (body.isCancelled()) {
                    releaseOnFailure();
                    Flow.Subscription current = subscription;
                    if (current != null) {
                        current.cancel();
                    }
                }
            });
        }

        @Override
        public void onSubscribe(Flow.Subscription subscription) {
            if (this.subscription != null) {
                subscription.cancel();
                return;
            }
            this.subscription = subscription;
            DirectReadBuffer allocated;
            try {
                allocated = allocateIfBodyOpen(factory, expectedLength, body, subscription);
            } catch (Exception e) {
                fail(e, true);
                return;
            }
            if (allocated == null) {
                return;
            }
            boolean published;
            synchronized (destinationLock) {
                if (body.isDone() || failed) {
                    published = false;
                } else {
                    destinationBuf = allocated;
                    published = true;
                }
            }
            if (published == false) {
                allocated.close();
                subscription.cancel();
                return;
            }
            if (body.isDone()) {
                releaseOnFailure();
                subscription.cancel();
                return;
            }
            try {
                subscription.request(Long.MAX_VALUE);
            } catch (RuntimeException e) {
                fail(e, true);
            }
        }

        @Override
        public void onNext(List<ByteBuffer> items) {
            IOException overflow = null;
            synchronized (destinationLock) {
                for (ByteBuffer chunk : items) {
                    DirectReadBuffer drb = destinationBuf;
                    if (drb == null || failed) {
                        return;
                    }
                    int remaining = chunk.remaining();
                    if (remaining > expectedLength - offset) {
                        overflow = new IOException(
                            "HTTP response body exceeded expected length: cumulative="
                                + ((long) offset + remaining)
                                + ", expected="
                                + expectedLength
                        );
                        break;
                    }
                    DirectByteBufferCopies.copyChunkIntoDestination(drb.buffer(), offset, chunk);
                    offset += remaining;
                }
            }
            if (overflow != null) {
                fail(overflow, true);
            }
        }

        @Override
        public void onError(Throwable throwable) {
            fail(throwable, false);
        }

        @Override
        public void onComplete() {
            DirectReadBuffer transferred;
            IOException shortRead;
            synchronized (destinationLock) {
                if (failed) {
                    return;
                }
                if (offset != expectedLength) {
                    transferred = null;
                    shortRead = new IOException(
                        "HTTP response body shorter than expected: received=" + offset + ", expected=" + expectedLength
                    );
                } else {
                    transferred = destinationBuf;
                    destinationBuf = null;
                    shortRead = null;
                }
            }
            if (shortRead != null) {
                fail(shortRead, false);
                return;
            }
            if (transferred == null) {
                return;
            }
            transferred.buffer().position(0).limit(offset);
            if (body.complete(transferred) == false) {
                transferred.close();
            }
        }

        @Override
        public CompletableFuture<DirectReadBuffer> getBody() {
            return body;
        }

        private void fail(Throwable error, boolean cancelSubscription) {
            DirectReadBuffer drb;
            synchronized (destinationLock) {
                if (failed) {
                    return;
                }
                failed = true;
                drb = destinationBuf;
                destinationBuf = null;
            }
            if (cancelSubscription) {
                Flow.Subscription current = subscription;
                if (current != null) {
                    current.cancel();
                }
            }
            if (drb != null) {
                drb.close();
            }
            body.completeExceptionally(error);
        }

        private void releaseOnFailure() {
            DirectReadBuffer drb;
            synchronized (destinationLock) {
                drb = destinationBuf;
                destinationBuf = null;
            }
            if (drb != null) {
                drb.close();
            }
        }
    }

    /**
     * Skips {@code skip} bytes then accumulates up to {@code length} bytes into a destination buffer.
     * Used when a server ignores {@code Range} and returns the full body with {@code 200 OK}.
     */
    static final class SkipThenFillDirectSubscriber implements HttpResponse.BodySubscriber<DirectReadBuffer> {
        private final long skip;
        private final int length;
        private final DirectBufferFactory factory;
        private final CompletableFuture<DirectReadBuffer> body = new CompletableFuture<>();
        private final Object destinationLock = new Object();
        private DirectReadBuffer destinationBuf;
        private long skipRemaining;
        private int fillOffset;
        private volatile Flow.Subscription subscription;
        private boolean failed;

        SkipThenFillDirectSubscriber(long skip, int length, DirectBufferFactory factory) {
            if (skip < 0) {
                throw new IllegalArgumentException("skip must be non-negative, got: " + skip);
            }
            if (length < 0) {
                throw new IllegalArgumentException("length must be non-negative, got: " + length);
            }
            this.skip = skip;
            this.length = length;
            this.skipRemaining = skip;
            this.factory = factory;
            body.whenComplete((ignored, error) -> {
                if (body.isCancelled()) {
                    releaseOnFailure();
                    Flow.Subscription current = subscription;
                    if (current != null) {
                        current.cancel();
                    }
                }
            });
        }

        @Override
        public void onSubscribe(Flow.Subscription subscription) {
            if (this.subscription != null) {
                subscription.cancel();
                return;
            }
            this.subscription = subscription;
            DirectReadBuffer allocated;
            try {
                allocated = allocateIfBodyOpen(factory, length, body, subscription);
            } catch (Exception e) {
                subscription.cancel();
                fail(e);
                return;
            }
            if (allocated == null) {
                return;
            }
            boolean published;
            synchronized (destinationLock) {
                if (body.isDone() || failed) {
                    published = false;
                } else {
                    destinationBuf = allocated;
                    published = true;
                }
            }
            if (published == false) {
                allocated.close();
                subscription.cancel();
                return;
            }
            if (body.isDone()) {
                releaseOnFailure();
                subscription.cancel();
                return;
            }
            try {
                subscription.request(Long.MAX_VALUE);
            } catch (RuntimeException e) {
                fail(e);
            }
        }

        @Override
        public void onNext(List<ByteBuffer> items) {
            synchronized (destinationLock) {
                for (ByteBuffer chunk : items) {
                    DirectReadBuffer drb = destinationBuf;
                    if (drb == null || failed) {
                        return;
                    }
                    if (skipRemaining > 0) {
                        long toSkip = Math.min(skipRemaining, chunk.remaining());
                        chunk.position(chunk.position() + (int) toSkip);
                        skipRemaining -= toSkip;
                    }
                    if (fillOffset < length && chunk.hasRemaining()) {
                        int toCopy = Math.min(chunk.remaining(), length - fillOffset);
                        ByteBuffer slice = chunk.slice();
                        slice.limit(toCopy);
                        DirectByteBufferCopies.copyChunkIntoDestination(drb.buffer(), fillOffset, slice);
                        chunk.position(chunk.position() + toCopy);
                        fillOffset += toCopy;
                    }
                }
            }
        }

        @Override
        public void onError(Throwable throwable) {
            fail(throwable);
        }

        @Override
        public void onComplete() {
            DirectReadBuffer transferred;
            IOException readFailure;
            synchronized (destinationLock) {
                if (failed) {
                    return;
                }
                if (skipRemaining > 0) {
                    transferred = null;
                    readFailure = new IOException("Position " + skip + " is beyond content length for HTTP response body");
                } else if (fillOffset != length) {
                    // Downstream consumers trust the requested length when slicing the returned buffer.
                    transferred = null;
                    readFailure = new IOException(
                        "HTTP response body shorter than expected: received=" + fillOffset + ", expected=" + length
                    );
                } else {
                    transferred = destinationBuf;
                    destinationBuf = null;
                    readFailure = null;
                }
            }
            if (readFailure != null) {
                fail(readFailure);
                return;
            }
            if (transferred == null) {
                return;
            }
            transferred.buffer().position(0).limit(fillOffset);
            if (body.complete(transferred) == false) {
                transferred.close();
            }
        }

        @Override
        public CompletableFuture<DirectReadBuffer> getBody() {
            return body;
        }

        private void fail(Throwable error) {
            DirectReadBuffer drb;
            synchronized (destinationLock) {
                if (failed) {
                    return;
                }
                failed = true;
                drb = destinationBuf;
                destinationBuf = null;
            }
            if (drb != null) {
                drb.close();
            }
            body.completeExceptionally(error);
        }

        private void releaseOnFailure() {
            DirectReadBuffer drb;
            synchronized (destinationLock) {
                drb = destinationBuf;
                destinationBuf = null;
            }
            if (drb != null) {
                drb.close();
            }
        }
    }

    private static DirectReadBuffer allocateIfBodyOpen(
        DirectBufferFactory factory,
        int length,
        CompletableFuture<DirectReadBuffer> body,
        Flow.Subscription subscription
    ) throws IOException {
        if (body.isDone()) {
            subscription.cancel();
            return null;
        }
        DirectReadBuffer allocated = factory.allocateWritableWindow(length);
        if (body.isDone()) {
            allocated.close();
            subscription.cancel();
            return null;
        }
        return allocated;
    }

    /**
     * Discards the response body for unexpected status codes. Returns a fresh empty heap buffer per
     * response so the caller can close it independently.
     */
    static final class DiscardingSubscriber implements HttpResponse.BodySubscriber<DirectReadBuffer> {
        private final CompletableFuture<DirectReadBuffer> body = new CompletableFuture<>();

        @Override
        public void onSubscribe(Flow.Subscription subscription) {
            subscription.request(Long.MAX_VALUE);
        }

        @Override
        public void onNext(List<ByteBuffer> items) {}

        @Override
        public void onError(Throwable throwable) {
            body.completeExceptionally(throwable);
        }

        @Override
        public void onComplete() {
            body.complete(new DirectReadBuffer(ByteBuffer.allocate(0), () -> {}));
        }

        @Override
        public CompletableFuture<DirectReadBuffer> getBody() {
            return body;
        }
    }

}
