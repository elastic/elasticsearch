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
        // Cross-callback fields are volatile as defense-in-depth. The Reactive Streams contract
        // guarantees serial signals with happens-before, but making the visibility explicit avoids
        // depending on each publisher implementation honoring that subtlety correctly.
        //
        // Volatile is sufficient here, unlike the destinationLock that KnownLengthAsyncResponseTransformer
        // needs on the S3 path: every signal that can release this destination is a Flow.Subscriber
        // callback, and those are serialized by the publisher. There is no out-of-band failure
        // callback, and cancelling the body future never re-enters the subscriber. Do not "restore
        // symmetry" by adding a lock that would guard against nothing.
        private volatile DirectReadBuffer destinationBuf;
        private int offset;
        private volatile Flow.Subscription subscription;
        private volatile boolean failed;

        FixedLengthDirectSubscriber(int expectedLength, DirectBufferFactory factory) {
            if (expectedLength < 0) {
                throw new IllegalArgumentException("expectedLength must be non-negative, got: " + expectedLength);
            }
            this.expectedLength = expectedLength;
            this.factory = factory;
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
                failed = true;
                subscription.cancel();
                body.completeExceptionally(e);
                return;
            }
            if (allocated == null) {
                return;
            }
            this.destinationBuf = allocated;
            if (body.isDone() || failed) {
                releaseOnFailure();
                subscription.cancel();
                return;
            }
            try {
                subscription.request(Long.MAX_VALUE);
            } catch (RuntimeException e) {
                failed = true;
                releaseOnFailure();
                body.completeExceptionally(e);
            }
        }

        @Override
        public void onNext(List<ByteBuffer> items) {
            for (ByteBuffer chunk : items) {
                DirectReadBuffer drb = destinationBuf;
                if (drb == null || failed) {
                    return;
                }
                int remaining = chunk.remaining();
                if (remaining > expectedLength - offset) {
                    fail(
                        new IOException(
                            "HTTP response body exceeded expected length: cumulative="
                                + ((long) offset + remaining)
                                + ", expected="
                                + expectedLength
                        )
                    );
                    return;
                }
                DirectByteBufferCopies.copyChunkIntoDestination(drb.buffer(), offset, chunk);
                offset += remaining;
            }
        }

        @Override
        public void onError(Throwable throwable) {
            if (failed) {
                return;
            }
            failed = true;
            releaseOnFailure();
            body.completeExceptionally(throwable);
        }

        @Override
        public void onComplete() {
            if (failed) {
                return;
            }
            if (offset != expectedLength) {
                failed = true;
                releaseOnFailure();
                body.completeExceptionally(
                    new IOException("HTTP response body shorter than expected: received=" + offset + ", expected=" + expectedLength)
                );
                return;
            }
            DirectReadBuffer transferred = destinationBuf;
            destinationBuf = null;
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

        private void fail(IOException error) {
            failed = true;
            subscription.cancel();
            releaseOnFailure();
            body.completeExceptionally(error);
        }

        private void releaseOnFailure() {
            DirectReadBuffer drb = destinationBuf;
            if (drb != null) {
                destinationBuf = null;
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
        // See FixedLengthDirectSubscriber for the volatility rationale and for why no lock is needed.
        private volatile DirectReadBuffer destinationBuf;
        private long skipRemaining;
        private int fillOffset;
        private volatile Flow.Subscription subscription;
        private volatile boolean failed;

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
                failed = true;
                subscription.cancel();
                body.completeExceptionally(e);
                return;
            }
            if (allocated == null) {
                return;
            }
            this.destinationBuf = allocated;
            if (body.isDone() || failed) {
                releaseOnFailure();
                subscription.cancel();
                return;
            }
            try {
                subscription.request(Long.MAX_VALUE);
            } catch (RuntimeException e) {
                failed = true;
                releaseOnFailure();
                body.completeExceptionally(e);
            }
        }

        @Override
        public void onNext(List<ByteBuffer> items) {
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

        @Override
        public void onError(Throwable throwable) {
            if (failed) {
                return;
            }
            failed = true;
            releaseOnFailure();
            body.completeExceptionally(throwable);
        }

        @Override
        public void onComplete() {
            if (failed) {
                return;
            }
            if (skipRemaining > 0) {
                failed = true;
                releaseOnFailure();
                body.completeExceptionally(new IOException("Position " + skip + " is beyond content length for HTTP response body"));
                return;
            }
            // Strict contract: a range read must deliver exactly {@code length} bytes after the skip.
            // Matches FixedLengthDirectSubscriber (206 path) and KnownLengthAsyncResponseTransformer (S3).
            // Downstream consumers like CoalescedRangeReader trust the requested length when slicing,
            // so returning a short buffer here would surface as an IllegalArgumentException at slice time.
            if (fillOffset != length) {
                failed = true;
                releaseOnFailure();
                body.completeExceptionally(
                    new IOException("HTTP response body shorter than expected: received=" + fillOffset + ", expected=" + length)
                );
                return;
            }
            DirectReadBuffer transferred = destinationBuf;
            destinationBuf = null;
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

        private void releaseOnFailure() {
            DirectReadBuffer drb = destinationBuf;
            if (drb != null) {
                destinationBuf = null;
                drb.close();
            }
        }
    }

    /**
     * Allocates a writable destination only if {@code body} is still open. Returns {@code null}
     * when the body was already cancelled or completed, after closing any unused allocation.
     */
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
