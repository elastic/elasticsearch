/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.http;

import org.apache.http.HttpStatus;
import org.elasticsearch.xpack.esql.datasources.DirectByteBufferCopies;

import java.io.IOException;
import java.net.http.HttpResponse;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Flow;

/**
 * {@link HttpResponse.BodyHandler} implementations that accumulate HTTP response bodies directly
 * into a pre-allocated direct {@link ByteBuffer}, avoiding {@code BodyHandlers.ofByteArray()}.
 */
final class DirectByteBufferBodyHandlers {

    private DirectByteBufferBodyHandlers() {}

    /**
     * Returns a body handler for range reads. When the server responds with {@code 206 Partial Content},
     * the body is accumulated into a direct buffer of {@code length}. When the server ignores the
     * {@code Range} header and responds with {@code 200 OK}, the first {@code skip} bytes are
     * discarded and the next {@code length} bytes are accumulated into a direct buffer.
     */
    static HttpResponse.BodyHandler<ByteBuffer> ofRangeRead(long skip, int length) {
        return responseInfo -> {
            int status = responseInfo.statusCode();
            if (status == HttpStatus.SC_PARTIAL_CONTENT) {
                return new FixedLengthDirectSubscriber(length);
            } else if (status == HttpStatus.SC_OK) {
                return new SkipThenFillDirectSubscriber(skip, length);
            } else {
                return new DiscardingSubscriber();
            }
        };
    }

    /**
     * Accumulates exactly {@code expectedLength} bytes into a direct buffer. Used for {@code 206} responses.
     */
    static final class FixedLengthDirectSubscriber implements HttpResponse.BodySubscriber<ByteBuffer> {
        private final int expectedLength;
        private final CompletableFuture<ByteBuffer> body = new CompletableFuture<>();
        // Cross-callback fields are volatile as defense-in-depth. The Reactive Streams contract
        // guarantees serial signals with happens-before, but making the visibility explicit avoids
        // depending on each publisher implementation honoring that subtlety correctly.
        private volatile ByteBuffer destination;
        private int offset;
        private volatile Flow.Subscription subscription;
        private volatile boolean failed;

        FixedLengthDirectSubscriber(int expectedLength) {
            if (expectedLength < 0) {
                throw new IllegalArgumentException("expectedLength must be non-negative, got: " + expectedLength);
            }
            this.expectedLength = expectedLength;
        }

        @Override
        public void onSubscribe(Flow.Subscription subscription) {
            if (this.subscription != null) {
                subscription.cancel();
                return;
            }
            this.subscription = subscription;
            try {
                this.destination = ByteBuffer.allocateDirect(expectedLength);
            } catch (OutOfMemoryError e) {
                failed = true;
                subscription.cancel();
                body.completeExceptionally(new IOException("failed to allocate " + expectedLength + " bytes of direct memory", e));
                return;
            }
            subscription.request(Long.MAX_VALUE);
        }

        @Override
        public void onNext(List<ByteBuffer> items) {
            if (failed) {
                return;
            }
            for (ByteBuffer chunk : items) {
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
                DirectByteBufferCopies.copyChunkIntoDestination(destination, offset, chunk);
                offset += remaining;
            }
        }

        @Override
        public void onError(Throwable throwable) {
            if (failed) {
                return;
            }
            failed = true;
            body.completeExceptionally(throwable);
        }

        @Override
        public void onComplete() {
            if (failed) {
                return;
            }
            if (offset != expectedLength) {
                body.completeExceptionally(
                    new IOException("HTTP response body shorter than expected: received=" + offset + ", expected=" + expectedLength)
                );
                return;
            }
            destination.position(0).limit(offset);
            body.complete(destination);
        }

        @Override
        public CompletableFuture<ByteBuffer> getBody() {
            return body;
        }

        private void fail(IOException error) {
            failed = true;
            subscription.cancel();
            body.completeExceptionally(error);
        }
    }

    /**
     * Skips {@code skip} bytes then accumulates up to {@code length} bytes into a direct buffer.
     * Used when a server ignores {@code Range} and returns the full body with {@code 200 OK}.
     */
    static final class SkipThenFillDirectSubscriber implements HttpResponse.BodySubscriber<ByteBuffer> {
        private final long skip;
        private final int length;
        private final CompletableFuture<ByteBuffer> body = new CompletableFuture<>();
        // See FixedLengthDirectSubscriber for the volatility rationale.
        private volatile ByteBuffer destination;
        private long skipRemaining;
        private int fillOffset;
        private volatile Flow.Subscription subscription;
        private volatile boolean failed;

        SkipThenFillDirectSubscriber(long skip, int length) {
            if (skip < 0) {
                throw new IllegalArgumentException("skip must be non-negative, got: " + skip);
            }
            if (length < 0) {
                throw new IllegalArgumentException("length must be non-negative, got: " + length);
            }
            this.skip = skip;
            this.length = length;
            this.skipRemaining = skip;
        }

        @Override
        public void onSubscribe(Flow.Subscription subscription) {
            if (this.subscription != null) {
                subscription.cancel();
                return;
            }
            this.subscription = subscription;
            try {
                this.destination = ByteBuffer.allocateDirect(length);
            } catch (OutOfMemoryError e) {
                failed = true;
                subscription.cancel();
                body.completeExceptionally(new IOException("failed to allocate " + length + " bytes of direct memory", e));
                return;
            }
            subscription.request(Long.MAX_VALUE);
        }

        @Override
        public void onNext(List<ByteBuffer> items) {
            if (failed) {
                return;
            }
            for (ByteBuffer chunk : items) {
                if (skipRemaining > 0) {
                    long toSkip = Math.min(skipRemaining, chunk.remaining());
                    chunk.position(chunk.position() + (int) toSkip);
                    skipRemaining -= toSkip;
                }
                if (fillOffset < length && chunk.hasRemaining()) {
                    int toCopy = Math.min(chunk.remaining(), length - fillOffset);
                    ByteBuffer slice = chunk.slice();
                    slice.limit(toCopy);
                    DirectByteBufferCopies.copyChunkIntoDestination(destination, fillOffset, slice);
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
            body.completeExceptionally(throwable);
        }

        @Override
        public void onComplete() {
            if (failed) {
                return;
            }
            if (skipRemaining > 0) {
                body.completeExceptionally(new IOException("Position " + skip + " is beyond content length for HTTP response body"));
                return;
            }
            // Strict contract: a range read must deliver exactly {@code length} bytes after the skip.
            // Matches FixedLengthDirectSubscriber (206 path) and KnownLengthAsyncResponseTransformer (S3).
            // Downstream consumers like CoalescedRangeReader trust the requested length when slicing,
            // so returning a short buffer here would surface as an IllegalArgumentException at slice time.
            if (fillOffset != length) {
                body.completeExceptionally(
                    new IOException("HTTP response body shorter than expected: received=" + fillOffset + ", expected=" + length)
                );
                return;
            }
            destination.position(0).limit(fillOffset);
            body.complete(destination);
        }

        @Override
        public CompletableFuture<ByteBuffer> getBody() {
            return body;
        }
    }

    /**
     * Discards the response body for unexpected status codes.
     */
    static final class DiscardingSubscriber implements HttpResponse.BodySubscriber<ByteBuffer> {
        private final CompletableFuture<ByteBuffer> body = new CompletableFuture<>();

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
            body.complete(ByteBuffer.allocateDirect(0));
        }

        @Override
        public CompletableFuture<ByteBuffer> getBody() {
            return body;
        }
    }

}
