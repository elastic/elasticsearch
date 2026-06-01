/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.s3;

import software.amazon.awssdk.core.SdkResponse;
import software.amazon.awssdk.core.async.AsyncResponseTransformer;
import software.amazon.awssdk.core.async.SdkPublisher;

import org.elasticsearch.xpack.esql.datasources.DirectByteBufferCopies;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;

/**
 * An {@link AsyncResponseTransformer} that accumulates the response body into a single, pre-sized
 * direct {@link ByteBuffer}, eliminating the redundant per-chunk allocations and copies performed
 * by the SDK's default {@link AsyncResponseTransformer#toBytes()} and avoiding a subsequent
 * heap-to-direct promotion in {@code S3StorageObject.readBytesAsync}.
 *
 * <p>The default {@code toBytes()} uses {@code ByteArrayAsyncResponseTransformer$BaosSubscriber},
 * which on every {@code onNext(ByteBuffer)} call:
 * <ol>
 *   <li>Allocates a fresh {@code byte[]} via {@code BinaryUtils.copyBytesFrom} (one
 *       {@code Arrays.copyOfRange}), then</li>
 *   <li>Writes that array into a {@code ByteArrayOutputStream} (a {@code System.arraycopy} into the
 *       BAOS internal buffer, with periodic doubling reallocations), and finally</li>
 *   <li>On {@code onComplete}, calls {@code BAOS.toByteArray()} which does another
 *       {@code Arrays.copyOf} to trim to size.</li>
 * </ol>
 * For a typical Parquet column-chunk fetch this materializes every byte three times after the
 * SDK's Netty pipeline has already copied it once into a heap {@code ByteBuffer}. On read-heavy
 * S3 workloads the cumulative cost shows up as ~10% of CPU and a corresponding amount of
 * young-gen pressure.
 *
 * <p>This transformer takes the expected payload length up front (which we always know for
 * range-read requests, and which the S3 service confirms in {@code Content-Length}), allocates
 * one direct destination buffer of exactly that size in {@link #prepare()}, and copies each
 * {@code onNext(ByteBuffer)} chunk directly into the destination at the running offset. That
 * collapses three SDK-internal copies into a single chunk-to-direct-buffer copy.
 *
 * <p><b>Synchronization:</b> the {@link AsyncResponseTransformer} contract guarantees that no two
 * methods on this class or on its {@link Subscriber} are invoked concurrently, so no locking is
 * required.
 *
 * <p><b>Retries:</b> the SDK calls {@link #prepare()} again on each retry, so a fresh destination
 * buffer is allocated for every attempt. Stale state from a previous attempt is not reused.
 *
 * @param <R> the unmarshalled SDK response type (e.g. {@code GetObjectResponse}).
 */
final class KnownLengthAsyncResponseTransformer<R extends SdkResponse> implements AsyncResponseTransformer<R, ByteBuffer> {

    private final int expectedLength;

    private volatile R response;
    private volatile CompletableFuture<ByteBuffer> resultFuture;

    KnownLengthAsyncResponseTransformer(int expectedLength) {
        if (expectedLength < 0) {
            throw new IllegalArgumentException("expectedLength must be non-negative, got: " + expectedLength);
        }
        this.expectedLength = expectedLength;
    }

    /**
     * Returns the unmarshalled SDK response, or {@code null} if {@link #onResponse(SdkResponse)}
     * has not yet been invoked by the SDK.
     * <p>
     * Safe to call only after the future returned by {@link #prepare()} has completed: the
     * {@link AsyncResponseTransformer} contract requires the SDK to invoke {@code onResponse}
     * before {@code onStream}, and the subscriber's terminal callback (which completes the
     * future) happens-after {@code onResponse}. Reading this field before the future completes
     * may return {@code null} or stale state, and on a failure path the SDK may skip
     * {@code onResponse} entirely — callers must null-check.
     */
    R response() {
        return response;
    }

    @Override
    public CompletableFuture<ByteBuffer> prepare() {
        // Allocated lazily here (not in the constructor) because prepare() is invoked again on each
        // retry; the previous attempt's buffer, if any, must be discarded.
        CompletableFuture<ByteBuffer> bufferFuture = new CompletableFuture<>();
        this.resultFuture = bufferFuture;
        return bufferFuture;
    }

    @Override
    public void onResponse(R response) {
        this.response = response;
    }

    @Override
    public void onStream(SdkPublisher<ByteBuffer> publisher) {
        publisher.subscribe(new ChunkCopyingSubscriber(resultFuture, expectedLength));
    }

    @Override
    public void exceptionOccurred(Throwable error) {
        CompletableFuture<ByteBuffer> f = resultFuture;
        if (f != null) {
            f.completeExceptionally(error);
        }
    }

    /**
     * Copies each incoming {@link ByteBuffer} chunk directly into a pre-sized direct destination,
     * tracking the running offset. Fails fast if the cumulative size of received chunks would
     * exceed the expected length (a mismatch between the requested range and the server's
     * response body) or falls short of it on completion.
     */
    private static final class ChunkCopyingSubscriber implements Subscriber<ByteBuffer> {
        private final CompletableFuture<ByteBuffer> resultFuture;
        private final int expectedLength;
        // Cross-callback fields are volatile as defense-in-depth. The Reactive Streams contract
        // guarantees serial signals with happens-before, but making the visibility explicit avoids
        // depending on each publisher implementation honoring that subtlety correctly.
        private volatile ByteBuffer destination;
        private int offset;
        private volatile Subscription subscription;
        private volatile boolean failed;

        ChunkCopyingSubscriber(CompletableFuture<ByteBuffer> resultFuture, int expectedLength) {
            this.resultFuture = resultFuture;
            this.expectedLength = expectedLength;
        }

        @Override
        public void onSubscribe(Subscription s) {
            // Reactive Streams §2.5: cancel additional subscriptions on a single Subscriber.
            if (this.subscription != null) {
                s.cancel();
                return;
            }
            this.subscription = s;
            // Allocate here (rather than the constructor) so a direct-memory OOM is routed through
            // the result future instead of escaping publisher.subscribe(...) as an Error.
            try {
                this.destination = ByteBuffer.allocateDirect(expectedLength);
            } catch (OutOfMemoryError e) {
                failed = true;
                s.cancel();
                resultFuture.completeExceptionally(new IOException("failed to allocate " + expectedLength + " bytes of direct memory", e));
                return;
            }
            s.request(Long.MAX_VALUE);
        }

        @Override
        public void onNext(ByteBuffer chunk) {
            if (failed) {
                return;
            }
            int remaining = chunk.remaining();
            // Overflow-safe form of `offset + remaining > destination.capacity()`. `offset` is always
            // in [0, destination.capacity()] thanks to this same guard on prior iterations, so the
            // subtraction never underflows.
            if (remaining > destination.capacity() - offset) {
                failed = true;
                IOException error = new IOException(
                    "S3 response body exceeded expected length: cumulative="
                        + ((long) offset + remaining)
                        + ", expected="
                        + destination.capacity()
                );
                subscription.cancel();
                resultFuture.completeExceptionally(error);
                return;
            }
            DirectByteBufferCopies.copyChunkIntoDestination(destination, offset, chunk);
            offset += remaining;
        }

        @Override
        public void onError(Throwable error) {
            if (failed) {
                return;
            }
            failed = true;
            resultFuture.completeExceptionally(error);
        }

        @Override
        public void onComplete() {
            if (failed) {
                return;
            }
            if (offset != destination.capacity()) {
                resultFuture.completeExceptionally(
                    new IOException("S3 response body shorter than expected: received=" + offset + ", expected=" + destination.capacity())
                );
                return;
            }
            destination.position(0).limit(offset);
            resultFuture.complete(destination);
        }
    }

}
