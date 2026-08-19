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
import org.elasticsearch.xpack.esql.datasources.spi.DirectBufferFactory;
import org.elasticsearch.xpack.esql.datasources.spi.DirectReadBuffer;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;

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
 * <p><b>Synchronization:</b> Reactive Streams serializes the {@link Subscriber}'s own signals, but
 * {@link #exceptionOccurred} is a transformer-level callback outside that ordering and can race the
 * terminal subscriber signal (e.g. the SDK drops the publisher on a transport error). The shared
 * destination buffer is therefore an {@link AtomicReference} and the terminal-state fields {@code volatile}.
 *
 * <p><b>Retries:</b> the SDK calls {@link #prepare()} again on each retry, so a fresh destination
 * buffer is allocated for every attempt. Stale state from a previous attempt is not reused.
 *
 * @param <R> the unmarshalled SDK response type (e.g. {@code GetObjectResponse}).
 */
final class KnownLengthAsyncResponseTransformer<R extends SdkResponse> implements AsyncResponseTransformer<R, DirectReadBuffer> {

    private final int expectedLength;
    private final DirectBufferFactory factory;

    private volatile R response;
    private volatile CompletableFuture<DirectReadBuffer> resultFuture;
    // Kept so exceptionOccurred() can release the buffer even if the subscriber's onError
    // is never delivered (e.g. SDK abandons the publisher after a transport error).
    private volatile ChunkCopyingSubscriber currentSubscriber;

    /**
     * @param expectedLength exact length of the response body in bytes
     * @param factory factory from which the destination {@link DirectReadBuffer} is obtained; the
     *     returned buffer is charged against the underlying allocator until {@link DirectReadBuffer#close()}
     *     is called by the caller
     */
    KnownLengthAsyncResponseTransformer(int expectedLength, DirectBufferFactory factory) {
        if (expectedLength < 0) {
            throw new IllegalArgumentException("expectedLength must be non-negative, got: " + expectedLength);
        }
        this.expectedLength = expectedLength;
        this.factory = factory;
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
    public CompletableFuture<DirectReadBuffer> prepare() {
        // Allocated lazily here (not in the constructor) because prepare() is invoked again on each
        // retry; the previous attempt's buffer, if any, must be discarded.
        CompletableFuture<DirectReadBuffer> bufferFuture = new CompletableFuture<>();
        this.resultFuture = bufferFuture;
        return bufferFuture;
    }

    @Override
    public void onResponse(R response) {
        this.response = response;
    }

    @Override
    public void onStream(SdkPublisher<ByteBuffer> publisher) {
        ChunkCopyingSubscriber subscriber = new ChunkCopyingSubscriber(resultFuture, expectedLength, factory);
        this.currentSubscriber = subscriber;
        publisher.subscribe(subscriber);
    }

    @Override
    public void exceptionOccurred(Throwable error) {
        // Release the buffer if it was allocated in onSubscribe but onError was never
        // delivered to the subscriber (e.g. SDK abandons the publisher after a transport
        // error). releaseOnFailure() is idempotent — if onError already fired it is a no-op.
        ChunkCopyingSubscriber subscriber = currentSubscriber;
        if (subscriber != null) {
            // Mark failed before releasing so any in-flight onNext that slips past the
            // AWS SDK's cancellation guarantee sees the flag and returns early instead of
            // writing into the already-released buffer.
            subscriber.failed = true;
            subscriber.releaseOnFailure();
        }
        CompletableFuture<DirectReadBuffer> f = resultFuture;
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
        private final CompletableFuture<DirectReadBuffer> resultFuture;
        private final int expectedLength;
        private final DirectBufferFactory factory;
        // Cross-callback fields are volatile as defense-in-depth. The Reactive Streams contract
        // guarantees serial signals with happens-before, but making the visibility explicit avoids
        // depending on each publisher implementation honoring that subtlety correctly.
        // destinationBuf uses AtomicReference so releaseOnFailure() is safe when called
        // concurrently from exceptionOccurred() on the transformer and onError() on the subscriber.
        private final AtomicReference<DirectReadBuffer> destinationBuf = new AtomicReference<>();
        private volatile ByteBuffer destination;
        private int offset;
        private volatile Subscription subscription;
        private volatile boolean failed;

        ChunkCopyingSubscriber(CompletableFuture<DirectReadBuffer> resultFuture, int expectedLength, DirectBufferFactory factory) {
            this.resultFuture = resultFuture;
            this.expectedLength = expectedLength;
            this.factory = factory;
        }

        @Override
        public void onSubscribe(Subscription s) {
            // Reactive Streams §2.5: cancel additional subscriptions on a single Subscriber.
            if (this.subscription != null) {
                s.cancel();
                return;
            }
            this.subscription = s;
            // Allocate here (rather than the constructor) so an allocator OOM/breaker trip is
            // routed through the result future instead of escaping publisher.subscribe(...) as
            // an Error.
            try {
                DirectReadBuffer drb = factory.allocate(expectedLength);
                this.destinationBuf.set(drb);
                this.destination = drb.buffer();
                // Guard against exceptionOccurred racing the window between allocate() and set()
                // above: if it fired first it saw null in destinationBuf and could not release,
                // so we must release now if the future was already completed exceptionally.
                if (resultFuture.isDone()) {
                    releaseOnFailure();
                    return;
                }
            } catch (Exception e) {
                failed = true;
                releaseOnFailure();
                s.cancel();
                resultFuture.completeExceptionally(e);
                return;
            }
            try {
                s.request(Long.MAX_VALUE);
            } catch (RuntimeException e) {
                failed = true;
                releaseOnFailure();
                resultFuture.completeExceptionally(e);
            }
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
                releaseOnFailure();
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
            releaseOnFailure();
            resultFuture.completeExceptionally(error);
        }

        @Override
        public void onComplete() {
            if (failed) {
                return;
            }
            if (offset != destination.capacity()) {
                int capacity = destination.capacity();
                failed = true;
                releaseOnFailure();
                resultFuture.completeExceptionally(
                    new IOException("S3 response body shorter than expected: received=" + offset + ", expected=" + capacity)
                );
                return;
            }
            destination.position(0).limit(offset);
            // Transfer ownership of the buffer to the caller; getAndSet(null) ensures any
            // concurrent releaseOnFailure (e.g. exceptionOccurred) sees null and does not
            // double-close. The destination ByteBuffer's position/limit set above is observable
            // through drb.buffer() since they share the same NIO view.
            DirectReadBuffer transferred = destinationBuf.getAndSet(null);
            // If exceptionOccurred raced ahead and already failed the future, complete() returns false and we
            // hold the buffer's only reference — release it rather than orphan its memory. A null transferred
            // means releaseOnFailure won that race and already failed the future, so there is nothing to do.
            if (transferred != null && resultFuture.complete(transferred) == false) {
                transferred.close();
            }
        }

        void releaseOnFailure() {
            DirectReadBuffer drb = destinationBuf.getAndSet(null);
            if (drb != null) {
                drb.close();
            }
        }
    }

}
