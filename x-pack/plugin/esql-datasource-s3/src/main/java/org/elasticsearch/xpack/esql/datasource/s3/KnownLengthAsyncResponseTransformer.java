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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * An {@link AsyncResponseTransformer} that accumulates the response body into a single, pre-sized
 * destination {@link ByteBuffer}, eliminating the redundant per-chunk allocations and copies performed
 * by the SDK's default {@link AsyncResponseTransformer#toBytes()} and avoiding a subsequent
 * extra copy in {@code S3StorageObject.readBytesAsync}.
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
 * one destination buffer of exactly that size, and copies each {@code onNext(ByteBuffer)} chunk
 * into the destination at the running offset. That collapses three SDK-internal copies into a
 * single chunk-to-destination copy.
 *
 * <p><b>Single execution — no SDK retries:</b> a transformer instance serves exactly one request
 * attempt. {@link #prepare()} throws if invoked twice, which is how the SDK signals a retry of the
 * same execution. The client that drives this transformer must therefore be configured with
 * {@code AwsRetryStrategy.doNotRetry()}; {@code S3StorageObject.readBytesAsync} owns the retry loop
 * and creates a fresh transformer per attempt. This is deliberate and load-bearing: the SDK reuses
 * one transformer across retry attempts, but {@link #exceptionOccurred} carries no attempt identity,
 * and netty can deliver late error notifications for a finished attempt (both the error the
 * subscriber already handled and a fresh {@code IOException} from the channel-inactive path) after
 * the SDK has already started the next attempt. A shared transformer cannot attribute such a stale
 * call, so it would either spuriously fail the next attempt's future and free its buffer, or — if it
 * ignored the call — hang a genuine pre-stream failure, because the future returned by
 * {@code prepare()} is the only thing that completes an attempt in the SDK's async pipeline. With
 * one instance per attempt every callback on this object belongs to its one execution and late
 * duplicates are no-ops via the {@code isDone()} guard.
 *
 * <p><b>Synchronization:</b> with retries disabled and no {@code apiCallAttemptTimeout}, the SDK
 * serializes {@link #exceptionOccurred} with the subscriber's signals on the channel event loop.
 * The locking below ({@link ChunkCopyingSubscriber#onNext} and
 * {@link ChunkCopyingSubscriber#releaseOnFailure} are {@code synchronized} on the subscriber, and
 * the shared buffer handle is an {@link AtomicReference}) is defense-in-depth so that enabling
 * either of the two SDK features that break that serialization (attempt timeouts, external future
 * cancellation) degrades to a spuriously failed request instead of a write into freed memory.
 *
 * @param <R> the unmarshalled SDK response type (e.g. {@code GetObjectResponse}).
 */
final class KnownLengthAsyncResponseTransformer<R extends SdkResponse> implements AsyncResponseTransformer<R, DirectReadBuffer> {

    private final int expectedLength;
    private final DirectBufferFactory factory;

    private final CompletableFuture<DirectReadBuffer> resultFuture = new CompletableFuture<>();
    private final AtomicBoolean prepared = new AtomicBoolean();

    private volatile R response;
    private volatile ChunkCopyingSubscriber subscriber;

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
        // A second prepare() means the SDK is retrying with this transformer, which would resurrect
        // the cross-attempt stale-exceptionOccurred race this class is designed out of (see class
        // javadoc). Fail the retry loudly rather than silently sharing state across attempts.
        if (prepared.compareAndSet(false, true) == false) {
            throw new IllegalStateException(
                "KnownLengthAsyncResponseTransformer is single-use: prepare() was called more than once. "
                    + "SDK-level retries must stay disabled on this client; retries are owned by the caller, "
                    + "which must create a fresh transformer per attempt."
            );
        }
        return resultFuture;
    }

    @Override
    public void onResponse(R response) {
        this.response = response;
    }

    @Override
    public void onStream(SdkPublisher<ByteBuffer> publisher) {
        ChunkCopyingSubscriber chunkCopyingSubscriber = new ChunkCopyingSubscriber(resultFuture, expectedLength, factory);
        this.subscriber = chunkCopyingSubscriber;
        publisher.subscribe(chunkCopyingSubscriber);
    }

    @Override
    public void exceptionOccurred(Throwable error) {
        // Late duplicate notifications are expected: after the subscriber handles its terminal
        // signal, netty still notifies the response handler (with the same throwable), and the
        // channel-inactive teardown can follow with a fresh IOException. Both belong to this one
        // execution (single-use contract), so once the future is done there is nothing left to do.
        if (resultFuture.isDone()) {
            return;
        }
        // Release the buffer if it was allocated in onSubscribe but onError was never delivered
        // (e.g. SDK abandons the publisher after a transport error). releaseOnFailure() is
        // idempotent and synchronized on the subscriber, so it safely races with an in-flight
        // onNext: the copy will complete before the buffer is freed, or the copy will find
        // destination == null and return early if releaseOnFailure wins the lock.
        ChunkCopyingSubscriber sub = subscriber;
        if (sub != null) {
            // Set failed before releasing so any onNext that is in-flight and hasn't yet
            // acquired the subscriber lock will short-circuit at its fast-path check.
            // This is critical for the onSubscribe window: between destinationBuf.set(drb)
            // and destination = drb.buffer(), releaseOnFailure nulls destination (a no-op
            // since it is not yet assigned). If isDone() then returns false, s.request()
            // fires; without failed=true, onNext would enter the lock and write into the
            // freed buffer that destination now points to.
            sub.failed = true;
            sub.releaseOnFailure();
        }
        resultFuture.completeExceptionally(error);
    }

    /**
     * Copies each incoming {@link ByteBuffer} chunk into a pre-sized destination,
     * tracking the running offset. Fails fast if the cumulative size of received chunks would
     * exceed the expected length (a mismatch between the requested range and the server's
     * response body) or falls short of it on completion.
     *
     * <p>The {@link #onNext} copy and the {@link #releaseOnFailure} buffer release are
     * {@code synchronized} on {@code this} to prevent a concurrent
     * {@link KnownLengthAsyncResponseTransformer#exceptionOccurred} from freeing the buffer
     * while a chunk copy is in progress (a use-after-free write). Within the lock,
     * {@code onNext} checks {@code destination == null} as the authoritative signal that the buffer
     * has already been released, making an early return safe.
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
        // Nulled inside the releaseOnFailure() lock so onNext can detect a freed buffer.
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
                    failed = true;
                    s.cancel();
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
                s.cancel();
                releaseOnFailure();
                resultFuture.completeExceptionally(e);
            }
        }

        @Override
        public void onNext(ByteBuffer chunk) {
            if (failed) {
                return;
            }
            // The copy and the release are mutually exclusive via the subscriber lock.
            // A concurrent exceptionOccurred -> releaseOnFailure waits for the copy to finish
            // (or the copy finds destination == null and returns early if release won the lock).
            int remaining = chunk.remaining();
            IOException overflowError = null;
            synchronized (this) {
                ByteBuffer dst = destination;
                if (dst == null) {
                    // Buffer was freed by a concurrent exceptionOccurred; skip this chunk.
                    return;
                }
                // Overflow-safe form of `offset + remaining > dst.capacity()`. `offset` is always
                // in [0, dst.capacity()] thanks to this same guard on prior iterations, so the
                // subtraction never underflows.
                if (remaining > dst.capacity() - offset) {
                    failed = true;
                    subscription.cancel();
                    releaseOnFailure();
                    overflowError = new IOException(
                        "S3 response body exceeded expected length: cumulative="
                            + ((long) offset + remaining)
                            + ", expected="
                            + dst.capacity()
                    );
                } else {
                    DirectByteBufferCopies.copyChunkIntoDestination(dst, offset, chunk);
                    offset += remaining;
                }
            }
            if (overflowError != null) {
                resultFuture.completeExceptionally(overflowError);
            }
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
            // Capture the volatile once; if releaseOnFailure nulled it concurrently, dst is null
            // and we bail out — destinationBuf.getAndSet(null) below will return null, and the
            // future was already completed exceptionally by exceptionOccurred.
            ByteBuffer dst = destination;
            if (dst == null) {
                return;
            }
            if (offset != dst.capacity()) {
                int capacity = dst.capacity();
                failed = true;
                releaseOnFailure();
                resultFuture.completeExceptionally(
                    new IOException("S3 response body shorter than expected: received=" + offset + ", expected=" + capacity)
                );
                return;
            }
            dst.position(0).limit(offset);
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

        synchronized void releaseOnFailure() {
            DirectReadBuffer drb = destinationBuf.getAndSet(null);
            if (drb != null) {
                // Null out before closing so any onNext that subsequently acquires this lock
                // finds destination == null and returns early instead of writing into freed memory.
                destination = null;
                drb.close();
            }
        }
    }

}
