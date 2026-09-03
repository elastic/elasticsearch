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
import org.elasticsearch.xpack.esql.datasources.spi.ExternalUnavailableException;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;

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
 * one destination buffer with at least that capacity for each subscription, and copies each
 * {@code onNext(ByteBuffer)} chunk into the destination at the running offset. That
 * collapses three SDK-internal copies into a single chunk-to-destination copy.
 *
 * <p><b>Synchronization:</b> Reactive Streams serializes the {@link Subscriber}'s own signals, but
 * {@link #exceptionOccurred} is a transformer-level callback outside that ordering and can race the
 * terminal subscriber signal (e.g. the SDK drops the publisher on a transport error). The
 * subscriber therefore serializes destination-buffer copies and ownership transitions under a
 * private lock.
 *
 * <p><b>Retries:</b> the SDK calls {@link #prepare()} again on each retry, so a fresh destination
 * buffer is allocated for every attempt. Stale state from a previous attempt is not reused.
 *
 * @param <R> the unmarshalled SDK response type (e.g. {@code GetObjectResponse}).
 */
final class KnownLengthAsyncResponseTransformer<R extends SdkResponse> implements AsyncResponseTransformer<R, DirectReadBuffer> {

    private final int expectedLength;
    private final DirectBufferFactory factory;
    private final StoragePath path;

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
     * @param path the object being read, named in the body-length failure messages. Those failures are
     *     surfaced to the user as-is (the read path's failure mapping preserves an already-typed exception
     *     rather than re-wrapping it), so the object has to be identified here or not at all
     */
    KnownLengthAsyncResponseTransformer(int expectedLength, DirectBufferFactory factory, StoragePath path) {
        if (expectedLength < 0) {
            throw new IllegalArgumentException("expectedLength must be non-negative, got: " + expectedLength);
        }
        this.expectedLength = expectedLength;
        this.factory = factory;
        this.path = path;
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
        this.currentSubscriber = null;
        this.resultFuture = bufferFuture;
        return bufferFuture;
    }

    @Override
    public void onResponse(R response) {
        this.response = response;
    }

    @Override
    public void onStream(SdkPublisher<ByteBuffer> publisher) {
        ChunkCopyingSubscriber subscriber = new ChunkCopyingSubscriber(resultFuture, expectedLength, factory, path);
        this.currentSubscriber = subscriber;
        publisher.subscribe(subscriber);
    }

    @Override
    public void exceptionOccurred(Throwable error) {
        CompletableFuture<DirectReadBuffer> f = resultFuture;
        ChunkCopyingSubscriber subscriber = currentSubscriber;
        if (subscriber != null && subscriber.resultFuture == f) {
            // The subscriber arbitrates this callback with its own terminal signals and closes
            // any published owner before delivering failure.
            subscriber.fail(error);
        } else if (f != null) {
            // No subscriber belongs to this prepared attempt yet.
            f.completeExceptionally(error);
        }
    }

    /**
     * Copies each incoming {@link ByteBuffer} chunk into a pre-sized destination,
     * tracking the running offset. Fails fast if the cumulative size of received chunks would
     * exceed the expected length (a mismatch between the requested range and the server's
     * response body) or falls short of it on completion.
     * <p>
     * Both mismatches are raised as {@link ExternalUnavailableException} (503, retryable): a body that does not
     * match the range we asked for is a truncated or over-long response from the store, which the next attempt
     * can well return correctly — the same typing the synchronous path gives a mid-body transport fault. The
     * cost of that choice is that a wrong {@code expectedLength} on our side is reported as the store being
     * unavailable, but it re-trips on every attempt and still fails once the bounded retry budget is spent.
     */
    private static final class ChunkCopyingSubscriber implements Subscriber<ByteBuffer> {
        private final CompletableFuture<DirectReadBuffer> resultFuture;
        private final int expectedLength;
        private final DirectBufferFactory factory;
        private final StoragePath path;
        private final Object destinationLock = new Object();
        // All four fields below are guarded by destinationLock, with no unsynchronized reads. A
        // published owner may leave destinationBuf only through a claim under that lock. Failure
        // claimants close before unlocking and completing failure; a successful claimant either
        // transfers ownership or closes if completion loses. An unpublished owner belongs to
        // onSubscribe, and a successfully transferred owner belongs to the consumer.
        private DirectReadBuffer destinationBuf;
        private int offset;
        private boolean failed;
        private boolean successClaimed;

        private volatile Subscription subscription;

        ChunkCopyingSubscriber(
            CompletableFuture<DirectReadBuffer> resultFuture,
            int expectedLength,
            DirectBufferFactory factory,
            StoragePath path
        ) {
            this.resultFuture = resultFuture;
            this.expectedLength = expectedLength;
            this.factory = factory;
            this.path = path;
        }

        @Override
        public void onSubscribe(Subscription s) {
            // Reactive Streams §2.5: cancel additional subscriptions on a single Subscriber.
            if (this.subscription != null) {
                s.cancel();
                return;
            }
            this.subscription = s;
            // Allocate outside destinationLock, and here rather than in the constructor, so an
            // allocator OOM/breaker trip is routed through the result future instead of escaping
            // publisher.subscribe(...) as an Error.
            final DirectReadBuffer allocated;
            try {
                allocated = factory.allocateWritableWindow(expectedLength);
            } catch (Exception e) {
                s.cancel();
                fail(e);
                return;
            }
            if (publishDestination(allocated) == false) {
                // This owner was never visible to another thread, so closing it here cannot race
                // a terminal callback. The claimant that beat us owns the failure completion.
                allocated.close();
                s.cancel();
                return;
            }
            if (destinationAbandoned()) {
                // Something terminated between publication and demand. fail() is a no-op when a
                // terminal callback already claimed the owner; otherwise it releases the owner
                // that nobody else will.
                fail(new IOException("S3 destination was abandoned before demand was requested"));
                s.cancel();
                return;
            }
            try {
                s.request(Long.MAX_VALUE);
            } catch (RuntimeException e) {
                fail(e);
            }
        }

        private boolean publishDestination(DirectReadBuffer allocated) {
            synchronized (destinationLock) {
                if (destinationUnavailable()) {
                    return false;
                }
                destinationBuf = allocated;
                return true;
            }
        }

        private boolean destinationAbandoned() {
            synchronized (destinationLock) {
                return destinationUnavailable();
            }
        }

        private boolean destinationUnavailable() {
            assert Thread.holdsLock(destinationLock);
            return failed || successClaimed || resultFuture.isDone();
        }

        @Override
        public void onNext(ByteBuffer chunk) {
            int remaining = chunk.remaining();
            ExternalUnavailableException overflow = null;
            synchronized (destinationLock) {
                DirectReadBuffer drb = destinationBuf;
                if (drb == null || failed || successClaimed) {
                    return;
                }
                // Overflow-safe because offset remains in [0, expectedLength].
                if (remaining > expectedLength - offset) {
                    failed = true;
                    overflow = new ExternalUnavailableException(
                        "S3 response body exceeded expected length reading [{}]: cumulative={}, expected={}",
                        path,
                        (long) offset + remaining,
                        expectedLength
                    );
                    destinationBuf = null;
                    drb.close();
                } else {
                    DirectByteBufferCopies.copyChunkIntoDestination(drb.buffer(), offset, chunk);
                    offset += remaining;
                }
            }
            if (overflow != null) {
                subscription.cancel();
                resultFuture.completeExceptionally(overflow);
            }
        }

        @Override
        public void onError(Throwable error) {
            fail(error);
        }

        @Override
        public void onComplete() {
            DirectReadBuffer transferred;
            ExternalUnavailableException shortRead = null;
            synchronized (destinationLock) {
                if (failed || successClaimed) {
                    return;
                }
                transferred = destinationBuf;
                if (transferred == null) {
                    return;
                }
                destinationBuf = null;
                if (offset != expectedLength) {
                    failed = true;
                    shortRead = new ExternalUnavailableException(
                        "S3 response body shorter than expected reading [{}]: received={}, expected={}",
                        path,
                        offset,
                        expectedLength
                    );
                    transferred.close();
                } else {
                    successClaimed = true;
                }
            }
            if (shortRead != null) {
                resultFuture.completeExceptionally(shortRead);
                return;
            }
            transferred.buffer().position(0).limit(offset);
            // Completion can run downstream listeners, so keep it outside destinationLock. If the
            // future was independently completed or cancelled, retain ownership and close here.
            if (resultFuture.complete(transferred) == false) {
                transferred.close();
            }
        }

        boolean fail(Throwable error) {
            synchronized (destinationLock) {
                if (failed || successClaimed) {
                    return false;
                }
                failed = true;
                DirectReadBuffer drb = destinationBuf;
                if (drb != null) {
                    // Close while holding the lock so another terminal callback cannot return
                    // before the published owner has been released.
                    destinationBuf = null;
                    drb.close();
                }
            }
            resultFuture.completeExceptionally(error);
            return true;
        }
    }

}
