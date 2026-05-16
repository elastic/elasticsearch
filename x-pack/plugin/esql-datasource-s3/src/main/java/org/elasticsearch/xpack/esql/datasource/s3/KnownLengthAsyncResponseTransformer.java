/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.s3;

import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.core.SdkResponse;
import software.amazon.awssdk.core.async.AsyncResponseTransformer;
import software.amazon.awssdk.core.async.SdkPublisher;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;

/**
 * An {@link AsyncResponseTransformer} that accumulates the response body into a single, pre-sized
 * {@code byte[]}, eliminating the redundant per-chunk allocations and copies performed by the SDK's
 * default {@link AsyncResponseTransformer#toBytes()}.
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
 * one destination array of exactly that size in {@link #prepare()}, and copies each
 * {@code onNext(ByteBuffer)} chunk directly into the destination at the running offset. That
 * collapses three SDK-internal copies into a single byte-for-byte chunk-to-final-array copy.
 *
 * <p>The result is exposed via {@link ResponseBytes#fromByteArrayUnsafe} so the destination array
 * is handed to the caller without a final defensive copy. This is the same trade-off that was
 * accepted in the existing {@code S3StorageObject.readBytesAsync} path (which already calls
 * {@code asByteArrayUnsafe()}).
 *
 * <p><b>Synchronization:</b> the {@link AsyncResponseTransformer} contract guarantees that no two
 * methods on this class or on its {@link Subscriber} are invoked concurrently, so no locking is
 * required.
 *
 * <p><b>Retries:</b> the SDK calls {@link #prepare()} again on each retry, so a fresh destination
 * array is allocated for every attempt. Stale state from a previous attempt is not reused.
 *
 * @param <R> the unmarshalled SDK response type (e.g. {@code GetObjectResponse}).
 */
final class KnownLengthAsyncResponseTransformer<R extends SdkResponse> implements AsyncResponseTransformer<R, ResponseBytes<R>> {

    private final int expectedLength;

    private volatile R response;
    private volatile CompletableFuture<byte[]> resultFuture;

    KnownLengthAsyncResponseTransformer(int expectedLength) {
        if (expectedLength < 0) {
            throw new IllegalArgumentException("expectedLength must be non-negative, got: " + expectedLength);
        }
        this.expectedLength = expectedLength;
    }

    @Override
    public CompletableFuture<ResponseBytes<R>> prepare() {
        // Allocated lazily here (not in the constructor) because prepare() is invoked again on each
        // retry; the previous attempt's array, if any, must be discarded.
        CompletableFuture<byte[]> bytesFuture = new CompletableFuture<>();
        this.resultFuture = bytesFuture;
        return bytesFuture.thenApply(arr -> ResponseBytes.fromByteArrayUnsafe(response, arr));
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
        CompletableFuture<byte[]> f = resultFuture;
        if (f != null) {
            f.completeExceptionally(error);
        }
    }

    /**
     * Copies each incoming {@link ByteBuffer} chunk directly into a pre-sized destination array,
     * tracking the running offset. Fails fast if the cumulative size of received chunks would
     * exceed the expected length (a mismatch between the requested range and the server's
     * response body) or falls short of it on completion.
     */
    private static final class ChunkCopyingSubscriber implements Subscriber<ByteBuffer> {
        private final CompletableFuture<byte[]> resultFuture;
        private final byte[] destination;
        private int offset;
        private Subscription subscription;
        private boolean failed;

        ChunkCopyingSubscriber(CompletableFuture<byte[]> resultFuture, int expectedLength) {
            this.resultFuture = resultFuture;
            this.destination = new byte[expectedLength];
        }

        @Override
        public void onSubscribe(Subscription s) {
            // Reactive Streams §2.5: cancel additional subscriptions on a single Subscriber.
            if (this.subscription != null) {
                s.cancel();
                return;
            }
            this.subscription = s;
            s.request(Long.MAX_VALUE);
        }

        @Override
        public void onNext(ByteBuffer chunk) {
            if (failed) {
                return;
            }
            int remaining = chunk.remaining();
            if (offset + remaining > destination.length) {
                failed = true;
                IOException error = new IOException(
                    "S3 response body exceeded expected length: cumulative=" + (offset + remaining) + ", expected=" + destination.length
                );
                subscription.cancel();
                resultFuture.completeExceptionally(error);
                return;
            }
            // Cheaper path when the SDK hands us a heap-backed ByteBuffer (the common case after
            // the pooled receive allocator landed): one System.arraycopy with no bounds-check loop
            // inside ByteBuffer.get.
            if (chunk.hasArray()) {
                System.arraycopy(chunk.array(), chunk.arrayOffset() + chunk.position(), destination, offset, remaining);
                chunk.position(chunk.position() + remaining);
            } else {
                chunk.get(destination, offset, remaining);
            }
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
            if (offset != destination.length) {
                resultFuture.completeExceptionally(
                    new IOException("S3 response body shorter than expected: received=" + offset + ", expected=" + destination.length)
                );
                return;
            }
            resultFuture.complete(destination);
        }
    }
}
