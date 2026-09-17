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

import org.elasticsearch.xpack.esql.datasources.spi.DirectBufferFactory;
import org.elasticsearch.xpack.esql.datasources.spi.DirectReadBuffer;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;

import java.nio.ByteBuffer;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;

/**
 * An {@link AsyncResponseTransformer} wrapper that supports being called by
 * {@code S3CrossRegionAsyncClient} — the SDK's cross-region redirect decorator enabled via
 * {@code crossRegionAccessEnabled(true)}. When the first attempt returns a 301 redirect (the
 * bucket is in a different region), {@code S3CrossRegionAsyncClient} calls {@code prepare()} a
 * second time on the same transformer to start the redirect attempt. Plain
 * {@link KnownLengthAsyncResponseTransformer} throws on a second {@code prepare()} because it is
 * intentionally single-use (see its class javadoc for the stale-exceptionOccurred rationale).
 *
 * <p>This wrapper resolves the conflict by creating a fresh {@link KnownLengthAsyncResponseTransformer}
 * on each {@code prepare()} call and delegating all subsequent SDK callbacks to it. All other
 * methods ({@link #onResponse}, {@link #onStream}, {@link #exceptionOccurred}) are forwarded to
 * the most recently created ("current") inner transformer via a volatile {@link AtomicReference}.
 *
 * <p><b>Why SDK-level retry safety is preserved:</b> {@code doNotRetry()} on the async client
 * disables the SDK's {@code AsyncRetryableStage}, which is the only other code path that calls
 * {@code prepare()} more than once. The only remaining caller of a second {@code prepare()} is
 * {@code S3CrossRegionAsyncClient}, which this wrapper is specifically designed for.
 *
 * <p><b>Stale-callback race:</b> a late Netty channel-teardown {@code exceptionOccurred(IOException)}
 * from the first attempt may arrive at this wrapper after {@link #prepare()} has already been called
 * for the second time (updating {@code currentInner} to the new inner transformer). Such a stale
 * call would then reach the second inner transformer, spuriously failing it. In practice this is
 * very unlikely: for 301 responses the Netty channel-inactive event fires on the same event-loop
 * thread that processed the response, sequentially before the {@code whenComplete} that triggers the
 * second {@code prepare()}. Worst case the spurious failure triggers one extra retry from
 * {@code S3StorageObject}'s outer retry loop, which succeeds because
 * {@code S3CrossRegionAsyncClient} has already cached the correct region for the bucket.
 *
 * <p><b>Prepare-before-publish ordering:</b> the new inner transformer is {@link KnownLengthAsyncResponseTransformer#prepare() prepared}
 * before it is published via {@code currentInner}. This closes the window where a callback racing
 * {@code prepare()} could reach an unprepared inner transformer.
 *
 * @param <R> the unmarshalled SDK response type (e.g. {@code GetObjectResponse}).
 */
final class CrossRegionAwareResponseTransformer<R extends SdkResponse> implements AsyncResponseTransformer<R, DirectReadBuffer> {

    private final int expectedLength;
    private final DirectBufferFactory factory;
    private final StoragePath path;

    private final AtomicReference<KnownLengthAsyncResponseTransformer<R>> currentInner = new AtomicReference<>();

    CrossRegionAwareResponseTransformer(int expectedLength, DirectBufferFactory factory, StoragePath path) {
        if (expectedLength < 0) {
            throw new IllegalArgumentException("expectedLength must be non-negative, got: " + expectedLength);
        }
        this.expectedLength = expectedLength;
        this.factory = factory;
        this.path = path;
    }

    /**
     * Returns the response from the most-recently-prepared inner transformer, or {@code null} if not
     * yet available. Mirrors {@link KnownLengthAsyncResponseTransformer#response()}: safe to call
     * only after the future returned by {@link #prepare()} has completed.
     */
    R response() {
        KnownLengthAsyncResponseTransformer<R> inner = currentInner.get();
        return inner != null ? inner.response() : null;
    }

    /**
     * Creates a fresh {@link KnownLengthAsyncResponseTransformer}, prepares it, publishes it as
     * the current inner transformer, and returns its future.
     *
     * <p>If a previous inner transformer exists (cross-region redirect case), {@link
     * KnownLengthAsyncResponseTransformer#exceptionOccurred} is called on it to release any
     * resources it holds. In practice the redirect exception has already completed the old inner
     * transformer's future, so the call is a no-op.
     *
     * <p>The new inner transformer is prepared before being published, so a callback thread that
     * races {@code prepare()} and reads {@code currentInner} always sees a fully initialized
     * (prepared) transformer.
     */
    @Override
    public CompletableFuture<DirectReadBuffer> prepare() {
        KnownLengthAsyncResponseTransformer<R> newInner = new KnownLengthAsyncResponseTransformer<>(expectedLength, factory, path);
        // Prepare before publishing to close the window where exceptionOccurred could reach
        // an unprepared transformer.
        CompletableFuture<DirectReadBuffer> future = newInner.prepare();
        KnownLengthAsyncResponseTransformer<R> old = currentInner.getAndSet(newInner);
        if (old != null) {
            // Cross-region redirect: abandon the previous inner transformer. If it still holds
            // resources (buffer allocated but redirect arrived before streaming started), release
            // them. The typical case — redirect exception already completed old's future — is a
            // no-op because KnownLengthAsyncResponseTransformer.exceptionOccurred is idempotent.
            old.exceptionOccurred(new CancellationException("cross-region redirect: superseded by new attempt"));
        }
        return future;
    }

    @Override
    public void onResponse(R response) {
        KnownLengthAsyncResponseTransformer<R> inner = currentInner.get();
        if (inner != null) {
            inner.onResponse(response);
        }
    }

    @Override
    public void onStream(SdkPublisher<ByteBuffer> publisher) {
        KnownLengthAsyncResponseTransformer<R> inner = currentInner.get();
        if (inner != null) {
            inner.onStream(publisher);
        }
    }

    @Override
    public void exceptionOccurred(Throwable error) {
        KnownLengthAsyncResponseTransformer<R> inner = currentInner.get();
        if (inner != null) {
            inner.exceptionOccurred(error);
        }
    }
}
