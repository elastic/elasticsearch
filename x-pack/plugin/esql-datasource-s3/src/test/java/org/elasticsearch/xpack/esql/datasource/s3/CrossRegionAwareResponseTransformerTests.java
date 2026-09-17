/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.s3;

import software.amazon.awssdk.core.async.SdkPublisher;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;

import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.datasources.spi.DirectBufferFactory;
import org.elasticsearch.xpack.esql.datasources.spi.DirectReadBuffer;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.reactivestreams.Subscription;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

/**
 * Unit tests for {@link CrossRegionAwareResponseTransformer}. Tests exercise the wrapper
 * directly through the {@link software.amazon.awssdk.core.async.AsyncResponseTransformer}
 * surface, using hand-rolled {@link SdkPublisher}s that emit chunks synchronously.
 */
public class CrossRegionAwareResponseTransformerTests extends ESTestCase {

    private static final DirectBufferFactory FACTORY = DirectBufferFactory.forBreaker(new NoopCircuitBreaker("test"));
    private static final StoragePath PATH = StoragePath.of("s3://test-bucket/data/file.parquet");

    public void testRejectsNegativeExpectedLength() {
        IllegalArgumentException ex = expectThrows(
            IllegalArgumentException.class,
            () -> new CrossRegionAwareResponseTransformer<>(-1, FACTORY, PATH)
        );
        assertThat(ex.getMessage(), containsString("non-negative"));
    }

    public void testSinglePrepare_happyPath() throws Exception {
        byte[] payload = randomByteArrayOfLength(between(1, 4096));
        CrossRegionAwareResponseTransformer<GetObjectResponse> wrapper = new CrossRegionAwareResponseTransformer<>(
            payload.length,
            FACTORY,
            PATH
        );

        CompletableFuture<DirectReadBuffer> future = wrapper.prepare();
        GetObjectResponse response = response(payload.length);
        wrapper.onResponse(response);
        wrapper.onStream(syncPublisher(List.of(ByteBuffer.wrap(payload))));

        try (DirectReadBuffer result = future.get()) {
            assertArrayEquals(payload, toByteArray(result.buffer()));
        }
        assertThat(wrapper.response().contentLength(), equalTo((long) payload.length));
    }

    /**
     * Simulates the cross-region redirect scenario: the SDK receives a 301 on the first attempt,
     * calls {@code exceptionOccurred} to complete F1 exceptionally, then calls {@code prepare()}
     * again. The second attempt completes successfully with the correct bytes.
     */
    public void testSecondPrepare_crossRegionRedirect() throws Exception {
        byte[] payload = randomByteArrayOfLength(between(1, 4096));
        CrossRegionAwareResponseTransformer<GetObjectResponse> wrapper = new CrossRegionAwareResponseTransformer<>(
            payload.length,
            FACTORY,
            PATH
        );

        // First attempt: wrong region → 301 redirect
        CompletableFuture<DirectReadBuffer> f1 = wrapper.prepare();
        RuntimeException redirectException = new RuntimeException("301 Moved Permanently");
        wrapper.exceptionOccurred(redirectException);

        // F1 must be done and failed with the redirect exception
        assertTrue("F1 must be failed after exceptionOccurred", f1.isCompletedExceptionally());
        ExecutionException f1Error = expectThrows(ExecutionException.class, f1::get);
        assertSame(redirectException, f1Error.getCause());

        // SDK calls prepare() again for the redirect attempt
        CompletableFuture<DirectReadBuffer> f2 = wrapper.prepare();

        // Second attempt: correct region → success
        GetObjectResponse response = response(payload.length);
        wrapper.onResponse(response);
        wrapper.onStream(syncPublisher(List.of(ByteBuffer.wrap(payload))));

        try (DirectReadBuffer result = f2.get()) {
            assertArrayEquals(payload, toByteArray(result.buffer()));
        }
        // response() must return from the redirect attempt's inner transformer
        assertThat(wrapper.response().contentLength(), equalTo((long) payload.length));
    }

    /**
     * If the SDK calls {@code prepare()} a second time before explicitly calling
     * {@code exceptionOccurred} on the first inner transformer (e.g., if the redirect path skips
     * the exception callback and calls {@code prepare()} directly), the wrapper must
     * cancel the first inner transformer itself to release any resources it holds.
     */
    public void testSecondPrepare_cancelsFirstInnerWithCancellationException() throws Exception {
        CrossRegionAwareResponseTransformer<GetObjectResponse> wrapper = new CrossRegionAwareResponseTransformer<>(8, FACTORY, PATH);
        CompletableFuture<DirectReadBuffer> f1 = wrapper.prepare();
        assertFalse("F1 should not be done before second prepare()", f1.isDone());

        CompletableFuture<DirectReadBuffer> f2 = wrapper.prepare();

        // F1 must be cancelled by the wrapper's prepare() via CancellationException.
        // Use handle() to extract the stored exception as a value — avoids JDK-version-specific
        // unwrapping differences in get() (JDK 25 wraps CancellationException) and the
        // IllegalStateException from exceptionNow() when the stored exception is a CancellationException.
        assertTrue("F1 must be done after second prepare()", f1.isDone());
        assertTrue("F1 must be failed", f1.isCompletedExceptionally());
        Throwable stored = f1.handle((r, e) -> e).get();
        assertThat(stored, instanceOf(CancellationException.class));
        assertThat(stored.getMessage(), containsString("cross-region redirect"));

        // F2 is the active future; it should not be done yet
        assertFalse("F2 must still be pending", f2.isDone());
    }

    /**
     * {@code response()} returns the response stored in the current inner transformer —
     * i.e., the redirect attempt's response after a second {@code prepare()} cycle.
     */
    public void testResponse_returnsFromCurrentInner() throws Exception {
        byte[] payload = randomByteArrayOfLength(between(8, 256));
        CrossRegionAwareResponseTransformer<GetObjectResponse> wrapper = new CrossRegionAwareResponseTransformer<>(
            payload.length,
            FACTORY,
            PATH
        );

        // First attempt fails (redirect)
        CompletableFuture<DirectReadBuffer> f1 = wrapper.prepare();
        wrapper.exceptionOccurred(new RuntimeException("redirect"));
        expectThrows(ExecutionException.class, f1::get);

        // Second attempt succeeds
        CompletableFuture<DirectReadBuffer> f2 = wrapper.prepare();
        GetObjectResponse expected = response(payload.length);
        wrapper.onResponse(expected);
        wrapper.onStream(syncPublisher(List.of(ByteBuffer.wrap(payload))));
        f2.get().close();

        assertSame("response() must return from the current (redirect) inner", expected, wrapper.response());
    }

    /**
     * {@code exceptionOccurred} called before {@code prepare()} has no current inner transformer
     * and must not throw.
     */
    public void testExceptionOccurred_beforePrepare_isNoOp() {
        CrossRegionAwareResponseTransformer<GetObjectResponse> wrapper = new CrossRegionAwareResponseTransformer<>(8, FACTORY, PATH);
        wrapper.exceptionOccurred(new RuntimeException("premature exception"));
    }

    /**
     * {@code response()} before any {@code prepare()} must return {@code null} without throwing.
     */
    public void testResponse_beforePrepare_returnsNull() {
        CrossRegionAwareResponseTransformer<GetObjectResponse> wrapper = new CrossRegionAwareResponseTransformer<>(8, FACTORY, PATH);
        assertNull(wrapper.response());
    }

    /**
     * Exercises the intra-wrapper stale-callback race documented in the class Javadoc: a late
     * {@code exceptionOccurred} from the first attempt arrives AFTER {@code prepare()} has already
     * been called a second time (updating {@code currentInner} to the new inner transformer). In that
     * case the stale call is forwarded to the second inner transformer, spuriously failing it.
     *
     * <p>In practice this is prevented by Netty's ordering guarantees, but the wrapper must not
     * crash or corrupt state when it does occur. The test verifies that after the stale call the
     * second future is failed (triggering the outer retry loop) and no assertion error is thrown.
     */
    public void testStaleExceptionOccurred_afterSecondPrepare_failsSecondInner() {
        byte[] payload = randomByteArrayOfLength(between(1, 256));
        CrossRegionAwareResponseTransformer<GetObjectResponse> wrapper = new CrossRegionAwareResponseTransformer<>(
            payload.length,
            FACTORY,
            PATH
        );

        // First prepare: simulates the first HTTP attempt starting.
        CompletableFuture<DirectReadBuffer> f1 = wrapper.prepare();

        // Second prepare: simulates S3CrossRegionAsyncClient calling prepare() for the redirect.
        // (F1 is NOT yet failed — this is the race scenario where the second prepare() wins.)
        CompletableFuture<DirectReadBuffer> f2 = wrapper.prepare();

        // F1 must be cancelled by the second prepare() (CancellationException injected on old inner).
        assertTrue("F1 must be done after second prepare()", f1.isDone());
        assertTrue("F1 must be completed exceptionally", f1.isCompletedExceptionally());

        // Stale exceptionOccurred from the first attempt arrives AFTER currentInner has been updated.
        // It now goes to the second inner transformer (F2), spuriously failing it.
        IOException staleError = new IOException("channel inactive — stale callback from first attempt");
        wrapper.exceptionOccurred(staleError);

        // F2 must be failed with the stale error (the outer retry loop handles this).
        assertTrue("F2 must be failed by the stale exceptionOccurred", f2.isCompletedExceptionally());
        ExecutionException f2Error = expectThrows(ExecutionException.class, f2::get);
        assertSame(staleError, f2Error.getCause());
    }

    private static SdkPublisher<ByteBuffer> syncPublisher(List<ByteBuffer> chunks) {
        return subscriber -> {
            subscriber.onSubscribe(new Subscription() {
                @Override
                public void request(long n) {}

                @Override
                public void cancel() {}
            });
            for (ByteBuffer chunk : chunks) {
                subscriber.onNext(chunk);
            }
            subscriber.onComplete();
        };
    }

    private static GetObjectResponse response(int contentLength) {
        return GetObjectResponse.builder().contentLength((long) contentLength).build();
    }

    private static byte[] toByteArray(ByteBuffer buffer) {
        byte[] bytes = new byte[buffer.remaining()];
        buffer.get(bytes);
        buffer.rewind();
        return bytes;
    }
}
