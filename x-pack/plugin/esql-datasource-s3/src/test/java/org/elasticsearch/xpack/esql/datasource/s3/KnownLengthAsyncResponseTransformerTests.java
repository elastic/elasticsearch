/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.s3;

import software.amazon.awssdk.core.async.SdkPublisher;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;

import org.elasticsearch.test.ESTestCase;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.instanceOf;

/**
 * Unit tests for {@link KnownLengthAsyncResponseTransformer}. The transformer is exercised
 * directly through its public {@link software.amazon.awssdk.core.async.AsyncResponseTransformer}
 * surface area (no real S3AsyncClient is needed); chunks are emitted via a hand-rolled
 * {@link SdkPublisher} that calls {@code onSubscribe}/{@code onNext}/{@code onComplete}
 * synchronously, mirroring the contract documented in the SDK.
 */
public class KnownLengthAsyncResponseTransformerTests extends ESTestCase {

    public void testRejectsNegativeExpectedLength() {
        IllegalArgumentException ex = expectThrows(IllegalArgumentException.class, () -> new KnownLengthAsyncResponseTransformer<>(-1));
        assertThat(ex.getMessage(), containsString("must be non-negative"));
    }

    public void testSingleChunkHeapByteBuffer() throws Exception {
        byte[] payload = randomByteArrayOfLength(between(1, 4096));
        ByteBuffer result = runTransformer(payload.length, response(payload.length), List.of(ByteBuffer.wrap(payload)));
        assertTrue(result.isDirect());
        assertArrayEquals(payload, toByteArray(result));
    }

    public void testMultiChunkHeapByteBuffer() throws Exception {
        byte[] payload = randomByteArrayOfLength(between(64, 8192));
        List<ByteBuffer> chunks = splitIntoChunks(payload, between(2, 8), false);
        ByteBuffer result = runTransformer(payload.length, response(payload.length), chunks);
        assertTrue(result.isDirect());
        assertArrayEquals(payload, toByteArray(result));
    }

    public void testMultiChunkDirectByteBuffer() throws Exception {
        byte[] payload = randomByteArrayOfLength(between(64, 8192));
        List<ByteBuffer> chunks = splitIntoChunks(payload, between(2, 8), true);
        ByteBuffer result = runTransformer(payload.length, response(payload.length), chunks);
        assertTrue(result.isDirect());
        assertArrayEquals(payload, toByteArray(result));
    }

    public void testHeapByteBufferWithArrayOffset() throws Exception {
        byte[] payload = randomByteArrayOfLength(between(64, 1024));
        // Wrap a backing array with a leading slack so arrayOffset() is non-zero.
        int slack = between(1, 16);
        byte[] backing = new byte[payload.length + slack + between(0, 16)];
        System.arraycopy(payload, 0, backing, slack, payload.length);
        ByteBuffer chunk = ByteBuffer.wrap(backing, slack, payload.length).slice();
        assertTrue("test fixture should have hasArray=true", chunk.hasArray());
        assertThat(chunk.arrayOffset(), greaterThanOrEqualTo(slack));

        ByteBuffer result = runTransformer(payload.length, response(payload.length), List.of(chunk));
        assertTrue(result.isDirect());
        assertArrayEquals(payload, toByteArray(result));
    }

    public void testEmptyResponse() throws Exception {
        ByteBuffer result = runTransformer(0, response(0), List.of());
        assertTrue(result.isDirect());
        assertEquals(0, result.remaining());
    }

    public void testOverflowFailsFastAndCancelsSubscription() {
        byte[] payload = randomByteArrayOfLength(64);
        AtomicBoolean cancelled = new AtomicBoolean(false);
        AtomicLong requested = new AtomicLong(0);

        KnownLengthAsyncResponseTransformer<GetObjectResponse> transformer = new KnownLengthAsyncResponseTransformer<>(payload.length - 1);
        CompletableFuture<ByteBuffer> future = transformer.prepare();
        transformer.onResponse(response(payload.length - 1));

        transformer.onStream(new SdkPublisher<>() {
            @Override
            public void subscribe(Subscriber<? super ByteBuffer> s) {
                s.onSubscribe(new Subscription() {
                    @Override
                    public void request(long n) {
                        requested.addAndGet(n);
                    }

                    @Override
                    public void cancel() {
                        cancelled.set(true);
                    }
                });
                s.onNext(ByteBuffer.wrap(payload));
            }
        });

        ExecutionException ex = expectThrows(ExecutionException.class, future::get);
        assertThat(ex.getCause(), instanceOf(IOException.class));
        assertThat(ex.getCause().getMessage(), containsString("exceeded expected length"));
        assertTrue("subscription should be cancelled on overflow", cancelled.get());
        // The subscriber requests unbounded demand on subscribe (Reactive Streams §3.4); guard
        // against a future regression that adds backpressure without considering this contract.
        assertThat(requested.get(), equalTo(Long.MAX_VALUE));
    }

    public void testUnderflowOnCompleteFails() {
        byte[] partial = randomByteArrayOfLength(32);
        KnownLengthAsyncResponseTransformer<GetObjectResponse> transformer = new KnownLengthAsyncResponseTransformer<>(partial.length + 8);
        CompletableFuture<ByteBuffer> future = transformer.prepare();
        transformer.onResponse(response(partial.length + 8));

        transformer.onStream(new SdkPublisher<>() {
            @Override
            public void subscribe(Subscriber<? super ByteBuffer> s) {
                s.onSubscribe(new TestSubscription());
                s.onNext(ByteBuffer.wrap(partial));
                s.onComplete();
            }
        });

        ExecutionException ex = expectThrows(ExecutionException.class, future::get);
        assertThat(ex.getCause(), instanceOf(IOException.class));
        assertThat(ex.getCause().getMessage(), containsString("shorter than expected"));
    }

    public void testOnErrorPropagates() {
        KnownLengthAsyncResponseTransformer<GetObjectResponse> transformer = new KnownLengthAsyncResponseTransformer<>(16);
        CompletableFuture<ByteBuffer> future = transformer.prepare();
        transformer.onResponse(response(16));

        RuntimeException boom = new RuntimeException("boom");
        transformer.onStream(new SdkPublisher<>() {
            @Override
            public void subscribe(Subscriber<? super ByteBuffer> s) {
                s.onSubscribe(new TestSubscription());
                s.onError(boom);
            }
        });

        ExecutionException ex = expectThrows(ExecutionException.class, future::get);
        assertSame(boom, ex.getCause());
    }

    public void testExceptionOccurredBeforeStreamPropagates() {
        KnownLengthAsyncResponseTransformer<GetObjectResponse> transformer = new KnownLengthAsyncResponseTransformer<>(16);
        CompletableFuture<ByteBuffer> future = transformer.prepare();

        IllegalStateException error = new IllegalStateException("connection reset");
        transformer.exceptionOccurred(error);

        ExecutionException ex = expectThrows(ExecutionException.class, future::get);
        assertSame(error, ex.getCause());
    }

    public void testRetryAllocatesFreshDestination() throws Exception {
        // The SDK invokes prepare() for every retry attempt; the result of the first attempt must
        // not contaminate the second.
        KnownLengthAsyncResponseTransformer<GetObjectResponse> transformer = new KnownLengthAsyncResponseTransformer<>(8);

        CompletableFuture<ByteBuffer> firstAttempt = transformer.prepare();
        transformer.onResponse(response(8));
        transformer.onStream(new SdkPublisher<>() {
            @Override
            public void subscribe(Subscriber<? super ByteBuffer> s) {
                s.onSubscribe(new TestSubscription());
                s.onError(new RuntimeException("first attempt failed"));
            }
        });
        expectThrows(ExecutionException.class, firstAttempt::get);

        // Second attempt — must produce the new payload, untainted by the first attempt's state.
        byte[] payload = new byte[] { 1, 2, 3, 4, 5, 6, 7, 8 };
        CompletableFuture<ByteBuffer> secondAttempt = transformer.prepare();
        transformer.onResponse(response(8));
        transformer.onStream(new SdkPublisher<>() {
            @Override
            public void subscribe(Subscriber<? super ByteBuffer> s) {
                s.onSubscribe(new TestSubscription());
                s.onNext(ByteBuffer.wrap(payload));
                s.onComplete();
            }
        });

        ByteBuffer result = secondAttempt.get();
        assertTrue(result.isDirect());
        assertArrayEquals(payload, toByteArray(result));
    }

    public void testResponseObjectExposedViaGetter() throws Exception {
        byte[] payload = randomByteArrayOfLength(between(8, 256));
        GetObjectResponse expectedResponse = response(payload.length);
        KnownLengthAsyncResponseTransformer<GetObjectResponse> transformer = new KnownLengthAsyncResponseTransformer<>(payload.length);
        runTransformer(transformer, expectedResponse, List.of(ByteBuffer.wrap(payload)));
        assertThat(transformer.response().contentLength(), equalTo((long) payload.length));
    }

    /**
     * Runs the transformer end-to-end given a fixed payload length, response, and list of
     * pre-computed chunks. Each chunk is emitted in order on the same calling thread, mirroring
     * the SDK's external-synchronization guarantee.
     */
    private static ByteBuffer runTransformer(int expectedLength, GetObjectResponse response, List<ByteBuffer> chunks) throws Exception {
        KnownLengthAsyncResponseTransformer<GetObjectResponse> transformer = new KnownLengthAsyncResponseTransformer<>(expectedLength);
        return runTransformer(transformer, response, chunks);
    }

    private static ByteBuffer runTransformer(
        KnownLengthAsyncResponseTransformer<GetObjectResponse> transformer,
        GetObjectResponse response,
        List<ByteBuffer> chunks
    ) throws Exception {
        CompletableFuture<ByteBuffer> future = transformer.prepare();
        transformer.onResponse(response);
        transformer.onStream(new SdkPublisher<>() {
            @Override
            public void subscribe(Subscriber<? super ByteBuffer> s) {
                s.onSubscribe(new TestSubscription());
                for (ByteBuffer chunk : chunks) {
                    s.onNext(chunk);
                }
                s.onComplete();
            }
        });
        ByteBuffer result = future.get();
        assertTrue(result.isDirect());
        return result;
    }

    private static byte[] toByteArray(ByteBuffer buffer) {
        byte[] bytes = new byte[buffer.remaining()];
        buffer.get(bytes);
        buffer.rewind();
        return bytes;
    }

    private static GetObjectResponse response(int contentLength) {
        return GetObjectResponse.builder().contentLength((long) contentLength).build();
    }

    private static List<ByteBuffer> splitIntoChunks(byte[] payload, int chunkCount, boolean direct) {
        List<ByteBuffer> chunks = new ArrayList<>(chunkCount);
        int offset = 0;
        int remaining = payload.length;
        for (int i = 0; i < chunkCount && remaining > 0; i++) {
            int size = (i == chunkCount - 1) ? remaining : Math.max(1, remaining / (chunkCount - i));
            ByteBuffer chunk;
            if (direct) {
                chunk = ByteBuffer.allocateDirect(size);
                chunk.put(payload, offset, size);
                chunk.flip();
            } else {
                chunk = ByteBuffer.wrap(payload, offset, size).slice();
            }
            chunks.add(chunk);
            offset += size;
            remaining -= size;
        }
        return chunks;
    }

    /**
     * Inert subscription for tests that don't care about request/cancel signals.
     */
    private static final class TestSubscription implements Subscription {
        @Override
        public void request(long n) {}

        @Override
        public void cancel() {}
    }
}
