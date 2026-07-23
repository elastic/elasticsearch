/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.s3;

import software.amazon.awssdk.core.async.SdkPublisher;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;

import org.apache.arrow.memory.BufferAllocator;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.datasources.spi.DirectBufferFactory;
import org.elasticsearch.xpack.esql.datasources.spi.DirectReadBuffer;
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

    // Hold a strong reference to the BlockFactory so the JVM Cleaner does not close the
    // arrow root allocator mid-test (BlockFactory.arrowAllocator() registers a cleaner action
    // on its own BlockFactory instance, which is otherwise unreachable from ALLOCATOR alone).
    private static final BlockFactory BLOCK_FACTORY = BlockFactory.builder(BigArrays.NON_RECYCLING_INSTANCE)
        .breaker(new NoopCircuitBreaker("test"))
        .build();
    private static final BufferAllocator ALLOCATOR = BLOCK_FACTORY.arrowAllocator();
    private static final DirectBufferFactory FACTORY = DirectBufferFactory.forAllocator(ALLOCATOR);

    public void testRejectsNegativeExpectedLength() {
        IllegalArgumentException ex = expectThrows(
            IllegalArgumentException.class,
            () -> new KnownLengthAsyncResponseTransformer<>(-1, FACTORY)
        );
        assertThat(ex.getMessage(), containsString("must be non-negative"));
    }

    public void testSingleChunkHeapByteBuffer() throws Exception {
        byte[] payload = randomByteArrayOfLength(between(1, 4096));
        try (DirectReadBuffer result = runTransformer(payload.length, response(payload.length), List.of(ByteBuffer.wrap(payload)))) {
            assertTrue(result.buffer().isDirect());
            assertArrayEquals(payload, toByteArray(result.buffer()));
        }
    }

    public void testMultiChunkHeapByteBuffer() throws Exception {
        byte[] payload = randomByteArrayOfLength(between(64, 8192));
        List<ByteBuffer> chunks = splitIntoChunks(payload, between(2, 8), false);
        try (DirectReadBuffer result = runTransformer(payload.length, response(payload.length), chunks)) {
            assertTrue(result.buffer().isDirect());
            assertArrayEquals(payload, toByteArray(result.buffer()));
        }
    }

    public void testMultiChunkDirectByteBuffer() throws Exception {
        byte[] payload = randomByteArrayOfLength(between(64, 8192));
        List<ByteBuffer> chunks = splitIntoChunks(payload, between(2, 8), true);
        try (DirectReadBuffer result = runTransformer(payload.length, response(payload.length), chunks)) {
            assertTrue(result.buffer().isDirect());
            assertArrayEquals(payload, toByteArray(result.buffer()));
        }
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

        try (DirectReadBuffer result = runTransformer(payload.length, response(payload.length), List.of(chunk))) {
            assertTrue(result.buffer().isDirect());
            assertArrayEquals(payload, toByteArray(result.buffer()));
        }
    }

    public void testEmptyResponse() throws Exception {
        try (DirectReadBuffer result = runTransformer(0, response(0), List.of())) {
            assertTrue(result.buffer().isDirect());
            assertEquals(0, result.buffer().remaining());
        }
    }

    public void testOverflowFailsFastAndCancelsSubscription() {
        byte[] payload = randomByteArrayOfLength(64);
        AtomicBoolean cancelled = new AtomicBoolean(false);
        AtomicLong requested = new AtomicLong(0);

        KnownLengthAsyncResponseTransformer<GetObjectResponse> transformer = new KnownLengthAsyncResponseTransformer<>(
            payload.length - 1,
            FACTORY
        );
        CompletableFuture<DirectReadBuffer> future = transformer.prepare();
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
        KnownLengthAsyncResponseTransformer<GetObjectResponse> transformer = new KnownLengthAsyncResponseTransformer<>(
            partial.length + 8,
            FACTORY
        );
        CompletableFuture<DirectReadBuffer> future = transformer.prepare();
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
        KnownLengthAsyncResponseTransformer<GetObjectResponse> transformer = new KnownLengthAsyncResponseTransformer<>(16, FACTORY);
        CompletableFuture<DirectReadBuffer> future = transformer.prepare();
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
        KnownLengthAsyncResponseTransformer<GetObjectResponse> transformer = new KnownLengthAsyncResponseTransformer<>(16, FACTORY);
        CompletableFuture<DirectReadBuffer> future = transformer.prepare();

        IllegalStateException error = new IllegalStateException("connection reset");
        transformer.exceptionOccurred(error);

        ExecutionException ex = expectThrows(ExecutionException.class, future::get);
        assertSame(error, ex.getCause());
    }

    public void testRetryAllocatesFreshDestination() throws Exception {
        // The SDK invokes prepare() for every retry attempt; the result of the first attempt must
        // not contaminate the second.
        KnownLengthAsyncResponseTransformer<GetObjectResponse> transformer = new KnownLengthAsyncResponseTransformer<>(8, FACTORY);

        CompletableFuture<DirectReadBuffer> firstAttempt = transformer.prepare();
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
        CompletableFuture<DirectReadBuffer> secondAttempt = transformer.prepare();
        transformer.onResponse(response(8));
        transformer.onStream(new SdkPublisher<>() {
            @Override
            public void subscribe(Subscriber<? super ByteBuffer> s) {
                s.onSubscribe(new TestSubscription());
                s.onNext(ByteBuffer.wrap(payload));
                s.onComplete();
            }
        });

        try (DirectReadBuffer result = secondAttempt.get()) {
            assertTrue(result.buffer().isDirect());
            assertArrayEquals(payload, toByteArray(result.buffer()));
        }
    }

    public void testOnCompleteReleasesBufferWhenItLosesTheCompletionRace() throws Exception {
        // If a concurrent exceptionOccurred fails the future before onComplete completes it, onComplete's
        // complete() returns false; it then solely owns the buffer it took via getAndSet and must release it,
        // or the direct memory leaks. The child allocator surfaces any leak as non-zero allocated bytes.
        try (BufferAllocator child = ALLOCATOR.newChildAllocator("onComplete-race", 0, Long.MAX_VALUE)) {
            DirectBufferFactory factory = DirectBufferFactory.forAllocator(child);
            byte[] payload = randomByteArrayOfLength(256);
            KnownLengthAsyncResponseTransformer<GetObjectResponse> transformer = new KnownLengthAsyncResponseTransformer<>(
                payload.length,
                factory
            );
            CompletableFuture<DirectReadBuffer> future = transformer.prepare();
            transformer.onResponse(response(payload.length));

            RuntimeException raced = new RuntimeException("exceptionOccurred won the completion race");
            transformer.onStream(new SdkPublisher<>() {
                @Override
                public void subscribe(Subscriber<? super ByteBuffer> s) {
                    s.onSubscribe(new TestSubscription());
                    s.onNext(ByteBuffer.wrap(payload)); // fills the destination: offset == capacity
                    future.completeExceptionally(raced); // a concurrent exceptionOccurred fails the future first
                    s.onComplete(); // onComplete loses the race; it must release the buffer it could not hand off
                }
            });

            ExecutionException ex = expectThrows(ExecutionException.class, future::get);
            assertSame(raced, ex.getCause());
            assertEquals("onComplete must release the buffer it could not hand off", 0L, child.getAllocatedMemory());
        }
    }

    public void testResponseObjectExposedViaGetter() throws Exception {
        byte[] payload = randomByteArrayOfLength(between(8, 256));
        GetObjectResponse expectedResponse = response(payload.length);
        KnownLengthAsyncResponseTransformer<GetObjectResponse> transformer = new KnownLengthAsyncResponseTransformer<>(
            payload.length,
            FACTORY
        );
        try (DirectReadBuffer ignored = runTransformer(transformer, expectedResponse, List.of(ByteBuffer.wrap(payload)))) {
            assertThat(transformer.response().contentLength(), equalTo((long) payload.length));
        }
    }

    /**
     * Runs the transformer end-to-end given a fixed payload length, response, and list of
     * pre-computed chunks. Each chunk is emitted in order on the same calling thread, mirroring
     * the SDK's external-synchronization guarantee.
     */
    private static DirectReadBuffer runTransformer(int expectedLength, GetObjectResponse response, List<ByteBuffer> chunks)
        throws Exception {
        KnownLengthAsyncResponseTransformer<GetObjectResponse> transformer = new KnownLengthAsyncResponseTransformer<>(
            expectedLength,
            FACTORY
        );
        return runTransformer(transformer, response, chunks);
    }

    private static DirectReadBuffer runTransformer(
        KnownLengthAsyncResponseTransformer<GetObjectResponse> transformer,
        GetObjectResponse response,
        List<ByteBuffer> chunks
    ) throws Exception {
        CompletableFuture<DirectReadBuffer> future = transformer.prepare();
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
        DirectReadBuffer result = future.get();
        assertTrue(result.buffer().isDirect());
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
