/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.http;

import org.elasticsearch.common.breaker.TestCircuitBreaker;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.After;
import org.junit.Before;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Flow;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static org.elasticsearch.xpack.inference.Utils.inferenceUtilityExecutors;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.sameInstance;

/**
 * Verifies the streaming semantics of {@link ByteArrayFlowPublisher}: chunks emitted by the http client's reactive
 * response publisher must be relayed to the downstream {@link Flow.Subscriber} in order as {@code byte[]} copies,
 * terminal signals must be delivered after all queued chunks, and demand/cancellation must be forwarded to the
 * upstream subscription.
 */
public class ByteArrayFlowPublisherTests extends ESTestCase {

    private ThreadPool threadPool;

    @Before
    public void init() {
        threadPool = createThreadPool(inferenceUtilityExecutors());
    }

    @After
    public void shutdown() {
        terminate(threadPool);
    }

    /**
     * Given a downstream subscriber with enough demand
     * When the upstream emits chunks and then completes
     * Then the chunks arrive in order as copies that are unaffected by later mutation of the source bytes
     */
    public void testRelaysChunksInOrderAsCopies() {
        var upstream = new TestUpstreamPublisher();
        var subscriber = new TestSubscriber(2);
        new ByteArrayFlowPublisher(upstream, threadPool, new TestCircuitBreaker(), "inference-id").subscribe(subscriber);

        assertThat("demand must be forwarded upstream before any chunk can be emitted", upstream.requested(), equalTo(2L));

        var firstChunk = "hello".getBytes(StandardCharsets.UTF_8);
        var secondChunk = "world".getBytes(StandardCharsets.UTF_8);
        var expectedFirstChunk = firstChunk.clone();
        var expectedSecondChunk = secondChunk.clone();

        upstream.emit(firstChunk);
        // mutating the source bytes after emission must not corrupt the relayed chunk
        Arrays.fill(firstChunk, (byte) 0);
        upstream.emit(secondChunk);
        upstream.complete();

        subscriber.awaitTerminalSignal();

        assertThat(subscriber.items, hasSize(2));
        assertArrayEquals(expectedFirstChunk, subscriber.items.get(0));
        assertArrayEquals(expectedSecondChunk, subscriber.items.get(1));
        assertThat(subscriber.events, contains("onNext", "onNext", "onComplete"));
    }

    /**
     * Given chunks queued ahead of an upstream failure
     * When the failure arrives
     * Then the queued chunks are delivered first and onError is the final signal
     */
    public void testOnErrorIsDeliveredAfterQueuedChunks() {
        var upstream = new TestUpstreamPublisher();
        // Delivery starts on the utility pool as soon as the first chunk arrives. Block the first delivery until all
        // upstream signals are queued so the error is guaranteed to race with queued (undelivered) chunks.
        var allSignalsQueued = new CountDownLatch(1);
        TestSubscriber subscriber = new TestSubscriber(2) {
            @Override
            public void onNext(byte[] item) {
                safeAwait(allSignalsQueued);
                super.onNext(item);
            }
        };
        new ByteArrayFlowPublisher(upstream, threadPool, new TestCircuitBreaker(), "inference-id").subscribe(subscriber);

        var exception = new IllegalStateException("failed");
        upstream.emit(randomByteArrayOfLength(5));
        upstream.emit(randomByteArrayOfLength(5));
        upstream.error(exception);
        allSignalsQueued.countDown();

        subscriber.awaitTerminalSignal();

        assertThat(subscriber.events, contains("onNext", "onNext", "onError"));
        assertThat(subscriber.error, sameInstance(exception));
    }

    /**
     * Given chunks queued ahead of the upstream completion
     * When the completion arrives
     * Then the queued chunks are delivered first and onComplete is the final signal
     */
    public void testOnCompleteIsDeliveredAfterQueuedChunks() {
        var upstream = new TestUpstreamPublisher();
        var subscriber = new TestSubscriber(2);
        new ByteArrayFlowPublisher(upstream, threadPool, new TestCircuitBreaker(), "inference-id").subscribe(subscriber);

        upstream.emit(randomByteArrayOfLength(5));
        upstream.emit(randomByteArrayOfLength(5));
        upstream.complete();

        subscriber.awaitTerminalSignal();

        assertThat(subscriber.events, contains("onNext", "onNext", "onComplete"));
        assertNull(subscriber.error);
    }

    /**
     * When the downstream requests chunks
     * Then the demand is forwarded to the upstream subscription
     */
    public void testRequestIsForwardedUpstream() {
        var upstream = new TestUpstreamPublisher();
        var subscriber = new TestSubscriber(0);
        new ByteArrayFlowPublisher(upstream, threadPool, new TestCircuitBreaker(), "inference-id").subscribe(subscriber);

        assertThat("no demand must be forwarded before the downstream requests", upstream.requested(), equalTo(0L));

        subscriber.subscription.request(5);

        assertThat(upstream.requested(), equalTo(5L));
    }

    /**
     * When the downstream cancels its subscription
     * Then the cancellation propagates to the upstream subscription
     */
    public void testCancelPropagatesToUpstreamSubscription() {
        var upstream = new TestUpstreamPublisher();
        var subscriber = new TestSubscriber(0);
        new ByteArrayFlowPublisher(upstream, threadPool, new TestCircuitBreaker(), "inference-id").subscribe(subscriber);

        assertFalse(upstream.isCancelled());

        subscriber.subscription.cancel();

        assertTrue(upstream.isCancelled());
    }

    /**
     * A hand-rolled upstream {@link Publisher} standing in for the http client's reactive response body publisher. It hands out a
     * subscription that records the forwarded demand and cancellation, and lets the test emit chunks and terminal signals on the
     * test thread, mimicking the IO reactor thread the real client uses.
     */
    private static class TestUpstreamPublisher implements Publisher<ByteBuffer> {
        private final AtomicLong requested = new AtomicLong();
        private final AtomicBoolean cancelled = new AtomicBoolean();
        private volatile Subscriber<? super ByteBuffer> subscriber;

        @Override
        public void subscribe(Subscriber<? super ByteBuffer> subscriber) {
            this.subscriber = subscriber;
            subscriber.onSubscribe(new Subscription() {
                @Override
                public void request(long n) {
                    requested.addAndGet(n);
                }

                @Override
                public void cancel() {
                    cancelled.set(true);
                }
            });
        }

        private void emit(byte[] chunk) {
            subscriber.onNext(ByteBuffer.wrap(chunk));
        }

        private void complete() {
            subscriber.onComplete();
        }

        private void error(Exception e) {
            subscriber.onError(e);
        }

        private long requested() {
            return requested.get();
        }

        private boolean isCancelled() {
            return cancelled.get();
        }
    }

    private static class TestSubscriber implements Flow.Subscriber<byte[]> {
        private final long initialDemand;
        private final List<byte[]> items = Collections.synchronizedList(new ArrayList<>());
        private final List<String> events = Collections.synchronizedList(new ArrayList<>());
        private final CountDownLatch terminalLatch = new CountDownLatch(1);
        private volatile Flow.Subscription subscription;
        private volatile Throwable error;

        TestSubscriber(long initialDemand) {
            this.initialDemand = initialDemand;
        }

        @Override
        public void onSubscribe(Flow.Subscription subscription) {
            this.subscription = subscription;
            if (initialDemand > 0) {
                subscription.request(initialDemand);
            }
        }

        @Override
        public void onNext(byte[] item) {
            items.add(item);
            events.add("onNext");
        }

        @Override
        public void onError(Throwable throwable) {
            error = throwable;
            events.add("onError");
            terminalLatch.countDown();
        }

        @Override
        public void onComplete() {
            events.add("onComplete");
            terminalLatch.countDown();
        }

        private void awaitTerminalSignal() {
            safeAwait(terminalLatch);
            assertThat("only one terminal signal may be delivered", terminalLatch.getCount(), is(0L));
        }
    }
}
