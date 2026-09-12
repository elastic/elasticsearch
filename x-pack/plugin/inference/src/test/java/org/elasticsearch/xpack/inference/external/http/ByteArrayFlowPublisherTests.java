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
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.sameInstance;

/**
 * Verifies the streaming semantics of {@link ByteArrayFlowPublisher}: chunks emitted by the http client's reactive
 * response publisher must be relayed to the downstream {@link Flow.Subscriber} in order as {@code byte[]} copies, and
 * demand must be honored — the downstream pairs each {@code request(n)} with exactly one signal (onNext, onComplete, or
 * onError), so a terminal signal must never be delivered without outstanding demand. Delivering one anyway trips an
 * assertion in the real subscriber ({@code ServerSentEventsRestActionListener}) and terminates the node, so
 * {@link TestSubscriber} enforces the same invariant here.
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

    private ByteArrayFlowPublisher publisher(Publisher<ByteBuffer> upstream) {
        return new ByteArrayFlowPublisher(upstream, threadPool, new TestCircuitBreaker(), "inference-id");
    }

    /**
     * Given a downstream subscriber with demand for every chunk and the terminal signal
     * When the upstream emits chunks and then completes
     * Then the chunks arrive in order as copies that are unaffected by later mutation of the source bytes
     */
    public void testRelaysChunksInOrderAsCopies() {
        var upstream = new TestUpstreamPublisher();
        // two data chunks plus the terminal signal each consume one unit of demand
        var subscriber = new TestSubscriber(3);
        publisher(upstream).subscribe(subscriber);

        assertThat("demand must be forwarded upstream before any chunk can be emitted", upstream.requested(), equalTo(3L));

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
     * Given a completion that arrives while the downstream has no outstanding demand
     * When the terminal signal would otherwise be delivered
     * Then it is withheld until the downstream requests again — this is the exact contract whose violation killed the node.
     */
    public void testCompletionIsWithheldUntilDemandIsAvailable() throws Exception {
        var upstream = new TestUpstreamPublisher();
        var subscriber = new TestSubscriber(0);
        publisher(upstream).subscribe(subscriber);

        subscriber.request(1); // exactly one unit of demand — consumed by the single chunk
        upstream.emit(randomByteArrayOfLength(5));
        upstream.complete(); // completion arrives, but there is no demand left to carry it

        // the onNext lands; onComplete must NOT be delivered yet (if it were, TestSubscriber's demand guard would fail)
        assertBusy(() -> assertThat(subscriber.events, contains("onNext")));

        subscriber.request(1); // now grant demand for the terminal signal
        subscriber.awaitTerminalSignal();
        assertThat(subscriber.events, contains("onNext", "onComplete"));
    }

    /**
     * Given chunks still queued (undelivered) when an error arrives
     * When demand becomes available
     * Then the error preempts the queued chunks and onError is the only signal delivered
     */
    public void testErrorPreemptsQueuedChunks() {
        var upstream = new TestUpstreamPublisher();
        var subscriber = new TestSubscriber(0); // no demand yet, so emitted chunks stay queued
        publisher(upstream).subscribe(subscriber);

        var exception = new IllegalStateException("failed");
        upstream.emit(randomByteArrayOfLength(5));
        upstream.emit(randomByteArrayOfLength(5));
        upstream.error(exception);

        subscriber.request(1); // demand arrives after everything is queued

        subscriber.awaitTerminalSignal();

        assertThat(subscriber.items, is(empty()));
        assertThat(subscriber.events, contains("onError"));
        assertThat(subscriber.error, sameInstance(exception));
    }

    /**
     * Given queued chunks and a completion, delivered one demand unit at a time
     * When the downstream requests one signal at a time (as the real SSE subscriber does)
     * Then the chunks are delivered in order and onComplete is the final signal
     */
    public void testOnCompleteIsDeliveredAfterQueuedChunks() {
        var upstream = new TestUpstreamPublisher();
        var subscriber = new TestSubscriber(3);
        publisher(upstream).subscribe(subscriber);

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
        publisher(upstream).subscribe(subscriber);

        assertThat("no demand must be forwarded before the downstream requests", upstream.requested(), equalTo(0L));

        subscriber.request(5);

        assertThat(upstream.requested(), equalTo(5L));
    }

    /**
     * When the downstream cancels its subscription
     * Then the cancellation propagates to the upstream subscription
     */
    public void testCancelPropagatesToUpstreamSubscription() {
        var upstream = new TestUpstreamPublisher();
        var subscriber = new TestSubscriber(0);
        publisher(upstream).subscribe(subscriber);

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

    /**
     * A downstream subscriber that models the real {@code ServerSentEventsRestActionListener}: it delivers exactly one signal per
     * {@code request(n)} unit of demand. {@link #consumeDemand} fails the test with the same message the production subscriber
     * asserts when a signal is delivered without outstanding demand — the defect that terminates the node.
     */
    private static class TestSubscriber implements Flow.Subscriber<byte[]> {
        private final long initialDemand;
        private final List<byte[]> items = Collections.synchronizedList(new ArrayList<>());
        private final List<String> events = Collections.synchronizedList(new ArrayList<>());
        private final CountDownLatch terminalLatch = new CountDownLatch(1);
        private final AtomicLong outstanding = new AtomicLong();
        private volatile Flow.Subscription subscription;
        private volatile Throwable error;

        TestSubscriber(long initialDemand) {
            this.initialDemand = initialDemand;
        }

        /** Request {@code n} more items, tracking demand so demand-less (illegal) deliveries can be detected. */
        void request(long n) {
            outstanding.addAndGet(n);
            subscription.request(n);
        }

        private void consumeDemand(String signal) {
            assertThat(
                "Subscriber signal [" + signal + "] was delivered without an outstanding request() — this crashes the node",
                outstanding.getAndDecrement(),
                greaterThan(0L)
            );
        }

        @Override
        public void onSubscribe(Flow.Subscription subscription) {
            this.subscription = subscription;
            if (initialDemand > 0) {
                request(initialDemand);
            }
        }

        @Override
        public void onNext(byte[] item) {
            consumeDemand("onNext");
            items.add(item);
            events.add("onNext");
        }

        @Override
        public void onError(Throwable throwable) {
            consumeDemand("onError");
            error = throwable;
            events.add("onError");
            terminalLatch.countDown();
        }

        @Override
        public void onComplete() {
            consumeDemand("onComplete");
            events.add("onComplete");
            terminalLatch.countDown();
        }

        private void awaitTerminalSignal() {
            safeAwait(terminalLatch);
            assertThat("only one terminal signal may be delivered", terminalLatch.getCount(), is(0L));
        }
    }
}
