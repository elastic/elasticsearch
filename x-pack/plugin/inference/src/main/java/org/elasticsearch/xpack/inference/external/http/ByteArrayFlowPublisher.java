/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.http;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.threadpool.ThreadPool;
import org.reactivestreams.FlowAdapters;
import org.reactivestreams.Publisher;

import java.nio.ByteBuffer;
import java.util.Deque;
import java.util.Objects;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.Flow;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.core.Strings.format;
import static org.elasticsearch.xpack.inference.InferencePlugin.UTILITY_THREAD_POOL_NAME;

/**
 * <p>Bridges the {@link Publisher} of response body chunks produced by the http client's reactive response consumer to the
 * {@link Flow.Publisher} the downstream response processors consume.</p>
 *
 * <p>The reactive consumer emits chunks on the http client's IO reactor threads. Response parsing must not run there, as it would
 * stall the IO reactor, so each signal is handed off to the {@code utility} thread pool. A queue plus {@link RequestBasedTaskRunner}
 * guarantees the signals are delivered serially, as required by the reactive spec, even though the thread pool has multiple threads.
 * Demand is forwarded to the upstream subscription, which the http client translates into channel-level backpressure.</p>
 *
 * <p>Every buffered chunk is accounted against the inference circuit breaker until it is delivered downstream, so many concurrent
 * streams with slow consumers trip the breaker instead of accumulating unaccounted heap.</p>
 *
 * <p>Stalled exchanges are bounded by {@code xpack.inference.http.socket_timeout}, enforced by the IO reactor at the connection
 * level; a downstream cancellation additionally runs {@code abortExchange}, which cancels the exchange future so the leased pool
 * connection is released promptly instead of waiting for that timeout (the reactive {@code Subscription#cancel()} alone is a flag
 * only observed when the next chunk arrives).</p>
 *
 * <p>One deliberate Reactive Streams spec deviation: terminal signals consume a unit of demand. §2.9 says a subscriber must be
 * prepared to receive {@code onComplete} without a preceding {@code request(n)}, but {@code ServerSentEventsRestActionListener}
 * asserts a body-part listener is present ({@code nextBodyPartListener()}), so {@code onComplete}/{@code onError} are withheld
 * until the downstream requests. This matches the 4.x {@code DataPublisher} this class replaces.</p>
 */
class ByteArrayFlowPublisher implements Flow.Publisher<byte[]> {

    private final Flow.Publisher<ByteBuffer> upstream;
    private final ThreadPool threadPool;
    private final CircuitBreaker circuitBreaker;
    private final String inferenceEntityId;
    private final Runnable abortExchange;
    private final AtomicReference<RelaySubscriber> relay = new AtomicReference<>();
    // set once the stream reached a terminal state; late chunks are dropped without breaker accounting
    private volatile boolean closed = false;

    ByteArrayFlowPublisher(
        Publisher<ByteBuffer> upstream,
        ThreadPool threadPool,
        CircuitBreaker circuitBreaker,
        String inferenceEntityId,
        Runnable abortExchange
    ) {
        this.upstream = FlowAdapters.toFlowPublisher(Objects.requireNonNull(upstream));
        this.threadPool = Objects.requireNonNull(threadPool);
        this.circuitBreaker = Objects.requireNonNull(circuitBreaker);
        this.inferenceEntityId = Objects.requireNonNull(inferenceEntityId);
        this.abortExchange = Objects.requireNonNull(abortExchange);
    }

    @Override
    public void subscribe(Flow.Subscriber<? super byte[]> subscriber) {
        var relaySubscriber = new RelaySubscriber(subscriber);
        if (relay.compareAndSet(null, relaySubscriber) == false) {
            subscriber.onSubscribe(new Flow.Subscription() {
                @Override
                public void request(long n) {}

                @Override
                public void cancel() {}
            });
            subscriber.onError(new IllegalStateException(format("Stream for inference id [%s] is no longer available", inferenceEntityId)));
            return;
        }
        upstream.subscribe(relaySubscriber);
    }

    private static byte[] toBytes(ByteBuffer buffer) {
        // always copy: the upstream owns the buffer and may reuse it after onNext returns
        var bytes = new byte[buffer.remaining()];
        buffer.get(bytes);
        return bytes;
    }

    private class RelaySubscriber implements Flow.Subscriber<ByteBuffer> {
        private final Flow.Subscriber<? super byte[]> downstream;
        private final RequestBasedTaskRunner taskRunner;
        private final Deque<byte[]> contentQueue = new ConcurrentLinkedDeque<>();
        private final AtomicBoolean terminated = new AtomicBoolean(false);
        private final AtomicLong unreleasedBytes = new AtomicLong(0);
        private final AtomicLong pendingRequests = new AtomicLong(0);
        private volatile Flow.Subscription upstreamSubscription;
        private final AtomicReference<Exception> error = new AtomicReference<>();
        private volatile boolean completed = false;

        RelaySubscriber(Flow.Subscriber<? super byte[]> downstream) {
            this.downstream = Objects.requireNonNull(downstream);
            this.taskRunner = new RequestBasedTaskRunner(this::sendToSubscriber, threadPool, UTILITY_THREAD_POOL_NAME);
        }

        @Override
        public void onSubscribe(Flow.Subscription subscription) {
            upstreamSubscription = subscription;
            downstream.onSubscribe(new Flow.Subscription() {
                @Override
                public void request(long n) {
                    if (n <= 0) {
                        abort(new IllegalArgumentException("Subscriber requested a non-positive number " + n));
                        return;
                    }
                    pendingRequests.addAndGet(n);
                    subscription.request(n);
                    taskRunner.requestNextRun();
                }

                @Override
                public void cancel() {
                    close();
                    subscription.cancel();
                    abortExchange.run();
                    taskRunner.cancel();
                }
            });
        }

        @Override
        public void onNext(ByteBuffer item) {
            var bytes = toBytes(item);
            if (closed) {
                return;
            }
            try {
                circuitBreaker.addEstimateBytesAndMaybeBreak(bytes.length, inferenceEntityId);
            } catch (Exception e) {
                abort(e);
                return;
            }
            unreleasedBytes.addAndGet(bytes.length);
            if (closed) {
                // the stream was closed while accounting; make sure the bytes do not stay claimed forever
                releaseBreakerBytes(bytes.length);
                return;
            }
            contentQueue.offer(bytes);
            taskRunner.requestNextRun();
        }

        @Override
        public void onError(Throwable throwable) {
            if (throwable instanceof Exception e) {
                error.compareAndSet(null, e);
            } else {
                ExceptionsHelper.maybeError(throwable).ifPresent(ExceptionsHelper::maybeDieOnAnotherThread);
                error.compareAndSet(null, new RuntimeException("Unhandled error while streaming", throwable));
            }
            // the terminal signal downstream still needs demand, which may never arrive; free the reservation now
            close();
            taskRunner.requestNextRun();
        }

        @Override
        public void onComplete() {
            completed = true;
            taskRunner.requestNextRun();
        }

        private void sendToSubscriber() {
            // Deliver at most `pendingRequests` chunks, decrementing per delivery, so the downstream never receives more signals
            // than it requested. An error preempts queued data, matching the previous publisher's behavior.
            byte[] nextBytes;
            while (error.get() == null && pendingRequests.get() > 0 && (nextBytes = contentQueue.poll()) != null) {
                pendingRequests.decrementAndGet();
                releaseBreakerBytes(nextBytes.length);
                downstream.onNext(nextBytes);
            }

            // Terminal signals also consume a unit of demand, so they are only delivered when the downstream has an outstanding
            // request. If it does not yet, the next request(n) reschedules this run and delivers them then.
            var failure = error.get();
            if (failure != null) {
                if (pendingRequests.get() > 0 && terminated.compareAndSet(false, true)) {
                    pendingRequests.decrementAndGet();
                    close();
                    downstream.onError(failure);
                }
            } else if (completed && contentQueue.isEmpty() && pendingRequests.get() > 0 && terminated.compareAndSet(false, true)) {
                pendingRequests.decrementAndGet();
                close();
                downstream.onComplete();
            }
        }

        /**
         * Cancels the upstream exchange (releasing the leased pool connection) and fails the downstream subscriber. Used when the
         * circuit breaker trips or the downstream violates the subscription contract.
         */
        void abort(Exception e) {
            error.compareAndSet(null, e);
            var subscription = upstreamSubscription;
            if (subscription != null) {
                subscription.cancel();
            }
            abortExchange.run();
            close();
            taskRunner.requestNextRun();
        }

        private void close() {
            closed = true;
            releaseBreakerBytes(unreleasedBytes.get());
        }

        /**
         * Releases up to {@code count} bytes from the breaker, clamped to what is still claimed so a concurrent
         * {@link #close()} and a delivery never release the same bytes twice.
         */
        private void releaseBreakerBytes(long count) {
            while (true) {
                long current = unreleasedBytes.get();
                long release = Math.min(current, count);
                if (release <= 0) {
                    return;
                }
                if (unreleasedBytes.compareAndSet(current, current - release)) {
                    circuitBreaker.addWithoutBreaking(-release);
                    return;
                }
            }
        }
    }
}
