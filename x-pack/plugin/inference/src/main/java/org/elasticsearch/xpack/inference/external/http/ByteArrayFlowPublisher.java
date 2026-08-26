/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.http;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.threadpool.Scheduler;
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
 * Demand and cancellation are forwarded to the upstream subscription, which the http client translates into channel-level
 * backpressure and connection release.</p>
 *
 * <p>Every buffered chunk is accounted against the inference circuit breaker until it is delivered downstream, so many concurrent
 * streams with slow consumers trip the breaker instead of accumulating unaccounted heap.</p>
 *
 * <p>A watchdog aborts the exchange when the stream makes no progress (no subscription, demand, or chunk) for
 * {@link #STALE_STREAM_TIMEOUT}. Without it, a stream whose consumer disappeared (e.g. the per-request timeout fired before the
 * response head arrived, so nobody drives demand) would stall on channel backpressure and hold its pooled connection leased
 * forever, eventually exhausting the pool.</p>
 */
class ByteArrayFlowPublisher implements Flow.Publisher<byte[]> {
    private static final Logger logger = LogManager.getLogger(ByteArrayFlowPublisher.class);

    // Package private for testing. Providers can pause between SSE chunks, so this must comfortably exceed legitimate
    // mid-stream gaps; it only needs to be short enough to reclaim leaked pool connections in a bounded amount of time.
    static final TimeValue STALE_STREAM_TIMEOUT = TimeValue.timeValueMinutes(5);
    private static final TimeValue WATCHDOG_INTERVAL = TimeValue.timeValueMinutes(1);

    private final Flow.Publisher<ByteBuffer> upstream;
    private final ThreadPool threadPool;
    private final CircuitBreaker circuitBreaker;
    private final String inferenceEntityId;
    private final AtomicLong lastActivityMillis;
    private final AtomicReference<RelaySubscriber> relay = new AtomicReference<>();
    private final AtomicBoolean abortedBeforeSubscribe = new AtomicBoolean(false);
    // set once the stream reached a terminal state; late chunks are dropped without breaker accounting
    private volatile boolean closed = false;
    private final Scheduler.Cancellable watchdog;

    ByteArrayFlowPublisher(Publisher<ByteBuffer> upstream, ThreadPool threadPool, CircuitBreaker circuitBreaker, String inferenceEntityId) {
        this.upstream = FlowAdapters.toFlowPublisher(Objects.requireNonNull(upstream));
        this.threadPool = Objects.requireNonNull(threadPool);
        this.circuitBreaker = Objects.requireNonNull(circuitBreaker);
        this.inferenceEntityId = Objects.requireNonNull(inferenceEntityId);
        this.lastActivityMillis = new AtomicLong(threadPool.relativeTimeInMillis());
        this.watchdog = threadPool.scheduleWithFixedDelay(
            this::checkProgress,
            WATCHDOG_INTERVAL,
            threadPool.executor(UTILITY_THREAD_POOL_NAME)
        );
    }

    @Override
    public void subscribe(Flow.Subscriber<? super byte[]> subscriber) {
        touch();
        var relaySubscriber = new RelaySubscriber(subscriber);
        if (abortedBeforeSubscribe.get() || relay.compareAndSet(null, relaySubscriber) == false) {
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

    private void touch() {
        lastActivityMillis.set(threadPool.relativeTimeInMillis());
    }

    private void cancelWatchdog() {
        var scheduled = watchdog;
        if (scheduled != null) {
            scheduled.cancel();
        }
    }

    private void checkProgress() {
        if (closed) {
            cancelWatchdog();
            return;
        }
        if (threadPool.relativeTimeInMillis() - lastActivityMillis.get() < STALE_STREAM_TIMEOUT.millis()) {
            return;
        }

        var relaySubscriber = relay.get();
        if (relaySubscriber != null) {
            relaySubscriber.abort(
                new IllegalStateException(
                    format("Aborting stream for inference id [%s] after [%s] without progress", inferenceEntityId, STALE_STREAM_TIMEOUT)
                )
            );
        } else if (abortedBeforeSubscribe.compareAndSet(false, true)) {
            closed = true;
            cancelWatchdog();
            logger.warn(
                "Cancelling stream for inference id [{}]: no consumer subscribed within [{}]",
                inferenceEntityId,
                STALE_STREAM_TIMEOUT
            );
            // subscribe only to cancel, which fails the exchange and releases the leased pool connection
            upstream.subscribe(new Flow.Subscriber<>() {
                @Override
                public void onSubscribe(Flow.Subscription subscription) {
                    subscription.cancel();
                }

                @Override
                public void onNext(ByteBuffer item) {}

                @Override
                public void onError(Throwable throwable) {}

                @Override
                public void onComplete() {}
            });
        }
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
        private volatile Flow.Subscription upstreamSubscription;
        private volatile Exception error;
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
                    touch();
                    subscription.request(n);
                }

                @Override
                public void cancel() {
                    close();
                    subscription.cancel();
                    taskRunner.cancel();
                }
            });
        }

        @Override
        public void onNext(ByteBuffer item) {
            touch();
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
            touch();
            if (throwable instanceof Exception e) {
                error = e;
            } else {
                ExceptionsHelper.maybeError(throwable).ifPresent(ExceptionsHelper::maybeDieOnAnotherThread);
                error = new RuntimeException("Unhandled error while streaming", throwable);
            }
            taskRunner.requestNextRun();
        }

        @Override
        public void onComplete() {
            touch();
            completed = true;
            taskRunner.requestNextRun();
        }

        private void sendToSubscriber() {
            byte[] nextBytes;
            while ((nextBytes = contentQueue.poll()) != null) {
                releaseBreakerBytes(nextBytes.length);
                downstream.onNext(nextBytes);
            }

            // the upstream only emits what the downstream requested, so the queue can only refill after another onNext delivery,
            // which will schedule another run; the terminal signal is delivered once the queue has fully drained
            if (error != null) {
                if (terminated.compareAndSet(false, true)) {
                    close();
                    downstream.onError(error);
                }
            } else if (completed && contentQueue.isEmpty() && terminated.compareAndSet(false, true)) {
                close();
                downstream.onComplete();
            }
        }

        /**
         * Cancels the upstream exchange (releasing the leased pool connection) and fails the downstream subscriber. Used when the
         * circuit breaker trips or the stream stalls without progress.
         */
        void abort(Exception e) {
            error = e;
            var subscription = upstreamSubscription;
            if (subscription != null) {
                subscription.cancel();
            }
            taskRunner.requestNextRun();
        }

        private void close() {
            closed = true;
            cancelWatchdog();
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
