/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.amazonbedrock.client;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.concurrent.Flow;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.xpack.inference.InferencePlugin.UTILITY_THREAD_POOL_NAME;

class AmazonBedrockStreamingProcessor<T> {
    private static final Logger logger = LogManager.getLogger(AmazonBedrockStreamingProcessor.class);

    final AtomicReference<Throwable> error = new AtomicReference<>(null);
    final AtomicLong demand = new AtomicLong(0);
    final AtomicBoolean isDone = new AtomicBoolean(false);
    final AtomicBoolean onCompleteCalled = new AtomicBoolean(false);
    final AtomicBoolean onErrorCalled = new AtomicBoolean(false);
    final ThreadPool threadPool;
    volatile Flow.Subscription upstream;

    volatile Flow.Subscriber<? super T> downstream;

    public void onSubscribe(Flow.Subscription subscription) {
        if (upstream == null) {
            upstream = subscription;
            var currentRequestCount = demand.getAndUpdate(i -> 0);
            if (currentRequestCount > 0) {
                upstream.request(currentRequestCount);
            }
        } else {
            subscription.cancel();
        }
    }

    public void subscribe(Flow.Subscriber<? super T> subscriber) {
        if (downstream == null) {
            downstream = subscriber;
            downstream.onSubscribe(new StreamSubscription());
        } else {
            subscriber.onError(new IllegalStateException("Subscriber already set."));
        }
    }

    public void onError(Throwable amazonBedrockRuntimeException) {
        ExceptionsHelper.maybeDieOnAnotherThread(amazonBedrockRuntimeException);
        error.set(
            new ElasticsearchException(
                Strings.format(
                    "AmazonBedrock StreamingChatProcessor failure: [%s]",
                    amazonBedrockRuntimeException.getMessage()
                ),
                amazonBedrockRuntimeException
            )
        );
        if (isDone.compareAndSet(false, true) && checkAndResetDemand() && onErrorCalled.compareAndSet(false, true)) {
            runOnUtilityThreadPool(() -> downstream.onError(amazonBedrockRuntimeException));
        }
    }

    private boolean checkAndResetDemand() {
        return demand.getAndUpdate(i -> 0L) > 0L;
    }

    public void onComplete() {
        if (isDone.compareAndSet(false, true) && checkAndResetDemand() && onCompleteCalled.compareAndSet(false, true)) {
            downstream.onComplete();
        }
    }

    protected AmazonBedrockStreamingProcessor(ThreadPool threadPool) {
        this.threadPool = threadPool;
    }

    void runOnUtilityThreadPool(Runnable runnable) {
        try {
            threadPool.executor(UTILITY_THREAD_POOL_NAME).execute(runnable);
        } catch (Exception e) {
            logger.error(Strings.format("failed to fork [%s] to utility thread pool", runnable), e);
        }
    }

    class StreamSubscription implements Flow.Subscription {
        @Override
        public void request(long n) {
            if (n > 0L) {
                demand.updateAndGet(i -> {
                    var sum = i + n;
                    return sum >= 0 ? sum : Long.MAX_VALUE;
                });
                if (upstream == null) {
                    // wait for upstream to subscribe before forwarding request
                    return;
                }
                if (upstreamIsRunning()) {
                    requestOnMlThread(n);
                } else if (error.get() != null && onErrorCalled.compareAndSet(false, true)) {
                    downstream.onError(error.get());
                } else if (onCompleteCalled.compareAndSet(false, true)) {
                    downstream.onComplete();
                }
            } else {
                cancel();
                downstream.onError(new IllegalStateException("Cannot request a negative number."));
            }
        }

        private boolean upstreamIsRunning() {
            return isDone.get() == false && error.get() == null;
        }

        private void requestOnMlThread(long n) {
            var currentThreadPool = EsExecutors.executorName(Thread.currentThread());
            if (UTILITY_THREAD_POOL_NAME.equalsIgnoreCase(currentThreadPool)) {
                upstream.request(n);
            } else {
                runOnUtilityThreadPool(() -> upstream.request(n));
            }
        }

        @Override
        public void cancel() {
            if (upstream != null && upstreamIsRunning()) {
                upstream.cancel();
            }
        }
    }
}
