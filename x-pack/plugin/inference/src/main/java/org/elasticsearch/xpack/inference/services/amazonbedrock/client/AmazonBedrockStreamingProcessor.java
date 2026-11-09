/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.amazonbedrock.client;

import software.amazon.awssdk.services.bedrockruntime.model.ConverseStreamOutput;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.inference.services.amazonbedrock.AmazonBedrockExceptions;

import java.util.concurrent.Flow;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.xpack.inference.InferencePlugin.UTILITY_THREAD_POOL_NAME;

abstract class AmazonBedrockStreamingProcessor<T> implements Flow.Processor<ConverseStreamOutput, T> {
    private static final Logger logger = LogManager.getLogger(AmazonBedrockStreamingProcessor.class);

    private final AtomicReference<Throwable> error = new AtomicReference<>(null);
    private final AtomicBoolean onErrorCalled = new AtomicBoolean(false);
    private final ThreadPool threadPool;
    /**
     * The purpose of demand is solely to guard against the situation where the bedrock sdk can complete the future before the publisher
     * and subscriber aren't connected together via {@link #subscribe(Flow.Subscriber)} and {@link AmazonBedrockInferenceClient}
     * getAmazonBedrockStreamingProcessor(). We should refactor this logic to be like
     * {@link org.elasticsearch.xpack.inference.services.sagemaker.SageMakerClient} which tracks the subscriber and publisher via atomic
     * references instead of using a demand variable.
     */
    final AtomicLong demand = new AtomicLong(0);
    final AtomicBoolean isDone = new AtomicBoolean(false);
    final AtomicBoolean onCompleteCalled = new AtomicBoolean(false);
    /**
     * Set once {@link Flow.Subscriber#onSubscribe} has returned for the downstream, so that terminal signals are never sent before it.
     */
    private final AtomicBoolean downstreamReady = new AtomicBoolean(false);
    private final AtomicBoolean downstreamCancelled = new AtomicBoolean(false);
    /**
     * The number of items the downstream has requested that have not been answered by {@link #emitNext} yet. Unlike {@link #demand}, which
     * is reset whenever an event starts being processed, this is only decremented when an item is really sent downstream. Terminal errors
     * are only signalled while this is positive, because downstream subscribers (e.g. the REST SSE subscriber) only expect signals in
     * response to a request. An error that arrives with no outstanding request is parked and delivered by the next request.
     */
    private final AtomicLong downstreamDemand = new AtomicLong(0);

    volatile Flow.Subscription upstream;

    volatile Flow.Subscriber<? super T> downstream;

    @Override
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

    @Override
    public void subscribe(Flow.Subscriber<? super T> subscriber) {
        if (downstream == null) {
            downstream = subscriber;
            downstream.onSubscribe(new StreamSubscription());
            downstreamReady.set(true);
            // the upstream may have failed before the downstream was ready, and errors do not require demand
            deliverErrorIfPending();
        } else {
            subscriber.onError(new IllegalStateException("Subscriber already set."));
        }
    }

    @Override
    public void onError(Throwable amazonBedrockRuntimeException) {
        ExceptionsHelper.maybeDieOnAnotherThread(amazonBedrockRuntimeException);
        var wrappedException = AmazonBedrockExceptions.wrap(
            Strings.format("AmazonBedrock StreamingChatProcessor failure: [%s]", amazonBedrockRuntimeException.getMessage()),
            amazonBedrockRuntimeException
        );
        error.set(wrappedException);
        isDone.set(true);
        deliverErrorIfPending();
    }

    /**
     * Sends the stored error downstream if there is one and the downstream is able to receive it: it has returned from onSubscribe, has
     * not cancelled, has an outstanding request and has not been completed. Otherwise the error stays stored and this is retried when the
     * downstream subscribes or requests. Safe to call repeatedly, the error is only ever delivered once.
     */
    private void deliverErrorIfPending() {
        if (downstreamReady.get()
            && downstreamCancelled.get() == false
            && error.get() != null
            && downstreamDemand.get() > 0
            && onCompleteCalled.get() == false
            && onErrorCalled.compareAndSet(false, true)) {
            runOnUtilityThreadPool(() -> downstream.onError(error.get()));
        }
    }

    /**
     * Sends an item downstream, consuming one of the downstream's outstanding requests.
     */
    void emitNext(T item) {
        downstreamDemand.updateAndGet(i -> i > 0 ? i - 1 : 0);
        downstream.onNext(item);
    }

    private boolean checkAndResetDemand() {
        return demand.getAndUpdate(i -> 0L) > 0L;
    }

    @Override
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
                downstreamDemand.updateAndGet(i -> {
                    var sum = i + n;
                    return sum >= 0 ? sum : Long.MAX_VALUE;
                });
                if (error.get() != null) {
                    // a stored error always wins over completion, even if its delivery has already been queued
                    deliverErrorIfPending();
                    return;
                }
                if (upstream == null) {
                    // wait for upstream to subscribe before forwarding request
                    return;
                }
                if (upstreamIsRunning()) {
                    requestOnMlThread(n);
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
            downstreamCancelled.set(true);
            if (upstream != null && upstreamIsRunning()) {
                upstream.cancel();
            }
        }
    }
}
