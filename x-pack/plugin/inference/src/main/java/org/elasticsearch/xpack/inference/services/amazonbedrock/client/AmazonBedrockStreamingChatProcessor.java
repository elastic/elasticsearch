/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.amazonbedrock.client;

import software.amazon.awssdk.services.bedrockruntime.model.ContentBlockDeltaEvent;
import software.amazon.awssdk.services.bedrockruntime.model.ConverseStreamOutput;
import software.amazon.awssdk.services.bedrockruntime.model.ConverseStreamResponseHandler;

import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.core.Strings;
import org.elasticsearch.exception.ElasticsearchException;
import org.elasticsearch.exception.ExceptionsHelper;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.inference.results.StreamingChatCompletionResults;

import java.util.ArrayDeque;
import java.util.concurrent.Flow;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.xpack.inference.InferencePlugin.UTILITY_THREAD_POOL_NAME;

class AmazonBedrockStreamingChatProcessor implements Flow.Processor<ConverseStreamOutput, StreamingChatCompletionResults.Results> {
    private static final Logger logger = LogManager.getLogger(AmazonBedrockStreamingChatProcessor.class);

    private final AtomicReference<Throwable> error = new AtomicReference<>(null);
    private final AtomicLong demand = new AtomicLong(0);
    private final AtomicBoolean isDone = new AtomicBoolean(false);
    private final AtomicBoolean onCompleteCalled = new AtomicBoolean(false);
    private final AtomicBoolean onErrorCalled = new AtomicBoolean(false);
    private final ThreadPool threadPool;
    private volatile Flow.Subscriber<? super StreamingChatCompletionResults.Results> downstream;
    private volatile Flow.Subscription upstream;

    AmazonBedrockStreamingChatProcessor(ThreadPool threadPool) {
        this.threadPool = threadPool;
    }

    @Override
    public void subscribe(Flow.Subscriber<? super StreamingChatCompletionResults.Results> subscriber) {
        if (downstream == null) {
            downstream = subscriber;
            downstream.onSubscribe(new StreamSubscription());
        } else {
            subscriber.onError(new IllegalStateException("Subscriber already set."));
        }
    }

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
    public void onNext(ConverseStreamOutput item) {
        if (item.sdkEventType() == ConverseStreamOutput.EventType.CONTENT_BLOCK_DELTA) {
            demand.set(0); // reset demand before we fork to another thread
            item.accept(ConverseStreamResponseHandler.Visitor.builder().onContentBlockDelta(this::sendDownstreamOnAnotherThread).build());
        } else {
            upstream.request(1);
        }
    }

    // this is always called from a netty thread maintained by the AWS SDK, we'll move it to our thread to process the response
    private void sendDownstreamOnAnotherThread(ContentBlockDeltaEvent event) {
        runOnUtilityThreadPool(() -> {
            var text = event.delta().text();
            var result = new ArrayDeque<StreamingChatCompletionResults.Result>(1);
            result.offer(new StreamingChatCompletionResults.Result(text));
            var results = new StreamingChatCompletionResults.Results(result);
            downstream.onNext(results);
        });
    }

    @Override
    public void onError(Throwable amazonBedrockRuntimeException) {
        ExceptionsHelper.maybeDieOnAnotherThread(amazonBedrockRuntimeException);
        error.set(
            new ElasticsearchException(
                Strings.format("AmazonBedrock StreamingChatProcessor failure: [%s]", amazonBedrockRuntimeException.getMessage()),
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

    @Override
    public void onComplete() {
        if (isDone.compareAndSet(false, true) && checkAndResetDemand() && onCompleteCalled.compareAndSet(false, true)) {
            downstream.onComplete();
        }
    }

    private void runOnUtilityThreadPool(Runnable runnable) {
        try {
            threadPool.executor(UTILITY_THREAD_POOL_NAME).execute(runnable);
        } catch (Exception e) {
            logger.error(Strings.format("failed to fork [%s] to utility thread pool", runnable), e);
        }
    }

    private class StreamSubscription implements Flow.Subscription {
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
            var currentThreadPool = EsExecutors.executorName(Thread.currentThread().getName());
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
