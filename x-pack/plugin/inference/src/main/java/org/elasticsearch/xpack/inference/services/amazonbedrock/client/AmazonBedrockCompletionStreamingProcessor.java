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

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.core.Strings;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.inference.results.StreamingChatCompletionResults;

import java.util.ArrayDeque;
import java.util.concurrent.Flow;

class AmazonBedrockCompletionStreamingProcessor extends AmazonBedrockStreamingProcessor<StreamingChatCompletionResults.Results>
    implements
        Flow.Processor<ConverseStreamOutput, StreamingChatCompletionResults.Results> {
    private static final Logger logger = LogManager.getLogger(AmazonBedrockCompletionStreamingProcessor.class);

    protected AmazonBedrockCompletionStreamingProcessor(ThreadPool threadPool) {
        super(threadPool);
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
}
