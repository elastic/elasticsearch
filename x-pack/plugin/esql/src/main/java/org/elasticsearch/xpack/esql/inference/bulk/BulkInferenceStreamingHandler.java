/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.inference.bulk;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.xpack.core.inference.action.InferenceAction;
import org.elasticsearch.xpack.core.inference.results.ChatCompletionResults;
import org.elasticsearch.xpack.core.inference.results.StreamingUnifiedChatCompletionResults;

import java.util.List;
import java.util.concurrent.Flow;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Handles streaming inference responses for chat completion requests in bulk inference operations.
 * <p>
 * This class implements the Reactive Streams {@link Flow.Subscriber} interface to process
 * streaming inference results from chat completion services. It accumulates content from
 * streaming chunks and delivers the final aggregated response to the completion listener.
 * <p>
 * Ultimately, it constructs a {@link ChatCompletionResults} object containing the
 * complete content from all streaming chunks received during the inference operation.
 * By doing so, the result of chat_completion requests is made available is the same as the legacu completion
 * and can be consumed in the same way.
 * </p>
 */
class BulkInferenceStreamingHandler implements Flow.Subscriber<InferenceServiceResults.Result> {

    /**
     * Flag to track whether this streaming session has completed to prevent duplicate processing.
     */
    private final AtomicBoolean isLastPart = new AtomicBoolean(false);

    /**
     * The subscription handle for controlling the flow of streaming data.
     */
    private Flow.Subscription subscription;

    /**
     * Buffer for accumulating content from streaming chunks into the final response.
     */
    private final StringBuilder resultBuilder = new StringBuilder();

    /**
     * Listener to receive the final aggregated inference response.
     */
    private final ActionListener<InferenceAction.Response> inferenceResponseListener;

    /**
     * Creates a new streaming handler for processing inference chat_completion responses.
     *
     * @param inferenceResponseListener The listener that will receive the final aggregated response
     *                                  once all streaming chunks have been processed
     */
    BulkInferenceStreamingHandler(ActionListener<InferenceAction.Response> inferenceResponseListener) {
        this.inferenceResponseListener = inferenceResponseListener;
    }

    /**
     * Called when the streaming publisher is ready to start sending data.
     * <p>
     * This method establishes the subscription and requests the first chunk of data.
     * If the streaming session has already completed, it cancels the subscription
     * to prevent resource leaks.
     * </p>
     *
     * @param subscription The subscription handle for controlling data flow
     */
    @Override
    public void onSubscribe(Flow.Subscription subscription) {
        if (isLastPart.get() == false) {
            this.subscription = subscription;
            subscription.request(1);
        } else {
            subscription.cancel();
        }
    }

    /**
     * Processes each streaming chunk as it arrives from the inference service.
     * <p>
     * This method extracts content from streaming chat completion chunks and accumulates
     * it in the result builder. It handles the specific structure of streaming unified
     * chat completion results, extracting text content from delta objects within choices.
     * </p>
     * <p>
     * After processing each chunk, it requests the next chunk from the subscription
     * to continue the streaming process.
     * </p>
     *
     * @param item The streaming result item containing chunk data from the inference service
     */
    @Override
    public void onNext(InferenceServiceResults.Result item) {
        if (isLastPart.get() == false) {
            if (item instanceof StreamingUnifiedChatCompletionResults.Results streamingChunkResults) {
                for (var chunk : streamingChunkResults.chunks()) {
                    for (var choice : chunk.choices()) {
                        if (choice.delta() != null && choice.delta().content() != null) {
                            resultBuilder.append(choice.delta().content());
                        }
                    }
                }
                subscription.request(1);
            } else {
                // Handle unexpected result types by requesting the next item
                subscription.request(1);
            }
        }
    }

    /**
     * Called when an error occurs during streaming processing.
     * <p>
     * This method ensures that errors are properly propagated to the inference listener
     * and that the streaming session is marked as completed to prevent further processing.
     * </p>
     *
     * @param throwable The error that occurred during streaming
     */
    @Override
    public void onError(Throwable throwable) {
        if (isLastPart.compareAndSet(false, true)) {
            inferenceResponseListener.onFailure(new RuntimeException("Streaming inference failed", throwable));
        }
    }

    /**
     * Called when the streaming process completes successfully.
     * <p>
     * This method finalizes the streaming process by creating a complete inference response
     * from the accumulated content and delivering it to the listener. It constructs a
     * {@link ChatCompletionResults} object containing the aggregated content from all
     * streaming chunks.
     * </p>
     */
    @Override
    public void onComplete() {
        if (isLastPart.compareAndSet(false, true)) {
            // Create the final aggregated response from accumulated content
            String finalContent = resultBuilder.toString();
            ChatCompletionResults.Result completionResult = new ChatCompletionResults.Result(finalContent);
            ChatCompletionResults chatResults = new ChatCompletionResults(List.of(completionResult));
            InferenceAction.Response response = new InferenceAction.Response(chatResults);

            // Deliver the final response to the listener
            inferenceResponseListener.onResponse(response);
        }
    }
}
