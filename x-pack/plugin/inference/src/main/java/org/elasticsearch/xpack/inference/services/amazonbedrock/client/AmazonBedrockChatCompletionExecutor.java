/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.amazonbedrock.client;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.xpack.core.inference.results.StreamingUnifiedChatCompletionResults;
import org.elasticsearch.xpack.inference.services.amazonbedrock.request.completion.AmazonBedrockChatCompletionRequest;
import org.elasticsearch.xpack.inference.services.amazonbedrock.response.AmazonBedrockResponseHandler;

import java.util.function.Supplier;

import static org.elasticsearch.xpack.inference.action.TransportInferenceActionProxy.CHAT_COMPLETION_STREAMING_ONLY_EXCEPTION;

public class AmazonBedrockChatCompletionExecutor extends AmazonBedrockExecutor {
    private final AmazonBedrockChatCompletionRequest chatCompletionRequest;

    protected AmazonBedrockChatCompletionExecutor(
        AmazonBedrockChatCompletionRequest request,
        AmazonBedrockResponseHandler responseHandler,
        Logger logger,
        Supplier<Boolean> hasRequestCompletedFunction,
        ActionListener<InferenceServiceResults> inferenceResultsListener,
        AmazonBedrockClientCache clientCache
    ) {
        super(request, responseHandler, logger, hasRequestCompletedFunction, inferenceResultsListener, clientCache);
        this.chatCompletionRequest = request;
    }

    @Override
    protected void executeClientRequest(AmazonBedrockBaseClient awsBedrockClient) {
        // Chat completions only supports streaming
        if (chatCompletionRequest.isStreaming() == false) {
            inferenceResultsListener.onFailure(CHAT_COMPLETION_STREAMING_ONLY_EXCEPTION);
            return;
        }

        var publisher = chatCompletionRequest.executeStreamChatCompletionRequest(awsBedrockClient);
        inferenceResultsListener.onResponse(new StreamingUnifiedChatCompletionResults(publisher));
    }
}
