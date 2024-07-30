/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.amazonbedrock;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.xpack.inference.external.request.amazonbedrock.completion.AmazonBedrockChatCompletionRequest;
import org.elasticsearch.xpack.inference.external.response.amazonbedrock.AmazonBedrockResponseHandler;
import org.elasticsearch.xpack.inference.external.response.amazonbedrock.completion.AmazonBedrockChatCompletionResponseListener;

import java.util.function.Supplier;

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
        var chatCompletionResponseListener = new AmazonBedrockChatCompletionResponseListener(
            chatCompletionRequest,
            responseHandler,
            inferenceResultsListener
        );
        chatCompletionRequest.executeChatCompletionRequest(awsBedrockClient, chatCompletionResponseListener);
    }
}
