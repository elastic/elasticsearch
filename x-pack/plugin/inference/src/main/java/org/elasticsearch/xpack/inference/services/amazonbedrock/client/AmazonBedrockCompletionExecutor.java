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
import org.elasticsearch.xpack.core.inference.results.StreamingChatCompletionResults;
import org.elasticsearch.xpack.inference.services.amazonbedrock.request.completion.AmazonBedrockCompletionRequest;
import org.elasticsearch.xpack.inference.services.amazonbedrock.response.AmazonBedrockResponseHandler;
import org.elasticsearch.xpack.inference.services.amazonbedrock.response.completion.AmazonBedrockCompletionResponseListener;

import java.util.function.Supplier;

public class AmazonBedrockCompletionExecutor extends AmazonBedrockExecutor {
    private final AmazonBedrockCompletionRequest completionRequest;

    protected AmazonBedrockCompletionExecutor(
        AmazonBedrockCompletionRequest request,
        AmazonBedrockResponseHandler responseHandler,
        Logger logger,
        Supplier<Boolean> hasRequestCompletedFunction,
        ActionListener<InferenceServiceResults> inferenceResultsListener,
        AmazonBedrockClientCache clientCache
    ) {
        super(request, responseHandler, logger, hasRequestCompletedFunction, inferenceResultsListener, clientCache);
        this.completionRequest = request;
    }

    @Override
    protected void executeClientRequest(AmazonBedrockBaseClient awsBedrockClient) {
        if (completionRequest.isStreaming()) {
            var publisher = completionRequest.executeStreamCompletionRequest(awsBedrockClient);
            inferenceResultsListener.onResponse(new StreamingChatCompletionResults(publisher));
        } else {
            var completionResponseListener = new AmazonBedrockCompletionResponseListener(
                completionRequest,
                responseHandler,
                inferenceResultsListener
            );
            completionRequest.executeCompletionRequest(awsBedrockClient, completionResponseListener);
        }
    }
}
