/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.response.amazonbedrock.completion;

import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.xpack.inference.external.http.HttpResult;
import org.elasticsearch.xpack.inference.external.http.retry.RetryException;
import org.elasticsearch.xpack.inference.external.request.Request;
import org.elasticsearch.xpack.inference.external.request.amazonbedrock.completion.AmazonBedrockChatCompletionRequest;
import org.elasticsearch.xpack.inference.external.response.amazonbedrock.AmazonBedrockResponseHandler;

public class AmazonBedrockChatCompletionResponseHandler extends AmazonBedrockResponseHandler {

    public AmazonBedrockChatCompletionResponseHandler() {}

    @Override
    public InferenceServiceResults parseResult(Request request, HttpResult result) throws RetryException {
        if (request instanceof AmazonBedrockChatCompletionRequest awsRequest) {
            var response = new AmazonBedrockChatCompletionResponse();
            return response.accept(awsRequest);
        }

        // TODO -- throw
        return null;
    }

    @Override
    public String getRequestType() {
        return "Amazon Bedrock Chat Completion";
    }
}
