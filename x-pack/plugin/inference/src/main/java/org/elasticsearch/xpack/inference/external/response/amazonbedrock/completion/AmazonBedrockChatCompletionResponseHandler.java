/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.response.amazonbedrock.completion;

import com.amazonaws.services.bedrockruntime.model.ConverseResult;

import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.xpack.inference.external.http.HttpResult;
import org.elasticsearch.xpack.inference.external.http.retry.RetryException;
import org.elasticsearch.xpack.inference.external.request.Request;
import org.elasticsearch.xpack.inference.external.request.amazonbedrock.AmazonBedrockRequest;
import org.elasticsearch.xpack.inference.external.response.amazonbedrock.AmazonBedrockResponseHandler;

public class AmazonBedrockChatCompletionResponseHandler extends AmazonBedrockResponseHandler {

    private ConverseResult responseResult;

    public AmazonBedrockChatCompletionResponseHandler() {}

    @Override
    public InferenceServiceResults parseResult(Request request, HttpResult result) throws RetryException {
        var response = new AmazonBedrockChatCompletionResponse(responseResult);
        return response.accept((AmazonBedrockRequest) request);
    }

    @Override
    public String getRequestType() {
        return "Amazon Bedrock Chat Completion";
    }

    public void acceptChatCompletionResponseObject(ConverseResult response) {
        this.responseResult = response;
    }
}
