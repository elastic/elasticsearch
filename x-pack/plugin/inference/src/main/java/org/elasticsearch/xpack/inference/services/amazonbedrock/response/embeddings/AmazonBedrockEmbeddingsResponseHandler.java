/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.amazonbedrock.response.embeddings;

import software.amazon.awssdk.services.bedrockruntime.model.InvokeModelResponse;

import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.xpack.inference.external.http.HttpResult;
import org.elasticsearch.xpack.inference.external.http.retry.RetryException;
import org.elasticsearch.xpack.inference.external.request.Request;
import org.elasticsearch.xpack.inference.services.amazonbedrock.request.AmazonBedrockRequest;
import org.elasticsearch.xpack.inference.services.amazonbedrock.response.AmazonBedrockResponseHandler;

public class AmazonBedrockEmbeddingsResponseHandler extends AmazonBedrockResponseHandler {

    private InvokeModelResponse invokeModelResult;

    @Override
    public InferenceServiceResults parseResult(Request request, HttpResult result) throws RetryException {
        var responseParser = new AmazonBedrockEmbeddingsResponse(invokeModelResult);
        return responseParser.accept((AmazonBedrockRequest) request);
    }

    @Override
    public String getRequestType() {
        return "Amazon Bedrock Embeddings";
    }

    public void acceptEmbeddingsResult(InvokeModelResponse result) {
        this.invokeModelResult = result;
    }
}
