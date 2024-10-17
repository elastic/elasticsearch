/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.response.amazonbedrock.embeddings;

import software.amazon.awssdk.services.bedrockruntime.model.InvokeModelResponse;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.xpack.inference.external.http.HttpResult;
import org.elasticsearch.xpack.inference.external.request.amazonbedrock.embeddings.AmazonBedrockEmbeddingsRequest;
import org.elasticsearch.xpack.inference.external.response.amazonbedrock.AmazonBedrockResponseHandler;
import org.elasticsearch.xpack.inference.external.response.amazonbedrock.AmazonBedrockResponseListener;

public class AmazonBedrockEmbeddingsResponseListener extends AmazonBedrockResponseListener implements ActionListener<InvokeModelResponse> {

    public AmazonBedrockEmbeddingsResponseListener(
        AmazonBedrockEmbeddingsRequest request,
        AmazonBedrockResponseHandler responseHandler,
        ActionListener<InferenceServiceResults> inferenceResultsListener
    ) {
        super(request, responseHandler, inferenceResultsListener);
    }

    @Override
    public void onResponse(InvokeModelResponse response) {
        ((AmazonBedrockEmbeddingsResponseHandler) responseHandler).acceptEmbeddingsResult(response);
        inferenceResultsListener.onResponse(responseHandler.parseResult(request, (HttpResult) null));
    }

    @Override
    public void onFailure(Exception e) {
        inferenceResultsListener.onFailure(e);
    }
}
