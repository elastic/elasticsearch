/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.amazonbedrock.response;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.xpack.inference.services.amazonbedrock.request.AmazonBedrockRequest;

import java.util.Objects;

public class AmazonBedrockResponseListener {
    protected final AmazonBedrockRequest request;
    protected final ActionListener<InferenceServiceResults> inferenceResultsListener;
    protected final AmazonBedrockResponseHandler responseHandler;

    public AmazonBedrockResponseListener(
        AmazonBedrockRequest request,
        AmazonBedrockResponseHandler responseHandler,
        ActionListener<InferenceServiceResults> inferenceResultsListener
    ) {
        this.request = Objects.requireNonNull(request);
        this.responseHandler = Objects.requireNonNull(responseHandler);
        this.inferenceResultsListener = Objects.requireNonNull(inferenceResultsListener);
    }
}
