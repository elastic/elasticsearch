/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.amazonbedrock;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.Strings;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.xpack.inference.external.request.amazonbedrock.AmazonBedrockRequest;
import org.elasticsearch.xpack.inference.external.response.amazonbedrock.AmazonBedrockResponseHandler;
import org.elasticsearch.xpack.inference.services.amazonbedrock.AmazonBedrockModel;

import java.util.function.Supplier;

public class AmazonBedrockExecutor implements Runnable {

    protected static final AmazonBedrockClientCache amazonBedrockClientCache = new AmazonBedrockInferenceClientCache(
        AmazonBedrockInferenceClient::create
    );

    protected final AmazonBedrockModel baseModel;
    protected final AmazonBedrockResponseHandler responseHandler;
    protected final Logger logger;
    protected final AmazonBedrockRequest request;
    protected final Supplier<Boolean> hasRequestCompletedFunction;
    protected final ActionListener<InferenceServiceResults> listener;
    private final AmazonBedrockClientCache clientCache;

    public AmazonBedrockExecutor(
        AmazonBedrockModel model,
        AmazonBedrockRequest request,
        AmazonBedrockResponseHandler responseHandler,
        Logger logger,
        Supplier<Boolean> hasRequestCompletedFunction,
        ActionListener<InferenceServiceResults> listener
    ) {
        this.baseModel = model;
        this.responseHandler = responseHandler;
        this.logger = logger;
        this.request = request;
        this.hasRequestCompletedFunction = hasRequestCompletedFunction;
        this.listener = listener;
        this.clientCache = amazonBedrockClientCache;
    }

    // only used for testing
    public AmazonBedrockExecutor(
        AmazonBedrockModel model,
        AmazonBedrockRequest request,
        AmazonBedrockResponseHandler responseHandler,
        Logger logger,
        Supplier<Boolean> hasRequestCompletedFunction,
        ActionListener<InferenceServiceResults> listener,
        @Nullable AmazonBedrockClientCache clientCache
    ) {
        this.baseModel = model;
        this.responseHandler = responseHandler;
        this.logger = logger;
        this.request = request;
        this.hasRequestCompletedFunction = hasRequestCompletedFunction;
        this.listener = listener;
        this.clientCache = (clientCache == null) ? amazonBedrockClientCache : clientCache;
    }

    @Override
    public void run() {
        if (hasRequestCompletedFunction != null && hasRequestCompletedFunction.get()) {
            // has already been run
            return;
        }

        var inferenceEntityId = baseModel.getInferenceEntityId();

        try {
            var awsBedrockClient = clientCache.getOrCreateClient(baseModel, request.timeout());
            request.executeRequest(awsBedrockClient);
            listener.onResponse(responseHandler.parseResult(request, null));
        } catch (Exception e) {
            var errorMessage = Strings.format("Failed to send request from inference entity id [%s]", inferenceEntityId);
            logger.warn(errorMessage, e);
            listener.onFailure(new ElasticsearchException(errorMessage, e));
        }
    }
}
