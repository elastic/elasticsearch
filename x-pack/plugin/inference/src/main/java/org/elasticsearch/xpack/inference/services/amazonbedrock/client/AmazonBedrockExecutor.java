/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.amazonbedrock.client;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.Strings;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.xpack.inference.services.amazonbedrock.AmazonBedrockModel;
import org.elasticsearch.xpack.inference.services.amazonbedrock.request.AmazonBedrockRequest;
import org.elasticsearch.xpack.inference.services.amazonbedrock.response.AmazonBedrockResponseHandler;

import java.util.Objects;
import java.util.function.Supplier;

public abstract class AmazonBedrockExecutor implements Runnable {
    protected final AmazonBedrockModel baseModel;
    protected final AmazonBedrockResponseHandler responseHandler;
    protected final Logger logger;
    protected final AmazonBedrockRequest request;
    protected final Supplier<Boolean> hasRequestCompletedFunction;
    protected final ActionListener<InferenceServiceResults> inferenceResultsListener;
    protected final AmazonBedrockClientCache clientCache;

    protected AmazonBedrockExecutor(
        AmazonBedrockRequest request,
        AmazonBedrockResponseHandler responseHandler,
        Logger logger,
        Supplier<Boolean> hasRequestCompletedFunction,
        ActionListener<InferenceServiceResults> inferenceResultsListener,
        AmazonBedrockClientCache clientCache
    ) {
        this.request = Objects.requireNonNull(request);
        this.responseHandler = Objects.requireNonNull(responseHandler);
        this.logger = Objects.requireNonNull(logger);
        this.hasRequestCompletedFunction = Objects.requireNonNull(hasRequestCompletedFunction);
        this.inferenceResultsListener = Objects.requireNonNull(inferenceResultsListener);
        this.clientCache = Objects.requireNonNull(clientCache);
        this.baseModel = request.model();
    }

    @Override
    public void run() {
        if (hasRequestCompletedFunction.get()) {
            // has already been run
            return;
        }

        var inferenceEntityId = baseModel.getInferenceEntityId();

        try {
            var awsBedrockClient = clientCache.getOrCreateClient(baseModel, request.timeout());
            executeClientRequest(awsBedrockClient);
        } catch (Exception e) {
            var errorMessage = Strings.format("Failed to send request from inference entity id [%s]", inferenceEntityId);
            logger.warn(errorMessage, e);
            inferenceResultsListener.onFailure(new ElasticsearchException(errorMessage, e));
        }
    }

    protected abstract void executeClientRequest(AmazonBedrockBaseClient awsBedrockClient);
}
