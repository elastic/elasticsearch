/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.http.sender;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.inference.common.Truncator;
import org.elasticsearch.xpack.inference.external.http.retry.RequestSender;
import org.elasticsearch.xpack.inference.external.request.amazonbedrock.embeddings.AmazonBedrockEmbeddingsEntityFactory;
import org.elasticsearch.xpack.inference.external.request.amazonbedrock.embeddings.AmazonBedrockEmbeddingsRequest;
import org.elasticsearch.xpack.inference.external.response.amazonbedrock.embeddings.AmazonBedrockEmbeddingsResponseHandler;
import org.elasticsearch.xpack.inference.services.amazonbedrock.embeddings.AmazonBedrockEmbeddingsModel;
import org.elasticsearch.xpack.inference.services.amazonbedrock.embeddings.AmazonBedrockEmbeddingsServiceSettings;

import java.util.List;
import java.util.function.Supplier;

public class AmazonBedrockEmbeddingsRequestManager extends AmazonBedrockRequestManager {
    private static final Logger logger = LogManager.getLogger(AmazonBedrockEmbeddingsRequestManager.class);

    private final AmazonBedrockEmbeddingsModel embeddingsModel;
    private final Truncator truncator;

    public AmazonBedrockEmbeddingsRequestManager(
        AmazonBedrockEmbeddingsModel model,
        Truncator truncator,
        ThreadPool threadPool,
        @Nullable TimeValue timeout
    ) {
        super(model, threadPool, timeout);
        this.embeddingsModel = model;
        this.truncator = truncator;
    }

    @Override
    public void execute(
        String query,
        List<String> input,
        RequestSender requestSender,
        Supplier<Boolean> hasRequestCompletedFunction,
        ActionListener<InferenceServiceResults> listener
    ) {
        var serviceSettings = (AmazonBedrockEmbeddingsServiceSettings) embeddingsModel.getServiceSettings();
        var requestEntity = AmazonBedrockEmbeddingsEntityFactory.createEntity(
            embeddingsModel,
            input,
            truncator,
            serviceSettings.maxInputTokens()
        );
        var request = new AmazonBedrockEmbeddingsRequest(embeddingsModel, requestEntity, timeout);
        var responseHandler = new AmazonBedrockEmbeddingsResponseHandler();
        var inferenceRequest = new ExecutableInferenceRequest(
            requestSender,
            logger,
            request,
            responseHandler,
            hasRequestCompletedFunction,
            listener
        );
        inferenceRequest.run();
    }
}
