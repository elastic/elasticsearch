/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.amazonbedrock;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.exception.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.Strings;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.inference.InputType;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.inference.common.Truncator;
import org.elasticsearch.xpack.inference.external.http.retry.RequestSender;
import org.elasticsearch.xpack.inference.external.http.sender.EmbeddingsInput;
import org.elasticsearch.xpack.inference.external.http.sender.InferenceInputs;
import org.elasticsearch.xpack.inference.services.amazonbedrock.embeddings.AmazonBedrockEmbeddingsModel;
import org.elasticsearch.xpack.inference.services.amazonbedrock.request.embeddings.AmazonBedrockEmbeddingsEntityFactory;
import org.elasticsearch.xpack.inference.services.amazonbedrock.request.embeddings.AmazonBedrockEmbeddingsRequest;
import org.elasticsearch.xpack.inference.services.amazonbedrock.response.embeddings.AmazonBedrockEmbeddingsResponseHandler;

import java.util.List;
import java.util.Objects;
import java.util.function.Supplier;

import static org.elasticsearch.xpack.inference.common.Truncator.truncate;

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
        this.truncator = Objects.requireNonNull(truncator);
    }

    @Override
    public void execute(
        InferenceInputs inferenceInputs,
        RequestSender requestSender,
        Supplier<Boolean> hasRequestCompletedFunction,
        ActionListener<InferenceServiceResults> listener
    ) {
        EmbeddingsInput input = EmbeddingsInput.of(inferenceInputs);
        List<String> docsInput = input.getStringInputs();
        InputType inputType = input.getInputType();

        var serviceSettings = embeddingsModel.getServiceSettings();
        var truncatedInput = truncate(docsInput, serviceSettings.maxInputTokens());
        var requestEntity = AmazonBedrockEmbeddingsEntityFactory.createEntity(embeddingsModel, truncatedInput, inputType);
        var responseHandler = new AmazonBedrockEmbeddingsResponseHandler();
        var request = new AmazonBedrockEmbeddingsRequest(truncator, truncatedInput, embeddingsModel, requestEntity, timeout);
        try {
            requestSender.send(logger, request, hasRequestCompletedFunction, responseHandler, listener);
        } catch (Exception e) {
            var errorMessage = Strings.format(
                "Failed to send [text_embedding] request from inference entity id [%s]",
                request.getInferenceEntityId()
            );
            logger.warn(errorMessage, e);
            listener.onFailure(new ElasticsearchException(errorMessage, e));
        }
    }
}
