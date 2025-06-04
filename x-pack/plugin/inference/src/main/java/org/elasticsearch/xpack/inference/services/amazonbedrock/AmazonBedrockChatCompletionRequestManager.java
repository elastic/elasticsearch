/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.amazonbedrock;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.Strings;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.exception.ElasticsearchException;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.inference.external.http.retry.RequestSender;
import org.elasticsearch.xpack.inference.external.http.sender.ChatCompletionInput;
import org.elasticsearch.xpack.inference.external.http.sender.InferenceInputs;
import org.elasticsearch.xpack.inference.services.amazonbedrock.completion.AmazonBedrockChatCompletionModel;
import org.elasticsearch.xpack.inference.services.amazonbedrock.request.completion.AmazonBedrockChatCompletionEntityFactory;
import org.elasticsearch.xpack.inference.services.amazonbedrock.request.completion.AmazonBedrockChatCompletionRequest;
import org.elasticsearch.xpack.inference.services.amazonbedrock.response.completion.AmazonBedrockChatCompletionResponseHandler;

import java.util.function.Supplier;

public class AmazonBedrockChatCompletionRequestManager extends AmazonBedrockRequestManager {
    private static final Logger logger = LogManager.getLogger(AmazonBedrockChatCompletionRequestManager.class);
    private final AmazonBedrockChatCompletionModel model;

    public AmazonBedrockChatCompletionRequestManager(
        AmazonBedrockChatCompletionModel model,
        ThreadPool threadPool,
        @Nullable TimeValue timeout
    ) {
        super(model, threadPool, timeout);
        this.model = model;
    }

    @Override
    public void execute(
        InferenceInputs inferenceInputs,
        RequestSender requestSender,
        Supplier<Boolean> hasRequestCompletedFunction,
        ActionListener<InferenceServiceResults> listener
    ) {
        var chatCompletionInput = inferenceInputs.castTo(ChatCompletionInput.class);
        var inputs = chatCompletionInput.getInputs();
        var stream = chatCompletionInput.stream();
        var requestEntity = AmazonBedrockChatCompletionEntityFactory.createEntity(model, inputs);
        var request = new AmazonBedrockChatCompletionRequest(model, requestEntity, timeout, stream);
        var responseHandler = new AmazonBedrockChatCompletionResponseHandler();

        try {
            requestSender.send(logger, request, hasRequestCompletedFunction, responseHandler, listener);
        } catch (Exception e) {
            var errorMessage = Strings.format(
                "Failed to send [completion] request from inference entity id [%s]",
                request.getInferenceEntityId()
            );
            logger.warn(errorMessage, e);
            listener.onFailure(new ElasticsearchException(errorMessage, e));
        }
    }
}
