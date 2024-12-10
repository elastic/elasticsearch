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
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.inference.external.http.retry.RequestSender;
import org.elasticsearch.xpack.inference.external.http.retry.ResponseHandler;
import org.elasticsearch.xpack.inference.external.openai.OpenAiUnifiedChatCompletionResponseHandler;
import org.elasticsearch.xpack.inference.external.request.openai.OpenAiUnifiedChatCompletionRequest;
import org.elasticsearch.xpack.inference.external.response.openai.OpenAiChatCompletionResponseEntity;
import org.elasticsearch.xpack.inference.services.openai.completion.OpenAiChatCompletionModel;

import java.util.Objects;
import java.util.function.Supplier;

public class OpenAiUnifiedCompletionRequestManager extends OpenAiRequestManager {

    private static final Logger logger = LogManager.getLogger(OpenAiUnifiedCompletionRequestManager.class);

    private static final ResponseHandler HANDLER = createCompletionHandler();

    public static OpenAiUnifiedCompletionRequestManager of(OpenAiChatCompletionModel model, ThreadPool threadPool) {
        return new OpenAiUnifiedCompletionRequestManager(Objects.requireNonNull(model), Objects.requireNonNull(threadPool));
    }

    private final OpenAiChatCompletionModel model;

    private OpenAiUnifiedCompletionRequestManager(OpenAiChatCompletionModel model, ThreadPool threadPool) {
        super(threadPool, model, OpenAiUnifiedChatCompletionRequest::buildDefaultUri);
        this.model = Objects.requireNonNull(model);
    }

    @Override
    public void execute(
        InferenceInputs inferenceInputs,
        RequestSender requestSender,
        Supplier<Boolean> hasRequestCompletedFunction,
        ActionListener<InferenceServiceResults> listener
    ) {

        OpenAiUnifiedChatCompletionRequest request = new OpenAiUnifiedChatCompletionRequest(
            inferenceInputs.castTo(UnifiedChatInput.class),
            model
        );

        execute(new ExecutableInferenceRequest(requestSender, logger, request, HANDLER, hasRequestCompletedFunction, listener));
    }

    private static ResponseHandler createCompletionHandler() {
        return new OpenAiUnifiedChatCompletionResponseHandler("openai completion", OpenAiChatCompletionResponseEntity::fromResponse);
    }
}
