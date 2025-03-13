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
import org.elasticsearch.xpack.inference.external.azureopenai.AzureOpenAiResponseHandler;
import org.elasticsearch.xpack.inference.external.http.retry.RequestSender;
import org.elasticsearch.xpack.inference.external.http.retry.ResponseHandler;
import org.elasticsearch.xpack.inference.external.request.azureopenai.AzureOpenAiCompletionRequest;
import org.elasticsearch.xpack.inference.external.response.azureopenai.AzureOpenAiCompletionResponseEntity;
import org.elasticsearch.xpack.inference.services.azureopenai.completion.AzureOpenAiCompletionModel;

import java.util.Objects;
import java.util.function.Supplier;

public class AzureOpenAiCompletionRequestManager extends AzureOpenAiRequestManager {

    private static final Logger logger = LogManager.getLogger(AzureOpenAiCompletionRequestManager.class);

    private static final ResponseHandler HANDLER = createCompletionHandler();

    private final AzureOpenAiCompletionModel model;

    private static ResponseHandler createCompletionHandler() {
        return new AzureOpenAiResponseHandler("azure openai completion", AzureOpenAiCompletionResponseEntity::fromResponse, true);
    }

    public AzureOpenAiCompletionRequestManager(AzureOpenAiCompletionModel model, ThreadPool threadPool) {
        super(threadPool, model);
        this.model = Objects.requireNonNull(model);
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
        AzureOpenAiCompletionRequest request = new AzureOpenAiCompletionRequest(inputs, model, stream);
        execute(new ExecutableInferenceRequest(requestSender, logger, request, HANDLER, hasRequestCompletedFunction, listener));
    }

}
