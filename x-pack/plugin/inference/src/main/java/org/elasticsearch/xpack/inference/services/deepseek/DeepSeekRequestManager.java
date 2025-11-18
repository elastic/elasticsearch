/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.deepseek;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.inference.external.http.retry.RequestSender;
import org.elasticsearch.xpack.inference.external.http.retry.ResponseHandler;
import org.elasticsearch.xpack.inference.external.http.sender.BaseRequestManager;
import org.elasticsearch.xpack.inference.external.http.sender.ChatCompletionInput;
import org.elasticsearch.xpack.inference.external.http.sender.ExecutableInferenceRequest;
import org.elasticsearch.xpack.inference.external.http.sender.InferenceInputs;
import org.elasticsearch.xpack.inference.external.http.sender.UnifiedChatInput;
import org.elasticsearch.xpack.inference.services.deepseek.request.DeepSeekChatCompletionRequest;
import org.elasticsearch.xpack.inference.services.openai.OpenAiChatCompletionResponseHandler;
import org.elasticsearch.xpack.inference.services.openai.OpenAiUnifiedChatCompletionResponseHandler;
import org.elasticsearch.xpack.inference.services.openai.response.OpenAiChatCompletionResponseEntity;

import java.util.Objects;
import java.util.function.Supplier;

import static org.elasticsearch.xpack.inference.external.http.sender.InferenceInputs.createUnsupportedTypeException;

public class DeepSeekRequestManager extends BaseRequestManager {

    private static final Logger logger = LogManager.getLogger(DeepSeekRequestManager.class);

    private static final ResponseHandler CHAT_COMPLETION = createChatCompletionHandler();
    private static final ResponseHandler COMPLETION = createCompletionHandler();

    private final DeepSeekChatCompletionModel model;

    public DeepSeekRequestManager(DeepSeekChatCompletionModel model, ThreadPool threadPool) {
        super(threadPool, model.getInferenceEntityId(), model.rateLimitGroup(), model.rateLimitSettings());
        this.model = Objects.requireNonNull(model);
    }

    @Override
    public void execute(
        InferenceInputs inferenceInputs,
        RequestSender requestSender,
        Supplier<Boolean> hasRequestCompletedFunction,
        ActionListener<InferenceServiceResults> listener
    ) {
        switch (inferenceInputs) {
            case UnifiedChatInput uci -> execute(uci, requestSender, hasRequestCompletedFunction, listener);
            case ChatCompletionInput cci -> execute(cci, requestSender, hasRequestCompletedFunction, listener);
            default -> throw createUnsupportedTypeException(inferenceInputs, UnifiedChatInput.class);
        }
    }

    private void execute(
        UnifiedChatInput inferenceInputs,
        RequestSender requestSender,
        Supplier<Boolean> hasRequestCompletedFunction,
        ActionListener<InferenceServiceResults> listener
    ) {
        var request = new DeepSeekChatCompletionRequest(inferenceInputs, model);
        execute(new ExecutableInferenceRequest(requestSender, logger, request, CHAT_COMPLETION, hasRequestCompletedFunction, listener));
    }

    private void execute(
        ChatCompletionInput inferenceInputs,
        RequestSender requestSender,
        Supplier<Boolean> hasRequestCompletedFunction,
        ActionListener<InferenceServiceResults> listener
    ) {
        var unifiedInputs = new UnifiedChatInput(inferenceInputs.getInputs(), "user", inferenceInputs.stream());
        var request = new DeepSeekChatCompletionRequest(unifiedInputs, model);
        execute(new ExecutableInferenceRequest(requestSender, logger, request, COMPLETION, hasRequestCompletedFunction, listener));
    }

    private static ResponseHandler createChatCompletionHandler() {
        return new OpenAiUnifiedChatCompletionResponseHandler("deepseek chat completion", OpenAiChatCompletionResponseEntity::fromResponse);
    }

    private static ResponseHandler createCompletionHandler() {
        return new OpenAiChatCompletionResponseHandler("deepseek completion", OpenAiChatCompletionResponseEntity::fromResponse);
    }
}
