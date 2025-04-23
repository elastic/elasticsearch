/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.huggingface;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.inference.external.http.retry.RequestSender;
import org.elasticsearch.xpack.inference.external.http.retry.ResponseHandler;
import org.elasticsearch.xpack.inference.external.http.sender.ExecutableInferenceRequest;
import org.elasticsearch.xpack.inference.external.http.sender.InferenceInputs;
import org.elasticsearch.xpack.inference.external.http.sender.UnifiedChatInput;
import org.elasticsearch.xpack.inference.services.huggingface.completion.HuggingFaceChatCompletionModel;
import org.elasticsearch.xpack.inference.services.huggingface.request.completion.HuggingFaceUnifiedChatCompletionRequest;

import java.util.Objects;
import java.util.function.Supplier;

public class HuggingFaceCompletionRequestManager extends HuggingFaceRequestManager {
    private static final Logger logger = LogManager.getLogger(HuggingFaceCompletionRequestManager.class);

    public static HuggingFaceCompletionRequestManager of(
        HuggingFaceChatCompletionModel model,
        ResponseHandler responseHandler,
        ThreadPool threadPool
    ) {
        return new HuggingFaceCompletionRequestManager(
            Objects.requireNonNull(model),
            Objects.requireNonNull(responseHandler),
            Objects.requireNonNull(threadPool)
        );
    }

    private final HuggingFaceChatCompletionModel model;
    private final ResponseHandler responseHandler;

    private HuggingFaceCompletionRequestManager(
        HuggingFaceChatCompletionModel model,
        ResponseHandler responseHandler,
        ThreadPool threadPool
    ) {
        super(model, threadPool);
        this.model = model;
        this.responseHandler = responseHandler;
    }

    @Override
    public void execute(
        InferenceInputs inferenceInputs,
        RequestSender requestSender,
        Supplier<Boolean> hasRequestCompletedFunction,
        ActionListener<InferenceServiceResults> listener
    ) {
        var chatCompletionInput = inferenceInputs.castTo(UnifiedChatInput.class);
        HuggingFaceUnifiedChatCompletionRequest request = new HuggingFaceUnifiedChatCompletionRequest(chatCompletionInput, model);

        execute(new ExecutableInferenceRequest(requestSender, logger, request, responseHandler, hasRequestCompletedFunction, listener));
    }
}
