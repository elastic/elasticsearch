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
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.inference.external.anthropic.AnthropicResponseHandler;
import org.elasticsearch.xpack.inference.external.http.retry.RequestSender;
import org.elasticsearch.xpack.inference.external.http.retry.ResponseHandler;
import org.elasticsearch.xpack.inference.external.request.anthropic.AnthropicChatCompletionRequest;
import org.elasticsearch.xpack.inference.external.response.anthropic.AnthropicChatCompletionResponseEntity;
import org.elasticsearch.xpack.inference.services.anthropic.completion.AnthropicChatCompletionModel;

import java.util.List;
import java.util.Objects;
import java.util.function.Supplier;

public class AnthropicCompletionRequestManager extends AnthropicRequestManager {

    private static final Logger logger = LogManager.getLogger(AnthropicCompletionRequestManager.class);

    private static final ResponseHandler HANDLER = createCompletionHandler();

    public static AnthropicCompletionRequestManager of(AnthropicChatCompletionModel model, ThreadPool threadPool) {
        return new AnthropicCompletionRequestManager(Objects.requireNonNull(model), Objects.requireNonNull(threadPool));
    }

    private final AnthropicChatCompletionModel model;

    private AnthropicCompletionRequestManager(AnthropicChatCompletionModel model, ThreadPool threadPool) {
        super(threadPool, model);
        this.model = Objects.requireNonNull(model);
    }

    @Override
    public void execute(
        @Nullable String query,
        List<String> input,
        RequestSender requestSender,
        Supplier<Boolean> hasRequestCompletedFunction,
        ActionListener<InferenceServiceResults> listener
    ) {
        AnthropicChatCompletionRequest request = new AnthropicChatCompletionRequest(input, model);

        execute(new ExecutableInferenceRequest(requestSender, logger, request, HANDLER, hasRequestCompletedFunction, listener));
    }

    private static ResponseHandler createCompletionHandler() {
        return new AnthropicResponseHandler("anthropic completions", AnthropicChatCompletionResponseEntity::fromResponse);
    }
}
