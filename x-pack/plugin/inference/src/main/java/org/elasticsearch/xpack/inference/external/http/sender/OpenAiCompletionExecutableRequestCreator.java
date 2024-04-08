/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.http.sender;

import org.apache.http.client.protocol.HttpClientContext;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.xpack.inference.external.http.retry.RequestSender;
import org.elasticsearch.xpack.inference.external.http.retry.ResponseHandler;
import org.elasticsearch.xpack.inference.external.openai.OpenAiAccount;
import org.elasticsearch.xpack.inference.external.openai.OpenAiChatCompletionResponseHandler;
import org.elasticsearch.xpack.inference.external.request.openai.OpenAiChatCompletionRequest;
import org.elasticsearch.xpack.inference.external.response.openai.OpenAiChatCompletionResponseEntity;
import org.elasticsearch.xpack.inference.services.openai.completion.OpenAiChatCompletionModel;

import java.util.List;
import java.util.Objects;
import java.util.function.Supplier;

public class OpenAiCompletionExecutableRequestCreator implements ExecutableRequestCreator {

    private static final Logger logger = LogManager.getLogger(OpenAiCompletionExecutableRequestCreator.class);

    private static final ResponseHandler HANDLER = createCompletionHandler();

    private final OpenAiChatCompletionModel model;

    private final OpenAiAccount account;

    public OpenAiCompletionExecutableRequestCreator(OpenAiChatCompletionModel model) {
        this.model = Objects.requireNonNull(model);
        this.account = new OpenAiAccount(
            this.model.getServiceSettings().uri(),
            this.model.getServiceSettings().organizationId(),
            this.model.getSecretSettings().apiKey()
        );
    }

    @Override
    public Runnable create(
        @Nullable String query,
        List<String> input,
        RequestSender requestSender,
        Supplier<Boolean> hasRequestCompletedFunction,
        HttpClientContext context,
        ActionListener<InferenceServiceResults> listener
    ) {
        OpenAiChatCompletionRequest request = new OpenAiChatCompletionRequest(account, input, model);

        return new ExecutableInferenceRequest(requestSender, logger, request, context, HANDLER, hasRequestCompletedFunction, listener);
    }

    private static ResponseHandler createCompletionHandler() {
        return new OpenAiChatCompletionResponseHandler("openai completion", OpenAiChatCompletionResponseEntity::fromResponse);
    }
}
