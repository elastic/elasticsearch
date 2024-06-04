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
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.inference.external.http.retry.RequestSender;
import org.elasticsearch.xpack.inference.external.http.retry.ResponseHandler;
import org.elasticsearch.xpack.inference.external.request.azureaistudio.AzureAiStudioChatCompletionRequest;
import org.elasticsearch.xpack.inference.external.response.AzureMistralOpenAiErrorResponseEntity;
import org.elasticsearch.xpack.inference.external.response.AzureMistralOpenAiExternalResponseHandler;
import org.elasticsearch.xpack.inference.external.response.azureaistudio.AzureAiStudioChatCompletionResponseEntity;
import org.elasticsearch.xpack.inference.services.azureaistudio.completion.AzureAiStudioChatCompletionModel;

import java.util.List;
import java.util.function.Supplier;

public class AzureAiStudioChatCompletionRequestManager extends AzureAiStudioRequestManager {
    private static final Logger logger = LogManager.getLogger(AzureAiStudioChatCompletionRequestManager.class);

    private static final ResponseHandler HANDLER = createCompletionHandler();

    private final AzureAiStudioChatCompletionModel model;

    public AzureAiStudioChatCompletionRequestManager(AzureAiStudioChatCompletionModel model, ThreadPool threadPool) {
        super(threadPool, model);
        this.model = model;
    }

    @Override
    public Runnable create(
        String query,
        List<String> input,
        RequestSender requestSender,
        Supplier<Boolean> hasRequestCompletedFunction,
        HttpClientContext context,
        ActionListener<InferenceServiceResults> listener
    ) {
        AzureAiStudioChatCompletionRequest request = new AzureAiStudioChatCompletionRequest(model, input);

        return new ExecutableInferenceRequest(requestSender, logger, request, context, HANDLER, hasRequestCompletedFunction, listener);
    }

    private static ResponseHandler createCompletionHandler() {
        return new AzureMistralOpenAiExternalResponseHandler(
            "azure ai studio completion",
            new AzureAiStudioChatCompletionResponseEntity(),
            AzureMistralOpenAiErrorResponseEntity::fromResponse
        );
    }

}
