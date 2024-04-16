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
import org.elasticsearch.xpack.inference.common.Truncator;
import org.elasticsearch.xpack.inference.external.azureopenai.AzureOpenAiAccount;
import org.elasticsearch.xpack.inference.external.azureopenai.AzureOpenAiResponseHandler;
import org.elasticsearch.xpack.inference.external.http.retry.RequestSender;
import org.elasticsearch.xpack.inference.external.http.retry.ResponseHandler;
import org.elasticsearch.xpack.inference.external.request.azureopenai.AzureOpenAiEmbeddingsRequest;
import org.elasticsearch.xpack.inference.external.response.openai.OpenAiEmbeddingsResponseEntity;
import org.elasticsearch.xpack.inference.services.azureopenai.embeddings.AzureOpenAiEmbeddingsModel;

import java.util.List;
import java.util.Objects;
import java.util.function.Supplier;

import static org.elasticsearch.xpack.inference.common.Truncator.truncate;

public class AzureOpenAiEmbeddingsExecutableRequestCreator implements ExecutableRequestCreator {

    private static final Logger logger = LogManager.getLogger(AzureOpenAiEmbeddingsExecutableRequestCreator.class);

    private static final ResponseHandler HANDLER = createEmbeddingsHandler();

    private static ResponseHandler createEmbeddingsHandler() {
        return new AzureOpenAiResponseHandler("azure openai text embedding", OpenAiEmbeddingsResponseEntity::fromResponse);
    }

    private final Truncator truncator;
    private final AzureOpenAiEmbeddingsModel model;
    private final AzureOpenAiAccount account;

    public AzureOpenAiEmbeddingsExecutableRequestCreator(AzureOpenAiEmbeddingsModel model, Truncator truncator) {
        this.model = Objects.requireNonNull(model);
        this.account = AzureOpenAiAccount.fromModel(model);
        this.truncator = Objects.requireNonNull(truncator);
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
        var truncatedInput = truncate(input, model.getServiceSettings().maxInputTokens());
        AzureOpenAiEmbeddingsRequest request = new AzureOpenAiEmbeddingsRequest(truncator, account, truncatedInput, model);
        return new ExecutableInferenceRequest(requestSender, logger, request, context, HANDLER, hasRequestCompletedFunction, listener);
    }
}
