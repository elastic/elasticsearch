/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.azureaistudio;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.inference.InputType;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.inference.common.Truncator;
import org.elasticsearch.xpack.inference.external.http.retry.RequestSender;
import org.elasticsearch.xpack.inference.external.http.retry.ResponseHandler;
import org.elasticsearch.xpack.inference.external.http.sender.EmbeddingsInput;
import org.elasticsearch.xpack.inference.external.http.sender.ExecutableInferenceRequest;
import org.elasticsearch.xpack.inference.external.http.sender.InferenceInputs;
import org.elasticsearch.xpack.inference.external.response.ErrorMessageResponseEntity;
import org.elasticsearch.xpack.inference.services.azureaistudio.embeddings.AzureAiStudioEmbeddingsModel;
import org.elasticsearch.xpack.inference.services.azureaistudio.request.AzureAiStudioEmbeddingsRequest;
import org.elasticsearch.xpack.inference.services.azureaistudio.response.AzureAiStudioEmbeddingsResponseEntity;
import org.elasticsearch.xpack.inference.services.azureopenai.response.AzureMistralOpenAiExternalResponseHandler;

import java.util.List;
import java.util.function.Supplier;

import static org.elasticsearch.xpack.inference.common.Truncator.truncate;

public class AzureAiStudioEmbeddingsRequestManager extends AzureAiStudioRequestManager {
    private static final Logger logger = LogManager.getLogger(AzureAiStudioEmbeddingsRequestManager.class);
    private static final ResponseHandler HANDLER = createEmbeddingsHandler();

    private final AzureAiStudioEmbeddingsModel model;
    private final Truncator truncator;

    public AzureAiStudioEmbeddingsRequestManager(AzureAiStudioEmbeddingsModel model, Truncator truncator, ThreadPool threadPool) {
        super(threadPool, model);
        this.model = model;
        this.truncator = truncator;
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

        var truncatedInput = truncate(docsInput, model.getServiceSettings().maxInputTokens());
        AzureAiStudioEmbeddingsRequest request = new AzureAiStudioEmbeddingsRequest(truncator, truncatedInput, inputType, model);
        execute(new ExecutableInferenceRequest(requestSender, logger, request, HANDLER, hasRequestCompletedFunction, listener));
    }

    private static ResponseHandler createEmbeddingsHandler() {
        return new AzureMistralOpenAiExternalResponseHandler(
            "azure ai studio text embedding",
            new AzureAiStudioEmbeddingsResponseEntity(),
            ErrorMessageResponseEntity::fromResponse,
            false
        );
    }

}
