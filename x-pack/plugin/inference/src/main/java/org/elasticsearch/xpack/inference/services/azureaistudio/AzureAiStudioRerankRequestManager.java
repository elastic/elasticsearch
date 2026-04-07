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
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.inference.external.http.retry.RequestSender;
import org.elasticsearch.xpack.inference.external.http.retry.ResponseHandler;
import org.elasticsearch.xpack.inference.external.http.sender.ExecutableInferenceRequest;
import org.elasticsearch.xpack.inference.external.http.sender.InferenceInputs;
import org.elasticsearch.xpack.inference.external.http.sender.QueryAndDocsInputs;
import org.elasticsearch.xpack.inference.external.response.ErrorMessageResponseEntity;
import org.elasticsearch.xpack.inference.services.azureaistudio.request.AzureAiStudioRerankRequest;
import org.elasticsearch.xpack.inference.services.azureaistudio.rerank.AzureAiStudioRerankModel;
import org.elasticsearch.xpack.inference.services.azureaistudio.response.AzureAiStudioRerankResponseEntity;
import org.elasticsearch.xpack.inference.services.azureopenai.response.AzureMistralOpenAiExternalResponseHandler;

import java.util.function.Supplier;

public class AzureAiStudioRerankRequestManager extends AzureAiStudioRequestManager {
    private static final Logger logger = LogManager.getLogger(AzureAiStudioRerankRequestManager.class);

    private static final ResponseHandler HANDLER = createRerankHandler();

    private final AzureAiStudioRerankModel model;

    public AzureAiStudioRerankRequestManager(AzureAiStudioRerankModel model, ThreadPool threadPool) {
        super(threadPool, model);
        this.model = model;
    }

    @Override
    public void execute(
        InferenceInputs inferenceInputs,
        RequestSender requestSender,
        Supplier<Boolean> hasRequestRerankFunction,
        ActionListener<InferenceServiceResults> listener
    ) {
        var rerankInput = QueryAndDocsInputs.of(inferenceInputs);
        AzureAiStudioRerankRequest request = new AzureAiStudioRerankRequest(
            model,
            rerankInput.getQuery(),
            rerankInput.getChunks(),
            rerankInput.getReturnDocuments(),
            rerankInput.getTopN()
        );

        execute(new ExecutableInferenceRequest(requestSender, logger, request, HANDLER, hasRequestRerankFunction, listener));
    }

    private static ResponseHandler createRerankHandler() {
        // This currently covers response handling for Azure AI Studio
        return new AzureMistralOpenAiExternalResponseHandler(
            "azure ai studio rerank",
            new AzureAiStudioRerankResponseEntity(),
            ErrorMessageResponseEntity::fromResponse,
            true
        );
    }
}
