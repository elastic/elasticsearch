/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.azureaistudio.action;

import org.elasticsearch.xpack.inference.external.action.ExecutableAction;
import org.elasticsearch.xpack.inference.external.action.SenderExecutableAction;
import org.elasticsearch.xpack.inference.external.http.retry.ResponseHandler;
import org.elasticsearch.xpack.inference.external.http.sender.ChatCompletionInput;
import org.elasticsearch.xpack.inference.external.http.sender.EmbeddingsInput;
import org.elasticsearch.xpack.inference.external.http.sender.GenericRequestManager;
import org.elasticsearch.xpack.inference.external.http.sender.QueryAndDocsInputs;
import org.elasticsearch.xpack.inference.external.http.sender.Sender;
import org.elasticsearch.xpack.inference.external.response.ErrorMessageResponseEntity;
import org.elasticsearch.xpack.inference.services.ServiceComponents;
import org.elasticsearch.xpack.inference.services.azureaistudio.completion.AzureAiStudioChatCompletionModel;
import org.elasticsearch.xpack.inference.services.azureaistudio.embeddings.AzureAiStudioEmbeddingsModel;
import org.elasticsearch.xpack.inference.services.azureaistudio.request.AzureAiStudioChatCompletionRequest;
import org.elasticsearch.xpack.inference.services.azureaistudio.request.AzureAiStudioEmbeddingsRequest;
import org.elasticsearch.xpack.inference.services.azureaistudio.request.AzureAiStudioRerankRequest;
import org.elasticsearch.xpack.inference.services.azureaistudio.rerank.AzureAiStudioRerankModel;
import org.elasticsearch.xpack.inference.services.azureaistudio.response.AzureAiStudioChatCompletionResponseEntity;
import org.elasticsearch.xpack.inference.services.azureaistudio.response.AzureAiStudioEmbeddingsResponseEntity;
import org.elasticsearch.xpack.inference.services.azureaistudio.response.AzureAiStudioRerankResponseEntity;
import org.elasticsearch.xpack.inference.services.azureopenai.response.AzureMistralOpenAiExternalResponseHandler;

import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.xpack.inference.common.Truncator.truncate;
import static org.elasticsearch.xpack.inference.external.action.ActionUtils.constructFailedToSendRequestMessage;

public class AzureAiStudioActionCreator implements AzureAiStudioActionVisitor {

    private static final ResponseHandler EMBEDDINGS_HANDLER = new AzureMistralOpenAiExternalResponseHandler(
        "azure ai studio embedding",
        new AzureAiStudioEmbeddingsResponseEntity(),
        ErrorMessageResponseEntity::fromResponse,
        false
    );

    private static final ResponseHandler COMPLETION_HANDLER = new AzureMistralOpenAiExternalResponseHandler(
        "azure ai studio completion",
        new AzureAiStudioChatCompletionResponseEntity(),
        ErrorMessageResponseEntity::fromResponse,
        true
    );

    private static final ResponseHandler RERANK_HANDLER = new AzureMistralOpenAiExternalResponseHandler(
        "azure ai studio rerank",
        new AzureAiStudioRerankResponseEntity(),
        ErrorMessageResponseEntity::fromResponse,
        true
    );

    private final Sender sender;
    private final ServiceComponents serviceComponents;

    public AzureAiStudioActionCreator(Sender sender, ServiceComponents serviceComponents) {
        this.sender = Objects.requireNonNull(sender);
        this.serviceComponents = Objects.requireNonNull(serviceComponents);
    }

    @Override
    public ExecutableAction create(AzureAiStudioChatCompletionModel completionModel, Map<String, Object> taskSettings) {
        var overriddenModel = AzureAiStudioChatCompletionModel.of(completionModel, taskSettings);
        var requestManager = new GenericRequestManager<>(
            serviceComponents.threadPool(),
            overriddenModel,
            COMPLETION_HANDLER,
            (chatCompletionInput) -> new AzureAiStudioChatCompletionRequest(
                overriddenModel,
                chatCompletionInput.getInputs(),
                chatCompletionInput.stream()
            ),
            ChatCompletionInput.class
        );
        return new SenderExecutableAction(sender, requestManager, constructFailedToSendRequestMessage("Azure AI Studio completion"));
    }

    @Override
    public ExecutableAction create(AzureAiStudioEmbeddingsModel embeddingsModel, Map<String, Object> taskSettings) {
        var overriddenModel = AzureAiStudioEmbeddingsModel.of(embeddingsModel, taskSettings);
        var requestManager = new GenericRequestManager<>(
            serviceComponents.threadPool(),
            overriddenModel,
            EMBEDDINGS_HANDLER,
            (embeddingsInput) -> new AzureAiStudioEmbeddingsRequest(
                serviceComponents.truncator(),
                truncate(embeddingsInput.getTextInputs(), overriddenModel.getServiceSettings().maxInputTokens()),
                embeddingsInput.getInputType(),
                overriddenModel
            ),
            EmbeddingsInput.class
        );
        return new SenderExecutableAction(sender, requestManager, constructFailedToSendRequestMessage("Azure AI Studio embeddings"));
    }

    @Override
    public ExecutableAction create(AzureAiStudioRerankModel rerankModel, Map<String, Object> taskSettings) {
        var overriddenModel = AzureAiStudioRerankModel.of(rerankModel, taskSettings);
        var requestManager = new GenericRequestManager<>(
            serviceComponents.threadPool(),
            overriddenModel,
            RERANK_HANDLER,
            (rerankInput) -> new AzureAiStudioRerankRequest(
                overriddenModel,
                rerankInput.getQueryAsString(),
                rerankInput.getDocsAsStrings(),
                rerankInput.getReturnDocuments(),
                rerankInput.getTopN()
            ),
            QueryAndDocsInputs.class
        );
        return new SenderExecutableAction(sender, requestManager, constructFailedToSendRequestMessage("Azure AI Studio rerank"));
    }
}
