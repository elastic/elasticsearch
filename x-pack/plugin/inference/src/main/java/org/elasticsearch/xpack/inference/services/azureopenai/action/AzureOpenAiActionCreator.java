/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.azureopenai.action;

import org.elasticsearch.xpack.inference.external.action.ExecutableAction;
import org.elasticsearch.xpack.inference.external.action.SenderExecutableAction;
import org.elasticsearch.xpack.inference.external.action.SingleInputSenderExecutableAction;
import org.elasticsearch.xpack.inference.external.http.retry.ResponseHandler;
import org.elasticsearch.xpack.inference.external.http.sender.ChatCompletionInput;
import org.elasticsearch.xpack.inference.external.http.sender.EmbeddingsInput;
import org.elasticsearch.xpack.inference.external.http.sender.GenericRequestManager;
import org.elasticsearch.xpack.inference.external.http.sender.Sender;
import org.elasticsearch.xpack.inference.services.ServiceComponents;
import org.elasticsearch.xpack.inference.services.azureopenai.AzureOpenAiResponseHandler;
import org.elasticsearch.xpack.inference.services.azureopenai.completion.AzureOpenAiCompletionModel;
import org.elasticsearch.xpack.inference.services.azureopenai.embeddings.AzureOpenAiEmbeddingsModel;
import org.elasticsearch.xpack.inference.services.azureopenai.request.AzureOpenAiCompletionRequest;
import org.elasticsearch.xpack.inference.services.azureopenai.request.AzureOpenAiEmbeddingsRequest;
import org.elasticsearch.xpack.inference.services.azureopenai.response.AzureOpenAiCompletionResponseEntity;
import org.elasticsearch.xpack.inference.services.openai.response.OpenAiEmbeddingsResponseEntity;

import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.xpack.inference.common.Truncator.truncate;
import static org.elasticsearch.xpack.inference.external.action.ActionUtils.constructFailedToSendRequestMessage;

/**
 * Provides a way to construct an {@link ExecutableAction} using the visitor pattern based on the openai model type.
 */
public class AzureOpenAiActionCreator implements AzureOpenAiActionVisitor {
    private static final String COMPLETION_ERROR_PREFIX = "Azure OpenAI completion";
    private static final String EMBEDDINGS_ERROR_PREFIX = "Azure OpenAI embeddings";

    private static final ResponseHandler EMBEDDINGS_HANDLER = new AzureOpenAiResponseHandler(
        "azure openai embedding",
        OpenAiEmbeddingsResponseEntity::fromResponse,
        false
    );

    private static final ResponseHandler COMPLETION_HANDLER = new AzureOpenAiResponseHandler(
        "azure openai completion",
        AzureOpenAiCompletionResponseEntity::fromResponse,
        true
    );

    private final Sender sender;
    private final ServiceComponents serviceComponents;

    public AzureOpenAiActionCreator(Sender sender, ServiceComponents serviceComponents) {
        this.sender = Objects.requireNonNull(sender);
        this.serviceComponents = Objects.requireNonNull(serviceComponents);
    }

    @Override
    public ExecutableAction create(AzureOpenAiEmbeddingsModel model, Map<String, Object> taskSettings) {
        var overriddenModel = AzureOpenAiEmbeddingsModel.of(model, taskSettings);
        var requestManager = new GenericRequestManager<>(
            serviceComponents.threadPool(),
            overriddenModel,
            EMBEDDINGS_HANDLER,
            (embeddingsInput) -> new AzureOpenAiEmbeddingsRequest(
                serviceComponents.truncator(),
                truncate(embeddingsInput.getTextInputs(), overriddenModel.getServiceSettings().maxInputTokens()),
                embeddingsInput.getInputType(),
                overriddenModel
            ),
            EmbeddingsInput.class
        );
        return new SenderExecutableAction(sender, requestManager, constructFailedToSendRequestMessage(EMBEDDINGS_ERROR_PREFIX));
    }

    @Override
    public ExecutableAction create(AzureOpenAiCompletionModel model, Map<String, Object> taskSettings) {
        var overriddenModel = AzureOpenAiCompletionModel.of(model, taskSettings);
        var requestManager = new GenericRequestManager<>(
            serviceComponents.threadPool(),
            overriddenModel,
            COMPLETION_HANDLER,
            (chatCompletionInput) -> new AzureOpenAiCompletionRequest(
                chatCompletionInput.getInputs(),
                overriddenModel,
                chatCompletionInput.stream()
            ),
            ChatCompletionInput.class
        );
        return new SingleInputSenderExecutableAction(
            sender,
            requestManager,
            constructFailedToSendRequestMessage(COMPLETION_ERROR_PREFIX),
            COMPLETION_ERROR_PREFIX
        );
    }
}
