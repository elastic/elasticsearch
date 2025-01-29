/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.action.azureopenai;

import org.elasticsearch.xpack.inference.external.action.ExecutableAction;
import org.elasticsearch.xpack.inference.external.action.SenderExecutableAction;
import org.elasticsearch.xpack.inference.external.action.SingleInputSenderExecutableAction;
import org.elasticsearch.xpack.inference.external.http.sender.AzureOpenAiCompletionRequestManager;
import org.elasticsearch.xpack.inference.external.http.sender.AzureOpenAiEmbeddingsRequestManager;
import org.elasticsearch.xpack.inference.external.http.sender.Sender;
import org.elasticsearch.xpack.inference.services.ServiceComponents;
import org.elasticsearch.xpack.inference.services.azureopenai.completion.AzureOpenAiCompletionModel;
import org.elasticsearch.xpack.inference.services.azureopenai.embeddings.AzureOpenAiEmbeddingsModel;

import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.xpack.inference.external.action.ActionUtils.constructFailedToSendRequestMessage;

/**
 * Provides a way to construct an {@link ExecutableAction} using the visitor pattern based on the openai model type.
 */
public class AzureOpenAiActionCreator implements AzureOpenAiActionVisitor {
    private static final String COMPLETION_ERROR_PREFIX = "Azure OpenAI completion";
    private final Sender sender;
    private final ServiceComponents serviceComponents;

    public AzureOpenAiActionCreator(Sender sender, ServiceComponents serviceComponents) {
        this.sender = Objects.requireNonNull(sender);
        this.serviceComponents = Objects.requireNonNull(serviceComponents);
    }

    @Override
    public ExecutableAction create(AzureOpenAiEmbeddingsModel model, Map<String, Object> taskSettings) {
        var overriddenModel = AzureOpenAiEmbeddingsModel.of(model, taskSettings);
        var requestCreator = new AzureOpenAiEmbeddingsRequestManager(
            overriddenModel,
            serviceComponents.truncator(),
            serviceComponents.threadPool()
        );
        var errorMessage = constructFailedToSendRequestMessage(overriddenModel.getUri(), "Azure OpenAI embeddings");
        return new SenderExecutableAction(sender, requestCreator, errorMessage);
    }

    @Override
    public ExecutableAction create(AzureOpenAiCompletionModel model, Map<String, Object> taskSettings) {
        var overriddenModel = AzureOpenAiCompletionModel.of(model, taskSettings);
        var requestCreator = new AzureOpenAiCompletionRequestManager(overriddenModel, serviceComponents.threadPool());
        var errorMessage = constructFailedToSendRequestMessage(overriddenModel.getUri(), COMPLETION_ERROR_PREFIX);
        return new SingleInputSenderExecutableAction(sender, requestCreator, errorMessage, COMPLETION_ERROR_PREFIX);
    }
}
