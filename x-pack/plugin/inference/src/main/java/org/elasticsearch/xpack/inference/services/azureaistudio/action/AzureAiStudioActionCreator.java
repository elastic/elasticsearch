/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.azureaistudio.action;

import org.elasticsearch.xpack.inference.external.action.ExecutableAction;
import org.elasticsearch.xpack.inference.external.action.SenderExecutableAction;
import org.elasticsearch.xpack.inference.external.http.sender.Sender;
import org.elasticsearch.xpack.inference.services.ServiceComponents;
import org.elasticsearch.xpack.inference.services.azureaistudio.AzureAiStudioChatCompletionRequestManager;
import org.elasticsearch.xpack.inference.services.azureaistudio.AzureAiStudioEmbeddingsRequestManager;
import org.elasticsearch.xpack.inference.services.azureaistudio.completion.AzureAiStudioChatCompletionModel;
import org.elasticsearch.xpack.inference.services.azureaistudio.embeddings.AzureAiStudioEmbeddingsModel;

import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.xpack.inference.external.action.ActionUtils.constructFailedToSendRequestMessage;

public class AzureAiStudioActionCreator implements AzureAiStudioActionVisitor {
    private final Sender sender;
    private final ServiceComponents serviceComponents;

    public AzureAiStudioActionCreator(Sender sender, ServiceComponents serviceComponents) {
        this.sender = Objects.requireNonNull(sender);
        this.serviceComponents = Objects.requireNonNull(serviceComponents);
    }

    @Override
    public ExecutableAction create(AzureAiStudioChatCompletionModel completionModel, Map<String, Object> taskSettings) {
        var overriddenModel = AzureAiStudioChatCompletionModel.of(completionModel, taskSettings);
        var requestManager = new AzureAiStudioChatCompletionRequestManager(overriddenModel, serviceComponents.threadPool());
        var errorMessage = constructFailedToSendRequestMessage("Azure AI Studio completion");
        return new SenderExecutableAction(sender, requestManager, errorMessage);
    }

    @Override
    public ExecutableAction create(AzureAiStudioEmbeddingsModel embeddingsModel, Map<String, Object> taskSettings) {
        var overriddenModel = AzureAiStudioEmbeddingsModel.of(embeddingsModel, taskSettings);
        var requestManager = new AzureAiStudioEmbeddingsRequestManager(
            overriddenModel,
            serviceComponents.truncator(),
            serviceComponents.threadPool()
        );
        var errorMessage = constructFailedToSendRequestMessage("Azure AI Studio embeddings");
        return new SenderExecutableAction(sender, requestManager, errorMessage);
    }
}
