/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.action.openai;

import org.elasticsearch.xpack.inference.external.action.ExecutableAction;
import org.elasticsearch.xpack.inference.external.action.SenderExecutableAction;
import org.elasticsearch.xpack.inference.external.action.SingleInputSenderExecutableAction;
import org.elasticsearch.xpack.inference.external.http.sender.OpenAiCompletionRequestManager;
import org.elasticsearch.xpack.inference.external.http.sender.OpenAiEmbeddingsRequestManager;
import org.elasticsearch.xpack.inference.external.http.sender.Sender;
import org.elasticsearch.xpack.inference.services.ServiceComponents;
import org.elasticsearch.xpack.inference.services.openai.completion.OpenAiChatCompletionModel;
import org.elasticsearch.xpack.inference.services.openai.embeddings.OpenAiEmbeddingsModel;

import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.xpack.inference.external.action.ActionUtils.constructFailedToSendRequestMessage;

/**
 * Provides a way to construct an {@link ExecutableAction} using the visitor pattern based on the openai model type.
 */
public class OpenAiActionCreator implements OpenAiActionVisitor {
    private static final String COMPLETION_ERROR_PREFIX = "OpenAI chat completions";
    private final Sender sender;
    private final ServiceComponents serviceComponents;

    public OpenAiActionCreator(Sender sender, ServiceComponents serviceComponents) {
        this.sender = Objects.requireNonNull(sender);
        this.serviceComponents = Objects.requireNonNull(serviceComponents);
    }

    @Override
    public ExecutableAction create(OpenAiEmbeddingsModel model, Map<String, Object> taskSettings) {
        var overriddenModel = OpenAiEmbeddingsModel.of(model, taskSettings);
        var requestCreator = OpenAiEmbeddingsRequestManager.of(
            overriddenModel,
            serviceComponents.truncator(),
            serviceComponents.threadPool()
        );
        var errorMessage = constructFailedToSendRequestMessage(overriddenModel.getServiceSettings().uri(), "OpenAI embeddings");
        return new SenderExecutableAction(sender, requestCreator, errorMessage);
    }

    @Override
    public ExecutableAction create(OpenAiChatCompletionModel model, Map<String, Object> taskSettings) {
        var overriddenModel = OpenAiChatCompletionModel.of(model, taskSettings);
        var requestCreator = OpenAiCompletionRequestManager.of(overriddenModel, serviceComponents.threadPool());
        var errorMessage = constructFailedToSendRequestMessage(overriddenModel.getServiceSettings().uri(), COMPLETION_ERROR_PREFIX);
        return new SingleInputSenderExecutableAction(sender, requestCreator, errorMessage, COMPLETION_ERROR_PREFIX);
    }
}
