/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.openai.action;

import org.elasticsearch.xpack.inference.external.action.ExecutableAction;
import org.elasticsearch.xpack.inference.external.action.SenderExecutableAction;
import org.elasticsearch.xpack.inference.external.action.SingleInputSenderExecutableAction;
import org.elasticsearch.xpack.inference.external.http.retry.ResponseHandler;
import org.elasticsearch.xpack.inference.external.http.sender.ChatCompletionInput;
import org.elasticsearch.xpack.inference.external.http.sender.GenericRequestManager;
import org.elasticsearch.xpack.inference.external.http.sender.Sender;
import org.elasticsearch.xpack.inference.external.http.sender.TruncatingRequestManager;
import org.elasticsearch.xpack.inference.external.http.sender.UnifiedChatInput;
import org.elasticsearch.xpack.inference.services.ServiceComponents;
import org.elasticsearch.xpack.inference.services.openai.OpenAiChatCompletionResponseHandler;
import org.elasticsearch.xpack.inference.services.openai.OpenAiResponseHandler;
import org.elasticsearch.xpack.inference.services.openai.completion.OpenAiChatCompletionModel;
import org.elasticsearch.xpack.inference.services.openai.embeddings.OpenAiEmbeddingsModel;
import org.elasticsearch.xpack.inference.services.openai.request.OpenAiEmbeddingsRequest;
import org.elasticsearch.xpack.inference.services.openai.request.OpenAiUnifiedChatCompletionRequest;
import org.elasticsearch.xpack.inference.services.openai.response.OpenAiChatCompletionResponseEntity;
import org.elasticsearch.xpack.inference.services.openai.response.OpenAiEmbeddingsResponseEntity;

import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.xpack.inference.external.action.ActionUtils.constructFailedToSendRequestMessage;

/**
 * Provides a way to construct an {@link ExecutableAction} using the visitor pattern based on the openai model type.
 */
public class OpenAiActionCreator implements OpenAiActionVisitor {
    public static final String COMPLETION_ERROR_PREFIX = "OpenAI chat completions";
    public static final String USER_ROLE = "user";

    static final ResponseHandler COMPLETION_HANDLER = new OpenAiChatCompletionResponseHandler(
        "openai completion",
        OpenAiChatCompletionResponseEntity::fromResponse
    );
    public static final ResponseHandler EMBEDDINGS_HANDLER = new OpenAiResponseHandler(
        "openai text embedding",
        OpenAiEmbeddingsResponseEntity::fromResponse,
        false
    );

    private final Sender sender;
    private final ServiceComponents serviceComponents;

    public OpenAiActionCreator(Sender sender, ServiceComponents serviceComponents) {
        this.sender = Objects.requireNonNull(sender);
        this.serviceComponents = Objects.requireNonNull(serviceComponents);
    }

    @Override
    public ExecutableAction create(OpenAiEmbeddingsModel model, Map<String, Object> taskSettings) {
        var overriddenModel = OpenAiEmbeddingsModel.of(model, taskSettings);
        var manager = new TruncatingRequestManager(
            serviceComponents.threadPool(),
            overriddenModel,
            EMBEDDINGS_HANDLER,
            (truncationResult) -> new OpenAiEmbeddingsRequest(serviceComponents.truncator(), truncationResult, overriddenModel),
            overriddenModel.getServiceSettings().maxInputTokens()
        );

        var errorMessage = constructFailedToSendRequestMessage("OpenAI embeddings");
        return new SenderExecutableAction(sender, manager, errorMessage);
    }

    @Override
    public ExecutableAction create(OpenAiChatCompletionModel model, Map<String, Object> taskSettings) {
        var overriddenModel = OpenAiChatCompletionModel.of(model, taskSettings);
        var manager = new GenericRequestManager<>(
            serviceComponents.threadPool(),
            overriddenModel,
            COMPLETION_HANDLER,
            (inputs) -> new OpenAiUnifiedChatCompletionRequest(new UnifiedChatInput(inputs, USER_ROLE), overriddenModel),
            ChatCompletionInput.class
        );

        var errorMessage = constructFailedToSendRequestMessage(COMPLETION_ERROR_PREFIX);
        return new SingleInputSenderExecutableAction(sender, manager, errorMessage, COMPLETION_ERROR_PREFIX);
    }
}
