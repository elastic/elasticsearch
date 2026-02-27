/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.fireworksai.action;

import org.elasticsearch.xpack.inference.external.action.ExecutableAction;
import org.elasticsearch.xpack.inference.external.action.SenderExecutableAction;
import org.elasticsearch.xpack.inference.external.action.SingleInputSenderExecutableAction;
import org.elasticsearch.xpack.inference.external.http.retry.ResponseHandler;
import org.elasticsearch.xpack.inference.external.http.sender.ChatCompletionInput;
import org.elasticsearch.xpack.inference.external.http.sender.EmbeddingsInput;
import org.elasticsearch.xpack.inference.external.http.sender.GenericRequestManager;
import org.elasticsearch.xpack.inference.external.http.sender.Sender;
import org.elasticsearch.xpack.inference.external.http.sender.UnifiedChatInput;
import org.elasticsearch.xpack.inference.services.ServiceComponents;
import org.elasticsearch.xpack.inference.services.fireworksai.FireworksAiResponseHandler;
import org.elasticsearch.xpack.inference.services.fireworksai.completion.FireworksAiChatCompletionModel;
import org.elasticsearch.xpack.inference.services.fireworksai.embeddings.FireworksAiEmbeddingsModel;
import org.elasticsearch.xpack.inference.services.fireworksai.request.FireworksAiEmbeddingsRequest;
import org.elasticsearch.xpack.inference.services.fireworksai.request.FireworksAiUnifiedChatCompletionRequest;
import org.elasticsearch.xpack.inference.services.openai.OpenAiChatCompletionResponseHandler;
import org.elasticsearch.xpack.inference.services.openai.response.OpenAiChatCompletionResponseEntity;
import org.elasticsearch.xpack.inference.services.openai.response.OpenAiEmbeddingsResponseEntity;

import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.xpack.inference.external.action.ActionUtils.constructFailedToSendRequestMessage;

public class FireworksAiActionCreator implements FireworksAiActionVisitor {

    public static final String COMPLETION_ERROR_PREFIX = "FireworksAI chat completions";
    public static final String COMPLETION_REQUEST_TYPE = "fireworksai completion";
    private static final String USER_ROLE = "user";

    private static final ResponseHandler EMBEDDINGS_HANDLER = new FireworksAiResponseHandler(
        "fireworksai embeddings",
        OpenAiEmbeddingsResponseEntity::fromResponse,
        false
    );

    private static final ResponseHandler COMPLETION_HANDLER = new OpenAiChatCompletionResponseHandler(
        COMPLETION_REQUEST_TYPE,
        OpenAiChatCompletionResponseEntity::fromResponse
    );

    private final Sender sender;
    private final ServiceComponents serviceComponents;

    public FireworksAiActionCreator(Sender sender, ServiceComponents serviceComponents) {
        this.sender = Objects.requireNonNull(sender);
        this.serviceComponents = Objects.requireNonNull(serviceComponents);
    }

    @Override
    public ExecutableAction create(FireworksAiEmbeddingsModel model, Map<String, Object> taskSettings) {
        var overriddenModel = FireworksAiEmbeddingsModel.of(model, taskSettings);
        var manager = new GenericRequestManager<>(
            serviceComponents.threadPool(),
            overriddenModel,
            EMBEDDINGS_HANDLER,
            (embeddingInput) -> new FireworksAiEmbeddingsRequest(embeddingInput.getTextInputs(), overriddenModel),
            EmbeddingsInput.class
        );

        var errorMessage = constructFailedToSendRequestMessage("FireworksAI embeddings");
        return new SenderExecutableAction(sender, manager, errorMessage);
    }

    @Override
    public ExecutableAction create(FireworksAiChatCompletionModel model, Map<String, Object> taskSettings) {
        var overriddenModel = FireworksAiChatCompletionModel.of(model, taskSettings);
        var manager = new GenericRequestManager<>(
            serviceComponents.threadPool(),
            overriddenModel,
            COMPLETION_HANDLER,
            inputs -> new FireworksAiUnifiedChatCompletionRequest(new UnifiedChatInput(inputs, USER_ROLE), overriddenModel),
            ChatCompletionInput.class
        );

        var errorMessage = constructFailedToSendRequestMessage(COMPLETION_ERROR_PREFIX);
        return new SingleInputSenderExecutableAction(sender, manager, errorMessage, COMPLETION_ERROR_PREFIX);
    }
}
