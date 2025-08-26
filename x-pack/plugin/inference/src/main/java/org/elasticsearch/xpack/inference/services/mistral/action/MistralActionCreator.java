/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.mistral.action;

import org.elasticsearch.inference.TaskType;
import org.elasticsearch.xpack.inference.external.action.ExecutableAction;
import org.elasticsearch.xpack.inference.external.action.SenderExecutableAction;
import org.elasticsearch.xpack.inference.external.action.SingleInputSenderExecutableAction;
import org.elasticsearch.xpack.inference.external.http.retry.ResponseHandler;
import org.elasticsearch.xpack.inference.external.http.sender.ChatCompletionInput;
import org.elasticsearch.xpack.inference.external.http.sender.GenericRequestManager;
import org.elasticsearch.xpack.inference.external.http.sender.Sender;
import org.elasticsearch.xpack.inference.external.http.sender.UnifiedChatInput;
import org.elasticsearch.xpack.inference.services.ServiceComponents;
import org.elasticsearch.xpack.inference.services.mistral.MistralCompletionResponseHandler;
import org.elasticsearch.xpack.inference.services.mistral.MistralEmbeddingsRequestManager;
import org.elasticsearch.xpack.inference.services.mistral.completion.MistralChatCompletionModel;
import org.elasticsearch.xpack.inference.services.mistral.embeddings.MistralEmbeddingsModel;
import org.elasticsearch.xpack.inference.services.mistral.request.completion.MistralChatCompletionRequest;
import org.elasticsearch.xpack.inference.services.openai.response.OpenAiChatCompletionResponseEntity;

import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.core.Strings.format;

/**
 * MistralActionCreator is responsible for creating executable actions for Mistral models.
 * It implements the MistralActionVisitor interface to provide specific implementations
 * for different types of Mistral models.
 */
public class MistralActionCreator implements MistralActionVisitor {

    public static final String COMPLETION_ERROR_PREFIX = "Mistral completions";
    static final String USER_ROLE = "user";
    static final ResponseHandler COMPLETION_HANDLER = new MistralCompletionResponseHandler(
        "mistral completions",
        OpenAiChatCompletionResponseEntity::fromResponse
    );
    private final Sender sender;
    private final ServiceComponents serviceComponents;

    public MistralActionCreator(Sender sender, ServiceComponents serviceComponents) {
        this.sender = Objects.requireNonNull(sender);
        this.serviceComponents = Objects.requireNonNull(serviceComponents);
    }

    @Override
    public ExecutableAction create(MistralEmbeddingsModel embeddingsModel, Map<String, Object> taskSettings) {
        var requestManager = new MistralEmbeddingsRequestManager(
            embeddingsModel,
            serviceComponents.truncator(),
            serviceComponents.threadPool()
        );
        var errorMessage = buildErrorMessage(TaskType.TEXT_EMBEDDING, embeddingsModel.getInferenceEntityId());
        return new SenderExecutableAction(sender, requestManager, errorMessage);
    }

    @Override
    public ExecutableAction create(MistralChatCompletionModel chatCompletionModel) {
        var manager = new GenericRequestManager<>(
            serviceComponents.threadPool(),
            chatCompletionModel,
            COMPLETION_HANDLER,
            inputs -> new MistralChatCompletionRequest(new UnifiedChatInput(inputs, USER_ROLE), chatCompletionModel),
            ChatCompletionInput.class
        );

        var errorMessage = buildErrorMessage(TaskType.COMPLETION, chatCompletionModel.getInferenceEntityId());
        return new SingleInputSenderExecutableAction(sender, manager, errorMessage, COMPLETION_ERROR_PREFIX);
    }

    /**
     * Builds an error message for Mistral actions.
     *
     * @param requestType The type of request (e.g., TEXT_EMBEDDING, COMPLETION).
     * @param inferenceId The ID of the inference entity.
     * @return A formatted error message.
     */
    public static String buildErrorMessage(TaskType requestType, String inferenceId) {
        return format("Failed to send Mistral %s request from inference entity id [%s]", requestType.toString(), inferenceId);
    }
}
