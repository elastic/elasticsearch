/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.ai21.action;

import org.elasticsearch.inference.TaskType;
import org.elasticsearch.xpack.inference.external.action.ExecutableAction;
import org.elasticsearch.xpack.inference.external.action.SingleInputSenderExecutableAction;
import org.elasticsearch.xpack.inference.external.http.retry.ErrorResponse;
import org.elasticsearch.xpack.inference.external.http.retry.ResponseHandler;
import org.elasticsearch.xpack.inference.external.http.sender.ChatCompletionInput;
import org.elasticsearch.xpack.inference.external.http.sender.GenericRequestManager;
import org.elasticsearch.xpack.inference.external.http.sender.Sender;
import org.elasticsearch.xpack.inference.external.http.sender.UnifiedChatInput;
import org.elasticsearch.xpack.inference.services.ServiceComponents;
import org.elasticsearch.xpack.inference.services.ai21.completion.Ai21ChatCompletionModel;
import org.elasticsearch.xpack.inference.services.ai21.request.Ai21ChatCompletionRequest;
import org.elasticsearch.xpack.inference.services.openai.OpenAiChatCompletionResponseHandler;
import org.elasticsearch.xpack.inference.services.openai.response.OpenAiChatCompletionResponseEntity;

import java.util.Objects;

import static org.elasticsearch.core.Strings.format;

/**
 * Ai21ActionCreator is responsible for creating actions for AI21 services.
 * It implements the Ai21ActionVisitor interface to handle completion requests.
 */
public class Ai21ActionCreator implements Ai21ActionVisitor {

    public static final String COMPLETION_ERROR_PREFIX = "AI21 completions";
    public static final String USER_ROLE = "user";
    public static final ResponseHandler COMPLETION_HANDLER = new OpenAiChatCompletionResponseHandler(
        "ai21 completions",
        OpenAiChatCompletionResponseEntity::fromResponse,
        ErrorResponse::fromResponse
    );
    private final Sender sender;
    private final ServiceComponents serviceComponents;

    public Ai21ActionCreator(Sender sender, ServiceComponents serviceComponents) {
        this.sender = Objects.requireNonNull(sender);
        this.serviceComponents = Objects.requireNonNull(serviceComponents);
    }

    @Override
    public ExecutableAction create(Ai21ChatCompletionModel chatCompletionModel) {
        var manager = new GenericRequestManager<>(
            serviceComponents.threadPool(),
            chatCompletionModel,
            COMPLETION_HANDLER,
            inputs -> new Ai21ChatCompletionRequest(new UnifiedChatInput(inputs, USER_ROLE), chatCompletionModel),
            ChatCompletionInput.class
        );

        var errorMessage = buildErrorMessage(TaskType.COMPLETION, chatCompletionModel.getInferenceEntityId());
        return new SingleInputSenderExecutableAction(sender, manager, errorMessage, COMPLETION_ERROR_PREFIX);
    }

    /**
     * Builds an error message for AI21 actions.
     *
     * @param requestType The type of request (e.g., COMPLETION).
     * @param inferenceId The ID of the inference entity.
     * @return A formatted error message.
     */
    public static String buildErrorMessage(TaskType requestType, String inferenceId) {
        return format("Failed to send AI21 %s request from inference entity id [%s]", requestType.toString(), inferenceId);
    }
}
