/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.anthropic.action;

import org.elasticsearch.xpack.inference.external.action.ExecutableAction;
import org.elasticsearch.xpack.inference.external.action.SingleInputSenderExecutableAction;
import org.elasticsearch.xpack.inference.external.http.retry.ResponseHandler;
import org.elasticsearch.xpack.inference.external.http.sender.ChatCompletionInput;
import org.elasticsearch.xpack.inference.external.http.sender.GenericRequestManager;
import org.elasticsearch.xpack.inference.external.http.sender.Sender;
import org.elasticsearch.xpack.inference.services.ServiceComponents;
import org.elasticsearch.xpack.inference.services.anthropic.AnthropicResponseHandler;
import org.elasticsearch.xpack.inference.services.anthropic.completion.AnthropicChatCompletionModel;
import org.elasticsearch.xpack.inference.services.anthropic.request.AnthropicChatCompletionRequest;
import org.elasticsearch.xpack.inference.services.anthropic.response.AnthropicChatCompletionResponseEntity;

import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.xpack.inference.external.action.ActionUtils.constructFailedToSendRequestMessage;

/**
 * Provides a way to construct an {@link ExecutableAction} using the visitor pattern based on the anthropic model type.
 */
public class AnthropicActionCreator implements AnthropicActionVisitor {
    private static final String ERROR_PREFIX = "Anthropic chat completions";

    private static final ResponseHandler COMPLETION_HANDLER = new AnthropicResponseHandler(
        "anthropic completions",
        AnthropicChatCompletionResponseEntity::fromResponse,
        true
    );

    private final Sender sender;
    private final ServiceComponents serviceComponents;

    public AnthropicActionCreator(Sender sender, ServiceComponents serviceComponents) {
        this.sender = Objects.requireNonNull(sender);
        this.serviceComponents = Objects.requireNonNull(serviceComponents);
    }

    @Override
    public ExecutableAction create(AnthropicChatCompletionModel model, Map<String, Object> taskSettings) {
        var overriddenModel = AnthropicChatCompletionModel.of(model, taskSettings);
        var requestManager = new GenericRequestManager<>(
            serviceComponents.threadPool(),
            overriddenModel,
            COMPLETION_HANDLER,
            (chatCompletionInput) -> new AnthropicChatCompletionRequest(
                chatCompletionInput.getInputs(),
                overriddenModel,
                chatCompletionInput.stream()
            ),
            ChatCompletionInput.class
        );
        return new SingleInputSenderExecutableAction(
            sender,
            requestManager,
            constructFailedToSendRequestMessage(ERROR_PREFIX),
            ERROR_PREFIX
        );
    }
}
