/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.action.anthropic;

import org.elasticsearch.xpack.inference.external.action.ExecutableAction;
import org.elasticsearch.xpack.inference.external.action.SingleInputSenderExecutableAction;
import org.elasticsearch.xpack.inference.external.http.sender.AnthropicCompletionRequestManager;
import org.elasticsearch.xpack.inference.external.http.sender.Sender;
import org.elasticsearch.xpack.inference.services.ServiceComponents;
import org.elasticsearch.xpack.inference.services.anthropic.completion.AnthropicChatCompletionModel;

import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.xpack.inference.external.action.ActionUtils.constructFailedToSendRequestMessage;

/**
 * Provides a way to construct an {@link ExecutableAction} using the visitor pattern based on the anthropic model type.
 */
public class AnthropicActionCreator implements AnthropicActionVisitor {
    private static final String ERROR_PREFIX = "Anthropic chat completions";
    private final Sender sender;
    private final ServiceComponents serviceComponents;

    public AnthropicActionCreator(Sender sender, ServiceComponents serviceComponents) {
        this.sender = Objects.requireNonNull(sender);
        this.serviceComponents = Objects.requireNonNull(serviceComponents);
    }

    @Override
    public ExecutableAction create(AnthropicChatCompletionModel model, Map<String, Object> taskSettings) {
        var overriddenModel = AnthropicChatCompletionModel.of(model, taskSettings);
        var requestCreator = AnthropicCompletionRequestManager.of(overriddenModel, serviceComponents.threadPool());
        var errorMessage = constructFailedToSendRequestMessage(overriddenModel.getUri(), ERROR_PREFIX);
        return new SingleInputSenderExecutableAction(sender, requestCreator, errorMessage, ERROR_PREFIX);
    }
}
