/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.action.anthropic;

import org.elasticsearch.xpack.inference.external.action.ExecutableAction;
import org.elasticsearch.xpack.inference.external.http.sender.Sender;
import org.elasticsearch.xpack.inference.services.ServiceComponents;
import org.elasticsearch.xpack.inference.services.anthropic.completion.AnthropicChatCompletionModel;

import java.util.Map;
import java.util.Objects;

/**
 * Provides a way to construct an {@link ExecutableAction} using the visitor pattern based on the anthropic model type.
 */
public class AnthropicActionCreator implements AnthropicActionVisitor {
    private final Sender sender;
    private final ServiceComponents serviceComponents;

    public AnthropicActionCreator(Sender sender, ServiceComponents serviceComponents) {
        this.sender = Objects.requireNonNull(sender);
        this.serviceComponents = Objects.requireNonNull(serviceComponents);
    }

    @Override
    public ExecutableAction create(AnthropicChatCompletionModel model, Map<String, Object> taskSettings) {
        var overriddenModel = AnthropicChatCompletionModel.of(model, taskSettings);

        return new AnthropicChatCompletionAction(sender, overriddenModel, serviceComponents);
    }
}
