/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.anthropic.completion;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.ModelSecrets;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.xpack.inference.external.action.ExecutableAction;
import org.elasticsearch.xpack.inference.external.action.anthropic.AnthropicActionVisitor;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.anthropic.AnthropicModel;
import org.elasticsearch.xpack.inference.services.settings.DefaultSecretSettings;

import java.util.Map;

public class AnthropicChatCompletionModel extends AnthropicModel {

    public static AnthropicChatCompletionModel of(AnthropicChatCompletionModel model, Map<String, Object> taskSettings) {
        if (taskSettings == null || taskSettings.isEmpty()) {
            return model;
        }

        var requestTaskSettings = AnthropicChatCompletionRequestTaskSettings.fromMap(taskSettings);
        return new AnthropicChatCompletionModel(
            model,
            AnthropicChatCompletionTaskSettings.of(model.getTaskSettings(), requestTaskSettings)
        );
    }

    public AnthropicChatCompletionModel(
        String inferenceEntityId,
        TaskType taskType,
        String service,
        Map<String, Object> serviceSettings,
        Map<String, Object> taskSettings,
        @Nullable Map<String, Object> secrets,
        ConfigurationParseContext context
    ) {
        this(
            inferenceEntityId,
            taskType,
            service,
            AnthropicChatCompletionServiceSettings.fromMap(serviceSettings, context),
            AnthropicChatCompletionTaskSettings.fromMap(taskSettings),
            DefaultSecretSettings.fromMap(secrets)
        );
    }

    AnthropicChatCompletionModel(
        String modelId,
        TaskType taskType,
        String service,
        AnthropicChatCompletionServiceSettings serviceSettings,
        AnthropicChatCompletionTaskSettings taskSettings,
        @Nullable DefaultSecretSettings secrets
    ) {
        super(
            new ModelConfigurations(modelId, taskType, service, serviceSettings, taskSettings),
            new ModelSecrets(secrets),
            serviceSettings,
            secrets
        );
    }

    private AnthropicChatCompletionModel(AnthropicChatCompletionModel originalModel, AnthropicChatCompletionTaskSettings taskSettings) {
        super(originalModel, taskSettings);
    }

    @Override
    public AnthropicChatCompletionServiceSettings getServiceSettings() {
        return (AnthropicChatCompletionServiceSettings) super.getServiceSettings();
    }

    @Override
    public AnthropicChatCompletionTaskSettings getTaskSettings() {
        return (AnthropicChatCompletionTaskSettings) super.getTaskSettings();
    }

    @Override
    public DefaultSecretSettings getSecretSettings() {
        return (DefaultSecretSettings) super.getSecretSettings();
    }

    @Override
    public ExecutableAction accept(AnthropicActionVisitor creator, Map<String, Object> taskSettings) {
        return creator.create(this, taskSettings);
    }
}
