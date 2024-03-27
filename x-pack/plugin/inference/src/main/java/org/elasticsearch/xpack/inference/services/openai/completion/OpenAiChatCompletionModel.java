/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.openai.completion;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.ModelSecrets;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.xpack.inference.external.action.ExecutableAction;
import org.elasticsearch.xpack.inference.external.action.openai.OpenAiActionVisitor;
import org.elasticsearch.xpack.inference.services.openai.OpenAiModel;
import org.elasticsearch.xpack.inference.services.settings.DefaultSecretSettings;

import java.util.Map;

public class OpenAiChatCompletionModel extends OpenAiModel {

    public static OpenAiChatCompletionModel of(OpenAiChatCompletionModel model, Map<String, Object> taskSettings) {
        if (taskSettings == null || taskSettings.isEmpty()) {
            return model;
        }

        var requestTaskSettings = OpenAiChatCompletionRequestTaskSettings.fromMap(taskSettings);
        return new OpenAiChatCompletionModel(model, OpenAiChatCompletionTaskSettings.of(model.getTaskSettings(), requestTaskSettings));
    }

    public OpenAiChatCompletionModel(
        String inferenceEntityId,
        TaskType taskType,
        String service,
        Map<String, Object> serviceSettings,
        Map<String, Object> taskSettings,
        @Nullable Map<String, Object> secrets
    ) {
        this(
            inferenceEntityId,
            taskType,
            service,
            OpenAiChatCompletionServiceSettings.fromMap(serviceSettings),
            OpenAiChatCompletionTaskSettings.fromMap(taskSettings),
            DefaultSecretSettings.fromMap(secrets)
        );
    }

    OpenAiChatCompletionModel(
        String modelId,
        TaskType taskType,
        String service,
        OpenAiChatCompletionServiceSettings serviceSettings,
        OpenAiChatCompletionTaskSettings taskSettings,
        @Nullable DefaultSecretSettings secrets
    ) {
        super(new ModelConfigurations(modelId, taskType, service, serviceSettings, taskSettings), new ModelSecrets(secrets));
    }

    private OpenAiChatCompletionModel(OpenAiChatCompletionModel originalModel, OpenAiChatCompletionTaskSettings taskSettings) {
        super(originalModel, taskSettings);
    }

    @Override
    public OpenAiChatCompletionServiceSettings getServiceSettings() {
        return (OpenAiChatCompletionServiceSettings) super.getServiceSettings();
    }

    @Override
    public OpenAiChatCompletionTaskSettings getTaskSettings() {
        return (OpenAiChatCompletionTaskSettings) super.getTaskSettings();
    }

    @Override
    public DefaultSecretSettings getSecretSettings() {
        return (DefaultSecretSettings) super.getSecretSettings();
    }

    @Override
    public ExecutableAction accept(OpenAiActionVisitor creator, Map<String, Object> taskSettings) {
        return creator.create(this, taskSettings);
    }
}
