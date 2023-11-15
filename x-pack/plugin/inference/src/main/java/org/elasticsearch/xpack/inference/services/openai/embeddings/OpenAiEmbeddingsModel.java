/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.openai.embeddings;

import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.ModelSecrets;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.xpack.inference.external.action.ExecutableAction;
import org.elasticsearch.xpack.inference.external.action.openai.OpenAiActionVisitor;
import org.elasticsearch.xpack.inference.services.openai.OpenAiModel;
import org.elasticsearch.xpack.inference.services.openai.OpenAiServiceSettings;
import org.elasticsearch.xpack.inference.services.settings.DefaultSecretSettings;

import java.util.Map;

public class OpenAiEmbeddingsModel extends OpenAiModel {
    public OpenAiEmbeddingsModel(
        String modelId,
        TaskType taskType,
        String service,
        Map<String, Object> serviceSettings,
        Map<String, Object> taskSettings,
        Map<String, Object> secrets
    ) {
        this(
            modelId,
            taskType,
            service,
            OpenAiServiceSettings.fromMap(serviceSettings),
            OpenAiEmbeddingsTaskSettings.fromMap(taskSettings),
            DefaultSecretSettings.fromMap(secrets)
        );
    }

    // Should only be used directly for testing
    OpenAiEmbeddingsModel(
        String modelId,
        TaskType taskType,
        String service,
        OpenAiServiceSettings serviceSettings,
        OpenAiEmbeddingsTaskSettings taskSettings,
        DefaultSecretSettings secrets
    ) {
        super(new ModelConfigurations(modelId, taskType, service, serviceSettings, taskSettings), new ModelSecrets(secrets));
    }

    private OpenAiEmbeddingsModel(OpenAiEmbeddingsModel originalModel, OpenAiEmbeddingsTaskSettings taskSettings) {
        super(
            new ModelConfigurations(
                originalModel.getConfigurations().getModelId(),
                originalModel.getConfigurations().getTaskType(),
                originalModel.getConfigurations().getService(),
                originalModel.getServiceSettings(),
                taskSettings
            ),
            new ModelSecrets(originalModel.getSecretSettings())
        );
    }

    @Override
    public OpenAiServiceSettings getServiceSettings() {
        return (OpenAiServiceSettings) super.getServiceSettings();
    }

    @Override
    public OpenAiEmbeddingsTaskSettings getTaskSettings() {
        return (OpenAiEmbeddingsTaskSettings) super.getTaskSettings();
    }

    @Override
    public DefaultSecretSettings getSecretSettings() {
        return (DefaultSecretSettings) super.getSecretSettings();
    }

    @Override
    public ExecutableAction accept(OpenAiActionVisitor creator, Map<String, Object> taskSettings) {
        return creator.create(this, taskSettings);
    }

    public OpenAiEmbeddingsModel overrideWith(Map<String, Object> taskSettings) {
        var requestTaskSettings = OpenAiEmbeddingsRequestTaskSettings.fromMap(taskSettings);

        return new OpenAiEmbeddingsModel(this, getTaskSettings().overrideWith(requestTaskSettings));
    }
}
