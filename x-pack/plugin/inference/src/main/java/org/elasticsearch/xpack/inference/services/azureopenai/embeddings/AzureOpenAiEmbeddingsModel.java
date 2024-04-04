/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.azureopenai.embeddings;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.ModelSecrets;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.xpack.inference.external.action.ExecutableAction;
import org.elasticsearch.xpack.inference.external.action.azureopenai.AzureOpenAiActionVisitor;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.azureopenai.AzureOpenAiModel;
import org.elasticsearch.xpack.inference.services.settings.AzureOpenAiSecretSettings;
import org.elasticsearch.xpack.inference.services.settings.DefaultSecretSettings;

import java.util.Map;

public class AzureOpenAiEmbeddingsModel extends AzureOpenAiModel {

    public static AzureOpenAiEmbeddingsModel of(AzureOpenAiEmbeddingsModel model, Map<String, Object> taskSettings) {
        if (taskSettings == null || taskSettings.isEmpty()) {
            return model;
        }

        var requestTaskSettings = AzureOpenAiEmbeddingsRequestTaskSettings.fromMap(taskSettings);
        return new AzureOpenAiEmbeddingsModel(model, AzureOpenAiEmbeddingsTaskSettings.of(model.getTaskSettings(), requestTaskSettings));
    }

    public AzureOpenAiEmbeddingsModel(
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
            AzureOpenAiEmbeddingsServiceSettings.fromMap(serviceSettings, context),
            AzureOpenAiEmbeddingsTaskSettings.fromMap(taskSettings, context),
            DefaultSecretSettings.fromMap(secrets)
        );
    }

    // Should only be used directly for testing
    AzureOpenAiEmbeddingsModel(
        String inferenceEntityId,
        TaskType taskType,
        String service,
        AzureOpenAiEmbeddingsServiceSettings serviceSettings,
        AzureOpenAiEmbeddingsTaskSettings taskSettings,
        @Nullable DefaultSecretSettings secrets
    ) {
        super(new ModelConfigurations(inferenceEntityId, taskType, service, serviceSettings, taskSettings), new ModelSecrets(secrets));
    }

    private AzureOpenAiEmbeddingsModel(AzureOpenAiEmbeddingsModel originalModel, AzureOpenAiEmbeddingsTaskSettings taskSettings) {
        super(originalModel, taskSettings);
    }

    public AzureOpenAiEmbeddingsModel(AzureOpenAiEmbeddingsModel originalModel, AzureOpenAiEmbeddingsServiceSettings serviceSettings) {
        super(originalModel, serviceSettings);
    }

    @Override
    public AzureOpenAiEmbeddingsServiceSettings getServiceSettings() {
        return (AzureOpenAiEmbeddingsServiceSettings) super.getServiceSettings();
    }

    @Override
    public AzureOpenAiEmbeddingsTaskSettings getTaskSettings() {
        return (AzureOpenAiEmbeddingsTaskSettings) super.getTaskSettings();
    }

    @Override
    public AzureOpenAiSecretSettings getSecretSettings() {
        return (AzureOpenAiSecretSettings) super.getSecretSettings();
    }

    @Override
    public ExecutableAction accept(AzureOpenAiActionVisitor creator, Map<String, Object> taskSettings) {
        return creator.create(this, taskSettings);
    }
}
