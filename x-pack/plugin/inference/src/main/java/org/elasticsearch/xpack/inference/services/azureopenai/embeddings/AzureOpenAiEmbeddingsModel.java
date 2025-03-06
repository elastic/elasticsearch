/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.azureopenai.embeddings;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.ChunkingSettings;
import org.elasticsearch.inference.InputType;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.ModelSecrets;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.xpack.inference.external.action.ExecutableAction;
import org.elasticsearch.xpack.inference.external.action.azureopenai.AzureOpenAiActionVisitor;
import org.elasticsearch.xpack.inference.external.request.azureopenai.AzureOpenAiUtils;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.azureopenai.AzureOpenAiModel;
import org.elasticsearch.xpack.inference.services.azureopenai.AzureOpenAiSecretSettings;

import java.net.URISyntaxException;
import java.util.Map;

public class AzureOpenAiEmbeddingsModel extends AzureOpenAiModel {

    public static AzureOpenAiEmbeddingsModel of(AzureOpenAiEmbeddingsModel model, Map<String, Object> taskSettings, InputType inputType) {
        if (taskSettings == null || taskSettings.isEmpty()) {
            return inputType == null ? model : new AzureOpenAiEmbeddingsModel(model, model.getTaskSettings(), inputType);
        }

        var requestTaskSettings = AzureOpenAiEmbeddingsRequestTaskSettings.fromMap(taskSettings);
        return new AzureOpenAiEmbeddingsModel(
            model,
            AzureOpenAiEmbeddingsTaskSettings.of(model.getTaskSettings(), requestTaskSettings),
            inputType
        );
    }

    private InputType inputType;

    public AzureOpenAiEmbeddingsModel(
        String inferenceEntityId,
        TaskType taskType,
        String service,
        Map<String, Object> serviceSettings,
        Map<String, Object> taskSettings,
        ChunkingSettings chunkingSettings,
        @Nullable Map<String, Object> secrets,
        ConfigurationParseContext context
    ) {
        this(
            inferenceEntityId,
            taskType,
            service,
            AzureOpenAiEmbeddingsServiceSettings.fromMap(serviceSettings, context),
            AzureOpenAiEmbeddingsTaskSettings.fromMap(taskSettings),
            chunkingSettings,
            AzureOpenAiSecretSettings.fromMap(secrets),
            null
        );
    }

    // Should only be used directly for testing
    AzureOpenAiEmbeddingsModel(
        String inferenceEntityId,
        TaskType taskType,
        String service,
        AzureOpenAiEmbeddingsServiceSettings serviceSettings,
        AzureOpenAiEmbeddingsTaskSettings taskSettings,
        ChunkingSettings chunkingSettings,
        @Nullable AzureOpenAiSecretSettings secrets,
        @Nullable InputType inputType
    ) {
        super(
            new ModelConfigurations(inferenceEntityId, taskType, service, serviceSettings, taskSettings, chunkingSettings),
            new ModelSecrets(secrets),
            serviceSettings
        );
        this.inputType = inputType;
        try {
            this.uri = buildUriString();
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }

    private AzureOpenAiEmbeddingsModel(
        AzureOpenAiEmbeddingsModel originalModel,
        AzureOpenAiEmbeddingsTaskSettings taskSettings,
        InputType inputType
    ) {
        super(originalModel, taskSettings);
        this.inputType = inputType;
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

    public InputType inputType() {
        return inputType;
    }

    @Override
    public ExecutableAction accept(AzureOpenAiActionVisitor creator, Map<String, Object> taskSettings, InputType inputType) {
        return creator.create(this, taskSettings, inputType);
    }

    @Override
    public String resourceName() {
        return getServiceSettings().resourceName();
    }

    @Override
    public String deploymentId() {
        return getServiceSettings().deploymentId();
    }

    @Override
    public String apiVersion() {
        return getServiceSettings().apiVersion();
    }

    @Override
    public String[] operationPathSegments() {
        return new String[] { AzureOpenAiUtils.EMBEDDINGS_PATH };
    }
}
