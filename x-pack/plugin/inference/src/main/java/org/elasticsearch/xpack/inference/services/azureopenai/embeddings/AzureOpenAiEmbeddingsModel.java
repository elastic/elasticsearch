/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.azureopenai.embeddings;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.ChunkingSettings;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.ModelSecrets;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.inference.external.action.ExecutableAction;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.azureopenai.AzureOpenAiModel;
import org.elasticsearch.xpack.inference.services.azureopenai.AzureOpenAiServiceSettings;
import org.elasticsearch.xpack.inference.services.azureopenai.action.AzureOpenAiActionVisitor;
import org.elasticsearch.xpack.inference.services.azureopenai.request.AzureOpenAiUtils;
import org.elasticsearch.xpack.inference.services.azureopenai.secrets.AzureOpenAiSecretSettings;

import java.net.URISyntaxException;
import java.util.Map;

public class AzureOpenAiEmbeddingsModel extends AzureOpenAiModel {

    public static AzureOpenAiEmbeddingsModel of(AzureOpenAiEmbeddingsModel model, Map<String, Object> taskSettings) {
        if (taskSettings == null || taskSettings.isEmpty()) {
            return model;
        }

        return new AzureOpenAiEmbeddingsModel(model, model.getTaskSettings().updatedTaskSettings(taskSettings));
    }

    public AzureOpenAiEmbeddingsModel(
        String inferenceEntityId,
        TaskType taskType,
        String service,
        Map<String, Object> serviceSettings,
        Map<String, Object> taskSettings,
        ChunkingSettings chunkingSettings,
        @Nullable Map<String, Object> secrets,
        ConfigurationParseContext context,
        ThreadPool threadPool
    ) {
        this(
            inferenceEntityId,
            taskType,
            service,
            AzureOpenAiEmbeddingsServiceSettings.fromMap(serviceSettings, context),
            AzureOpenAiEmbeddingsTaskSettings.fromMap(taskSettings, context),
            chunkingSettings,
            AzureOpenAiSecretSettings.fromMap(secrets),
            threadPool
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
        ThreadPool threadPool
    ) {
        this(
            new ModelConfigurations(inferenceEntityId, taskType, service, serviceSettings, taskSettings, chunkingSettings),
            new ModelSecrets(secrets),
            threadPool
        );
    }

    public AzureOpenAiEmbeddingsModel(ModelConfigurations modelConfigurations, ModelSecrets modelSecrets, ThreadPool threadPool) {
        super(modelConfigurations, modelSecrets, (AzureOpenAiServiceSettings) modelConfigurations.getServiceSettings(), threadPool);
        try {
            this.uri = buildUriString();
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
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
    public ExecutableAction accept(AzureOpenAiActionVisitor creator, Map<String, Object> taskSettings) {
        return creator.create(this, taskSettings);
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
