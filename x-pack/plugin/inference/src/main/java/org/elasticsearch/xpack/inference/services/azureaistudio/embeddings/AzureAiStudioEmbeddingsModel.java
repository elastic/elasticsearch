/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.azureaistudio.embeddings;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.ChunkingSettings;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.ModelSecrets;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.xpack.inference.external.action.ExecutableAction;
import org.elasticsearch.xpack.inference.external.action.azureaistudio.AzureAiStudioActionVisitor;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.azureaistudio.AzureAiStudioEndpointType;
import org.elasticsearch.xpack.inference.services.azureaistudio.AzureAiStudioModel;
import org.elasticsearch.xpack.inference.services.azureaistudio.AzureAiStudioProvider;
import org.elasticsearch.xpack.inference.services.settings.DefaultSecretSettings;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;

import static org.elasticsearch.xpack.inference.services.azureaistudio.AzureAiStudioConstants.EMBEDDINGS_URI_PATH;

public class AzureAiStudioEmbeddingsModel extends AzureAiStudioModel {

    public static AzureAiStudioEmbeddingsModel of(AzureAiStudioEmbeddingsModel model, Map<String, Object> taskSettings) {
        if (taskSettings == null || taskSettings.isEmpty()) {
            return model;
        }

        var requestTaskSettings = AzureAiStudioEmbeddingsRequestTaskSettings.fromMap(taskSettings);
        var taskSettingToUse = AzureAiStudioEmbeddingsTaskSettings.of(model.getTaskSettings(), requestTaskSettings);

        return new AzureAiStudioEmbeddingsModel(model, taskSettingToUse);
    }

    public AzureAiStudioEmbeddingsModel(
        String inferenceEntityId,
        TaskType taskType,
        String service,
        AzureAiStudioEmbeddingsServiceSettings serviceSettings,
        AzureAiStudioEmbeddingsTaskSettings taskSettings,
        ChunkingSettings chunkingSettings,
        DefaultSecretSettings secrets
    ) {
        super(
            new ModelConfigurations(inferenceEntityId, taskType, service, serviceSettings, taskSettings, chunkingSettings),
            new ModelSecrets(secrets)
        );
    }

    public AzureAiStudioEmbeddingsModel(
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
            AzureAiStudioEmbeddingsServiceSettings.fromMap(serviceSettings, context),
            AzureAiStudioEmbeddingsTaskSettings.fromMap(taskSettings),
            chunkingSettings,
            DefaultSecretSettings.fromMap(secrets)
        );
    }

    private AzureAiStudioEmbeddingsModel(AzureAiStudioEmbeddingsModel model, AzureAiStudioEmbeddingsTaskSettings taskSettings) {
        super(model, taskSettings, model.getServiceSettings().rateLimitSettings());
    }

    public AzureAiStudioEmbeddingsModel(AzureAiStudioEmbeddingsModel model, AzureAiStudioEmbeddingsServiceSettings serviceSettings) {
        super(model, serviceSettings);
    }

    @Override
    public AzureAiStudioEmbeddingsServiceSettings getServiceSettings() {
        return (AzureAiStudioEmbeddingsServiceSettings) super.getServiceSettings();
    }

    @Override
    public AzureAiStudioEmbeddingsTaskSettings getTaskSettings() {
        return (AzureAiStudioEmbeddingsTaskSettings) super.getTaskSettings();
    }

    @Override
    protected URI getEndpointUri() throws URISyntaxException {
        if (this.provider == AzureAiStudioProvider.OPENAI || this.endpointType == AzureAiStudioEndpointType.REALTIME) {
            return new URI(this.target);
        }

        return new URI(this.target + EMBEDDINGS_URI_PATH);
    }

    @Override
    public ExecutableAction accept(AzureAiStudioActionVisitor creator, Map<String, Object> taskSettings) {
        return creator.create(this, taskSettings);
    }
}
