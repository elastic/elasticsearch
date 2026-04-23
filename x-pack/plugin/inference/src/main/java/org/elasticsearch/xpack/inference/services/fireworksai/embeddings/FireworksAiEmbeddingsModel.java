/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.fireworksai.embeddings;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.ChunkingSettings;
import org.elasticsearch.inference.EmptyTaskSettings;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.ModelSecrets;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.xpack.inference.external.action.ExecutableAction;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.fireworksai.FireworksAiModel;
import org.elasticsearch.xpack.inference.services.fireworksai.FireworksAiRateLimitServiceSettings;
import org.elasticsearch.xpack.inference.services.fireworksai.action.FireworksAiActionVisitor;
import org.elasticsearch.xpack.inference.services.settings.DefaultSecretSettings;

import java.net.URI;
import java.util.Map;

/**
 * Model class for FireworksAI embeddings inference.
 */
public class FireworksAiEmbeddingsModel extends FireworksAiModel {

    public static FireworksAiEmbeddingsModel of(FireworksAiEmbeddingsModel model, Map<String, Object> taskSettings) {
        return model;
    }

    public FireworksAiEmbeddingsModel(
        String inferenceEntityId,
        String service,
        Map<String, Object> serviceSettings,
        Map<String, Object> taskSettings,
        ChunkingSettings chunkingSettings,
        @Nullable Map<String, Object> secrets,
        ConfigurationParseContext context
    ) {
        this(
            inferenceEntityId,
            service,
            FireworksAiEmbeddingsServiceSettings.fromMap(serviceSettings, context),
            chunkingSettings,
            DefaultSecretSettings.fromMap(secrets)
        );
    }

    // Should only be used directly for testing
    public FireworksAiEmbeddingsModel(
        String inferenceEntityId,
        String service,
        FireworksAiEmbeddingsServiceSettings serviceSettings,
        ChunkingSettings chunkingSettings,
        @Nullable DefaultSecretSettings secrets
    ) {
        super(
            new ModelConfigurations(
                inferenceEntityId,
                TaskType.TEXT_EMBEDDING,
                service,
                serviceSettings,
                EmptyTaskSettings.INSTANCE,
                chunkingSettings
            ),
            new ModelSecrets(secrets),
            secrets,
            serviceSettings
        );
    }

    public FireworksAiEmbeddingsModel(ModelConfigurations modelConfigurations, ModelSecrets modelSecrets) {
        super(
            modelConfigurations,
            modelSecrets,
            (DefaultSecretSettings) modelSecrets.getSecretSettings(),
            (FireworksAiRateLimitServiceSettings) modelConfigurations.getServiceSettings()
        );
    }

    public FireworksAiEmbeddingsModel(FireworksAiEmbeddingsModel originalModel, FireworksAiEmbeddingsServiceSettings serviceSettings) {
        super(originalModel, serviceSettings);
    }

    @Override
    public FireworksAiEmbeddingsServiceSettings getServiceSettings() {
        return (FireworksAiEmbeddingsServiceSettings) super.getServiceSettings();
    }

    @Override
    public DefaultSecretSettings getSecretSettings() {
        return (DefaultSecretSettings) super.getSecretSettings();
    }

    public URI uri() {
        return getServiceSettings().uri();
    }

    public String modelId() {
        return getServiceSettings().modelId();
    }

    @Override
    public ExecutableAction accept(FireworksAiActionVisitor creator, Map<String, Object> taskSettings) {
        return creator.create(this, taskSettings);
    }
}
