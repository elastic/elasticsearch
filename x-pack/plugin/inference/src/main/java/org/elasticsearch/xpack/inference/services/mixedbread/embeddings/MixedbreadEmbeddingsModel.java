/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.mixedbread.embeddings;

import org.apache.http.client.utils.URIBuilder;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.ChunkingSettings;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.ModelSecrets;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.xpack.inference.external.action.ExecutableAction;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.ServiceUtils;
import org.elasticsearch.xpack.inference.services.mixedbread.MixedbreadModel;
import org.elasticsearch.xpack.inference.services.mixedbread.MixedbreadService;
import org.elasticsearch.xpack.inference.services.mixedbread.MixedbreadUtils;
import org.elasticsearch.xpack.inference.services.mixedbread.action.MixedbreadActionVisitor;
import org.elasticsearch.xpack.inference.services.settings.DefaultSecretSettings;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;

import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.xpack.inference.external.request.RequestUtils.buildUri;

/**
 * Represents a Mixedbread embeddings model for inference.
 * This class extends the {@link MixedbreadModel} and provides specific configurations and settings for embeddings tasks.
 */
public class MixedbreadEmbeddingsModel extends MixedbreadModel {
    private static final URIBuilder DEFAULT_URI_BUILDER = new URIBuilder().setScheme("https")
        .setHost(MixedbreadUtils.HOST)
        .setPathSegments(MixedbreadUtils.VERSION_1, MixedbreadUtils.EMBEDDINGS_PATH);

    /**
     * Creates a new {@link MixedbreadEmbeddingsModel} with updated task settings if they differ from the existing ones.
     *
     * @param model the existing Mixedbread embeddings model
     * @param taskSettings the task-specific settings to be applied
     * @return a new {@link MixedbreadEmbeddingsModel} with updated task settings, or the original model if settings are unchanged
     */
    public static MixedbreadEmbeddingsModel of(MixedbreadEmbeddingsModel model, Map<String, Object> taskSettings) {
        var requestTaskSettings = MixedbreadEmbeddingsTaskSettings.fromMap(taskSettings);
        if (requestTaskSettings.isEmpty() || requestTaskSettings.equals(model.getTaskSettings())) {
            return model;
        }
        return new MixedbreadEmbeddingsModel(model, MixedbreadEmbeddingsTaskSettings.of(model.getTaskSettings(), requestTaskSettings));
    }

    /**
     * Constructor for creating an {@link MixedbreadEmbeddingsModel} with specified parameters.
     *
     * @param inferenceId the unique identifier for the inference entity
     * @param serviceSettings the settings for the inference service, specific to embeddings
     * @param taskSettings the task-specific settings to be applied
     * @param chunkingSettings the chunking settings for processing input data
     * @param secrets the secret settings for the model, such as API keys or tokens
     * @param context the context for parsing configuration settings
     */
    public MixedbreadEmbeddingsModel(
        String inferenceId,
        Map<String, Object> serviceSettings,
        Map<String, Object> taskSettings,
        ChunkingSettings chunkingSettings,
        @Nullable Map<String, Object> secrets,
        ConfigurationParseContext context
    ) {
        this(
            inferenceId,
            MixedbreadEmbeddingsServiceSettings.fromMap(serviceSettings, context),
            MixedbreadEmbeddingsTaskSettings.fromMap(taskSettings),
            chunkingSettings,
            DefaultSecretSettings.fromMap(secrets),
            null
        );
    }

    /**
     * Constructor for creating an {@link MixedbreadEmbeddingsModel} with specified parameters.
     * Should only be used for testing
     *
     * @param inferenceId the unique identifier for the inference entity
     * @param serviceSettings the settings for the inference service, specific to embeddings
     * @param taskSettings the task-specific settings to be applied
     * @param chunkingSettings the chunking settings for processing input data
     * @param secretSettings the secret settings for the model, such as API keys or tokens
     */
    public MixedbreadEmbeddingsModel(
        String inferenceId,
        MixedbreadEmbeddingsServiceSettings serviceSettings,
        MixedbreadEmbeddingsTaskSettings taskSettings,
        ChunkingSettings chunkingSettings,
        @Nullable DefaultSecretSettings secretSettings,
        @Nullable String uri
    ) {
        super(
            new ModelConfigurations(
                inferenceId,
                TaskType.TEXT_EMBEDDING,
                MixedbreadService.NAME,
                serviceSettings,
                taskSettings,
                chunkingSettings
            ),
            new ModelSecrets(secretSettings),
            Objects.requireNonNullElse(ServiceUtils.createOptionalUri(uri), buildUri("Mixedbread", DEFAULT_URI_BUILDER::build))
        );
    }

    /**
     * Constructor for creating a {@link MixedbreadEmbeddingsModel} with specified parameters.
     *
     * @param modelConfigurations the model configurations
     * @param modelSecrets the secret settings for the model
     */
    public MixedbreadEmbeddingsModel(ModelConfigurations modelConfigurations, ModelSecrets modelSecrets) {
        super(modelConfigurations, modelSecrets, buildUri("Mixedbread", DEFAULT_URI_BUILDER::build));
    }

    @Override
    public RateLimitSettings rateLimitSettings() {
        return getServiceSettings().rateLimitSettings();
    }

    /**
     * Constructor for creating a {@link MixedbreadEmbeddingsModel} with specified parameters.
     *
     * @param model the base {@link MixedbreadEmbeddingsModel} to copy properties from
     * @param taskSettings the task-specific settings to be applied
     */
    MixedbreadEmbeddingsModel(MixedbreadEmbeddingsModel model, MixedbreadEmbeddingsTaskSettings taskSettings) {
        super(model, taskSettings);
    }

    /**
     * Constructor for creating a {@link MixedbreadEmbeddingsModel} with specified parameters.
     *
     * @param model the base {@link MixedbreadEmbeddingsModel} to copy properties from
     * @param serviceSettings the settings for the inference service, specific to embeddings
     */
    public MixedbreadEmbeddingsModel(MixedbreadEmbeddingsModel model, MixedbreadEmbeddingsServiceSettings serviceSettings) {
        super(model, serviceSettings);
    }

    @Override
    public MixedbreadEmbeddingsServiceSettings getServiceSettings() {
        return (MixedbreadEmbeddingsServiceSettings) super.getServiceSettings();
    }

    @Override
    public MixedbreadEmbeddingsTaskSettings getTaskSettings() {
        return (MixedbreadEmbeddingsTaskSettings) super.getTaskSettings();
    }

    @Override
    public DefaultSecretSettings getSecretSettings() {
        return super.getSecretSettings();
    }

    /**
     * Accepts a visitor to create an executable action for this Mixedbread embeddings model.
     *
     * @param visitor the visitor that creates the executable action
     * @param taskSettings the task-specific settings to be applied
     * @return an {@link ExecutableAction} representing the Mixedbread embeddings model
     */
    @Override
    public ExecutableAction accept(MixedbreadActionVisitor visitor, Map<String, Object> taskSettings) {
        return visitor.create(this, taskSettings);
    }
}
