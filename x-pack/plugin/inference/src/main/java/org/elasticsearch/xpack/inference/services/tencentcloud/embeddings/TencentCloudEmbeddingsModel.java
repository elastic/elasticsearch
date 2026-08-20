/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.tencentcloud.embeddings;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.ChunkingSettings;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.ModelSecrets;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.xpack.inference.external.action.ExecutableAction;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.settings.DefaultSecretSettings;
import org.elasticsearch.xpack.inference.services.tencentcloud.TencentCloudModel;
import org.elasticsearch.xpack.inference.services.tencentcloud.TencentCloudService;
import org.elasticsearch.xpack.inference.services.tencentcloud.action.TencentCloudActionVisitor;
import org.elasticsearch.xpack.inference.services.tencentcloud.request.TencentCloudUtils;

import java.util.Map;

/**
 * Represents a TencentCloud embeddings model for inference.
 * This class extends the {@link TencentCloudModel} and provides specific configurations and settings for text
 * embedding tasks against the TencentCloud AI Gateway.
 */
public class TencentCloudEmbeddingsModel extends TencentCloudModel {

    /**
     * Constructor for creating a TencentCloudEmbeddingsModel from raw configuration maps.
     *
     * @param inferenceId the unique identifier for the inference entity
     * @param serviceSettings the raw service settings map, specific to embeddings
     * @param taskSettings the raw task settings map for the embeddings request
     * @param chunkingSettings the chunking settings for processing input data
     * @param secrets the raw secret settings map for the model, such as the API key (may be null)
     * @param context the context for parsing configuration settings
     */
    public TencentCloudEmbeddingsModel(
        String inferenceId,
        Map<String, Object> serviceSettings,
        Map<String, Object> taskSettings,
        ChunkingSettings chunkingSettings,
        @Nullable Map<String, Object> secrets,
        ConfigurationParseContext context
    ) {
        this(
            inferenceId,
            TencentCloudEmbeddingsServiceSettings.fromMap(serviceSettings, context),
            TencentCloudEmbeddingsTaskSettings.fromMap(taskSettings),
            chunkingSettings,
            DefaultSecretSettings.fromMap(secrets, context)
        );
    }

    /**
     * Constructor for creating a TencentCloudEmbeddingsModel with typed parameters.
     *
     * @param inferenceId the unique identifier for the inference entity
     * @param serviceSettings the service settings specific to embeddings
     * @param taskSettings the task settings for the embeddings request
     * @param chunkingSettings the chunking settings for processing input data
     * @param secretSettings the secret settings for the model, such as the API key (may be null)
     */
    public TencentCloudEmbeddingsModel(
        String inferenceId,
        TencentCloudEmbeddingsServiceSettings serviceSettings,
        TencentCloudEmbeddingsTaskSettings taskSettings,
        ChunkingSettings chunkingSettings,
        @Nullable DefaultSecretSettings secretSettings
    ) {
        super(
            new ModelConfigurations(
                inferenceId,
                TaskType.TEXT_EMBEDDING,
                TencentCloudService.NAME,
                serviceSettings,
                taskSettings,
                chunkingSettings
            ),
            new ModelSecrets(secretSettings),
            resolveUri(serviceSettings)
        );
    }

    /**
     * Constructor for creating a TencentCloudEmbeddingsModel from model configurations and secrets.
     *
     * @param config the configurations for the model
     * @param secrets the secret settings for the model
     */
    public TencentCloudEmbeddingsModel(ModelConfigurations config, ModelSecrets secrets) {
        super(config, secrets, resolveUri((TencentCloudEmbeddingsServiceSettings) config.getServiceSettings()));
    }

    /**
     * Constructor for creating a TencentCloudEmbeddingsModel by copying an existing model with new service settings.
     *
     * @param model the base TencentCloudEmbeddingsModel to copy properties from
     * @param serviceSettings the new service settings to apply
     */
    public TencentCloudEmbeddingsModel(TencentCloudEmbeddingsModel model, TencentCloudEmbeddingsServiceSettings serviceSettings) {
        super(model, serviceSettings);
    }

    /**
     * Returns the service settings specific to TencentCloud embeddings.
     *
     * @return the TencentCloudEmbeddingsServiceSettings associated with this model
     */
    @Override
    public TencentCloudEmbeddingsServiceSettings getServiceSettings() {
        return (TencentCloudEmbeddingsServiceSettings) super.getServiceSettings();
    }

    /**
     * Returns the task settings specific to TencentCloud embeddings.
     *
     * @return the TencentCloudEmbeddingsTaskSettings associated with this model
     */
    @Override
    public TencentCloudEmbeddingsTaskSettings getTaskSettings() {
        return (TencentCloudEmbeddingsTaskSettings) super.getTaskSettings();
    }

    /**
     * Returns the secret settings for this model.
     *
     * @return the DefaultSecretSettings associated with this model
     */
    @Override
    public DefaultSecretSettings getSecretSettings() {
        return super.getSecretSettings();
    }

    /**
     * Accepts a visitor that creates an executable action for this TencentCloud embeddings model.
     *
     * @param visitor the visitor that creates the executable action
     * @param taskSettings the task-level settings for this request
     * @return an ExecutableAction representing this model
     */
    @Override
    public ExecutableAction accept(TencentCloudActionVisitor visitor, Map<String, Object> taskSettings) {
        return visitor.create(this, taskSettings);
    }

    private static java.net.URI resolveUri(TencentCloudEmbeddingsServiceSettings serviceSettings) {
        return TencentCloudUtils.buildUri(serviceSettings.region(), TencentCloudUtils.VERSION_1, TencentCloudUtils.EMBEDDINGS_PATH);
    }
}
