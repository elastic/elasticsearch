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
import org.elasticsearch.inference.TaskSettings;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.xpack.inference.external.action.ExecutableAction;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.settings.DefaultSecretSettings;
import org.elasticsearch.xpack.inference.services.tencentcloud.TencentCloudModel;
import org.elasticsearch.xpack.inference.services.tencentcloud.TencentCloudService;
import org.elasticsearch.xpack.inference.services.tencentcloud.action.TencentCloudActionVisitor;
import org.elasticsearch.xpack.inference.services.tencentcloud.request.TencentCloudUtils;

import java.net.URI;
import java.util.HashMap;
import java.util.Map;

/**
 * Model for the TencentCloud {@code text_embedding} task, which calls {@code /v1/embeddings}, and the {@code embedding} task, which
 * calls {@code /v1/multimodal-embeddings}.
 */
public class TencentCloudEmbeddingsModel extends TencentCloudModel {

    /**
     * Constructor for creating a TencentCloudEmbeddingsModel from raw configuration maps.
     *
     * @param inferenceId the unique identifier for the inference entity
     * @param taskType the task type, either {@code text_embedding} or {@code embedding}
     * @param serviceSettings the raw service settings map
     * @param taskSettings the raw task settings map for the embeddings request
     * @param chunkingSettings the chunking settings for processing input data
     * @param secrets the raw secret settings map for the model, such as the API key (may be null)
     * @param context the context for parsing configuration settings
     */
    public TencentCloudEmbeddingsModel(
        String inferenceId,
        TaskType taskType,
        Map<String, Object> serviceSettings,
        Map<String, Object> taskSettings,
        ChunkingSettings chunkingSettings,
        @Nullable Map<String, Object> secrets,
        ConfigurationParseContext context
    ) {
        this(
            inferenceId,
            taskType,
            createServiceSettings(serviceSettings, taskType, context),
            createTaskSettings(taskSettings, taskType),
            chunkingSettings,
            DefaultSecretSettings.fromMap(secrets, context)
        );
    }

    /**
     * Constructor for creating a {@code text_embedding} TencentCloudEmbeddingsModel with typed parameters.
     */
    public TencentCloudEmbeddingsModel(
        String inferenceId,
        TencentCloudTextEmbeddingServiceSettings serviceSettings,
        TencentCloudEmbeddingsTaskSettings taskSettings,
        ChunkingSettings chunkingSettings,
        @Nullable DefaultSecretSettings secretSettings
    ) {
        this(inferenceId, TaskType.TEXT_EMBEDDING, serviceSettings, taskSettings, chunkingSettings, secretSettings);
    }

    /**
     * Constructor for creating a TencentCloudEmbeddingsModel with typed parameters.
     *
     * @param inferenceId the unique identifier for the inference entity
     * @param taskType the task type, either {@code text_embedding} or {@code embedding}
     * @param serviceSettings the service settings matching the task type
     * @param taskSettings the task settings for the embeddings request
     * @param chunkingSettings the chunking settings for processing input data
     * @param secretSettings the secret settings for the model, such as the API key (may be null)
     */
    public TencentCloudEmbeddingsModel(
        String inferenceId,
        TaskType taskType,
        BaseTencentCloudEmbeddingsServiceSettings serviceSettings,
        TaskSettings taskSettings,
        ChunkingSettings chunkingSettings,
        @Nullable DefaultSecretSettings secretSettings
    ) {
        this(
            inferenceId,
            taskType,
            serviceSettings,
            taskSettings,
            chunkingSettings,
            secretSettings,
            resolveUri(taskType, serviceSettings.region())
        );
    }

    // should only be used directly for testing
    public TencentCloudEmbeddingsModel(
        String inferenceId,
        TaskType taskType,
        BaseTencentCloudEmbeddingsServiceSettings serviceSettings,
        TaskSettings taskSettings,
        ChunkingSettings chunkingSettings,
        @Nullable DefaultSecretSettings secretSettings,
        URI uri
    ) {
        super(
            new ModelConfigurations(inferenceId, taskType, TencentCloudService.NAME, serviceSettings, taskSettings, chunkingSettings),
            new ModelSecrets(secretSettings),
            uri
        );
    }

    /**
     * Constructor for creating a TencentCloudEmbeddingsModel from model configurations and secrets.
     *
     * @param config the configurations for the model
     * @param secrets the secret settings for the model
     */
    public TencentCloudEmbeddingsModel(ModelConfigurations config, ModelSecrets secrets) {
        super(
            config,
            secrets,
            resolveUri(config.getTaskType(), ((BaseTencentCloudEmbeddingsServiceSettings) config.getServiceSettings()).region())
        );
    }

    /**
     * Constructor for creating a TencentCloudEmbeddingsModel by copying an existing model with new service settings.
     *
     * @param model the base TencentCloudEmbeddingsModel to copy properties from
     * @param serviceSettings the new service settings to apply
     */
    public TencentCloudEmbeddingsModel(TencentCloudEmbeddingsModel model, BaseTencentCloudEmbeddingsServiceSettings serviceSettings) {
        super(model, serviceSettings);
    }

    private TencentCloudEmbeddingsModel(TencentCloudEmbeddingsModel model, TencentCloudMultimodalEmbeddingsTaskSettings taskSettings) {
        super(model, taskSettings);
    }

    /**
     * Returns a copy of the model with the request task settings applied. Only the {@code embedding} task has task settings; the
     * request task settings of a {@code text_embedding} model are ignored.
     *
     * @param model the model to copy
     * @param taskSettings the request task settings
     * @return the model to run the request with
     */
    public static TencentCloudEmbeddingsModel of(TencentCloudEmbeddingsModel model, @Nullable Map<String, Object> taskSettings) {
        if (model.getTaskType() != TaskType.EMBEDDING || taskSettings == null || taskSettings.isEmpty()) {
            return model;
        }
        var requestTaskSettings = TencentCloudMultimodalEmbeddingsTaskSettings.fromMap(new HashMap<>(taskSettings));
        return new TencentCloudEmbeddingsModel(
            model,
            TencentCloudMultimodalEmbeddingsTaskSettings.of(model.getMultimodalTaskSettings(), requestTaskSettings)
        );
    }

    /**
     * Returns the service settings specific to TencentCloud embeddings.
     *
     * @return the embeddings service settings associated with this model
     */
    @Override
    public BaseTencentCloudEmbeddingsServiceSettings getServiceSettings() {
        return (BaseTencentCloudEmbeddingsServiceSettings) super.getServiceSettings();
    }

    /**
     * Returns the task settings of an {@code embedding} model.
     *
     * @return the TencentCloudMultimodalEmbeddingsTaskSettings associated with this model
     */
    public TencentCloudMultimodalEmbeddingsTaskSettings getMultimodalTaskSettings() {
        return (TencentCloudMultimodalEmbeddingsTaskSettings) super.getTaskSettings();
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

    private static BaseTencentCloudEmbeddingsServiceSettings createServiceSettings(
        Map<String, Object> serviceSettings,
        TaskType taskType,
        ConfigurationParseContext context
    ) {
        return switch (taskType) {
            case TEXT_EMBEDDING -> TencentCloudTextEmbeddingServiceSettings.fromMap(serviceSettings, context);
            case EMBEDDING -> TencentCloudEmbeddingServiceSettings.fromMap(serviceSettings, context);
            default -> throw new IllegalArgumentException("Unsupported task type for TencentCloud embeddings: " + taskType);
        };
    }

    private static TaskSettings createTaskSettings(Map<String, Object> taskSettings, TaskType taskType) {
        // text_embedding has no task settings and leaves the map untouched, so request parsing rejects any key
        return taskType == TaskType.EMBEDDING
            ? TencentCloudMultimodalEmbeddingsTaskSettings.fromMap(taskSettings)
            : TencentCloudEmbeddingsTaskSettings.fromMap(taskSettings);
    }

    private static URI resolveUri(TaskType taskType, String region) {
        var path = taskType == TaskType.EMBEDDING ? TencentCloudUtils.MULTIMODAL_EMBEDDINGS_PATH : TencentCloudUtils.EMBEDDINGS_PATH;
        return TencentCloudUtils.buildUri(region, TencentCloudUtils.VERSION_1, path);
    }
}
