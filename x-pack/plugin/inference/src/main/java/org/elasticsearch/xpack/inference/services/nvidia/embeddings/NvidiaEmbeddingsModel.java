/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.nvidia.embeddings;

import org.elasticsearch.inference.ChunkingSettings;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.ModelSecrets;
import org.elasticsearch.inference.SecretSettings;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.xpack.inference.external.action.ExecutableAction;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.nvidia.NvidiaModel;
import org.elasticsearch.xpack.inference.services.nvidia.action.NvidiaActionVisitor;
import org.elasticsearch.xpack.inference.services.settings.DefaultSecretSettings;

import java.util.Map;

/**
 * Represents an Nvidia embeddings model for inference.
 * This class extends the {@link NvidiaModel} and provides specific configurations and settings for embeddings tasks.
 */
public class NvidiaEmbeddingsModel extends NvidiaModel {

    /**
     * Creates a new {@link NvidiaEmbeddingsModel} with updated task settings if they differ from the existing ones.
     *
     * @param model the existing Nvidia embeddings model
     * @param taskSettings the new task settings to apply
     * @return a new {@link NvidiaEmbeddingsModel} with updated task settings, or the original model if settings are unchanged
     */
    public static NvidiaEmbeddingsModel of(NvidiaEmbeddingsModel model, Map<String, Object> taskSettings) {
        var requestTaskSettings = NvidiaEmbeddingsTaskSettings.fromMap(taskSettings);
        if (requestTaskSettings.isEmpty() || requestTaskSettings.equals(model.getTaskSettings())) {
            return model;
        }
        return new NvidiaEmbeddingsModel(model, NvidiaEmbeddingsTaskSettings.of(model.getTaskSettings(), requestTaskSettings));
    }

    /**
     * Constructor for creating an {@link NvidiaEmbeddingsModel} with specified parameters.
     *
     * @param inferenceEntityId the unique identifier for the inference entity
     * @param taskType the type of task this model is designed for
     * @param service the name of the inference service
     * @param serviceSettings the settings for the inference service, specific to embeddings
     * @param taskSettings the task-specific settings to be applied
     * @param chunkingSettings the chunking settings for processing input data
     * @param secrets the secret settings for the model, such as API keys or tokens
     * @param context the context for parsing configuration settings
     */
    public NvidiaEmbeddingsModel(
        String inferenceEntityId,
        TaskType taskType,
        String service,
        Map<String, Object> serviceSettings,
        Map<String, Object> taskSettings,
        ChunkingSettings chunkingSettings,
        Map<String, Object> secrets,
        ConfigurationParseContext context
    ) {
        this(
            inferenceEntityId,
            taskType,
            service,
            NvidiaEmbeddingsServiceSettings.fromMap(serviceSettings, context),
            NvidiaEmbeddingsTaskSettings.fromMap(taskSettings),
            chunkingSettings,
            DefaultSecretSettings.fromMap(secrets)
        );
    }

    /**
     * Constructor for creating an {@link NvidiaEmbeddingsModel} with specified parameters.
     *
     * @param model the base NvidiaEmbeddingsModel to copy properties from
     * @param serviceSettings the settings for the inference service, specific to embeddings
     */
    public NvidiaEmbeddingsModel(NvidiaEmbeddingsModel model, NvidiaEmbeddingsServiceSettings serviceSettings) {
        super(model, serviceSettings);
    }

    /**
     * Constructor for creating an {@link NvidiaEmbeddingsModel} with specified parameters.
     *
     * @param model the base {@link NvidiaEmbeddingsModel} to copy properties from
     * @param taskSettings the task-specific settings to be applied
     */
    public NvidiaEmbeddingsModel(NvidiaEmbeddingsModel model, NvidiaEmbeddingsTaskSettings taskSettings) {
        super(model, taskSettings);
    }

    /**
     * Constructor for creating an {@link NvidiaEmbeddingsModel} with specified parameters.
     *
     * @param inferenceEntityId the unique identifier for the inference entity
     * @param taskType the type of task this model is designed for
     * @param service the name of the inference service
     * @param serviceSettings the settings for the inference service, specific to embeddings
     * @param taskSettings the task-specific settings to be applied
     * @param chunkingSettings the chunking settings for processing input data
     * @param secrets the secret settings for the model, such as API keys or tokens
     */
    public NvidiaEmbeddingsModel(
        String inferenceEntityId,
        TaskType taskType,
        String service,
        NvidiaEmbeddingsServiceSettings serviceSettings,
        NvidiaEmbeddingsTaskSettings taskSettings,
        ChunkingSettings chunkingSettings,
        SecretSettings secrets
    ) {
        super(
            new ModelConfigurations(inferenceEntityId, taskType, service, serviceSettings, taskSettings, chunkingSettings),
            new ModelSecrets(secrets)
        );
    }

    @Override
    public NvidiaEmbeddingsServiceSettings getServiceSettings() {
        return (NvidiaEmbeddingsServiceSettings) super.getServiceSettings();
    }

    @Override
    public NvidiaEmbeddingsTaskSettings getTaskSettings() {
        return (NvidiaEmbeddingsTaskSettings) super.getTaskSettings();
    }

    /**
     * Accepts a visitor to create an executable action for this Nvidia embeddings model.
     *
     * @param creator the visitor that creates the executable action
     * @param taskSettings the task-specific settings to be applied
     * @return an {@link ExecutableAction} representing the Nvidia embeddings model
     */
    @Override
    public ExecutableAction accept(NvidiaActionVisitor creator, Map<String, Object> taskSettings) {
        return creator.create(this, taskSettings);
    }
}
