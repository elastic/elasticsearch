/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.llama.embeddings;

import org.elasticsearch.inference.ChunkingSettings;
import org.elasticsearch.inference.EmptyTaskSettings;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.ModelSecrets;
import org.elasticsearch.inference.SecretSettings;
import org.elasticsearch.inference.TaskSettings;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.xpack.inference.external.action.ExecutableAction;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.llama.LlamaModel;
import org.elasticsearch.xpack.inference.services.llama.action.LlamaActionVisitor;

import java.util.Map;

/**
 * Represents a Llama embeddings model for inference.
 * This class extends the LlamaModel and provides specific configurations and settings for embeddings tasks.
 */
public class LlamaEmbeddingsModel extends LlamaModel {

    /**
     * Constructor for creating a LlamaEmbeddingsModel with specified parameters.
     *
     * @param inferenceEntityId the unique identifier for the inference entity
     * @param taskType the type of task this model is designed for
     * @param service the name of the inference service
     * @param serviceSettings the settings for the inference service, specific to embeddings
     * @param secrets the secret settings for the model, such as API keys or tokens
     * @param context the context for parsing configuration settings
     */
    public LlamaEmbeddingsModel(
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
            LlamaEmbeddingsServiceSettings.fromMap(serviceSettings, context),
            EmptyTaskSettings.INSTANCE,    // no task settings for Llama embeddings
            chunkingSettings,
            retrieveSecretSettings(secrets)
        );
    }

    /**
     * Constructor for creating a LlamaEmbeddingsModel with specified parameters.
     *
     * @param model the base LlamaEmbeddingsModel to copy properties from
     * @param serviceSettings the settings for the inference service, specific to embeddings
     */
    public LlamaEmbeddingsModel(LlamaEmbeddingsModel model, LlamaEmbeddingsServiceSettings serviceSettings) {
        super(model, serviceSettings);
        setPropertiesFromServiceSettings(serviceSettings);
    }

    /**
     * Sets properties from the provided LlamaEmbeddingsServiceSettings.
     *
     * @param serviceSettings the service settings to extract properties from
     */
    private void setPropertiesFromServiceSettings(LlamaEmbeddingsServiceSettings serviceSettings) {
        this.modelId = serviceSettings.modelId();
        this.uri = serviceSettings.uri();
        this.rateLimitSettings = serviceSettings.rateLimitSettings();
    }

    /**
     * Constructor for creating a LlamaEmbeddingsModel with specified parameters.
     *
     * @param inferenceEntityId the unique identifier for the inference entity
     * @param taskType the type of task this model is designed for
     * @param service the name of the inference service
     * @param serviceSettings the settings for the inference service, specific to embeddings
     * @param taskSettings the task settings for the model
     * @param chunkingSettings the chunking settings for processing input data
     * @param secrets the secret settings for the model, such as API keys or tokens
     */
    public LlamaEmbeddingsModel(
        String inferenceEntityId,
        TaskType taskType,
        String service,
        LlamaEmbeddingsServiceSettings serviceSettings,
        TaskSettings taskSettings,
        ChunkingSettings chunkingSettings,
        SecretSettings secrets
    ) {
        super(
            new ModelConfigurations(inferenceEntityId, taskType, service, serviceSettings, EmptyTaskSettings.INSTANCE, chunkingSettings),
            new ModelSecrets(secrets)
        );
        setPropertiesFromServiceSettings(serviceSettings);
    }

    @Override
    public LlamaEmbeddingsServiceSettings getServiceSettings() {
        return (LlamaEmbeddingsServiceSettings) super.getServiceSettings();
    }

    /**
     * Accepts a visitor to create an executable action for this Llama embeddings model.
     *
     * @param creator the visitor that creates the executable action
     * @return an ExecutableAction representing the Llama embeddings model
     */
    public ExecutableAction accept(LlamaActionVisitor creator) {
        return creator.create(this);
    }
}
