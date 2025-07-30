/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.llama.embeddings;

import org.elasticsearch.inference.ChunkingSettings;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.ModelSecrets;
import org.elasticsearch.inference.SecretSettings;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.xpack.inference.external.action.ExecutableAction;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.llama.LlamaModel;
import org.elasticsearch.xpack.inference.services.llama.action.LlamaActionVisitor;
import org.elasticsearch.xpack.inference.services.openai.embeddings.OpenAiEmbeddingsRequestTaskSettings;
import org.elasticsearch.xpack.inference.services.openai.embeddings.OpenAiEmbeddingsTaskSettings;

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
     * @param taskSettings the settings for the task
     * @param chunkingSettings the chunking settings for processing input data
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
            OpenAiEmbeddingsTaskSettings.fromMap(taskSettings, context),
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
     * @param taskSettings the settings for the task
     * @param chunkingSettings the chunking settings for processing input data
     * @param secrets the secret settings for the model, such as API keys or tokens
     */
    public LlamaEmbeddingsModel(
        String inferenceEntityId,
        TaskType taskType,
        String service,
        LlamaEmbeddingsServiceSettings serviceSettings,
        OpenAiEmbeddingsTaskSettings taskSettings,
        ChunkingSettings chunkingSettings,
        SecretSettings secrets
    ) {
        super(
            new ModelConfigurations(inferenceEntityId, taskType, service, serviceSettings, taskSettings, chunkingSettings),
            new ModelSecrets(secrets)
        );
        setPropertiesFromServiceSettings(serviceSettings);
    }

    /**
     * Factory method to create a LlamaEmbeddingsModel with overridden task settings based on the request.
     * If the request does not specify task settings, the original model is returned.
     *
     * @param model the original LlamaEmbeddingsModel
     * @param taskSettings the task settings to override
     * @return a new LlamaEmbeddingsModel with overridden task settings or the original model if no overrides are specified
     */
    public static LlamaEmbeddingsModel of(LlamaEmbeddingsModel model, Map<String, Object> taskSettings) {
        if (taskSettings == null || taskSettings.isEmpty()) {
            return model;
        }

        var requestTaskSettings = OpenAiEmbeddingsRequestTaskSettings.fromMap(taskSettings);
        return new LlamaEmbeddingsModel(model, OpenAiEmbeddingsTaskSettings.of(model.getTaskSettings(), requestTaskSettings));
    }

    private LlamaEmbeddingsModel(LlamaEmbeddingsModel originalModel, OpenAiEmbeddingsTaskSettings taskSettings) {
        super(originalModel, taskSettings);
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
    @Override
    public ExecutableAction accept(LlamaActionVisitor creator, Map<String, Object> taskSettings) {
        return creator.create(this, taskSettings);
    }

    @Override
    public OpenAiEmbeddingsTaskSettings getTaskSettings() {
        return (OpenAiEmbeddingsTaskSettings) super.getTaskSettings();
    }
}
