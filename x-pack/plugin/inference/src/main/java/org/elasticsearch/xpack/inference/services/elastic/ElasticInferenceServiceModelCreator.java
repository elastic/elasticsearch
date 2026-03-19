/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.elastic;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.ChunkingSettings;
import org.elasticsearch.inference.Model;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.ModelSecrets;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.inference.metadata.EndpointMetadata;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;

import java.util.Map;

/**
 * Abstract base class for {@link ElasticInferenceServiceModel} creator instances from config maps
 * or {@link ModelConfigurations} and {@link ModelSecrets} objects.
 */
public abstract class ElasticInferenceServiceModelCreator<M extends ElasticInferenceServiceModel> {

    /**
     * Creates a {@link Model} instance from configuration maps.
     * @param inferenceId the inference entity ID
     * @param taskType the task type
     * @param serviceSettings the service settings map
     * @param chunkingSettings the chunking settings
     * @param context the configuration parse context
     * @param endpointMetadata the endpoint metadata
     * @return the created {@link Model} instance
     */
    public abstract M createFromMaps(
        String inferenceId,
        TaskType taskType,
        Map<String, Object> serviceSettings,
        @Nullable ChunkingSettings chunkingSettings,
        ConfigurationParseContext context,
        @Nullable EndpointMetadata endpointMetadata
    );

    /**
     * Creates a {@link Model} instance from {@link ModelConfigurations} and {@link ModelSecrets}.
     * @param modelConfigurations the model configurations
     * @param modelSecrets the model secrets
     * @return the created {@link Model} instance
     */
    public abstract M createFromModelConfigurationsAndSecrets(ModelConfigurations modelConfigurations, ModelSecrets modelSecrets);

    protected final ElasticInferenceServiceComponents elasticInferenceServiceComponents;

    protected ElasticInferenceServiceModelCreator(ElasticInferenceServiceComponents elasticInferenceServiceComponents) {
        this.elasticInferenceServiceComponents = elasticInferenceServiceComponents;
    }
}
