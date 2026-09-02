/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.ChunkingSettings;
import org.elasticsearch.inference.Model;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.ModelSecrets;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.inference.metadata.EndpointMetadata;
import org.elasticsearch.xpack.inference.services.elastic.ElasticInferenceServiceModelCreator;

import java.util.Map;

/**
 * Creates {@link Model} instances from configuration maps or {@link ModelConfigurations} and {@link ModelSecrets} objects.
 */
public interface ModelCreator<M extends Model> {
    /**
     * Creates a {@link Model} instance from configuration maps.
     * @param inferenceId the inference entity ID
     * @param taskType the task type
     * @param service the service name
     * @param serviceSettings the service settings map
     * @param taskSettings the task settings map
     * @param chunkingSettings the chunking settings
     * @param secretSettings the secret settings map
     * @param context the configuration parse context
     * @return the created {@link Model} instance
     */
    M createFromMaps(
        String inferenceId,
        TaskType taskType,
        String service,
        Map<String, Object> serviceSettings,
        @Nullable Map<String, Object> taskSettings,
        @Nullable ChunkingSettings chunkingSettings,
        @Nullable Map<String, Object> secretSettings,
        ConfigurationParseContext context
    );

    /**
     * Creates a {@link Model} instance from configuration maps and endpoint metadata. The default implementation ignores the endpoint
     * metadata. Only overridden by {@link ElasticInferenceServiceModelCreator}.
     * @param inferenceId the inference entity ID
     * @param taskType the task type
     * @param service the service name
     * @param serviceSettings the service settings map
     * @param taskSettings the task settings map
     * @param chunkingSettings the chunking settings
     * @param secretSettings the secret settings map
     * @param context the configuration parse context
     * @param endpointMetadata the endpoint metadata
     * @return the created {@link Model} instance
     */
    default M createFromMaps(
        String inferenceId,
        TaskType taskType,
        String service,
        Map<String, Object> serviceSettings,
        @Nullable Map<String, Object> taskSettings,
        @Nullable ChunkingSettings chunkingSettings,
        @Nullable Map<String, Object> secretSettings,
        ConfigurationParseContext context,
        @Nullable EndpointMetadata endpointMetadata
    ) {
        return createFromMaps(inferenceId, taskType, service, serviceSettings, taskSettings, chunkingSettings, secretSettings, context);
    }

    /**
     * Creates a {@link Model} instance from {@link ModelConfigurations} and {@link ModelSecrets}.
     * @param modelConfigurations the model configurations
     * @param modelSecrets the model secrets
     * @return the created {@link Model} instance
     */
    M createFromModelConfigurationsAndSecrets(ModelConfigurations modelConfigurations, ModelSecrets modelSecrets);
}
