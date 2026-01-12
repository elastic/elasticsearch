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

import java.util.Map;

/**
 * Interface for creating Model instances from configuration maps or configuration objects.
 */
public interface ModelCreator {
    /**
     * Creates a Model instance from configuration maps.
     * @param inferenceId the inference ID
     * @param taskType the task type
     * @param service the service name
     * @param serviceSettings the service settings map
     * @param taskSettings the task settings map
     * @param chunkingSettings the chunking settings
     * @param secretSettings the secret settings map
     * @param context the configuration parse context
     * @return the created Model instance
     */
    Model createFromMaps(
        String inferenceId,
        TaskType taskType,
        String service,
        Map<String, Object> serviceSettings,
        Map<String, Object> taskSettings,
        ChunkingSettings chunkingSettings,
        @Nullable Map<String, Object> secretSettings,
        ConfigurationParseContext context
    );

    /**
     * Creates a Model instance from configuration objects.
     * @param modelConfigurations model configurations to create the model from
     * @param modelSecrets model secrets to create the model from
     * @return the created Model instance
     */
    Model createFromModelConfigurationsAndSecrets(ModelConfigurations modelConfigurations, ModelSecrets modelSecrets);

}
