/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.ChunkingSettings;
import org.elasticsearch.inference.Model;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.ModelSecrets;
import org.elasticsearch.inference.TaskType;

import java.util.Map;

import static org.elasticsearch.xpack.inference.services.ServiceUtils.createInvalidTaskTypeException;

/**
 * Creates {@link Model} instances from configuration maps or {@link ModelConfigurations} and {@link ModelSecrets} objects.
 */
public interface ModelFactory<M extends Model> {

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
     * Creates a {@link Model} instance from {@link ModelConfigurations} and {@link ModelSecrets}.
     * @param modelConfigurations the model configurations
     * @param modelSecrets the model secrets
     * @return the created {@link Model} instance
     */
    M createFromModelConfigurationsAndSecrets(ModelConfigurations modelConfigurations, ModelSecrets modelSecrets);

    /**
     * Retrieves a {@link ModelCreator} from the provided map based on the task type, or throws an exception if not found.
     * @param modelCreators the map of task types to model creators
     * @param inferenceId the inference entity ID
     * @param taskType the task type
     * @param service the service name
     * @param context the configuration parse context
     * @param <C> the type of {@link ModelCreator}
     * @return the retrieved {@link ModelCreator}
     * @throws ElasticsearchStatusException if no {@link ModelCreator} is found for the given task type
     */
    static <C extends ModelCreator<? extends Model>> C retrieveModelCreatorFromMapOrThrow(
        Map<TaskType, C> modelCreators,
        String inferenceId,
        TaskType taskType,
        String service,
        ConfigurationParseContext context
    ) {
        C modelCreator = modelCreators.get(taskType);
        if (modelCreator == null) {
            throw createInvalidTaskTypeException(inferenceId, service, taskType, context);
        }
        return modelCreator;
    }
}
