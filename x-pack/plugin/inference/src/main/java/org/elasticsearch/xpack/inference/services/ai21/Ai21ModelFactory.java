/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.ai21;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.ChunkingSettings;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.ModelSecrets;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.ai21.completion.Ai21ChatCompletionModelCreator;

import java.util.Map;

import static org.elasticsearch.xpack.inference.services.ServiceUtils.createInvalidTaskTypeException;

/**
 * Factory class for creating Ai21Model instances based on task type.
 */
public class Ai21ModelFactory {
    private static final Ai21ChatCompletionModelCreator COMPLETION_MODEL_CREATOR = new Ai21ChatCompletionModelCreator();
    private static final Map<TaskType, Ai21ModelCreator> MODEL_CREATORS = Map.of(
        TaskType.CHAT_COMPLETION,
        COMPLETION_MODEL_CREATOR,
        TaskType.COMPLETION,
        COMPLETION_MODEL_CREATOR
    );

    /**
     * Creates an Ai21Model from the provided configuration maps.
     *
     * @param inferenceId the inference ID
     * @param taskType the task type
     * @param service the service name
     * @param serviceSettings the service settings map
     * @param taskSettings the task settings map
     * @param chunkingSettings the chunking settings
     * @param secretSettings the secret settings map
     * @param context the configuration parse context
     * @return an Ai21Model instance
     */
    public static Ai21Model createFromMaps(
        String inferenceId,
        TaskType taskType,
        String service,
        Map<String, Object> serviceSettings,
        @Nullable Map<String, Object> taskSettings,
        @Nullable ChunkingSettings chunkingSettings,
        @Nullable Map<String, Object> secretSettings,
        ConfigurationParseContext context
    ) {
        var modelCreator = MODEL_CREATORS.get(taskType);
        if (modelCreator == null) {
            throw createInvalidTaskTypeException(inferenceId, service, taskType, context);
        }
        return modelCreator.createFromMaps(
            inferenceId,
            taskType,
            service,
            serviceSettings,
            taskSettings,
            chunkingSettings,
            secretSettings,
            context
        );
    }

    /**
     * Creates an Ai21Model from the provided model configurations and secrets.
     *
     * @param config the model configurations
     * @param secrets the model secrets
     * @return an Ai21Model instance
     */
    public static Ai21Model createFromModelConfigurationsAndSecrets(ModelConfigurations config, ModelSecrets secrets) {
        var modelCreator = MODEL_CREATORS.get(config.getTaskType());
        if (modelCreator == null) {
            throw createInvalidTaskTypeException(
                config.getInferenceEntityId(),
                config.getService(),
                config.getTaskType(),
                ConfigurationParseContext.PERSISTENT
            );
        }
        return modelCreator.createFromModelConfigurationsAndSecrets(config, secrets);
    }

    private Ai21ModelFactory() {}
}
