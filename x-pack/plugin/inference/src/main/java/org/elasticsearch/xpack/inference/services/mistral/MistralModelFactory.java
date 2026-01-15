/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.mistral;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.ChunkingSettings;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.ModelSecrets;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.ModelCreator;
import org.elasticsearch.xpack.inference.services.ModelFactory;
import org.elasticsearch.xpack.inference.services.mistral.completion.MistralChatCompletionModelCreator;
import org.elasticsearch.xpack.inference.services.mistral.embeddings.MistralEmbeddingsModelCreator;

import java.util.Map;

/**
 * Factory class for creating {@link MistralModel} instances based on task type.
 */
public class MistralModelFactory implements ModelFactory<MistralModel> {
    private static final MistralChatCompletionModelCreator COMPLETION_MODEL_CREATOR = new MistralChatCompletionModelCreator();
    private static final Map<TaskType, ModelCreator<? extends MistralModel>> MODEL_CREATORS = Map.of(
        TaskType.TEXT_EMBEDDING,
        new MistralEmbeddingsModelCreator(),
        TaskType.COMPLETION,
        COMPLETION_MODEL_CREATOR,
        TaskType.CHAT_COMPLETION,
        COMPLETION_MODEL_CREATOR
    );

    @Override
    public MistralModel createFromMaps(
        String inferenceId,
        TaskType taskType,
        String service,
        Map<String, Object> serviceSettings,
        @Nullable Map<String, Object> taskSettings,
        @Nullable ChunkingSettings chunkingSettings,
        @Nullable Map<String, Object> secretSettings,
        ConfigurationParseContext context
    ) {
        return ModelFactory.retrieveModelCreatorFromMapOrThrow(MODEL_CREATORS, inferenceId, taskType, service, context)
            .createFromMaps(inferenceId, taskType, service, serviceSettings, taskSettings, chunkingSettings, secretSettings, context);
    }

    @Override
    public MistralModel createFromModelConfigurationsAndSecrets(ModelConfigurations config, ModelSecrets secrets) {
        return ModelFactory.retrieveModelCreatorFromMapOrThrow(
            MODEL_CREATORS,
            config.getInferenceEntityId(),
            config.getTaskType(),
            config.getService(),
            ConfigurationParseContext.PERSISTENT
        ).createFromModelConfigurationsAndSecrets(config, secrets);
    }
}
