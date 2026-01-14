/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.amazonbedrock;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.ChunkingSettings;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.ModelSecrets;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.ModelCreator;
import org.elasticsearch.xpack.inference.services.ModelFactory;
import org.elasticsearch.xpack.inference.services.amazonbedrock.completion.AmazonBedrockChatCompletionModelCreator;
import org.elasticsearch.xpack.inference.services.amazonbedrock.embeddings.AmazonBedrockEmbeddingsModelCreator;

import java.util.Map;

import static org.elasticsearch.xpack.inference.services.amazonbedrock.AmazonBedrockProviderCapabilities.checkProviderForTask;

/**
 * Factory class for creating {@link AmazonBedrockModel} instances based on task type.
 */
public class AmazonBedrockModelFactory implements ModelFactory<AmazonBedrockModel> {
    private static final Map<TaskType, ModelCreator<? extends AmazonBedrockModel>> MODEL_CREATORS = Map.of(
        TaskType.TEXT_EMBEDDING,
        new AmazonBedrockEmbeddingsModelCreator(),
        TaskType.COMPLETION,
        new AmazonBedrockChatCompletionModelCreator()
    );

    @Override
    public AmazonBedrockModel createFromMaps(
        String inferenceId,
        TaskType taskType,
        String service,
        Map<String, Object> serviceSettings,
        @Nullable Map<String, Object> taskSettings,
        @Nullable ChunkingSettings chunkingSettings,
        @Nullable Map<String, Object> secretSettings,
        ConfigurationParseContext context
    ) {
        var model = ModelFactory.retrieveModelCreatorFromMapOrThrow(MODEL_CREATORS, inferenceId, taskType, service, context)
            .createFromMaps(inferenceId, taskType, service, serviceSettings, taskSettings, chunkingSettings, secretSettings, context);
        checkProviderForTask(taskType, model.provider());
        return model;
    }

    @Override
    public AmazonBedrockModel createFromModelConfigurationsAndSecrets(ModelConfigurations config, ModelSecrets secrets) {
        var model = ModelFactory.retrieveModelCreatorFromMapOrThrow(
            MODEL_CREATORS,
            config.getInferenceEntityId(),
            config.getTaskType(),
            config.getService(),
            ConfigurationParseContext.PERSISTENT
        ).createFromModelConfigurationsAndSecrets(config, secrets);
        checkProviderForTask(config.getTaskType(), model.provider());
        return model;
    }
}
