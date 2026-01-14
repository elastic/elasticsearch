/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.azureaistudio;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.ChunkingSettings;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.ModelSecrets;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.ModelCreator;
import org.elasticsearch.xpack.inference.services.ModelFactory;
import org.elasticsearch.xpack.inference.services.azureaistudio.completion.AzureAiStudioChatCompletionModelCreator;
import org.elasticsearch.xpack.inference.services.azureaistudio.embeddings.AzureAiStudioEmbeddingsModelCreator;
import org.elasticsearch.xpack.inference.services.azureaistudio.rerank.AzureAiStudioRerankModelCreator;

import java.util.Map;

import static org.elasticsearch.xpack.inference.services.azureaistudio.AzureAiStudioProviderCapabilities.providerAllowsEndpointTypeForTask;
import static org.elasticsearch.xpack.inference.services.azureaistudio.AzureAiStudioProviderCapabilities.providerAllowsTaskType;

/**
 * Factory class for creating {@link AzureAiStudioModel} instances based on task type.
 */
public class AzureAiStudioModelFactory implements ModelFactory<AzureAiStudioModel> {
    private static final Map<TaskType, ModelCreator<? extends AzureAiStudioModel>> MODEL_CREATORS = Map.of(
        TaskType.TEXT_EMBEDDING,
        new AzureAiStudioEmbeddingsModelCreator(),
        TaskType.COMPLETION,
        new AzureAiStudioChatCompletionModelCreator(),
        TaskType.RERANK,
        new AzureAiStudioRerankModelCreator()
    );

    @Override
    public AzureAiStudioModel createFromMaps(
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
        checkProviderAndEndpointTypeForTask(taskType, model.provider(), model.endpointType());
        return model;
    }

    @Override
    public AzureAiStudioModel createFromModelConfigurationsAndSecrets(ModelConfigurations config, ModelSecrets secrets) {
        var model = ModelFactory.retrieveModelCreatorFromMapOrThrow(
            MODEL_CREATORS,
            config.getInferenceEntityId(),
            config.getTaskType(),
            config.getService(),
            ConfigurationParseContext.PERSISTENT
        ).createFromModelConfigurationsAndSecrets(config, secrets);
        checkProviderAndEndpointTypeForTask(config.getTaskType(), model.provider(), model.endpointType());
        return model;
    }

    private static void checkProviderAndEndpointTypeForTask(
        TaskType taskType,
        AzureAiStudioProvider provider,
        AzureAiStudioEndpointType endpointType
    ) {
        if (providerAllowsTaskType(provider, taskType) == false) {
            throw new ElasticsearchStatusException(
                Strings.format("The [%s] task type for provider [%s] is not available", taskType, provider),
                RestStatus.BAD_REQUEST
            );
        }

        if (providerAllowsEndpointTypeForTask(provider, taskType, endpointType) == false) {
            throw new ElasticsearchStatusException(
                Strings.format(
                    "The [%s] endpoint type with [%s] task type for provider [%s] is not available",
                    endpointType,
                    taskType,
                    provider
                ),
                RestStatus.BAD_REQUEST
            );
        }
    }
}
