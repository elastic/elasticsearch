/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.elasticsearch;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Strings;
import org.elasticsearch.inference.ChunkingSettings;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.ModelSecrets;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.ModelFactory;

import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.inference.services.elasticsearch.ElasticsearchInternalServiceSettings.MODEL_ID;

/**
 * Factory class for creating {@link ElasticsearchInternalModel} instances based on task type.
 */
public class ElasticsearchInternalModelFactory implements ModelFactory<ElasticsearchInternalModel> {
    private static final List<ElasticsearchInternalModelCreator<? extends ElasticsearchInternalModel>> MODEL_CREATORS = List.of(
        new MultilingualE5SmallModelCreator(),
        new ElserInternalModelCreator(),
        new ElasticRerankerModelCreator(),
        new CustomElandEmbeddingModelCreator(),
        new CustomElandModelCreator(),
        new CustomElandRerankModelCreator()
    );

    private static ElasticsearchInternalModelCreator<? extends ElasticsearchInternalModel> retrieveModelCreatorFromListOrThrow(
        String inferenceId,
        TaskType taskType,
        String modelId,
        String service
    ) {
        validateModelId(inferenceId, modelId);
        return MODEL_CREATORS.stream()
            .filter(creator -> creator.matches(taskType, modelId))
            .findFirst()
            .orElseThrow(
                () -> new ElasticsearchStatusException(TaskType.unsupportedTaskTypeErrorMsg(taskType, service), RestStatus.BAD_REQUEST)
            );
    }

    @Override
    public ElasticsearchInternalModel createFromMaps(
        String inferenceId,
        TaskType taskType,
        String service,
        Map<String, Object> serviceSettings,
        @Nullable Map<String, Object> taskSettings,
        @Nullable ChunkingSettings chunkingSettings,
        @Nullable Map<String, Object> secretSettings,
        ConfigurationParseContext context
    ) {
        String modelId = (String) serviceSettings.get(MODEL_ID);
        return retrieveModelCreatorFromListOrThrow(inferenceId, taskType, modelId, service).createFromMaps(
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

    @Override
    public ElasticsearchInternalModel createFromModelConfigurationsAndSecrets(ModelConfigurations config, ModelSecrets secrets) {
        String modelId = config.getServiceSettings().modelId();
        return retrieveModelCreatorFromListOrThrow(config.getInferenceEntityId(), config.getTaskType(), modelId, config.getService())
            .createFromModelConfigurationsAndSecrets(config, secrets);
    }

    private static void validateModelId(String inferenceEntityId, String modelId) {
        if (modelId == null) {
            throw new IllegalArgumentException(
                Strings.format("Error parsing request config, model id is missing for inference id: %s", inferenceEntityId)
            );
        }
    }
}
