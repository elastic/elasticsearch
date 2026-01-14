/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.elastic;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.ChunkingSettings;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.ModelSecrets;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.ModelCreator;
import org.elasticsearch.xpack.inference.services.ModelFactory;
import org.elasticsearch.xpack.inference.services.elastic.completion.ElasticInferenceServiceCompletionModelCreator;
import org.elasticsearch.xpack.inference.services.elastic.densetextembeddings.ElasticInferenceServiceDenseTextEmbeddingsModelCreator;
import org.elasticsearch.xpack.inference.services.elastic.rerank.ElasticInferenceServiceRerankModelCreator;
import org.elasticsearch.xpack.inference.services.elastic.sparseembeddings.ElasticInferenceServiceSparseEmbeddingsModelCreator;

import java.util.Map;

/**
 * Factory class for creating {@link ElasticInferenceServiceModel} instances based on task type.
 */
public class ElasticInferenceServiceModelFactory implements ModelFactory<ElasticInferenceServiceModel> {

    private final Map<TaskType, ModelCreator<? extends ElasticInferenceServiceModel>> modelCreators;

    /**
     * Constructs an {@link ElasticInferenceServiceModelFactory} with the provided components.
     *
     * @param elasticInferenceServiceComponents The components required for model creation.
     */
    public ElasticInferenceServiceModelFactory(ElasticInferenceServiceComponents elasticInferenceServiceComponents) {
        var completionModelCreator = new ElasticInferenceServiceCompletionModelCreator(elasticInferenceServiceComponents);
        this.modelCreators = Map.of(
            TaskType.TEXT_EMBEDDING,
            new ElasticInferenceServiceDenseTextEmbeddingsModelCreator(elasticInferenceServiceComponents),
            TaskType.SPARSE_EMBEDDING,
            new ElasticInferenceServiceSparseEmbeddingsModelCreator(elasticInferenceServiceComponents),
            TaskType.COMPLETION,
            completionModelCreator,
            TaskType.CHAT_COMPLETION,
            completionModelCreator,
            TaskType.RERANK,
            new ElasticInferenceServiceRerankModelCreator(elasticInferenceServiceComponents)
        );
    }

    @Override
    public ElasticInferenceServiceModel createFromMaps(
        String inferenceId,
        TaskType taskType,
        String service,
        Map<String, Object> serviceSettings,
        @Nullable Map<String, Object> taskSettings,
        @Nullable ChunkingSettings chunkingSettings,
        @Nullable Map<String, Object> secretSettings,
        ConfigurationParseContext context
    ) {
        return ModelFactory.retrieveModelCreatorFromMapOrThrow(modelCreators, inferenceId, taskType, service, context)
            .createFromMaps(inferenceId, taskType, service, serviceSettings, taskSettings, chunkingSettings, secretSettings, context);
    }

    @Override
    public ElasticInferenceServiceModel createFromModelConfigurationsAndSecrets(ModelConfigurations config, ModelSecrets secrets) {
        return ModelFactory.retrieveModelCreatorFromMapOrThrow(
            modelCreators,
            config.getInferenceEntityId(),
            config.getTaskType(),
            config.getService(),
            ConfigurationParseContext.PERSISTENT
        ).createFromModelConfigurationsAndSecrets(config, secrets);
    }
}
