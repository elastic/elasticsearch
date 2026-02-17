/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.elastic.completion;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.ChunkingSettings;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.ModelSecrets;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.inference.metadata.EndpointMetadata;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.elastic.ElasticInferenceServiceComponents;
import org.elasticsearch.xpack.inference.services.elastic.ElasticInferenceServiceModelCreator;

import java.util.Map;

/**
 * Creates {@link ElasticInferenceServiceCompletionModel} instances from config maps
 * or {@link ModelConfigurations} and {@link ModelSecrets} objects.
 */
public class ElasticInferenceServiceCompletionModelCreator extends ElasticInferenceServiceModelCreator<
    ElasticInferenceServiceCompletionModel> {
    public ElasticInferenceServiceCompletionModelCreator(ElasticInferenceServiceComponents elasticInferenceServiceComponents) {
        super(elasticInferenceServiceComponents);
    }

    @Override
    public ElasticInferenceServiceCompletionModel createFromMaps(
        String inferenceId,
        TaskType taskType,
        Map<String, Object> serviceSettings,
        @Nullable ChunkingSettings chunkingSettings,
        ConfigurationParseContext context,
        @Nullable EndpointMetadata endpointMetadata
    ) {
        return new ElasticInferenceServiceCompletionModel(
            inferenceId,
            taskType,
            serviceSettings,
            elasticInferenceServiceComponents,
            context,
            endpointMetadata
        );
    }

    @Override
    public ElasticInferenceServiceCompletionModel createFromModelConfigurationsAndSecrets(
        ModelConfigurations config,
        ModelSecrets secrets
    ) {
        return new ElasticInferenceServiceCompletionModel(config, secrets, elasticInferenceServiceComponents);
    }
}
