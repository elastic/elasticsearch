/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.elastic.documentextraction;

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

import static org.elasticsearch.xpack.inference.services.elastic.ElasticInferenceServiceSettingsUtils.ensureEmptyTaskSettingsInRequestContext;

/**
 * Creates {@link ElasticInferenceServiceDocumentExtractionModel} instances from config maps
 * or {@link ModelConfigurations} and {@link ModelSecrets} objects.
 */
public class ElasticInferenceServiceDocumentExtractionModelCreator extends ElasticInferenceServiceModelCreator<
    ElasticInferenceServiceDocumentExtractionModel> {
    public ElasticInferenceServiceDocumentExtractionModelCreator(ElasticInferenceServiceComponents elasticInferenceServiceComponents) {
        super(elasticInferenceServiceComponents);
    }

    @Override
    public ElasticInferenceServiceDocumentExtractionModel createFromMaps(
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
        // The extracted content format is hardcoded to markdown for now, so no task settings are exposed
        ensureEmptyTaskSettingsInRequestContext(taskSettings, context);

        return new ElasticInferenceServiceDocumentExtractionModel(
            inferenceId,
            taskType,
            serviceSettings,
            elasticInferenceServiceComponents,
            context,
            endpointMetadata
        );
    }

    @Override
    public ElasticInferenceServiceDocumentExtractionModel createFromModelConfigurationsAndSecrets(
        ModelConfigurations config,
        ModelSecrets secrets
    ) {
        return new ElasticInferenceServiceDocumentExtractionModel(config, secrets, elasticInferenceServiceComponents);
    }
}
