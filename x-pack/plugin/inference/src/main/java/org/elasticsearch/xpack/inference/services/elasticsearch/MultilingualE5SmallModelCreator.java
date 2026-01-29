/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.elasticsearch;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.ChunkingSettings;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.ModelSecrets;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;

import java.util.Map;

/**
 * Creates {@link MultilingualE5SmallModel} instances from config maps
 * or {@link ModelConfigurations} and {@link ModelSecrets} objects.
 */
public class MultilingualE5SmallModelCreator extends ElasticsearchInternalModelCreator<MultilingualE5SmallModel> {
    @Override
    public MultilingualE5SmallModel createFromMaps(
        String inferenceId,
        TaskType taskType,
        String service,
        Map<String, Object> serviceSettings,
        @Nullable Map<String, Object> taskSettings,
        @Nullable ChunkingSettings chunkingSettings,
        @Nullable Map<String, Object> secretSettings,
        ConfigurationParseContext context
    ) {
        return new MultilingualE5SmallModel(
            inferenceId,
            taskType,
            service,
            new MultilingualE5SmallInternalServiceSettings(ElasticsearchInternalServiceSettings.fromPersistedMap(serviceSettings)),
            chunkingSettings
        );
    }

    @Override
    public MultilingualE5SmallModel createFromModelConfigurationsAndSecrets(ModelConfigurations config, ModelSecrets secrets) {
        return new MultilingualE5SmallModel(config);
    }

    @Override
    public boolean matches(TaskType taskType, String modelId) {
        return ElasticsearchInternalService.MULTILINGUAL_E5_SMALL_VALID_IDS.contains(modelId);
    }
}
