/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.contextualai.rerank;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.ChunkingSettings;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.ModelSecrets;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.ModelCreator;

import java.util.Map;

/**
 * Creates {@link ContextualAiRerankModel} instances from config maps
 * or {@link ModelConfigurations} and {@link ModelSecrets} objects.
 */
public class ContextualAiRerankModelCreator implements ModelCreator<ContextualAiRerankModel> {
    @Override
    public ContextualAiRerankModel createFromMaps(
        String inferenceId,
        TaskType taskType,
        String service,
        Map<String, Object> serviceSettings,
        @Nullable Map<String, Object> taskSettings,
        @Nullable ChunkingSettings chunkingSettings,
        @Nullable Map<String, Object> secretSettings,
        ConfigurationParseContext context
    ) {
        return new ContextualAiRerankModel(inferenceId, serviceSettings, taskSettings, secretSettings, context);
    }

    @Override
    public ContextualAiRerankModel createFromModelConfigurationsAndSecrets(ModelConfigurations config, ModelSecrets secrets) {
        return new ContextualAiRerankModel(config, secrets);
    }
}
