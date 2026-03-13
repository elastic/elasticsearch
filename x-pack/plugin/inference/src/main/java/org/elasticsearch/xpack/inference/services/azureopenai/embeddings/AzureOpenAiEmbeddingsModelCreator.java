/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.azureopenai.embeddings;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.ChunkingSettings;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.ModelSecrets;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.ModelCreator;

import java.util.Map;
import java.util.Objects;

/**
 * Creates {@link AzureOpenAiEmbeddingsModel} instances from config maps
 * or {@link ModelConfigurations} and {@link ModelSecrets} objects.
 */
public class AzureOpenAiEmbeddingsModelCreator implements ModelCreator<AzureOpenAiEmbeddingsModel> {

    public final ThreadPool threadPool;

    // TODO pull up into a common base class
    public AzureOpenAiEmbeddingsModelCreator(ThreadPool threadPool) {
        this.threadPool = Objects.requireNonNull(threadPool);
    }

    @Override
    public AzureOpenAiEmbeddingsModel createFromMaps(
        String inferenceId,
        TaskType taskType,
        String service,
        Map<String, Object> serviceSettings,
        @Nullable Map<String, Object> taskSettings,
        @Nullable ChunkingSettings chunkingSettings,
        @Nullable Map<String, Object> secretSettings,
        ConfigurationParseContext context
    ) {
        return new AzureOpenAiEmbeddingsModel(
            inferenceId,
            taskType,
            service,
            serviceSettings,
            taskSettings,
            chunkingSettings,
            secretSettings,
            context,
            threadPool
        );
    }

    @Override
    public AzureOpenAiEmbeddingsModel createFromModelConfigurationsAndSecrets(ModelConfigurations config, ModelSecrets secrets) {
        return new AzureOpenAiEmbeddingsModel(config, secrets, threadPool);
    }
}
