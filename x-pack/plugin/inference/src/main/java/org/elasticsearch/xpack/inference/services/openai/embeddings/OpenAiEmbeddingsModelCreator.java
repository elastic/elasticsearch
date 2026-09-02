/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.openai.embeddings;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.ChunkingSettings;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.ModelSecrets;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.inference.common.oauth2.OAuth2ClusterSettings;
import org.elasticsearch.xpack.inference.common.oauth2.TokenCache;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.openai.OpenAiModelCreator;

import java.util.Map;

/**
 * Creates {@link OpenAiEmbeddingsModel} instances from config maps
 * or {@link ModelConfigurations} and {@link ModelSecrets} objects.
 */
public class OpenAiEmbeddingsModelCreator extends OpenAiModelCreator<OpenAiEmbeddingsModel> {

    public OpenAiEmbeddingsModelCreator(ThreadPool threadPool, TokenCache tokenCache, OAuth2ClusterSettings oauth2ClusterSettings) {
        super(threadPool, tokenCache, oauth2ClusterSettings);
    }

    @Override
    public OpenAiEmbeddingsModel createFromMaps(
        String inferenceId,
        TaskType taskType,
        String service,
        Map<String, Object> serviceSettings,
        @Nullable Map<String, Object> taskSettings,
        @Nullable ChunkingSettings chunkingSettings,
        @Nullable Map<String, Object> secretSettings,
        ConfigurationParseContext context
    ) {
        return new OpenAiEmbeddingsModel(
            inferenceId,
            taskType,
            service,
            serviceSettings,
            taskSettings,
            chunkingSettings,
            secretSettings,
            threadPool,
            tokenCache,
            oauth2ClusterSettings,
            context
        );
    }

    @Override
    public OpenAiEmbeddingsModel createFromModelConfigurationsAndSecrets(ModelConfigurations config, ModelSecrets secrets) {
        return new OpenAiEmbeddingsModel(config, secrets, threadPool, tokenCache, oauth2ClusterSettings);
    }
}
