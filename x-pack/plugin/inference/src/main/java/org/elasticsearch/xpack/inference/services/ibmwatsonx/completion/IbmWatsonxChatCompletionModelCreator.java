/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.ibmwatsonx.completion;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.ChunkingSettings;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.ModelSecrets;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.ModelCreator;

import java.util.Map;

/**
 * Creates {@link IbmWatsonxChatCompletionModel} instances from config maps
 * or {@link ModelConfigurations} and {@link ModelSecrets} objects.
 */
public class IbmWatsonxChatCompletionModelCreator implements ModelCreator<IbmWatsonxChatCompletionModel> {
    @Override
    public IbmWatsonxChatCompletionModel createFromMaps(
        String inferenceId,
        TaskType taskType,
        String service,
        Map<String, Object> serviceSettings,
        @Nullable Map<String, Object> taskSettings,
        @Nullable ChunkingSettings chunkingSettings,
        @Nullable Map<String, Object> secretSettings,
        ConfigurationParseContext context
    ) {
        return new IbmWatsonxChatCompletionModel(inferenceId, taskType, service, serviceSettings, secretSettings, context);
    }

    @Override
    public IbmWatsonxChatCompletionModel createFromModelConfigurationsAndSecrets(ModelConfigurations config, ModelSecrets secrets) {
        return new IbmWatsonxChatCompletionModel(config, secrets);
    }
}
