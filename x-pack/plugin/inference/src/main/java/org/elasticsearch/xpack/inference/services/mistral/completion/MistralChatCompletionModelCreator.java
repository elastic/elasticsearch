/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.mistral.completion;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.ChunkingSettings;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.ModelSecrets;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.ModelCreator;
import org.elasticsearch.xpack.inference.services.mistral.compatibility.CompatibilityService;

import java.util.Map;
import java.util.Objects;

/**
 * Creates {@link MistralChatCompletionModel} instances from config maps
 * or {@link ModelConfigurations} and {@link ModelSecrets} objects.
 */
public class MistralChatCompletionModelCreator implements ModelCreator<MistralChatCompletionModel> {

    private final CompatibilityService compatibilityService;

    public MistralChatCompletionModelCreator(CompatibilityService compatibilityService) {
        this.compatibilityService = Objects.requireNonNull(compatibilityService);
    }

    @Override
    public MistralChatCompletionModel createFromMaps(
        String inferenceId,
        TaskType taskType,
        String service,
        Map<String, Object> serviceSettings,
        @Nullable Map<String, Object> taskSettings,
        @Nullable ChunkingSettings chunkingSettings,
        @Nullable Map<String, Object> secretSettings,
        ConfigurationParseContext context
    ) {
        var model = new MistralChatCompletionModel(inferenceId, taskType, service, serviceSettings, secretSettings, context);
        compatibilityService.checkCompatibility(model.getServiceSettings().url(), context);
        return model;
    }

    @Override
    public MistralChatCompletionModel createFromModelConfigurationsAndSecrets(ModelConfigurations config, ModelSecrets secrets) {
        return new MistralChatCompletionModel(config, secrets);
    }
}
