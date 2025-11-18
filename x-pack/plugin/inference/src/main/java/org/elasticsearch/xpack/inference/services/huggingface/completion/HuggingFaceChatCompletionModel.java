/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.huggingface.completion;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.ModelSecrets;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.inference.UnifiedCompletionRequest;
import org.elasticsearch.xpack.inference.external.action.ExecutableAction;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.huggingface.HuggingFaceModel;
import org.elasticsearch.xpack.inference.services.huggingface.action.HuggingFaceActionVisitor;
import org.elasticsearch.xpack.inference.services.settings.DefaultSecretSettings;

import java.util.Map;

public class HuggingFaceChatCompletionModel extends HuggingFaceModel {

    /**
     * Creates a new {@link HuggingFaceChatCompletionModel} by copying properties from an existing model,
     * replacing the {@code modelId} in the service settings with the one from the given {@link UnifiedCompletionRequest},
     * if present. If the request does not specify a model ID, the original value is retained.
     *
     * @param model   the original model to copy from
     * @param request the request potentially containing an overridden model ID
     * @return a new {@link HuggingFaceChatCompletionModel} with updated service settings
     */
    public static HuggingFaceChatCompletionModel of(HuggingFaceChatCompletionModel model, UnifiedCompletionRequest request) {
        var originalModelServiceSettings = model.getServiceSettings();
        var overriddenServiceSettings = new HuggingFaceChatCompletionServiceSettings(
            request.model() != null ? request.model() : originalModelServiceSettings.modelId(),
            originalModelServiceSettings.uri(),
            originalModelServiceSettings.rateLimitSettings()
        );

        return new HuggingFaceChatCompletionModel(
            model.getInferenceEntityId(),
            model.getTaskType(),
            model.getConfigurations().getService(),
            overriddenServiceSettings,
            model.getSecretSettings()
        );
    }

    public HuggingFaceChatCompletionModel(
        String inferenceEntityId,
        TaskType taskType,
        String service,
        Map<String, Object> serviceSettings,
        @Nullable Map<String, Object> secrets,
        ConfigurationParseContext context
    ) {
        this(
            inferenceEntityId,
            taskType,
            service,
            HuggingFaceChatCompletionServiceSettings.fromMap(serviceSettings, context),
            DefaultSecretSettings.fromMap(secrets)
        );
    }

    HuggingFaceChatCompletionModel(
        String inferenceEntityId,
        TaskType taskType,
        String service,
        HuggingFaceChatCompletionServiceSettings serviceSettings,
        @Nullable DefaultSecretSettings secretSettings
    ) {
        super(
            new ModelConfigurations(inferenceEntityId, taskType, service, serviceSettings),
            new ModelSecrets(secretSettings),
            serviceSettings,
            secretSettings
        );
    }

    @Override
    public HuggingFaceChatCompletionServiceSettings getServiceSettings() {
        return (HuggingFaceChatCompletionServiceSettings) super.getServiceSettings();
    }

    @Override
    public DefaultSecretSettings getSecretSettings() {
        return (DefaultSecretSettings) super.getSecretSettings();
    }

    @Override
    public ExecutableAction accept(HuggingFaceActionVisitor creator) {
        return creator.create(this);
    }

    @Override
    public Integer getTokenLimit() {
        throw new UnsupportedOperationException("Token Limit for chat completion is sent in request and not retrieved from the model");
    }
}
