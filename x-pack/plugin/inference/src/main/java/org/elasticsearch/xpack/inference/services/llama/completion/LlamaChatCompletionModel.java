/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.llama.completion;

import org.elasticsearch.inference.EmptyTaskSettings;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.ModelSecrets;
import org.elasticsearch.inference.SecretSettings;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.inference.UnifiedCompletionRequest;
import org.elasticsearch.xpack.inference.external.action.ExecutableAction;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.llama.LlamaModel;
import org.elasticsearch.xpack.inference.services.llama.action.LlamaActionVisitor;

import java.util.Map;

public class LlamaChatCompletionModel extends LlamaModel {

    public LlamaChatCompletionModel(
        String inferenceEntityId,
        TaskType taskType,
        String service,
        Map<String, Object> serviceSettings,
        Map<String, Object> secrets,
        ConfigurationParseContext context
    ) {
        this(
            inferenceEntityId,
            taskType,
            service,
            LlamaChatCompletionServiceSettings.fromMap(serviceSettings, context),
            retrieveSecretSettings(secrets)
        );
    }

    public LlamaChatCompletionModel(
        String inferenceEntityId,
        TaskType taskType,
        String service,
        LlamaChatCompletionServiceSettings serviceSettings,
        SecretSettings secrets
    ) {
        super(
            new ModelConfigurations(inferenceEntityId, taskType, service, serviceSettings, new EmptyTaskSettings()),
            new ModelSecrets(secrets)
        );
        setPropertiesFromServiceSettings(serviceSettings);
    }

    public static LlamaChatCompletionModel of(LlamaChatCompletionModel model, UnifiedCompletionRequest request) {
        if (request.model() == null) {
            // If no model id is specified in the request, return the original model
            return model;
        }

        var originalModelServiceSettings = model.getServiceSettings();
        var overriddenServiceSettings = new LlamaChatCompletionServiceSettings(
            request.model(),
            originalModelServiceSettings.uri(),
            originalModelServiceSettings.rateLimitSettings()
        );

        return new LlamaChatCompletionModel(
            model.getInferenceEntityId(),
            model.getTaskType(),
            model.getConfigurations().getService(),
            overriddenServiceSettings,
            model.getSecretSettings()
        );
    }

    private void setPropertiesFromServiceSettings(LlamaChatCompletionServiceSettings serviceSettings) {
        this.modelId = serviceSettings.modelId();
        this.uri = serviceSettings.uri();
        this.rateLimitSettings = serviceSettings.rateLimitSettings();
    }

    @Override
    public LlamaChatCompletionServiceSettings getServiceSettings() {
        return (LlamaChatCompletionServiceSettings) super.getServiceSettings();
    }

    public ExecutableAction accept(LlamaActionVisitor creator) {
        return creator.create(this);
    }
}
