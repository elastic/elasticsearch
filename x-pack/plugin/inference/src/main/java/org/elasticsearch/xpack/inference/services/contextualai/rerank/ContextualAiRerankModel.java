/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.contextualai.rerank;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.ModelSecrets;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.xpack.inference.external.action.ExecutableAction;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.ServiceUtils;
import org.elasticsearch.xpack.inference.services.contextualai.ContextualAiModel;
import org.elasticsearch.xpack.inference.services.contextualai.ContextualAiService;
import org.elasticsearch.xpack.inference.services.contextualai.action.ContextualAiActionVisitor;
import org.elasticsearch.xpack.inference.services.settings.DefaultSecretSettings;

import java.net.URI;
import java.util.Map;

public class ContextualAiRerankModel extends ContextualAiModel {
    public static final URI DEFAULT_RERANK_URI = ServiceUtils.createUri("https://api.contextual.ai/v1/rerank");

    public static ContextualAiRerankModel of(ContextualAiRerankModel model, Map<String, Object> taskSettingsMap) {
        var originalSettings = model.getTaskSettings();
        var requestTaskSettings = ContextualAiRerankTaskSettings.fromMap(taskSettingsMap);

        if (requestTaskSettings.isEmpty() || originalSettings.equals(requestTaskSettings)) {
            return model;
        }

        var mergedTaskSettings = ContextualAiRerankTaskSettings.of(originalSettings, requestTaskSettings);

        if (originalSettings.equals(mergedTaskSettings)) {
            return model;
        }

        return new ContextualAiRerankModel(model, mergedTaskSettings);
    }

    public ContextualAiRerankModel(
        String inferenceEntityId,
        Map<String, Object> serviceSettings,
        Map<String, Object> taskSettings,
        @Nullable Map<String, Object> secretSettings,
        ConfigurationParseContext context
    ) {
        this(
            inferenceEntityId,
            ContextualAiRerankServiceSettings.fromMap(serviceSettings, context),
            ContextualAiRerankTaskSettings.fromMap(taskSettings),
            DefaultSecretSettings.fromMap(secretSettings)
        );
    }

    public ContextualAiRerankModel(
        String inferenceEntityId,
        ContextualAiRerankServiceSettings serviceSettings,
        ContextualAiRerankTaskSettings taskSettings,
        @Nullable DefaultSecretSettings secretSettings
    ) {
        this(
            new ModelConfigurations(inferenceEntityId, TaskType.RERANK, ContextualAiService.NAME, serviceSettings, taskSettings),
            new ModelSecrets(secretSettings)
        );
    }

    /**
     * This constructor must be used only for testing.
     * @param inferenceEntityId the inference entity id for this model
     * @param serviceSettings the service settings for this model
     * @param taskSettings the task settings for this model
     * @param secretSettings the secret settings for this model, can be null
     * @param uri the URI to use for this model's requests, must not be null
     */
    ContextualAiRerankModel(
        String inferenceEntityId,
        ContextualAiRerankServiceSettings serviceSettings,
        ContextualAiRerankTaskSettings taskSettings,
        @Nullable DefaultSecretSettings secretSettings,
        URI uri
    ) {
        this(
            new ModelConfigurations(inferenceEntityId, TaskType.RERANK, ContextualAiService.NAME, serviceSettings, taskSettings),
            new ModelSecrets(secretSettings),
            uri
        );
    }

    public ContextualAiRerankModel(ModelConfigurations modelConfigurations, ModelSecrets modelSecrets) {
        this(modelConfigurations, modelSecrets, DEFAULT_RERANK_URI);
    }

    public ContextualAiRerankModel(ModelConfigurations modelConfigurations, ModelSecrets modelSecrets, URI uri) {
        super(modelConfigurations, modelSecrets, uri);
    }

    private ContextualAiRerankModel(ContextualAiRerankModel model, ContextualAiRerankTaskSettings taskSettings) {
        super(model, taskSettings);
    }

    @Override
    public ContextualAiRerankServiceSettings getServiceSettings() {
        return (ContextualAiRerankServiceSettings) super.getServiceSettings();
    }

    @Override
    public ContextualAiRerankTaskSettings getTaskSettings() {
        return (ContextualAiRerankTaskSettings) super.getTaskSettings();
    }

    /**
     * Accepts a visitor to create an executable action for reranking.
     * @param visitor      interface for creating {@link ExecutableAction} instances for ContextualAI models
     * @param taskSettings settings in the request to override the model's defaults
     * @return the rerank action
     */
    @Override
    public ExecutableAction accept(ContextualAiActionVisitor visitor, Map<String, Object> taskSettings) {
        return visitor.create(this, taskSettings);
    }
}
