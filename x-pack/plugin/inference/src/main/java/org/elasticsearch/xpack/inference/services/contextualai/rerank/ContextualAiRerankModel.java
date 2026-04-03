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
import org.elasticsearch.xpack.inference.services.contextualai.ContextualAiModel;
import org.elasticsearch.xpack.inference.services.contextualai.ContextualAiService;
import org.elasticsearch.xpack.inference.services.contextualai.action.ContextualAiActionVisitor;
import org.elasticsearch.xpack.inference.services.settings.DefaultSecretSettings;

import java.util.Map;

public class ContextualAiRerankModel extends ContextualAiModel {
    public static ContextualAiRerankModel of(ContextualAiRerankModel model, Map<String, Object> taskSettingsMap) {
        var requestTaskSettings = ContextualAiRerankTaskSettings.fromMap(taskSettingsMap);
        return new ContextualAiRerankModel(model, ContextualAiRerankTaskSettings.of(model.getTaskSettings(), requestTaskSettings));
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

    public ContextualAiRerankModel(ModelConfigurations modelConfigurations, ModelSecrets modelSecrets) {
        super(modelConfigurations, modelSecrets);
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
     * When {@link ContextualAiRerankTaskSettings#getReturnDocuments()} is true, the provider may echo document text in the response.
     *
     * @param visitor      interface for creating {@link ExecutableAction} instances for ContextualAI models
     * @param taskSettings settings in the request to override the model's defaults
     * @return the rerank action
     */
    @Override
    public ExecutableAction accept(ContextualAiActionVisitor visitor, Map<String, Object> taskSettings) {
        return visitor.create(this, taskSettings);
    }
}
