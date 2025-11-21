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

import java.net.URI;
import java.util.Map;

public class ContextualAiRerankModel extends ContextualAiModel {
    public static ContextualAiRerankModel of(ContextualAiRerankModel model, Map<String, Object> taskSettings) {
        var requestTaskSettings = ContextualAiRerankTaskSettings.fromMap(taskSettings);
        return new ContextualAiRerankModel(model, ContextualAiRerankTaskSettings.of(model.getTaskSettings(), requestTaskSettings));
    }

    public ContextualAiRerankModel(
        String modelId,
        Map<String, Object> serviceSettings,
        Map<String, Object> taskSettings,
        @Nullable Map<String, Object> secrets,
        ConfigurationParseContext context
    ) {
        this(
            modelId,
            ContextualAiRerankServiceSettings.fromMap(serviceSettings, context),
            ContextualAiRerankTaskSettings.fromMap(taskSettings),
            DefaultSecretSettings.fromMap(secrets)
        );
    }

    public ContextualAiRerankModel(
        String modelId,
        ContextualAiRerankServiceSettings serviceSettings,
        ContextualAiRerankTaskSettings taskSettings,
        @Nullable DefaultSecretSettings secretSettings
    ) {
        super(
            new ModelConfigurations(modelId, TaskType.RERANK, ContextualAiService.NAME, serviceSettings, taskSettings),
            new ModelSecrets(secretSettings),
            secretSettings,
            serviceSettings
        );
    }

    private ContextualAiRerankModel(ContextualAiRerankModel model, ContextualAiRerankTaskSettings taskSettings) {
        super(model, taskSettings);
    }

    public ContextualAiRerankModel(ContextualAiRerankModel model, ContextualAiRerankServiceSettings serviceSettings) {
        super(model, serviceSettings);
    }

    @Override
    public ContextualAiRerankServiceSettings getServiceSettings() {
        return (ContextualAiRerankServiceSettings) super.getServiceSettings();
    }

    @Override
    public ContextualAiRerankTaskSettings getTaskSettings() {
        return (ContextualAiRerankTaskSettings) super.getTaskSettings();
    }

    @Override
    public DefaultSecretSettings getSecretSettings() {
        return (DefaultSecretSettings) super.getSecretSettings();
    }

    public URI uri() {
        return getServiceSettings().uri();
    }

    public String modelId() {
        return getServiceSettings().modelId();
    }

    /**
     * Accepts a visitor to create an executable action. The returned action will not return documents in the response.
     * @param visitor          Interface for creating {@link ExecutableAction} instances for ContextualAI models.
     * @param taskSettings     Settings in the request to override the model's defaults
     * @return the rerank action
     */
    @Override
    public ExecutableAction accept(ContextualAiActionVisitor visitor, Map<String, Object> taskSettings) {
        return visitor.create(this, taskSettings);
    }
}
