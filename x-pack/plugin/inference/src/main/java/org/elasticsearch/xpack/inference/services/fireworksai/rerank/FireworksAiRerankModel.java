/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.fireworksai.rerank;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.ModelSecrets;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.xpack.inference.external.action.ExecutableAction;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.fireworksai.FireworksAiModel;
import org.elasticsearch.xpack.inference.services.fireworksai.FireworksAiService;
import org.elasticsearch.xpack.inference.services.fireworksai.action.FireworksAiActionVisitor;
import org.elasticsearch.xpack.inference.services.settings.DefaultSecretSettings;

import java.net.URI;
import java.util.Map;

/**
 * Model class for FireworksAI rerank inference.
 */
public class FireworksAiRerankModel extends FireworksAiModel {

    public static FireworksAiRerankModel of(FireworksAiRerankModel model, Map<String, Object> taskSettings) {
        var requestTaskSettings = FireworksAiRerankTaskSettings.fromMap(taskSettings);
        return new FireworksAiRerankModel(model, FireworksAiRerankTaskSettings.of(model.getTaskSettings(), requestTaskSettings));
    }

    public FireworksAiRerankModel(
        String modelId,
        Map<String, Object> serviceSettings,
        Map<String, Object> taskSettings,
        @Nullable Map<String, Object> secrets,
        ConfigurationParseContext context
    ) {
        this(
            modelId,
            FireworksAiRerankServiceSettings.fromMap(serviceSettings, context),
            FireworksAiRerankTaskSettings.fromMap(taskSettings),
            DefaultSecretSettings.fromMap(secrets)
        );
    }

    public FireworksAiRerankModel(
        String modelId,
        FireworksAiRerankServiceSettings serviceSettings,
        FireworksAiRerankTaskSettings taskSettings,
        @Nullable DefaultSecretSettings secretSettings
    ) {
        super(
            new ModelConfigurations(modelId, TaskType.RERANK, FireworksAiService.NAME, serviceSettings, taskSettings),
            new ModelSecrets(secretSettings),
            secretSettings,
            serviceSettings
        );
    }

    private FireworksAiRerankModel(FireworksAiRerankModel model, FireworksAiRerankTaskSettings taskSettings) {
        super(model, taskSettings);
    }

    public FireworksAiRerankModel(FireworksAiRerankModel model, FireworksAiRerankServiceSettings serviceSettings) {
        super(model, serviceSettings);
    }

    @Override
    public FireworksAiRerankServiceSettings getServiceSettings() {
        return (FireworksAiRerankServiceSettings) super.getServiceSettings();
    }

    @Override
    public FireworksAiRerankTaskSettings getTaskSettings() {
        return (FireworksAiRerankTaskSettings) super.getTaskSettings();
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
     * Accepts a visitor to create an executable action.
     * @param visitor          Interface for creating ExecutableAction instances for FireworksAI models
     * @param taskSettings     Settings in the request to override the model's defaults
     * @return the rerank action
     */
    @Override
    public ExecutableAction accept(FireworksAiActionVisitor visitor, Map<String, Object> taskSettings) {
        return visitor.create(this, taskSettings);
    }
}
