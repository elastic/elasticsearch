/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.contextualai.rerank;

import java.net.URI;
import java.util.Map;
import java.util.Objects;

import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.ModelSecrets;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.RateLimitGroupingModel;
import org.elasticsearch.xpack.inference.services.settings.DefaultSecretSettings;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;

public class ContextualAiRerankModel extends RateLimitGroupingModel {

    private final ContextualAiRerankServiceSettings serviceSettings;
    private final DefaultSecretSettings secretSettings;

    public static ContextualAiRerankModel of(ContextualAiRerankModel model, ContextualAiRerankTaskSettings taskSettings) {
        return new ContextualAiRerankModel(model, taskSettings);
    }

    public static ContextualAiRerankModel of(ContextualAiRerankModel model, Map<String, Object> taskSettings) {
        var requestTaskSettings = ContextualAiRerankTaskSettings.fromMap(taskSettings);
        return new ContextualAiRerankModel(model, ContextualAiRerankTaskSettings.of(model.getTaskSettings(), requestTaskSettings));
    }

    public ContextualAiRerankModel(
        String inferenceEntityId,
        TaskType taskType,
        String service,
        Map<String, Object> serviceSettings,
        Map<String, Object> taskSettings,
        @Nullable Map<String, Object> secrets,
        ConfigurationParseContext context
    ) {
        this(
            inferenceEntityId,
            taskType,
            service,
            ContextualAiRerankServiceSettings.fromMap(serviceSettings, context),
            ContextualAiRerankTaskSettings.fromMap(taskSettings),
            DefaultSecretSettings.fromMap(secrets)
        );
    }

    public ContextualAiRerankModel(
        String inferenceEntityId,
        TaskType taskType,
        String service,
        ContextualAiRerankServiceSettings serviceSettings,
        ContextualAiRerankTaskSettings taskSettings,
        @Nullable DefaultSecretSettings secretSettings
    ) {
        super(
            new ModelConfigurations(inferenceEntityId, taskType, service, serviceSettings, taskSettings),
            new ModelSecrets(secretSettings)
        );
        this.serviceSettings = Objects.requireNonNull(serviceSettings);
        this.secretSettings = secretSettings;
    }

    private ContextualAiRerankModel(ContextualAiRerankModel model, ContextualAiRerankTaskSettings taskSettings) {
        super(model, taskSettings);
        this.serviceSettings = model.serviceSettings;
        this.secretSettings = model.secretSettings;
    }

    @Override
    public ContextualAiRerankServiceSettings getServiceSettings() {
        return serviceSettings;
    }

    public ContextualAiRerankTaskSettings getTaskSettings() {
        return (ContextualAiRerankTaskSettings) getConfigurations().getTaskSettings();
    }

    public DefaultSecretSettings getSecretSettings() {
        return secretSettings;
    }

    @Override
    public int rateLimitGroupingHash() {
        return Objects.hash(serviceSettings.uri(), serviceSettings.modelId());
    }

    @Override
    public RateLimitSettings rateLimitSettings() {
        return serviceSettings.rateLimitSettings();
    }

    public URI uri() {
        return serviceSettings.uri();
    }

    public SecureString apiKey() {
        return secretSettings != null ? secretSettings.apiKey() : null;
    }

    public String modelId() {
        return serviceSettings.modelId();
    }
}
