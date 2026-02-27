/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.azureaistudio.rerank;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.ModelSecrets;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.xpack.inference.external.action.ExecutableAction;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.azureaistudio.AzureAiStudioModel;
import org.elasticsearch.xpack.inference.services.azureaistudio.AzureAiStudioService;
import org.elasticsearch.xpack.inference.services.azureaistudio.action.AzureAiStudioActionVisitor;
import org.elasticsearch.xpack.inference.services.settings.DefaultSecretSettings;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;

import static org.elasticsearch.xpack.inference.services.azureaistudio.AzureAiStudioConstants.RERANK_URI_PATH;

public class AzureAiStudioRerankModel extends AzureAiStudioModel {

    public static AzureAiStudioRerankModel of(AzureAiStudioRerankModel model, Map<String, Object> taskSettings) {
        if (taskSettings == null || taskSettings.isEmpty()) {
            return model;
        }

        final var requestTaskSettings = AzureAiStudioRerankRequestTaskSettings.fromMap(taskSettings);
        final var taskSettingToUse = AzureAiStudioRerankTaskSettings.of(model.getTaskSettings(), requestTaskSettings);

        return new AzureAiStudioRerankModel(model, taskSettingToUse);
    }

    public AzureAiStudioRerankModel(
        String inferenceEntityId,
        AzureAiStudioRerankServiceSettings serviceSettings,
        AzureAiStudioRerankTaskSettings taskSettings,
        DefaultSecretSettings secrets
    ) {
        this(
            new ModelConfigurations(inferenceEntityId, TaskType.RERANK, AzureAiStudioService.NAME, serviceSettings, taskSettings),
            new ModelSecrets(secrets)
        );
    }

    public AzureAiStudioRerankModel(ModelConfigurations modelConfigurations, ModelSecrets modelSecrets) {
        super(modelConfigurations, modelSecrets);
    }

    public AzureAiStudioRerankModel(
        String inferenceEntityId,
        Map<String, Object> serviceSettings,
        Map<String, Object> taskSettings,
        @Nullable Map<String, Object> secrets,
        ConfigurationParseContext context
    ) {
        this(
            inferenceEntityId,
            AzureAiStudioRerankServiceSettings.fromMap(serviceSettings, context),
            AzureAiStudioRerankTaskSettings.fromMap(taskSettings),
            DefaultSecretSettings.fromMap(secrets)
        );
    }

    public AzureAiStudioRerankModel(AzureAiStudioRerankModel model, AzureAiStudioRerankTaskSettings taskSettings) {
        super(model, taskSettings, model.getServiceSettings().rateLimitSettings());
    }

    @Override
    public AzureAiStudioRerankServiceSettings getServiceSettings() {
        return (AzureAiStudioRerankServiceSettings) super.getServiceSettings();
    }

    @Override
    public AzureAiStudioRerankTaskSettings getTaskSettings() {
        return (AzureAiStudioRerankTaskSettings) super.getTaskSettings();
    }

    @Override
    public DefaultSecretSettings getSecretSettings() {
        return super.getSecretSettings();
    }

    @Override
    protected URI getEndpointUri() throws URISyntaxException {
        return new URI(stripTrailingSlash(this.target) + RERANK_URI_PATH);
    }

    @Override
    public ExecutableAction accept(AzureAiStudioActionVisitor creator, Map<String, Object> taskSettings) {
        return creator.create(this, taskSettings);
    }
}
