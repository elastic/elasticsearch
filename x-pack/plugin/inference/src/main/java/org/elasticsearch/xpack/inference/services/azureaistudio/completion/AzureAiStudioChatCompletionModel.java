/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.azureaistudio.completion;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.ModelSecrets;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.xpack.inference.external.action.ExecutableAction;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.azureaistudio.AzureAiStudioEndpointType;
import org.elasticsearch.xpack.inference.services.azureaistudio.AzureAiStudioModel;
import org.elasticsearch.xpack.inference.services.azureaistudio.AzureAiStudioProvider;
import org.elasticsearch.xpack.inference.services.azureaistudio.action.AzureAiStudioActionVisitor;
import org.elasticsearch.xpack.inference.services.settings.DefaultSecretSettings;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;

import static org.elasticsearch.xpack.inference.services.azureaistudio.AzureAiStudioConstants.COMPLETIONS_URI_PATH;

public class AzureAiStudioChatCompletionModel extends AzureAiStudioModel {

    public static AzureAiStudioChatCompletionModel of(AzureAiStudioModel model, Map<String, Object> taskSettings) {
        var modelAsCompletionModel = (AzureAiStudioChatCompletionModel) model;

        if (taskSettings == null || taskSettings.isEmpty()) {
            return modelAsCompletionModel;
        }

        var requestTaskSettings = AzureAiStudioChatCompletionRequestTaskSettings.fromMap(taskSettings);
        var taskSettingToUse = AzureAiStudioChatCompletionTaskSettings.of(modelAsCompletionModel.getTaskSettings(), requestTaskSettings);

        return new AzureAiStudioChatCompletionModel(modelAsCompletionModel, taskSettingToUse);
    }

    public AzureAiStudioChatCompletionModel(
        String inferenceEntityId,
        TaskType taskType,
        String service,
        AzureAiStudioChatCompletionServiceSettings serviceSettings,
        AzureAiStudioChatCompletionTaskSettings taskSettings,
        DefaultSecretSettings secrets
    ) {
        super(new ModelConfigurations(inferenceEntityId, taskType, service, serviceSettings, taskSettings), new ModelSecrets(secrets));
    }

    public AzureAiStudioChatCompletionModel(
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
            AzureAiStudioChatCompletionServiceSettings.fromMap(serviceSettings, context),
            AzureAiStudioChatCompletionTaskSettings.fromMap(taskSettings),
            DefaultSecretSettings.fromMap(secrets)
        );
    }

    public AzureAiStudioChatCompletionModel(AzureAiStudioChatCompletionModel model, AzureAiStudioChatCompletionTaskSettings taskSettings) {
        super(model, taskSettings, model.getServiceSettings().rateLimitSettings());
    }

    @Override
    public AzureAiStudioChatCompletionServiceSettings getServiceSettings() {
        return (AzureAiStudioChatCompletionServiceSettings) super.getServiceSettings();
    }

    @Override
    public AzureAiStudioChatCompletionTaskSettings getTaskSettings() {
        return (AzureAiStudioChatCompletionTaskSettings) super.getTaskSettings();
    }

    @Override
    public DefaultSecretSettings getSecretSettings() {
        return super.getSecretSettings();
    }

    @Override
    protected URI getEndpointUri() throws URISyntaxException {
        if (this.provider == AzureAiStudioProvider.OPENAI || this.endpointType == AzureAiStudioEndpointType.REALTIME) {
            return new URI(this.target);
        }

        return new URI(this.target + COMPLETIONS_URI_PATH);
    }

    @Override
    public ExecutableAction accept(AzureAiStudioActionVisitor creator, Map<String, Object> taskSettings) {
        return creator.create(this, taskSettings);
    }
}
