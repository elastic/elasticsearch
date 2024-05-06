/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.azureaistudio.completion;

import org.elasticsearch.common.ValidationException;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.ModelSecrets;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.azureaistudio.AzureAiStudioEndpointType;
import org.elasticsearch.xpack.inference.services.azureaistudio.AzureAiStudioModel;
import org.elasticsearch.xpack.inference.services.azureaistudio.AzureAiStudioProvider;
import org.elasticsearch.xpack.inference.services.settings.DefaultSecretSettings;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;

import static org.elasticsearch.xpack.inference.services.azureaistudio.AzureAiStudioConstants.COMPLETIONS_URI_PATH;

public class AzureAiStudioCompletionModel extends AzureAiStudioModel {

    public static AzureAiStudioCompletionModel of(AzureAiStudioModel model, Map<String, Object> taskSettings) {
        var modelAsCompletionModel = (AzureAiStudioCompletionModel) model;

        if (taskSettings == null || taskSettings.isEmpty()) {
            return modelAsCompletionModel;
        }

        var requestTaskSettings = AzureAiStudioCompletionRequestTaskSettings.fromMap(taskSettings);
        var taskSettingToUse = AzureAiStudioCompletionTaskSettings.of(modelAsCompletionModel.getTaskSettings(), requestTaskSettings);

        ValidationException validationException = new ValidationException();

        if (taskSettingToUse.temperature() != null) {
            // TODO -- add validation error message
            validationException.addValidationError("");
        }
        if (taskSettingToUse.topP() != null) {
            // TODO -- add validation error message
            validationException.addValidationError("");
        }
        if (taskSettingToUse.doSample() != null) {
            // TODO -- add validation error message
            validationException.addValidationError("");
        }
        if (taskSettingToUse.maxTokens() != null) {
            // TODO -- add validation error message
            validationException.addValidationError("");
        }

        if (validationException.validationErrors().isEmpty() == false) {
            throw validationException;
        }

        // TODO - add request limiting

        return new AzureAiStudioCompletionModel(modelAsCompletionModel, taskSettingToUse);
    }

    public AzureAiStudioCompletionModel(
        String inferenceEntityId,
        TaskType taskType,
        String service,
        AzureAiStudioCompletionServiceSettings serviceSettings,
        AzureAiStudioCompletionTaskSettings taskSettings,
        DefaultSecretSettings secrets
    ) {
        super(new ModelConfigurations(inferenceEntityId, taskType, service, serviceSettings, taskSettings), new ModelSecrets(secrets));
    }

    public AzureAiStudioCompletionModel(
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
            AzureAiStudioCompletionServiceSettings.fromMap(serviceSettings, context),
            AzureAiStudioCompletionTaskSettings.fromMap(taskSettings),
            DefaultSecretSettings.fromMap(secrets)
        );
    }

    public AzureAiStudioCompletionModel(AzureAiStudioCompletionModel model, AzureAiStudioCompletionServiceSettings serviceSettings) {
        super(model, serviceSettings);
    }

    public AzureAiStudioCompletionModel(AzureAiStudioCompletionModel model, AzureAiStudioCompletionTaskSettings taskSettings) {
        super(model, taskSettings, model.getServiceSettings().rateLimitSettings());
    }

    protected AzureAiStudioCompletionModel(ModelConfigurations modelConfigurations, ModelSecrets modelSecrets) {
        super(modelConfigurations, modelSecrets);
    }

    @Override
    public AzureAiStudioCompletionServiceSettings getServiceSettings() {
        return (AzureAiStudioCompletionServiceSettings) super.getServiceSettings();
    }

    @Override
    public AzureAiStudioCompletionTaskSettings getTaskSettings() {
        return (AzureAiStudioCompletionTaskSettings) super.getTaskSettings();
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
}
