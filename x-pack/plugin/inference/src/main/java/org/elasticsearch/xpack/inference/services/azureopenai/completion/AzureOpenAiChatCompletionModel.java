/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.azureopenai.completion;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.ModelSecrets;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.xpack.inference.external.action.ExecutableAction;
import org.elasticsearch.xpack.inference.external.action.azureopenai.AzureOpenAiActionVisitor;
import org.elasticsearch.xpack.inference.external.request.azureopenai.AzureOpenAiUtils;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.azureopenai.AzureOpenAiModel;
import org.elasticsearch.xpack.inference.services.azureopenai.AzureOpenAiSecretSettings;

import java.net.URISyntaxException;
import java.util.Map;

public class AzureOpenAiChatCompletionModel extends AzureOpenAiModel {

    public static AzureOpenAiChatCompletionModel of(AzureOpenAiChatCompletionModel model, Map<String, Object> taskSettings) {
        if (taskSettings == null || taskSettings.isEmpty()) {
            return model;
        }

        var requestTaskSettings = AzureOpenAiChatCompletionRequestTaskSettings.fromMap(taskSettings);
        return new AzureOpenAiChatCompletionModel(
            model,
            AzureOpenAiChatCompletionTaskSettings.of(model.getTaskSettings(), requestTaskSettings)
        );
    }

    public AzureOpenAiChatCompletionModel(
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
            AzureOpenAiCompletionServiceSettings.fromMap(serviceSettings, context),
            AzureOpenAiChatCompletionTaskSettings.fromMap(taskSettings),
            AzureOpenAiSecretSettings.fromMap(secrets)
        );
    }

    // Should only be used directly for testing
    AzureOpenAiChatCompletionModel(
        String inferenceEntityId,
        TaskType taskType,
        String service,
        AzureOpenAiCompletionServiceSettings serviceSettings,
        AzureOpenAiChatCompletionTaskSettings taskSettings,
        @Nullable AzureOpenAiSecretSettings secrets
    ) {
        super(
            new ModelConfigurations(inferenceEntityId, taskType, service, serviceSettings, taskSettings),
            new ModelSecrets(secrets),
            serviceSettings
        );
        try {
            this.uri = buildUriString();
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }

    public AzureOpenAiChatCompletionModel(
        AzureOpenAiChatCompletionModel originalModel,
        AzureOpenAiCompletionServiceSettings serviceSettings
    ) {
        super(originalModel, serviceSettings);
    }

    private AzureOpenAiChatCompletionModel(
        AzureOpenAiChatCompletionModel originalModel,
        AzureOpenAiChatCompletionTaskSettings taskSettings
    ) {
        super(originalModel, taskSettings);
    }

    @Override
    public AzureOpenAiCompletionServiceSettings getServiceSettings() {
        return (AzureOpenAiCompletionServiceSettings) super.getServiceSettings();
    }

    @Override
    public AzureOpenAiChatCompletionTaskSettings getTaskSettings() {
        return (AzureOpenAiChatCompletionTaskSettings) super.getTaskSettings();
    }

    @Override
    public AzureOpenAiSecretSettings getSecretSettings() {
        return (AzureOpenAiSecretSettings) super.getSecretSettings();
    }

    @Override
    public ExecutableAction accept(AzureOpenAiActionVisitor creator, Map<String, Object> taskSettings) {
        return creator.create(this, taskSettings);
    }

    @Override
    public String resourceName() {
        return getServiceSettings().resourceName();
    }

    @Override
    public String deploymentId() {
        return getServiceSettings().deploymentId();
    }

    @Override
    public String apiVersion() {
        return getServiceSettings().apiVersion();
    }

    @Override
    public String[] operationPathSegments() {
        return new String[] { AzureOpenAiUtils.CHAT_PATH, AzureOpenAiUtils.COMPLETIONS_PATH };
    }

}
