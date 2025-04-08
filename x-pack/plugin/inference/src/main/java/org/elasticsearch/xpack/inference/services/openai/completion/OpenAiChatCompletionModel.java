/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.openai.completion;

import org.apache.http.client.utils.URIBuilder;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.ModelSecrets;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.inference.UnifiedCompletionRequest;
import org.elasticsearch.xpack.inference.external.action.ExecutableAction;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.openai.OpenAiModel;
import org.elasticsearch.xpack.inference.services.openai.OpenAiService;
import org.elasticsearch.xpack.inference.services.openai.OpenAiUtils;
import org.elasticsearch.xpack.inference.services.openai.action.OpenAiActionVisitor;
import org.elasticsearch.xpack.inference.services.settings.DefaultSecretSettings;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.xpack.inference.external.request.RequestUtils.buildUri;

public class OpenAiChatCompletionModel extends OpenAiModel {

    public static OpenAiChatCompletionModel of(OpenAiChatCompletionModel model, Map<String, Object> taskSettings) {
        if (taskSettings == null || taskSettings.isEmpty()) {
            return model;
        }

        var requestTaskSettings = OpenAiChatCompletionRequestTaskSettings.fromMap(taskSettings);
        return new OpenAiChatCompletionModel(model, OpenAiChatCompletionTaskSettings.of(model.getTaskSettings(), requestTaskSettings));
    }

    public static OpenAiChatCompletionModel of(OpenAiChatCompletionModel model, UnifiedCompletionRequest request) {
        var originalModelServiceSettings = model.getServiceSettings();
        var overriddenServiceSettings = new OpenAiChatCompletionServiceSettings(
            Objects.requireNonNullElse(request.model(), originalModelServiceSettings.modelId()),
            originalModelServiceSettings.uri(),
            originalModelServiceSettings.organizationId(),
            originalModelServiceSettings.maxInputTokens(),
            originalModelServiceSettings.rateLimitSettings()
        );

        return new OpenAiChatCompletionModel(
            model.getInferenceEntityId(),
            model.getTaskType(),
            model.getConfigurations().getService(),
            overriddenServiceSettings,
            model.getTaskSettings(),
            model.getSecretSettings()
        );
    }

    public OpenAiChatCompletionModel(
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
            OpenAiChatCompletionServiceSettings.fromMap(serviceSettings, context),
            OpenAiChatCompletionTaskSettings.fromMap(taskSettings),
            DefaultSecretSettings.fromMap(secrets)
        );
    }

    OpenAiChatCompletionModel(
        String modelId,
        TaskType taskType,
        String service,
        OpenAiChatCompletionServiceSettings serviceSettings,
        OpenAiChatCompletionTaskSettings taskSettings,
        @Nullable DefaultSecretSettings secrets
    ) {
        super(
            new ModelConfigurations(modelId, taskType, service, serviceSettings, taskSettings),
            new ModelSecrets(secrets),
            serviceSettings,
            secrets,
            buildUri(serviceSettings.uri(), OpenAiService.NAME, OpenAiChatCompletionModel::buildDefaultUri)
        );
    }

    public static URI buildDefaultUri() throws URISyntaxException {
        return new URIBuilder().setScheme("https")
            .setHost(OpenAiUtils.HOST)
            .setPathSegments(OpenAiUtils.VERSION_1, OpenAiUtils.CHAT_PATH, OpenAiUtils.COMPLETIONS_PATH)
            .build();
    }

    private OpenAiChatCompletionModel(OpenAiChatCompletionModel originalModel, OpenAiChatCompletionTaskSettings taskSettings) {
        super(originalModel, taskSettings);
    }

    @Override
    public OpenAiChatCompletionServiceSettings getServiceSettings() {
        return (OpenAiChatCompletionServiceSettings) super.getServiceSettings();
    }

    @Override
    public OpenAiChatCompletionTaskSettings getTaskSettings() {
        return (OpenAiChatCompletionTaskSettings) super.getTaskSettings();
    }

    @Override
    public DefaultSecretSettings getSecretSettings() {
        return (DefaultSecretSettings) super.getSecretSettings();
    }

    @Override
    public ExecutableAction accept(OpenAiActionVisitor creator, Map<String, Object> taskSettings) {
        return creator.create(this, taskSettings);
    }
}
