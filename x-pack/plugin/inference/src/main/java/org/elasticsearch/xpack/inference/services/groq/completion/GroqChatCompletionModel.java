/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.groq.completion;

import org.apache.http.client.utils.URIBuilder;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.ModelSecrets;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.inference.UnifiedCompletionRequest;
import org.elasticsearch.xpack.inference.external.action.ExecutableAction;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.groq.GroqModel;
import org.elasticsearch.xpack.inference.services.groq.GroqService;
import org.elasticsearch.xpack.inference.services.groq.GroqUtils;
import org.elasticsearch.xpack.inference.services.groq.action.GroqActionVisitor;
import org.elasticsearch.xpack.inference.services.settings.DefaultSecretSettings;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.xpack.inference.external.request.RequestUtils.buildUri;

public class GroqChatCompletionModel extends GroqModel {

    public static GroqChatCompletionModel of(GroqChatCompletionModel model, Map<String, Object> taskSettings) {
        if (taskSettings == null || taskSettings.isEmpty()) {
            return model;
        }

        return new GroqChatCompletionModel(model, model.getTaskSettings().updatedTaskSettings(taskSettings));
    }

    public static GroqChatCompletionModel of(GroqChatCompletionModel model, UnifiedCompletionRequest request) {
        var originalServiceSettings = model.getServiceSettings();
        var overriddenServiceSettings = new GroqChatCompletionServiceSettings(
            Objects.requireNonNullElse(request.model(), originalServiceSettings.modelId()),
            originalServiceSettings.uri(),
            originalServiceSettings.organizationId(),
            originalServiceSettings.rateLimitSettings()
        );

        return new GroqChatCompletionModel(
            model.getInferenceEntityId(),
            model.getTaskType(),
            model.getConfigurations().getService(),
            overriddenServiceSettings,
            model.getTaskSettings(),
            model.getSecretSettings()
        );
    }

    public GroqChatCompletionModel(
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
            GroqChatCompletionServiceSettings.fromMap(serviceSettings, context),
            new GroqChatCompletionTaskSettings(taskSettings),
            DefaultSecretSettings.fromMap(secrets)
        );
    }

    GroqChatCompletionModel(
        String modelId,
        TaskType taskType,
        String service,
        GroqChatCompletionServiceSettings serviceSettings,
        GroqChatCompletionTaskSettings taskSettings,
        @Nullable DefaultSecretSettings secrets
    ) {
        super(
            new ModelConfigurations(modelId, taskType, service, serviceSettings, taskSettings),
            new ModelSecrets(secrets),
            serviceSettings,
            secrets,
            buildUri(serviceSettings.uri(), GroqService.NAME, GroqChatCompletionModel::buildDefaultUri)
        );
    }

    public static URI buildDefaultUri() throws URISyntaxException {
        return new URIBuilder().setScheme("https")
            .setHost(GroqUtils.HOST)
            .setPathSegments(GroqUtils.OPENAI_PATH, GroqUtils.VERSION_1, GroqUtils.CHAT_PATH, GroqUtils.COMPLETIONS_PATH)
            .build();
    }

    private GroqChatCompletionModel(GroqChatCompletionModel originalModel, GroqChatCompletionTaskSettings taskSettings) {
        super(originalModel, taskSettings);
    }

    private GroqChatCompletionModel(GroqChatCompletionModel originalModel, GroqChatCompletionServiceSettings serviceSettings) {
        super(originalModel, serviceSettings);
    }

    @Override
    public GroqChatCompletionServiceSettings getServiceSettings() {
        return (GroqChatCompletionServiceSettings) super.getServiceSettings();
    }

    @Override
    public GroqChatCompletionTaskSettings getTaskSettings() {
        return (GroqChatCompletionTaskSettings) super.getTaskSettings();
    }

    @Override
    public DefaultSecretSettings getSecretSettings() {
        return (DefaultSecretSettings) super.getSecretSettings();
    }

    public GroqChatCompletionModel withServiceSettings(GroqChatCompletionServiceSettings serviceSettings) {
        return new GroqChatCompletionModel(this, serviceSettings);
    }

    @Override
    public ExecutableAction accept(GroqActionVisitor creator, Map<String, Object> taskSettings) {
        return creator.create(this, taskSettings);
    }
}
