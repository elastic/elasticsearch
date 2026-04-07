/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.fireworksai.completion;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.ModelSecrets;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.inference.UnifiedCompletionRequest;
import org.elasticsearch.xpack.inference.external.action.ExecutableAction;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.fireworksai.FireworksAiModel;
import org.elasticsearch.xpack.inference.services.fireworksai.FireworksAiService;
import org.elasticsearch.xpack.inference.services.fireworksai.action.FireworksAiActionVisitor;
import org.elasticsearch.xpack.inference.services.settings.DefaultSecretSettings;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.xpack.inference.external.request.RequestUtils.buildUri;

public class FireworksAiChatCompletionModel extends FireworksAiModel {

    private static final String DEFAULT_CHAT_COMPLETIONS_URL = "https://api.fireworks.ai/inference/v1/chat/completions";

    private final URI uri;

    public static FireworksAiChatCompletionModel of(FireworksAiChatCompletionModel model, Map<String, Object> taskSettings) {
        if (taskSettings == null || taskSettings.isEmpty()) {
            return model;
        }

        return new FireworksAiChatCompletionModel(model, model.getTaskSettings().updatedTaskSettings(taskSettings));
    }

    public static FireworksAiChatCompletionModel of(FireworksAiChatCompletionModel model, UnifiedCompletionRequest request) {
        var originalServiceSettings = model.getServiceSettings();
        var overriddenServiceSettings = new FireworksAiChatCompletionServiceSettings(
            Objects.requireNonNullElse(request.model(), originalServiceSettings.modelId()),
            originalServiceSettings.uri(),
            originalServiceSettings.rateLimitSettings()
        );

        return new FireworksAiChatCompletionModel(model, overriddenServiceSettings);
    }

    public FireworksAiChatCompletionModel(
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
            FireworksAiChatCompletionServiceSettings.fromMap(serviceSettings, context),
            new FireworksAiChatCompletionTaskSettings(taskSettings),
            DefaultSecretSettings.fromMap(secrets)
        );
    }

    public FireworksAiChatCompletionModel(
        String inferenceEntityId,
        TaskType taskType,
        String service,
        FireworksAiChatCompletionServiceSettings serviceSettings,
        FireworksAiChatCompletionTaskSettings taskSettings,
        @Nullable DefaultSecretSettings secrets
    ) {
        this(new ModelConfigurations(inferenceEntityId, taskType, service, serviceSettings, taskSettings), new ModelSecrets(secrets));
    }

    public FireworksAiChatCompletionModel(ModelConfigurations modelConfigurations, ModelSecrets modelSecrets) {
        super(
            modelConfigurations,
            modelSecrets,
            (DefaultSecretSettings) modelSecrets.getSecretSettings(),
            (FireworksAiChatCompletionServiceSettings) modelConfigurations.getServiceSettings()
        );
        this.uri = buildUri(
            ((FireworksAiChatCompletionServiceSettings) modelConfigurations.getServiceSettings()).uri(),
            FireworksAiService.NAME,
            FireworksAiChatCompletionModel::buildDefaultUri
        );
    }

    private FireworksAiChatCompletionModel(
        FireworksAiChatCompletionModel originalModel,
        FireworksAiChatCompletionTaskSettings taskSettings
    ) {
        super(originalModel, taskSettings);
        this.uri = originalModel.uri;
    }

    private FireworksAiChatCompletionModel(
        FireworksAiChatCompletionModel originalModel,
        FireworksAiChatCompletionServiceSettings serviceSettings
    ) {
        super(originalModel, serviceSettings);
        this.uri = buildUri(serviceSettings.uri(), FireworksAiService.NAME, FireworksAiChatCompletionModel::buildDefaultUri);
    }

    public static URI buildDefaultUri() throws URISyntaxException {
        return new URI(DEFAULT_CHAT_COMPLETIONS_URL);
    }

    public URI uri() {
        return uri;
    }

    @Override
    public FireworksAiChatCompletionServiceSettings getServiceSettings() {
        return (FireworksAiChatCompletionServiceSettings) super.getServiceSettings();
    }

    @Override
    public FireworksAiChatCompletionTaskSettings getTaskSettings() {
        return (FireworksAiChatCompletionTaskSettings) super.getTaskSettings();
    }

    @Override
    public DefaultSecretSettings getSecretSettings() {
        return (DefaultSecretSettings) super.getSecretSettings();
    }

    @Override
    public ExecutableAction accept(FireworksAiActionVisitor creator, Map<String, Object> taskSettings) {
        return creator.create(this, taskSettings);
    }
}
