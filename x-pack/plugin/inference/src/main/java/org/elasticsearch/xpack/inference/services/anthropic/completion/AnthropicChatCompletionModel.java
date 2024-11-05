/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.anthropic.completion;

import org.apache.http.client.utils.URIBuilder;
import org.elasticsearch.common.util.LazyInitializable;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.ModelSecrets;
import org.elasticsearch.inference.SettingsConfiguration;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.inference.configuration.SettingsConfigurationDisplayType;
import org.elasticsearch.inference.configuration.SettingsConfigurationFieldType;
import org.elasticsearch.xpack.inference.external.action.ExecutableAction;
import org.elasticsearch.xpack.inference.external.action.anthropic.AnthropicActionVisitor;
import org.elasticsearch.xpack.inference.external.request.anthropic.AnthropicRequestUtils;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.ServiceUtils;
import org.elasticsearch.xpack.inference.services.anthropic.AnthropicModel;
import org.elasticsearch.xpack.inference.services.settings.DefaultSecretSettings;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.xpack.inference.services.anthropic.AnthropicServiceFields.MAX_TOKENS;
import static org.elasticsearch.xpack.inference.services.anthropic.AnthropicServiceFields.TEMPERATURE_FIELD;
import static org.elasticsearch.xpack.inference.services.anthropic.AnthropicServiceFields.TOP_K_FIELD;
import static org.elasticsearch.xpack.inference.services.anthropic.AnthropicServiceFields.TOP_P_FIELD;

public class AnthropicChatCompletionModel extends AnthropicModel {

    public static AnthropicChatCompletionModel of(AnthropicChatCompletionModel model, Map<String, Object> taskSettings) {
        if (taskSettings == null || taskSettings.isEmpty()) {
            return model;
        }

        var requestTaskSettings = AnthropicChatCompletionRequestTaskSettings.fromMap(taskSettings);
        return new AnthropicChatCompletionModel(
            model,
            AnthropicChatCompletionTaskSettings.of(model.getTaskSettings(), requestTaskSettings)
        );
    }

    public AnthropicChatCompletionModel(
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
            AnthropicChatCompletionServiceSettings.fromMap(serviceSettings, context),
            AnthropicChatCompletionTaskSettings.fromMap(taskSettings, context),
            DefaultSecretSettings.fromMap(secrets)
        );
    }

    AnthropicChatCompletionModel(
        String inferenceEntityId,
        TaskType taskType,
        String service,
        AnthropicChatCompletionServiceSettings serviceSettings,
        AnthropicChatCompletionTaskSettings taskSettings,
        @Nullable DefaultSecretSettings secrets
    ) {
        super(
            new ModelConfigurations(inferenceEntityId, taskType, service, serviceSettings, taskSettings),
            new ModelSecrets(secrets),
            serviceSettings,
            AnthropicChatCompletionModel::buildDefaultUri,
            secrets
        );
    }

    // This should only be used for testing
    AnthropicChatCompletionModel(
        String inferenceEntityId,
        TaskType taskType,
        String service,
        String url,
        AnthropicChatCompletionServiceSettings serviceSettings,
        AnthropicChatCompletionTaskSettings taskSettings,
        @Nullable DefaultSecretSettings secrets
    ) {
        super(
            new ModelConfigurations(inferenceEntityId, taskType, service, serviceSettings, taskSettings),
            new ModelSecrets(secrets),
            serviceSettings,
            () -> ServiceUtils.createUri(url),
            secrets
        );
    }

    private AnthropicChatCompletionModel(AnthropicChatCompletionModel originalModel, AnthropicChatCompletionTaskSettings taskSettings) {
        super(originalModel, taskSettings);
    }

    @Override
    public AnthropicChatCompletionServiceSettings getServiceSettings() {
        return (AnthropicChatCompletionServiceSettings) super.getServiceSettings();
    }

    @Override
    public AnthropicChatCompletionTaskSettings getTaskSettings() {
        return (AnthropicChatCompletionTaskSettings) super.getTaskSettings();
    }

    @Override
    public DefaultSecretSettings getSecretSettings() {
        return (DefaultSecretSettings) super.getSecretSettings();
    }

    @Override
    public ExecutableAction accept(AnthropicActionVisitor creator, Map<String, Object> taskSettings) {
        return creator.create(this, taskSettings);
    }

    private static URI buildDefaultUri() throws URISyntaxException {
        return new URIBuilder().setScheme("https")
            .setHost(AnthropicRequestUtils.HOST)
            .setPathSegments(AnthropicRequestUtils.API_VERSION_1, AnthropicRequestUtils.MESSAGES_PATH)
            .build();
    }

    public static class Configuration {
        public static Map<String, SettingsConfiguration> get() {
            return configuration.getOrCompute();
        }

        private static final LazyInitializable<Map<String, SettingsConfiguration>, RuntimeException> configuration =
            new LazyInitializable<>(() -> {
                var configurationMap = new HashMap<String, SettingsConfiguration>();

                configurationMap.put(
                    MAX_TOKENS,
                    new SettingsConfiguration.Builder().setDisplay(SettingsConfigurationDisplayType.NUMERIC)
                        .setLabel("Max Tokens")
                        .setOrder(1)
                        .setRequired(true)
                        .setSensitive(false)
                        .setTooltip("The maximum number of tokens to generate before stopping.")
                        .setType(SettingsConfigurationFieldType.INTEGER)
                        .build()
                );
                configurationMap.put(
                    TEMPERATURE_FIELD,
                    new SettingsConfiguration.Builder().setDisplay(SettingsConfigurationDisplayType.TEXTBOX)
                        .setLabel("Temperature")
                        .setOrder(2)
                        .setRequired(false)
                        .setSensitive(false)
                        .setTooltip("The amount of randomness injected into the response.")
                        .setType(SettingsConfigurationFieldType.STRING)
                        .build()
                );
                configurationMap.put(
                    TOP_K_FIELD,
                    new SettingsConfiguration.Builder().setDisplay(SettingsConfigurationDisplayType.NUMERIC)
                        .setLabel("Top K")
                        .setOrder(3)
                        .setRequired(false)
                        .setSensitive(false)
                        .setTooltip("Specifies to only sample from the top K options for each subsequent token.")
                        .setType(SettingsConfigurationFieldType.INTEGER)
                        .build()
                );
                configurationMap.put(
                    TOP_P_FIELD,
                    new SettingsConfiguration.Builder().setDisplay(SettingsConfigurationDisplayType.NUMERIC)
                        .setLabel("Top P")
                        .setOrder(4)
                        .setRequired(false)
                        .setSensitive(false)
                        .setTooltip("Specifies to use Anthropic’s nucleus sampling.")
                        .setType(SettingsConfigurationFieldType.INTEGER)
                        .build()
                );

                return Collections.unmodifiableMap(configurationMap);
            });
    }
}
