/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.openai.completion;

import org.elasticsearch.common.util.LazyInitializable;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.ModelSecrets;
import org.elasticsearch.inference.SettingsConfiguration;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.inference.configuration.SettingsConfigurationDisplayType;
import org.elasticsearch.inference.configuration.SettingsConfigurationFieldType;
import org.elasticsearch.xpack.inference.external.action.ExecutableAction;
import org.elasticsearch.xpack.inference.external.action.openai.OpenAiActionVisitor;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.openai.OpenAiModel;
import org.elasticsearch.xpack.inference.services.settings.DefaultSecretSettings;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.xpack.inference.services.openai.OpenAiServiceFields.USER;

public class OpenAiChatCompletionModel extends OpenAiModel {

    public static OpenAiChatCompletionModel of(OpenAiChatCompletionModel model, Map<String, Object> taskSettings) {
        if (taskSettings == null || taskSettings.isEmpty()) {
            return model;
        }

        var requestTaskSettings = OpenAiChatCompletionRequestTaskSettings.fromMap(taskSettings);
        return new OpenAiChatCompletionModel(model, OpenAiChatCompletionTaskSettings.of(model.getTaskSettings(), requestTaskSettings));
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
            secrets
        );
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

    public static class Configuration {
        public static Map<String, SettingsConfiguration> get() {
            return configuration.getOrCompute();
        }

        private static final LazyInitializable<Map<String, SettingsConfiguration>, RuntimeException> configuration =
            new LazyInitializable<>(() -> {
                var configurationMap = new HashMap<String, SettingsConfiguration>();

                configurationMap.put(
                    USER,
                    new SettingsConfiguration.Builder().setDisplay(SettingsConfigurationDisplayType.TEXTBOX)
                        .setLabel("User")
                        .setOrder(1)
                        .setRequired(false)
                        .setSensitive(false)
                        .setTooltip("Specifies the user issuing the request.")
                        .setType(SettingsConfigurationFieldType.STRING)
                        .setValue("")
                        .build()
                );

                return Collections.unmodifiableMap(configurationMap);
            });
    }
}
