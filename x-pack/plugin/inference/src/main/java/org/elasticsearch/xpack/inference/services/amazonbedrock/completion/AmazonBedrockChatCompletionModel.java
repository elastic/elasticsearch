/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.amazonbedrock.completion;

import org.elasticsearch.common.util.LazyInitializable;
import org.elasticsearch.inference.Model;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.ModelSecrets;
import org.elasticsearch.inference.SettingsConfiguration;
import org.elasticsearch.inference.TaskSettings;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.inference.configuration.SettingsConfigurationDisplayType;
import org.elasticsearch.inference.configuration.SettingsConfigurationFieldType;
import org.elasticsearch.xpack.inference.external.action.ExecutableAction;
import org.elasticsearch.xpack.inference.external.action.amazonbedrock.AmazonBedrockActionVisitor;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.amazonbedrock.AmazonBedrockModel;
import org.elasticsearch.xpack.inference.services.amazonbedrock.AmazonBedrockSecretSettings;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.xpack.inference.services.amazonbedrock.AmazonBedrockConstants.MAX_NEW_TOKENS_FIELD;
import static org.elasticsearch.xpack.inference.services.amazonbedrock.AmazonBedrockConstants.TEMPERATURE_FIELD;
import static org.elasticsearch.xpack.inference.services.amazonbedrock.AmazonBedrockConstants.TOP_K_FIELD;
import static org.elasticsearch.xpack.inference.services.amazonbedrock.AmazonBedrockConstants.TOP_P_FIELD;

public class AmazonBedrockChatCompletionModel extends AmazonBedrockModel {

    public static AmazonBedrockChatCompletionModel of(AmazonBedrockChatCompletionModel completionModel, Map<String, Object> taskSettings) {
        if (taskSettings == null || taskSettings.isEmpty()) {
            return completionModel;
        }

        var requestTaskSettings = AmazonBedrockChatCompletionRequestTaskSettings.fromMap(taskSettings);
        var taskSettingsToUse = AmazonBedrockChatCompletionTaskSettings.of(completionModel.getTaskSettings(), requestTaskSettings);
        return new AmazonBedrockChatCompletionModel(completionModel, taskSettingsToUse);
    }

    public AmazonBedrockChatCompletionModel(
        String inferenceEntityId,
        TaskType taskType,
        String name,
        Map<String, Object> serviceSettings,
        Map<String, Object> taskSettings,
        Map<String, Object> secretSettings,
        ConfigurationParseContext context
    ) {
        this(
            inferenceEntityId,
            taskType,
            name,
            AmazonBedrockChatCompletionServiceSettings.fromMap(serviceSettings, context),
            AmazonBedrockChatCompletionTaskSettings.fromMap(taskSettings),
            AmazonBedrockSecretSettings.fromMap(secretSettings)
        );
    }

    public AmazonBedrockChatCompletionModel(
        String inferenceEntityId,
        TaskType taskType,
        String service,
        AmazonBedrockChatCompletionServiceSettings serviceSettings,
        AmazonBedrockChatCompletionTaskSettings taskSettings,
        AmazonBedrockSecretSettings secrets
    ) {
        super(new ModelConfigurations(inferenceEntityId, taskType, service, serviceSettings, taskSettings), new ModelSecrets(secrets));
    }

    public AmazonBedrockChatCompletionModel(Model model, TaskSettings taskSettings) {
        super(model, taskSettings);
    }

    @Override
    public ExecutableAction accept(AmazonBedrockActionVisitor creator, Map<String, Object> taskSettings) {
        return creator.create(this, taskSettings);
    }

    @Override
    public AmazonBedrockChatCompletionServiceSettings getServiceSettings() {
        return (AmazonBedrockChatCompletionServiceSettings) super.getServiceSettings();
    }

    @Override
    public AmazonBedrockChatCompletionTaskSettings getTaskSettings() {
        return (AmazonBedrockChatCompletionTaskSettings) super.getTaskSettings();
    }

    public static class Configuration {
        public static Map<String, SettingsConfiguration> get() {
            return configuration.getOrCompute();
        }

        private static final LazyInitializable<Map<String, SettingsConfiguration>, RuntimeException> configuration =
            new LazyInitializable<>(() -> {
                var configurationMap = new HashMap<String, SettingsConfiguration>();

                configurationMap.put(
                    MAX_NEW_TOKENS_FIELD,
                    new SettingsConfiguration.Builder().setDisplay(SettingsConfigurationDisplayType.NUMERIC)
                        .setLabel("Max New Tokens")
                        .setOrder(1)
                        .setRequired(false)
                        .setSensitive(false)
                        .setTooltip("Sets the maximum number for the output tokens to be generated.")
                        .setType(SettingsConfigurationFieldType.INTEGER)
                        .build()
                );
                configurationMap.put(
                    TEMPERATURE_FIELD,
                    new SettingsConfiguration.Builder().setDisplay(SettingsConfigurationDisplayType.NUMERIC)
                        .setLabel("Temperature")
                        .setOrder(2)
                        .setRequired(false)
                        .setSensitive(false)
                        .setTooltip("A number between 0.0 and 1.0 that controls the apparent creativity of the results.")
                        .setType(SettingsConfigurationFieldType.INTEGER)
                        .build()
                );
                configurationMap.put(
                    TOP_P_FIELD,
                    new SettingsConfiguration.Builder().setDisplay(SettingsConfigurationDisplayType.NUMERIC)
                        .setLabel("Top P")
                        .setOrder(3)
                        .setRequired(false)
                        .setSensitive(false)
                        .setTooltip("Alternative to temperature. A number in the range of 0.0 to 1.0, to eliminate low-probability tokens.")
                        .setType(SettingsConfigurationFieldType.INTEGER)
                        .build()
                );
                configurationMap.put(
                    TOP_K_FIELD,
                    new SettingsConfiguration.Builder().setDisplay(SettingsConfigurationDisplayType.NUMERIC)
                        .setLabel("Top K")
                        .setOrder(4)
                        .setRequired(false)
                        .setSensitive(false)
                        .setTooltip("Only available for anthropic, cohere, and mistral providers. Alternative to temperature.")
                        .setType(SettingsConfigurationFieldType.INTEGER)
                        .build()
                );

                return Collections.unmodifiableMap(configurationMap);
            });
    }
}
