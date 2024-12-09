/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.jinaai.embeddings;

import org.elasticsearch.common.util.LazyInitializable;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.ChunkingSettings;
import org.elasticsearch.inference.InputType;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.ModelSecrets;
import org.elasticsearch.inference.SettingsConfiguration;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.inference.configuration.SettingsConfigurationDisplayType;
import org.elasticsearch.inference.configuration.SettingsConfigurationFieldType;
import org.elasticsearch.inference.configuration.SettingsConfigurationSelectOption;
import org.elasticsearch.xpack.inference.external.action.ExecutableAction;
import org.elasticsearch.xpack.inference.external.action.jinaai.JinaAIActionVisitor;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.jinaai.JinaAIModel;
import org.elasticsearch.xpack.inference.services.settings.DefaultSecretSettings;

import java.net.URI;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

import static org.elasticsearch.xpack.inference.external.request.jinaai.JinaAIEmbeddingsRequestEntity.INPUT_TYPE_FIELD;

public class JinaAIEmbeddingsModel extends JinaAIModel {
    public static JinaAIEmbeddingsModel of(JinaAIEmbeddingsModel model, Map<String, Object> taskSettings, InputType inputType) {
        var requestTaskSettings = JinaAIEmbeddingsTaskSettings.fromMap(taskSettings);
        return new JinaAIEmbeddingsModel(model, JinaAIEmbeddingsTaskSettings.of(model.getTaskSettings(), requestTaskSettings, inputType));
    }

    public JinaAIEmbeddingsModel(
        String inferenceId,
        TaskType taskType,
        String service,
        Map<String, Object> serviceSettings,
        Map<String, Object> taskSettings,
        @Nullable Map<String, Object> secrets,
        ConfigurationParseContext context
    ) {
        this(
            inferenceId,
            taskType,
            service,
            JinaAIEmbeddingsServiceSettings.fromMap(serviceSettings, context),
            JinaAIEmbeddingsTaskSettings.fromMap(taskSettings),
            DefaultSecretSettings.fromMap(secrets)
        );
    }

    // should only be used for testing
    JinaAIEmbeddingsModel(
        String modelId,
        TaskType taskType,
        String service,
        JinaAIEmbeddingsServiceSettings serviceSettings,
        JinaAIEmbeddingsTaskSettings taskSettings,
        @Nullable DefaultSecretSettings secretSettings
    ) {
        super(
            new ModelConfigurations(modelId, taskType, service, serviceSettings, taskSettings),
            new ModelSecrets(secretSettings),
            secretSettings,
            serviceSettings.getCommonSettings()
        );
    }

    private JinaAIEmbeddingsModel(JinaAIEmbeddingsModel model, JinaAIEmbeddingsTaskSettings taskSettings) {
        super(model, taskSettings);
    }

    public JinaAIEmbeddingsModel(JinaAIEmbeddingsModel model, JinaAIEmbeddingsServiceSettings serviceSettings) {
        super(model, serviceSettings);
    }

    @Override
    public JinaAIEmbeddingsServiceSettings getServiceSettings() {
        return (JinaAIEmbeddingsServiceSettings) super.getServiceSettings();
    }

    @Override
    public JinaAIEmbeddingsTaskSettings getTaskSettings() {
        return (JinaAIEmbeddingsTaskSettings) super.getTaskSettings();
    }

    @Override
    public DefaultSecretSettings getSecretSettings() {
        return (DefaultSecretSettings) super.getSecretSettings();
    }

    @Override
    public ExecutableAction accept(JinaAIActionVisitor visitor, Map<String, Object> taskSettings, InputType inputType) {
        return visitor.create(this, taskSettings, inputType);
    }

    @Override
    public URI uri() {
        return getServiceSettings().getCommonSettings().uri();
    }

    public static class Configuration {
        public static Map<String, SettingsConfiguration> get() {
            return configuration.getOrCompute();
        }

        private static final LazyInitializable<Map<String, SettingsConfiguration>, RuntimeException> configuration =
            new LazyInitializable<>(() -> {
                var configurationMap = new HashMap<String, SettingsConfiguration>();

                configurationMap.put(
                    INPUT_TYPE_FIELD,
                    new SettingsConfiguration.Builder().setDisplay(SettingsConfigurationDisplayType.DROPDOWN)
                        .setLabel("Input Type")
                        .setOrder(1)
                        .setRequired(false)
                        .setSensitive(false)
                        .setTooltip("Specifies the type of input passed to the model.")
                        .setType(SettingsConfigurationFieldType.STRING)
                        .setOptions(
                            Stream.of("classification", "clustering", "ingest", "search")
                                .map(v -> new SettingsConfigurationSelectOption.Builder().setLabelAndValue(v).build())
                                .toList()
                        )
                        .setValue("")
                        .build()
                );

                return Collections.unmodifiableMap(configurationMap);
            });
    }
}
