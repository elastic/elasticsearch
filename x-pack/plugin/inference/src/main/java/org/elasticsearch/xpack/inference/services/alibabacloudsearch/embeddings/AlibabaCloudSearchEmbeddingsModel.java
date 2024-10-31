/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.alibabacloudsearch.embeddings;

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
import org.elasticsearch.xpack.inference.external.action.alibabacloudsearch.AlibabaCloudSearchActionVisitor;
import org.elasticsearch.xpack.inference.external.request.alibabacloudsearch.AlibabaCloudSearchEmbeddingsRequestEntity;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.alibabacloudsearch.AlibabaCloudSearchModel;
import org.elasticsearch.xpack.inference.services.settings.DefaultSecretSettings;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

public class AlibabaCloudSearchEmbeddingsModel extends AlibabaCloudSearchModel {
    public static AlibabaCloudSearchEmbeddingsModel of(
        AlibabaCloudSearchEmbeddingsModel model,
        Map<String, Object> taskSettings,
        InputType inputType
    ) {
        var requestTaskSettings = AlibabaCloudSearchEmbeddingsTaskSettings.fromMap(taskSettings);
        return new AlibabaCloudSearchEmbeddingsModel(
            model,
            AlibabaCloudSearchEmbeddingsTaskSettings.of(model.getTaskSettings(), requestTaskSettings, inputType)
        );
    }

    public AlibabaCloudSearchEmbeddingsModel(
        String modelId,
        TaskType taskType,
        String service,
        Map<String, Object> serviceSettings,
        Map<String, Object> taskSettings,
        ChunkingSettings chunkingSettings,
        @Nullable Map<String, Object> secrets,
        ConfigurationParseContext context
    ) {
        this(
            modelId,
            taskType,
            service,
            AlibabaCloudSearchEmbeddingsServiceSettings.fromMap(serviceSettings, context),
            AlibabaCloudSearchEmbeddingsTaskSettings.fromMap(taskSettings),
            chunkingSettings,
            DefaultSecretSettings.fromMap(secrets)
        );
    }

    // should only be used for testing
    AlibabaCloudSearchEmbeddingsModel(
        String modelId,
        TaskType taskType,
        String service,
        AlibabaCloudSearchEmbeddingsServiceSettings serviceSettings,
        AlibabaCloudSearchEmbeddingsTaskSettings taskSettings,
        ChunkingSettings chunkingSettings,
        @Nullable DefaultSecretSettings secretSettings
    ) {
        super(
            new ModelConfigurations(modelId, taskType, service, serviceSettings, taskSettings, chunkingSettings),
            new ModelSecrets(secretSettings),
            serviceSettings.getCommonSettings()
        );
    }

    private AlibabaCloudSearchEmbeddingsModel(
        AlibabaCloudSearchEmbeddingsModel model,
        AlibabaCloudSearchEmbeddingsTaskSettings taskSettings
    ) {
        super(model, taskSettings);
    }

    public AlibabaCloudSearchEmbeddingsModel(
        AlibabaCloudSearchEmbeddingsModel model,
        AlibabaCloudSearchEmbeddingsServiceSettings serviceSettings
    ) {
        super(model, serviceSettings);
    }

    @Override
    public AlibabaCloudSearchEmbeddingsServiceSettings getServiceSettings() {
        return (AlibabaCloudSearchEmbeddingsServiceSettings) super.getServiceSettings();
    }

    @Override
    public AlibabaCloudSearchEmbeddingsTaskSettings getTaskSettings() {
        return (AlibabaCloudSearchEmbeddingsTaskSettings) super.getTaskSettings();
    }

    @Override
    public DefaultSecretSettings getSecretSettings() {
        return (DefaultSecretSettings) super.getSecretSettings();
    }

    @Override
    public ExecutableAction accept(AlibabaCloudSearchActionVisitor visitor, Map<String, Object> taskSettings, InputType inputType) {
        return visitor.create(this, taskSettings, inputType);
    }

    public static class Configuration {
        public static Map<String, SettingsConfiguration> get() {
            return configuration.getOrCompute();
        }

        private static final LazyInitializable<Map<String, SettingsConfiguration>, RuntimeException> configuration =
            new LazyInitializable<>(() -> {
                var configurationMap = new HashMap<String, SettingsConfiguration>();

                configurationMap.put(
                    AlibabaCloudSearchEmbeddingsRequestEntity.INPUT_TYPE_FIELD,
                    new SettingsConfiguration.Builder().setDisplay(SettingsConfigurationDisplayType.DROPDOWN)
                        .setLabel("Input Type")
                        .setOrder(1)
                        .setRequired(false)
                        .setSensitive(false)
                        .setTooltip("Specifies the type of input passed to the model.")
                        .setType(SettingsConfigurationFieldType.STRING)
                        .setOptions(
                            Stream.of("ingest", "search")
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
