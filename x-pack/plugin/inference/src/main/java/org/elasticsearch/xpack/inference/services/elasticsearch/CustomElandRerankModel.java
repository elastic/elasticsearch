/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.elasticsearch;

import org.elasticsearch.common.util.LazyInitializable;
import org.elasticsearch.inference.SettingsConfiguration;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.inference.configuration.SettingsConfigurationDisplayType;
import org.elasticsearch.inference.configuration.SettingsConfigurationFieldType;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.xpack.inference.services.elasticsearch.CustomElandRerankTaskSettings.RETURN_DOCUMENTS;

public class CustomElandRerankModel extends CustomElandModel {

    public CustomElandRerankModel(
        String inferenceEntityId,
        TaskType taskType,
        String service,
        CustomElandInternalServiceSettings serviceSettings,
        CustomElandRerankTaskSettings taskSettings
    ) {
        super(inferenceEntityId, taskType, service, serviceSettings, taskSettings);
    }

    @Override
    public CustomElandInternalServiceSettings getServiceSettings() {
        return (CustomElandInternalServiceSettings) super.getServiceSettings();
    }

    public static class Configuration {
        public static Map<String, SettingsConfiguration> get() {
            return configuration.getOrCompute();
        }

        private static final LazyInitializable<Map<String, SettingsConfiguration>, RuntimeException> configuration =
            new LazyInitializable<>(() -> {
                var configurationMap = new HashMap<String, SettingsConfiguration>();

                configurationMap.put(
                    RETURN_DOCUMENTS,
                    new SettingsConfiguration.Builder().setDisplay(SettingsConfigurationDisplayType.TOGGLE)
                        .setLabel("Return Documents")
                        .setOrder(1)
                        .setRequired(false)
                        .setSensitive(false)
                        .setTooltip("Returns the document instead of only the index.")
                        .setType(SettingsConfigurationFieldType.BOOLEAN)
                        .setValue(true)
                        .build()
                );

                return Collections.unmodifiableMap(configurationMap);
            });
    }
}
