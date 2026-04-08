/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.spi;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

/**
 * {@link DatasourceValidator} for file-based external sources (S3, GCS, Azure).
 */
public class FileDatasourceValidator implements DatasourceValidator {

    private static final ConfigSetting PARTITION_DETECTION = new ConfigSetting("partition_detection", false);
    private static final ConfigSetting SCHEMA_SAMPLE_SIZE = new ConfigSetting("schema_sample_size", false);
    private static final ConfigSetting ERROR_MODE = new ConfigSetting("error_mode", false);
    private static final Map<String, ConfigSetting> DATASET_SETTINGS = ConfigSetting.mapOf(
        PARTITION_DETECTION,
        SCHEMA_SAMPLE_SIZE,
        ERROR_MODE
    );

    private final String type;
    private final Function<Map<String, Object>, DatasourceConfiguration> configFactory;
    private final Set<String> validSchemes;

    public FileDatasourceValidator(
        String type,
        Function<Map<String, Object>, DatasourceConfiguration> configFactory,
        Set<String> validSchemes
    ) {
        this.type = type;
        this.configFactory = configFactory;
        this.validSchemes = validSchemes;
    }

    @Override
    public String type() {
        return type;
    }

    @Override
    public Map<ConfigSetting, Object> validateDatasource(Map<String, Object> settings) {
        DatasourceConfiguration config = configFactory.apply(settings);
        return config != null ? config.toConfigSettings() : Map.of();
    }

    @Override
    public Map<ConfigSetting, Object> validateDataset(
        Map<String, Object> datasourceSettings,
        String resource,
        Map<String, Object> datasetSettings
    ) {
        if (resource == null || resource.isBlank()) {
            throw new IllegalArgumentException("[resource] is required");
        }
        boolean schemeMatch = false;
        for (String scheme : validSchemes) {
            if (resource.startsWith(scheme)) {
                schemeMatch = true;
                break;
            }
        }
        if (schemeMatch == false) {
            throw new IllegalArgumentException("[resource] must start with one of " + validSchemes + " but was [" + resource + "]");
        }

        if (datasetSettings == null) {
            datasetSettings = Map.of();
        }
        for (String key : datasetSettings.keySet()) {
            if (DATASET_SETTINGS.containsKey(key) == false) {
                throw new IllegalArgumentException("unknown dataset setting [" + key + "]; known settings: " + DATASET_SETTINGS.keySet());
            }
        }

        Map<ConfigSetting, Object> result = new LinkedHashMap<>();
        putIfPresent(datasetSettings, result, PARTITION_DETECTION);
        putIfPresent(datasetSettings, result, ERROR_MODE);

        Object sampleSize = datasetSettings.get(SCHEMA_SAMPLE_SIZE.name());
        if (sampleSize != null) {
            int value;
            try {
                value = Integer.parseInt(sampleSize.toString());
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException("[schema_sample_size] must be a number, got [" + sampleSize + "]");
            }
            if (value < 1) {
                throw new IllegalArgumentException("[schema_sample_size] must be at least 1, got [" + value + "]");
            }
            result.put(SCHEMA_SAMPLE_SIZE, value);
        }

        return result;
    }

    private static void putIfPresent(Map<String, Object> source, Map<ConfigSetting, Object> dest, ConfigSetting setting) {
        Object value = source.get(setting.name());
        if (value != null) {
            dest.put(setting, value);
        }
    }
}
