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
 * {@link DataSourceValidator} for file-based external sources (S3, GCS, Azure).
 */
public class FileDataSourceValidator implements DataSourceValidator {

    // Dataset settings are plain values — no secrets. Credentials are inherited from the parent datasource.
    private static final String PARTITION_DETECTION = "partition_detection";
    private static final String SCHEMA_SAMPLE_SIZE = "schema_sample_size";
    private static final String ERROR_MODE = "error_mode";
    private static final Set<String> DATASET_FIELDS = Set.of(PARTITION_DETECTION, SCHEMA_SAMPLE_SIZE, ERROR_MODE);

    private final String type;
    private final Function<Map<String, Object>, DataSourceConfiguration> configFactory;
    private final Set<String> validSchemes;

    public FileDataSourceValidator(
        String type,
        Function<Map<String, Object>, DataSourceConfiguration> configFactory,
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
    public Map<String, DataSourceStoredSetting> validateDatasource(Map<String, Object> settings) {
        if (settings == null || settings.isEmpty()) {
            return Map.of();
        }
        DataSourceConfiguration config = configFactory.apply(settings);
        return config != null ? config.toStoredSettings() : Map.of();
    }

    @Override
    public Map<String, Object> validateDataset(
        Map<String, DataSourceStoredSetting> datasourceSettings,
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
            if (DATASET_FIELDS.contains(key) == false) {
                throw new IllegalArgumentException("unknown dataset setting [" + key + "]; known settings: " + DATASET_FIELDS);
            }
        }

        Map<String, Object> result = new LinkedHashMap<>();
        copyIfPresent(datasetSettings, result, PARTITION_DETECTION);
        copyIfPresent(datasetSettings, result, ERROR_MODE);

        Object sampleSize = datasetSettings.get(SCHEMA_SAMPLE_SIZE);
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

    private static void copyIfPresent(Map<String, Object> source, Map<String, Object> dest, String key) {
        Object value = source.get(key);
        if (value != null) {
            dest.put(key, value);
        }
    }
}
