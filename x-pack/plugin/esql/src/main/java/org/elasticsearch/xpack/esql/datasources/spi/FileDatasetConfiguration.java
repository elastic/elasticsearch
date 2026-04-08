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

/**
 * Shared dataset configuration for file-based external sources (S3, GCS, Azure).
 */
public record FileDatasetConfiguration(String partitionDetection, Integer schemaSampleSize, String errorMode) {

    private static final ConfigSetting PARTITION_DETECTION = new ConfigSetting("partition_detection", false);
    private static final ConfigSetting SCHEMA_SAMPLE_SIZE = new ConfigSetting("schema_sample_size", false);
    private static final ConfigSetting ERROR_MODE = new ConfigSetting("error_mode", false);
    public static final Set<String> KNOWN_FIELDS = Set.of(PARTITION_DETECTION.name(), SCHEMA_SAMPLE_SIZE.name(), ERROR_MODE.name());

    /** Validates dataset settings and resource path for a file-based source. */
    public static Map<ConfigSetting, String> validate(String resource, Set<String> validSchemes, Map<String, Object> datasetSettings) {
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
            if (KNOWN_FIELDS.contains(key) == false) {
                throw new IllegalArgumentException("unknown dataset setting [" + key + "]; known settings: " + KNOWN_FIELDS);
            }
        }

        Map<ConfigSetting, String> result = new LinkedHashMap<>();
        addString(datasetSettings, result, PARTITION_DETECTION);
        addString(datasetSettings, result, ERROR_MODE);

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
            result.put(SCHEMA_SAMPLE_SIZE, String.valueOf(value));
        }

        return result;
    }

    private static void addString(Map<String, Object> source, Map<ConfigSetting, String> dest, ConfigSetting setting) {
        Object value = source.get(setting.name());
        if (value != null) {
            dest.put(setting, value.toString());
        }
    }
}
