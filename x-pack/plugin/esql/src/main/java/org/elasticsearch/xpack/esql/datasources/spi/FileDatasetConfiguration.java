/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.spi;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Shared dataset configuration for file-based external sources (S3, GCS, Azure).
 * Validates and normalizes dataset-level settings common across all file-based storage types.
 */
public record FileDatasetConfiguration(String partitionDetection, Integer schemaSampleSize, String errorMode) {

    public static final Set<String> KNOWN_FIELDS = Set.of("partition_detection", "schema_sample_size", "error_mode");

    /**
     * Validates dataset settings and resource path for a file-based source.
     * Dataset settings have no secrets.
     */
    public static List<ConfigSetting> validate(String resource, Set<String> validSchemes, Map<String, Object> datasetSettings) {
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

        List<ConfigSetting> result = new ArrayList<>();
        addString(datasetSettings, result, "partition_detection");
        addString(datasetSettings, result, "error_mode");

        Object sampleSize = datasetSettings.get("schema_sample_size");
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
            result.add(new ConfigSetting("schema_sample_size", false, String.valueOf(value)));
        }

        return result;
    }

    private static void addString(Map<String, Object> source, List<ConfigSetting> dest, String key) {
        Object value = source.get(key);
        if (value != null) {
            dest.add(new ConfigSetting(key, false, value.toString()));
        }
    }
}
