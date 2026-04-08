/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.cache;

import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

/**
 * Cache key for schema inference results. Includes mtime-in-key for invalidation.
 * Endpoint and region are included because the same canonical path on different
 * endpoints resolves to different objects.
 */
public record SchemaCacheKey(
    String canonicalPath,
    long lastModifiedEpochMillis,
    String formatType,
    String formatConfig,
    String endpoint,
    String region
) {
    private static final Set<String> FORMAT_AFFECTING_PARAMS = Set.of(
        "delimiter",
        "quote",
        "escape",
        "encoding",
        "datetime_format",
        "hive_partitioning",
        "partition_detection",
        "partition_path",
        "format",
        "null_value",
        "header",
        "skip_rows",
        "trim_whitespace"
    );

    private static final Set<String> CREDENTIAL_PARAMS = Set.of(
        "access_key",
        "secret_key",
        "connection_string",
        "key",
        "sas_token",
        "credentials",
        "token"
    );

    public static SchemaCacheKey build(String canonicalPath, long mtime, String formatType, Map<String, Object> config) {
        String endpoint = config != null ? String.valueOf(config.getOrDefault("endpoint", "")) : "";
        String region = config != null ? String.valueOf(config.getOrDefault("region", "")) : "";
        String formatConfig = buildFormatConfig(config);
        return new SchemaCacheKey(canonicalPath, mtime, formatType != null ? formatType : "", formatConfig, endpoint, region);
    }

    static String buildFormatConfig(Map<String, Object> config) {
        if (config == null || config.isEmpty()) {
            return "";
        }
        TreeMap<String, String> sorted = new TreeMap<>();
        for (Map.Entry<String, Object> entry : config.entrySet()) {
            String key = entry.getKey();
            if (FORMAT_AFFECTING_PARAMS.contains(key) && CREDENTIAL_PARAMS.contains(key) == false) {
                sorted.put(key, String.valueOf(entry.getValue()));
            }
        }
        if (sorted.isEmpty()) {
            return "";
        }
        StringBuilder sb = new StringBuilder();
        for (Map.Entry<String, String> entry : sorted.entrySet()) {
            if (sb.length() > 0) {
                sb.append(',');
            }
            sb.append(entry.getKey()).append('=').append(entry.getValue());
        }
        return sb.toString();
    }
}
