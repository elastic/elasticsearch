/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/**
 * Closed vocabularies for configuration-inventory telemetry other than storage type.
 * Type tokens live on {@link org.elasticsearch.xpack.esql.datasources.spi.DataSourceTelemetryVocabulary.Type}.
 * Every other token that reaches a phone-home key segment or an APM attribute is one of these nouns.
 */
public final class DataSourceInventoryVocabulary {

    public static final List<String> AUTH_MODES = List.of(
        "anonymous",
        "static_credentials",
        "federated_identity",
        "managed_identity",
        "unknown"
    );
    public static final List<String> FORMATS = List.of("parquet", "csv", "tsv", "ndjson", "orc", "other", "unresolved");
    public static final List<String> SCHEMAS = List.of("inferred", "declared_dynamic", "declared_strict");
    public static final List<String> PARTITIONING = List.of("auto", "hive", "template", "none");
    public static final List<String> COMPRESSIONS = List.of(
        "gzip",
        "snappy",
        "zstd",
        "brotli",
        "lz4",
        "bzip2",
        "uncompressed",
        "other",
        "unknown"
    );

    private static final Set<String> AUTH_SET = Set.copyOf(AUTH_MODES);
    private static final Set<String> FORMAT_SET = Set.copyOf(FORMATS);
    private static final Set<String> SCHEMA_SET = Set.copyOf(SCHEMAS);
    private static final Set<String> PARTITIONING_SET = Set.copyOf(PARTITIONING);
    private static final Set<String> COMPRESSION_SET = Set.copyOf(COMPRESSIONS);

    /** Extension (with leading dot) to shipped codec name. */
    public static final Map<String, String> COMPRESSION_BY_EXTENSION = Map.ofEntries(
        Map.entry(".gz", "gzip"),
        Map.entry(".gzip", "gzip"),
        Map.entry(".snappy", "snappy"),
        Map.entry(".zst", "zstd"),
        Map.entry(".zstd", "zstd"),
        Map.entry(".br", "brotli"),
        Map.entry(".lz4", "lz4"),
        Map.entry(".bz2", "bzip2"),
        Map.entry(".bz", "bzip2")
    );

    private DataSourceInventoryVocabulary() {}

    public static String authToken(String auth) {
        return closed(auth, AUTH_SET, "unknown");
    }

    public static String formatToken(String format) {
        if (format == null || format.isBlank() || "auto".equalsIgnoreCase(format.trim())) {
            return "unresolved";
        }
        String lower = format.trim().toLowerCase(Locale.ROOT);
        if (FORMAT_SET.contains(lower)) {
            return lower;
        }
        return "other";
    }

    public static String schemaToken(String schema) {
        return closed(schema, SCHEMA_SET, "inferred");
    }

    public static String partitioningToken(String partitioning) {
        return closed(partitioning, PARTITIONING_SET, "auto");
    }

    public static String compressionToken(String compression) {
        return closed(compression, COMPRESSION_SET, "unknown");
    }

    public static String compressionFromExtension(String extension) {
        if (extension == null || extension.isBlank()) {
            return "uncompressed";
        }
        String normalized = extension.toLowerCase(Locale.ROOT);
        if (normalized.startsWith(".") == false) {
            normalized = "." + normalized;
        }
        String name = COMPRESSION_BY_EXTENSION.get(normalized);
        return name != null ? name : "other";
    }

    private static String closed(String raw, Set<String> allowed, String fallback) {
        if (raw == null || raw.isBlank()) {
            return fallback;
        }
        String lower = raw.trim().toLowerCase(Locale.ROOT);
        return allowed.contains(lower) ? lower : fallback;
    }
}
