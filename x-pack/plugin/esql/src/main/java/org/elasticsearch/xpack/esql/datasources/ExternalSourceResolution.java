/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.datasources;

import java.util.Map;

/**
 * Holds the result of external source resolution (Iceberg/Parquet metadata).
 * This is carried in AnalyzerContext alongside IndexResolution, following the same pattern.
 */
public record ExternalSourceResolution(Map<String, ExternalSourceMetadata> resolved) {

    public static final ExternalSourceResolution EMPTY = new ExternalSourceResolution(Map.of());

    public ExternalSourceResolution {
        if (resolved == null) {
            throw new IllegalArgumentException("resolved metadata map must not be null");
        }
    }

    /**
     * Get the metadata for a specific table path.
     *
     * @param path the table path or identifier
     * @return the resolved metadata, or null if not found
     */
    public ExternalSourceMetadata get(String path) {
        return resolved.get(path);
    }

    /**
     * Check if any external sources were resolved.
     *
     * @return true if there are resolved external sources
     */
    public boolean isEmpty() {
        return resolved.isEmpty();
    }
}
