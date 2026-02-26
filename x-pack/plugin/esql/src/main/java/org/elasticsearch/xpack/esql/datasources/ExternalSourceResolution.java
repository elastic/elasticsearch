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
 * Each resolved source pairs its metadata with a {@link FileSet} describing the files to read.
 */
public record ExternalSourceResolution(Map<String, ResolvedSource> resolved) {

    public static final ExternalSourceResolution EMPTY = new ExternalSourceResolution(Map.of());

    public record ResolvedSource(ExternalSourceMetadata metadata, FileSet fileSet) {}

    public ExternalSourceResolution {
        if (resolved == null) {
            throw new IllegalArgumentException("resolved metadata map must not be null");
        }
    }

    public ResolvedSource resolvedSource(String path) {
        return resolved.get(path);
    }

    public boolean isEmpty() {
        return resolved.isEmpty();
    }
}
