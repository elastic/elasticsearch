/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.xpack.esql.datasources.spi.FileList;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;

import java.util.List;
import java.util.Map;

/**
 * Holds the result of external source resolution (Iceberg/Parquet metadata).
 * This is carried in AnalyzerContext alongside IndexResolution, following the same pattern.
 * Each resolved source pairs its metadata with a {@link FileList} of files to read and a
 * {@code schemaMap} of per-file planner-resolved schemas (one entry per discovered file —
 * single-file gets an identity-mapped one-entry map; multi-file modes get FFW/STRICT/UBN
 * shaped maps from {@link SchemaReconciliation}).
 *
 * @param warnings raw Hive-partition shadow-column warning bodies collected during this resolve.
 *                 Coordinator-only; {@code EsqlSession} merges them into {@code DriverCompletionInfo}
 *                 so {@code TransportEsqlQueryAction#toResponse} can emit them as client {@code Warning}
 *                 headers. Empty when nothing was shadowed.
 */
public record ExternalSourceResolution(Map<String, ResolvedSource> resolved, List<String> warnings) {

    public static final ExternalSourceResolution EMPTY = new ExternalSourceResolution(Map.of());

    /** Compact overload defaulting {@link #warnings} to empty. */
    public ExternalSourceResolution(Map<String, ResolvedSource> resolved) {
        this(resolved, List.of());
    }

    public record ResolvedSource(
        ExternalSourceMetadata metadata,
        FileList fileList,
        Map<StoragePath, SchemaReconciliation.FileSchemaInfo> schemaMap,
        DeclaredReadSpec declaredReadSpec
    ) {
        /** Compact overload defaulting {@link #declaredReadSpec} to {@link DeclaredReadSpec#NONE}. */
        public ResolvedSource(
            ExternalSourceMetadata metadata,
            FileList fileList,
            Map<StoragePath, SchemaReconciliation.FileSchemaInfo> schemaMap
        ) {
            this(metadata, fileList, schemaMap, DeclaredReadSpec.NONE);
        }

        /** Returns a copy carrying the given declared read-instructions. */
        public ResolvedSource withDeclaredReadSpec(DeclaredReadSpec spec) {
            return new ResolvedSource(metadata, fileList, schemaMap, spec);
        }
    }

    public ExternalSourceResolution {
        if (resolved == null) {
            throw new IllegalArgumentException("resolved metadata map must not be null");
        }
        warnings = warnings == null || warnings.isEmpty() ? List.of() : List.copyOf(warnings);
    }

    public ResolvedSource resolvedSource(String path) {
        return resolved.get(path);
    }

    public boolean isEmpty() {
        return resolved.isEmpty();
    }
}
