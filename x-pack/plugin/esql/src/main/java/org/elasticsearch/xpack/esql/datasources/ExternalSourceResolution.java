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
public record ExternalSourceResolution(Map<ExternalRead, ResolvedSource> resolved, List<String> warnings) {

    public static final ExternalSourceResolution EMPTY = new ExternalSourceResolution(Map.of());

    /** Compact overload defaulting {@link #warnings} to empty. */
    public ExternalSourceResolution(Map<ExternalRead, ResolvedSource> resolved) {
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

    /**
     * The resolution for one read, or {@code null} when nothing resolved it. Keyed by the read rather than by its
     * resource: two relations reading one resource under different configurations are two reads, and sharing one
     * listing between them would resolve one of them against the other's settings.
     */
    public ResolvedSource resolvedSource(ExternalRead read) {
        return resolved.get(read);
    }

    /**
     * The resolution of the <b>only</b> read of {@code path}, or {@code null} when nothing resolved it. The read dual
     * of {@code ExternalSourceResolver#resolve(paths, pathConfigs, listener)}: a caller that resolved one path under
     * one configuration gets it back by that path, with no key to rebuild.
     * <p>
     * Never a stand-in for {@link #resolvedSource(ExternalRead)} when resolving a plan. Two relations can read one
     * path under two configurations, and then the path alone does not say which resolution a relation wants — so this
     * throws rather than return the one it happens to reach first, which is the defect {@link ExternalRead} exists to
     * close.
     *
     * No production caller: resolution of a plan goes through {@link #resolvedSource(ExternalRead)}. This is the
     * accessor for the path-keyed {@code resolve} forms, which are themselves test entry points.
     *
     * @throws IllegalStateException if more than one read of {@code path} resolved
     */
    public ResolvedSource soleResolvedSource(String path) {
        ResolvedSource found = null;
        for (Map.Entry<ExternalRead, ResolvedSource> entry : resolved.entrySet()) {
            if (entry.getKey().path().equals(path)) {
                if (found != null) {
                    throw new IllegalStateException(
                        "path [" + path + "] was read more than one way; ask for a ResolvedSource by its ExternalRead"
                    );
                }
                found = entry.getValue();
            }
        }
        return found;
    }

    public boolean isEmpty() {
        return resolved.isEmpty();
    }
}
