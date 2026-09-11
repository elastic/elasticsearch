/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.xpack.esql.datasources.spi.FileList;
import org.elasticsearch.xpack.esql.datasources.spi.SkipWarnings;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;

import java.util.ArrayList;
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
 * @param warnings raw warning bodies collected during this resolve that apply to the source as a whole,
 *                 chiefly Hive-partition shadow columns. Unconditional: they are emitted however the query
 *                 projects. Coordinator-only; {@code EsqlSession} merges them into
 *                 {@code DriverCompletionInfo} so {@code TransportEsqlQueryAction#toResponse} can emit
 *                 them as client {@code Warning} headers. Empty when nothing was collected.
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

    /**
     * Caps and de-duplicates {@code warnings} with one {@link InformationalWarningBudget}, the same
     * mechanism that bounds the scan-side channel, so a wide glob cannot produce undeliverable
     * headers. Shared by {@link #budgetedWarnings()} and the coordinator warm-gate notices so
     * main-source construction of the budget stays in one place.
     */
    public static List<String> budgeted(List<String> warnings) {
        if (warnings == null || warnings.isEmpty()) {
            return List.of();
        }
        InformationalWarningBudget budget = new InformationalWarningBudget(SkipWarnings.MAX_ADDED_WARNINGS);
        List<String> out = new ArrayList<>(Math.min(warnings.size(), SkipWarnings.MAX_ADDED_WARNINGS + 1));
        for (String warning : warnings) {
            String accepted = budget.accept(warning);
            if (accepted != null) {
                out.add(accepted);
            }
        }
        return List.copyOf(out);
    }

    /**
     * The client {@code Warning} bodies for this resolve's unconditional channel (Hive-partition
     * shadows and similar source-wide notices). Capped and de-duplicated by one
     * {@link InformationalWarningBudget}. Column-scoped FIRST_FILE_WINS incompatibility notices
     * are not on this channel: the coordinator warm gate emits those from the aggregates it
     * actually serves, and a scanning relation's reader emits them for projected columns.
     */
    public List<String> budgetedWarnings() {
        return budgeted(warnings);
    }

    public ResolvedSource resolvedSource(String path) {
        return resolved.get(path);
    }

    public boolean isEmpty() {
        return resolved.isEmpty();
    }
}
