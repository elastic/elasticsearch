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
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.BiPredicate;

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
 * @param deferredColumnWarnings warning bodies that are only meaningful if the query actually reads the column
 *                 they are about, keyed by physical column name and then by the file the clash was found in:
 *                 today, FIRST_FILE_WINS columns a footer read returns as null because the anchor type cannot
 *                 represent the file type. Deferred rather than emitted at resolve because the resolve-time fold
 *                 walks EVERY column of EVERY file, while the Parquet/ORC readers only warn about columns the
 *                 query projects -- emitting these eagerly would make a warm {@code COUNT(*)} warn about a column
 *                 the user never asked for, and would spend the shared warning budget on it. {@code EsqlSession}
 *                 resolves them through {@link #warningsMatching} once the optimized plan says which columns each
 *                 relation produces.
 *                 <p>
 *                 The file is part of the key because one resolution covers EVERY path in the query. Column names
 *                 are not unique across sources, so filtering on the name alone would let a source whose {@code x}
 *                 is never read warn just because another source's {@code x} is. Column names are physical (file)
 *                 names, which is safe because a declared column never reaches this map: the fold safe-misses a
 *                 declared coercion instead of rewriting it, so no renamed column is ever warned about.
 */
public record ExternalSourceResolution(
    Map<String, ResolvedSource> resolved,
    List<String> warnings,
    Map<String, Map<String, List<String>>> deferredColumnWarnings
) {

    public static final ExternalSourceResolution EMPTY = new ExternalSourceResolution(Map.of());

    /** Compact overload defaulting {@link #warnings} and {@link #deferredColumnWarnings} to empty. */
    public ExternalSourceResolution(Map<String, ResolvedSource> resolved) {
        this(resolved, List.of(), Map.of());
    }

    /** Compact overload defaulting {@link #deferredColumnWarnings} to empty. */
    public ExternalSourceResolution(Map<String, ResolvedSource> resolved, List<String> warnings) {
        this(resolved, warnings, Map.of());
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
        deferredColumnWarnings = deferredColumnWarnings == null || deferredColumnWarnings.isEmpty()
            ? Map.of()
            : Map.copyOf(deferredColumnWarnings);
    }

    /**
     * The client {@code Warning} bodies for this resolve. Unconditional {@link #warnings} always survive; a
     * {@link #deferredColumnWarnings} entry survives only when {@code columnIsReadFromFile} accepts its
     * {@code (column, fileLocation)} pair, so warm and cold warn about the same columns instead of the fold's
     * schema-wide view.
     * <p>
     * The predicate takes both coordinates rather than just the column name because one resolution spans every
     * external source in the query: the caller answers "does the relation that reads THIS file produce THIS
     * column", which a column name alone cannot express when two sources share a name.
     * <p>
     * The result is capped and de-duplicated by one {@link InformationalWarningBudget}, the same mechanism that
     * bounds the scan-side channel, so a wide glob cannot produce undeliverable headers. Unconditional warnings
     * are offered FIRST so a many-column type clash can never starve them. Columns are visited in name order and
     * files in collection order, so the emitted list is deterministic for a given plan.
     */
    public List<String> warningsMatching(BiPredicate<String, String> columnIsReadFromFile) {
        if (warnings.isEmpty() && deferredColumnWarnings.isEmpty()) {
            return List.of();
        }
        // THE budget for the resolve-time channel. Nothing upstream spends it: the resolver only collects
        // (de-duplicated and bounded), because at collection time it cannot yet know which column notices
        // matter. Unconditional warnings are offered first, which is what guarantees a wide type clash cannot
        // starve a Hive-shadow notice -- no eager reservation required.
        InformationalWarningBudget budget = new InformationalWarningBudget(SkipWarnings.MAX_ADDED_WARNINGS);
        List<String> out = new ArrayList<>(warnings.size());
        for (String warning : warnings) {
            String accepted = budget.accept(warning);
            if (accepted != null) {
                out.add(accepted);
            }
        }
        if (columnIsReadFromFile == null || deferredColumnWarnings.isEmpty()) {
            return List.copyOf(out);
        }
        List<String> columns = new ArrayList<>(deferredColumnWarnings.keySet());
        Collections.sort(columns);
        for (String column : columns) {
            for (Map.Entry<String, List<String>> perFile : deferredColumnWarnings.get(column).entrySet()) {
                if (columnIsReadFromFile.test(column, perFile.getKey()) == false) {
                    continue;
                }
                for (String warning : perFile.getValue()) {
                    String accepted = budget.accept(warning);
                    if (accepted != null) {
                        out.add(accepted);
                    }
                }
            }
        }
        return List.copyOf(out);
    }

    public ResolvedSource resolvedSource(String path) {
        return resolved.get(path);
    }

    public boolean isEmpty() {
        return resolved.isEmpty();
    }
}
