/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.cluster.metadata.DatasetFieldMapping;
import org.elasticsearch.cluster.metadata.DatasetMapping;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Bridges a {@link DatasetMapping} (the String-typed, server-side declaration) into the ES|QL currency the
 * external-source resolver speaks: {@link Attribute}s typed with {@link DataType}, plus the logical&rarr;physical
 * rename map. This is the one place the declared {@code type} String becomes a {@code DataType}; everything
 * downstream (planner, reader, output) sees logical names only.
 *
 * <p>Used by the resolver for both supply modes: in strict mode ({@code dynamic: false}) {@link #declaredAttributes}
 * is the entire resolved schema (no file is read); in non-strict mode the declared attributes overlay (and beat) the
 * inferred ones of the same column. The {@link #renameMap} is injected into the read config
 * ({@link ExternalSourceResolver#CONFIG_DECLARED_RENAMES}) and consumed at the reader-facing boundaries via
 * {@link PhysicalNames}; {@code ColumnMapping}/reconciliation stay in logical space. It carries only renamed columns.
 */
public final class DeclaredSchemaResolver {

    private DeclaredSchemaResolver() {}

    /** A declared logical column: the physical (file) column it reads and its resolved type. */
    private record ColSpec(String physical, DataType type) {}

    /**
     * Expand a {@code mappings} block into its declared <b>base</b> logical columns, keyed by logical name in
     * declaration order. A property contributes exactly one column ({@code source} is a <em>move</em>: physical =
     * source, or the logical name when unset), so this is a 1:1 physical↔logical mapping.
     *
     * <p>A {@code copy_to} target is deliberately NOT expanded here: a copy is materialized as an {@code EVAL
     * target = source} projected above the external relation (see {@code Analyzer.ResolveExternalRelations}), reusing
     * the plan's projection/pushdown/cast machinery. Keeping copies out of the read/reconciliation schema is what lets
     * every format get copy for free and leaves the read path untouched.
     */
    private static LinkedHashMap<String, ColSpec> declaredLogicalColumns(DatasetMapping.Mappings mappings) {
        LinkedHashMap<String, ColSpec> cols = new LinkedHashMap<>();
        for (Map.Entry<String, DatasetFieldMapping> e : mappings.properties().entrySet()) {
            String logical = e.getKey();
            DatasetFieldMapping f = e.getValue();
            String physical = f.source() != null ? f.source() : logical;
            cols.put(logical, new ColSpec(physical, resolveType(logical, f.type())));
        }
        return cols;
    }

    /**
     * The declared columns as ES|QL attributes, keyed by <b>logical</b> name and in declaration order (a source column
     * immediately followed by its {@code copy_to} target). Returns an empty list when there is no {@code mappings}
     * block (role-only declarations contribute no columns).
     */
    public static List<Attribute> declaredAttributes(DatasetMapping mapping) {
        DatasetMapping.Mappings mappings = mapping == null ? null : mapping.mappings();
        if (mappings == null) {
            return List.of();
        }
        LinkedHashMap<String, ColSpec> cols = declaredLogicalColumns(mappings);
        List<Attribute> attributes = new ArrayList<>(cols.size());
        for (Map.Entry<String, ColSpec> e : cols.entrySet()) {
            attributes.add(new ReferenceAttribute(Source.EMPTY, null, e.getKey(), e.getValue().type()));
        }
        return attributes;
    }

    /**
     * Logical&rarr;physical name map for every declared logical whose name differs from its physical column — a
     * {@code source} move ({@code L → source}) and a {@code copy_to} target ({@code target → the source's physical}).
     * Empty when nothing renames or copies. Several logicals may share one physical (copy) — the map is still a
     * function (logical keys are distinct); only its inverse fans out. {@link PhysicalNames} uses it at the last-mile
     * reader-facing boundaries; the reader and {@code ColumnMapping} stay rename-agnostic.
     */
    public static Map<String, String> renameMap(DatasetMapping mapping) {
        DatasetMapping.Mappings mappings = mapping == null ? null : mapping.mappings();
        if (mappings == null) {
            return Map.of();
        }
        Map<String, String> renames = new LinkedHashMap<>();
        for (Map.Entry<String, ColSpec> e : declaredLogicalColumns(mappings).entrySet()) {
            if (e.getKey().equals(e.getValue().physical()) == false) {
                renames.put(e.getKey(), e.getValue().physical());
            }
        }
        return renames;
    }

    /**
     * Result of a non-strict overlay: the user-facing {@code output} (declared columns renamed to their logical
     * name and retyped, undeclared columns untouched) and the per-file {@code fileSchema} the reader resolves against
     * the file. Both carry <b>logical</b> names — the operator's projection/column-mapping code stays in logical names
     * and never sees the physical names; a {@code source} rename is applied at the reader-facing boundary via
     * {@link PhysicalNames} (physical names then reach by-name readers; text readers read positionally). The two lists
     * pair position-for-position.
     */
    public record Overlaid(List<Attribute> output, List<Attribute> fileSchema) {}

    /**
     * Apply a non-strict ({@code dynamic: true}) mapping over an inferred schema: every declared column overrides the
     * inferred column of the same physical name — renamed to its logical name and pinned to its declared type — while
     * undeclared inferred columns pass through unchanged. A declared column whose physical name is absent from the
     * inferred schema is an error (it references a column the source does not have).
     */
    public static Overlaid overlayNonStrict(List<Attribute> inferred, DatasetMapping mapping) {
        return overlayNonStrict(inferred, mapping, false);
    }

    /**
     * As {@link #overlayNonStrict(List, DatasetMapping)} but {@code lenient} controls the unmatched-declared-column
     * policy: strict ({@code false}) errors when a declared column is absent from {@code inferred} (used against the
     * unified schema, where every declared column must appear); lenient ({@code true}) skips it (used per-file, where
     * a column may legitimately be absent from one file under union-by-name).
     */
    public static Overlaid overlayNonStrict(List<Attribute> inferred, DatasetMapping mapping, boolean lenient) {
        DatasetMapping.Mappings mappings = mapping == null ? null : mapping.mappings();
        if (mappings == null || mappings.properties().isEmpty()) {
            return new Overlaid(inferred, inferred);
        }
        // Logical-keyed (not physical-keyed): several declared logicals may share one physical (a copy), so keying by
        // physical would last-wins-clobber them. Each declared logical reads a physical column, retyped.
        LinkedHashMap<String, ColSpec> declaredLogicals = declaredLogicalColumns(mappings);
        // physical column -> logicals reading it, in declaration order (a move emits its logical at the physical's
        // position, preserving column order; two logicals sharing one physical are both emitted there — a copy).
        LinkedHashMap<String, List<String>> logicalsByPhysical = new LinkedHashMap<>();
        for (Map.Entry<String, ColSpec> e : declaredLogicals.entrySet()) {
            logicalsByPhysical.computeIfAbsent(e.getValue().physical(), k -> new ArrayList<>()).add(e.getKey());
        }
        Set<String> inferredNames = new HashSet<>(inferred.size());
        for (Attribute a : inferred) {
            inferredNames.add(a.name());
        }
        // Every physical a declared logical reads must exist in the (authoritative unified) inferred schema.
        if (lenient == false) {
            List<String> missing = logicalsByPhysical.keySet().stream().filter(p -> inferredNames.contains(p) == false).sorted().toList();
            if (missing.isEmpty() == false) {
                throw new IllegalArgumentException("declared columns not found in the source: " + missing);
            }
        }
        List<Attribute> output = new ArrayList<>(inferred.size() + declaredLogicals.size());
        Set<String> emitted = new HashSet<>();
        // 1. Walk inferred: retype-in-place if declared under its own name; if the physical is consumed by moves/copies
        // (no logical keeps its name), emit those logicals at its position; otherwise pass the inferred column through.
        for (Attribute a : inferred) {
            ColSpec asIs = declaredLogicals.get(a.name());
            if (asIs != null) {
                output.add(new ReferenceAttribute(Source.EMPTY, null, a.name(), asIs.type()));
                emitted.add(a.name());
            } else if (logicalsByPhysical.containsKey(a.name())) {
                for (String logical : logicalsByPhysical.get(a.name())) {
                    if (emitted.add(logical)) {
                        output.add(new ReferenceAttribute(Source.EMPTY, null, logical, declaredLogicals.get(logical).type()));
                    }
                }
            } else {
                output.add(a);
            }
        }
        // 2. Copy targets whose source column was kept under its own name (not emitted in step 1), in declaration order.
        for (Map.Entry<String, ColSpec> e : declaredLogicals.entrySet()) {
            if (emitted.contains(e.getKey())) {
                continue;
            }
            if (lenient && inferredNames.contains(e.getValue().physical()) == false) {
                continue; // per-file union-by-name: this file lacks the source column, so skip the logical here.
            }
            output.add(new ReferenceAttribute(Source.EMPTY, null, e.getKey(), e.getValue().type()));
            emitted.add(e.getKey());
        }
        // A declared logical whose name collides with a surviving (undeclared) inferred column would produce two output
        // columns with the same name (e.g. declare logical `y` with source `x` when the file also has an undeclared `y`).
        // Reject against the authoritative unified schema (lenient == false); PUT cannot catch this — it needs the file.
        if (lenient == false) {
            Set<String> seen = new HashSet<>(output.size());
            for (Attribute a : output) {
                if (seen.add(a.name()) == false) {
                    throw new IllegalArgumentException(
                        "declared rename produces a duplicate column [" + a.name() + "]: it collides with an inferred column"
                    );
                }
            }
        }
        // Both lists carry LOGICAL names; a source rename / copy is physicalized (with dedup + fan-out) at the reader
        // boundary via PhysicalNames, so the operator and reconciliation never see physical names.
        return new Overlaid(output, output);
    }

    private static DataType resolveType(String column, String type) {
        DataType resolved = DataType.fromNameOrAlias(type);
        // PUT-time DeclaredSchemaValidator already rejects undeclarable types; this is the defensive backstop
        // for a mapping that reached resolution another way (e.g. a hand-edited cluster state).
        if (resolved == DataType.UNSUPPORTED) {
            throw new IllegalArgumentException("declared type [" + type + "] for column [" + column + "] is not a known type");
        }
        return resolved;
    }
}
