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
 * inferred ones of the same column. The {@link #renameMap} is consumed where physical columns are located in the
 * file (the per-file {@code ColumnMapping}); it carries only the columns that actually rename.
 */
public final class DeclaredSchemaResolver {

    private DeclaredSchemaResolver() {}

    /**
     * The declared columns as ES|QL attributes, keyed by <b>logical</b> name and in declaration order. Returns an
     * empty list when there is no {@code mappings} block (role-only declarations contribute no columns).
     */
    public static List<Attribute> declaredAttributes(DatasetMapping mapping) {
        DatasetMapping.Mappings mappings = mapping == null ? null : mapping.mappings();
        if (mappings == null) {
            return List.of();
        }
        List<Attribute> attributes = new ArrayList<>(mappings.properties().size());
        for (Map.Entry<String, DatasetFieldMapping> e : mappings.properties().entrySet()) {
            attributes.add(new ReferenceAttribute(Source.EMPTY, null, e.getKey(), resolveType(e.getKey(), e.getValue().type())));
        }
        return attributes;
    }

    /**
     * Logical&rarr;physical name map for the columns that declare a {@code source} rename. Empty when nothing renames.
     * The reader/{@code ColumnMapping} uses this to find a renamed column's physical column in the file; nothing above
     * the reader ever sees the physical name.
     */
    public static Map<String, String> renameMap(DatasetMapping mapping) {
        DatasetMapping.Mappings mappings = mapping == null ? null : mapping.mappings();
        if (mappings == null) {
            return Map.of();
        }
        Map<String, String> renames = new LinkedHashMap<>();
        for (Map.Entry<String, DatasetFieldMapping> e : mappings.properties().entrySet()) {
            if (e.getValue().source() != null) {
                renames.put(e.getKey(), e.getValue().source());
            }
        }
        return renames;
    }

    /**
     * Result of a non-strict overlay: the user-facing {@code output} (declared columns renamed to their logical
     * name and retyped, undeclared columns untouched) and the per-file {@code fileSchema} the reader resolves against
     * the file. Both carry <b>logical</b> names — the operator's projection/column-mapping code stays in logical names
     * and never sees the physical names; a {@code source} rename is applied only at the by-name reader via the rename
     * map ({@link #renameMap}), while text readers read positionally. The two lists pair position-for-position.
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
        // physical (file) name -> declared {logical name, type}
        Map<String, DataType> declaredTypeByPhysical = new LinkedHashMap<>();
        Map<String, String> logicalByPhysical = new LinkedHashMap<>();
        for (Map.Entry<String, DatasetFieldMapping> e : mappings.properties().entrySet()) {
            String logical = e.getKey();
            String physical = e.getValue().source() != null ? e.getValue().source() : logical;
            declaredTypeByPhysical.put(physical, resolveType(logical, e.getValue().type()));
            logicalByPhysical.put(physical, logical);
        }
        List<Attribute> output = new ArrayList<>(inferred.size());
        List<Attribute> fileSchema = new ArrayList<>(inferred.size());
        Set<String> matched = new HashSet<>();
        for (Attribute a : inferred) {
            DataType declaredType = declaredTypeByPhysical.get(a.name());
            if (declaredType != null) {
                matched.add(a.name());
                // Both output and file schema carry the LOGICAL name (retyped). The reader resolves the physical
                // column via the rename map (by-name readers) or by position (text), so the operator never sees physical.
                ReferenceAttribute logical = new ReferenceAttribute(Source.EMPTY, null, logicalByPhysical.get(a.name()), declaredType);
                output.add(logical);
                fileSchema.add(logical);
            } else {
                output.add(a);
                fileSchema.add(a);
            }
        }
        if (lenient == false && matched.size() < declaredTypeByPhysical.size()) {
            List<String> missing = declaredTypeByPhysical.keySet().stream().filter(p -> matched.contains(p) == false).toList();
            throw new IllegalArgumentException("declared columns not found in the source: " + missing);
        }
        // A declared rename whose logical name collides with a surviving (undeclared) inferred column would produce two
        // output columns with the same name (e.g. declare logical `y` with source `x` when the file also has `y`).
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
        return new Overlaid(output, fileSchema);
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
