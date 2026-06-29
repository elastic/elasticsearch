/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.cluster.metadata.DatasetFieldMapping;
import org.elasticsearch.cluster.metadata.DatasetSchema;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Bridges a {@link DatasetSchema} (the String-typed, server-side declaration) into the ES|QL currency the
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
    public static List<Attribute> declaredAttributes(DatasetSchema schema) {
        DatasetSchema.Mappings mappings = schema == null ? null : schema.mappings();
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
    public static Map<String, String> renameMap(DatasetSchema schema) {
        DatasetSchema.Mappings mappings = schema == null ? null : schema.mappings();
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

    private static DataType resolveType(String column, String type) {
        DataType resolved = DataType.fromNameOrAlias(type);
        // PUT-time DeclaredSchemaValidator already rejects undeclarable types; this is the defensive backstop
        // for a schema that reached resolution another way (e.g. a hand-edited cluster state).
        if (resolved == DataType.UNSUPPORTED) {
            throw new IllegalArgumentException("declared type [" + type + "] for column [" + column + "] is not a known type");
        }
        return resolved;
    }
}
