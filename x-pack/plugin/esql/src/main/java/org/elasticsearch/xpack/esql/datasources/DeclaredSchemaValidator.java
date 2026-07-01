/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.cluster.metadata.DatasetFieldMapping;
import org.elasticsearch.cluster.metadata.DatasetMapping;
import org.elasticsearch.xpack.esql.core.type.DataType;

import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

/**
 * Shape-only validation of a dataset's {@link DatasetMapping} at PUT time — no file I/O.
 *
 * <p>What is checked here:
 * <ul>
 *   <li>every declared {@code type} resolves to a type the external readers can actually produce
 *       (the {@link #DECLARABLE_TYPES} whitelist — declaring {@code ip}/{@code geo_point}/etc. is rejected
 *       until the readers grow them);</li>
 *   <li>under strict mode ({@code dynamic: false}) the {@code id_field} must point at a declared column —
 *       nothing is inferred to satisfy it.</li>
 * </ul>
 *
 * <p>What is deliberately <b>not</b> checked here (deferred to first-query mapping resolution, because PUT does no
 * I/O and the files may not exist yet): that a role designation pointing at an <i>inferred</i> column exists; that
 * a declared {@code source}/type matches the physical file; per-format narrowing (e.g. {@code unsigned_long} is
 * Parquet-only) — the producing format is authoritative at read time.
 */
public final class DeclaredSchemaValidator {

    private DeclaredSchemaValidator() {}

    /** ES|QL types the external readers (CSV/NDJSON/Parquet/ORC) can currently produce, hence the declarable set. */
    static final Set<DataType> DECLARABLE_TYPES = Set.of(
        DataType.KEYWORD,
        DataType.TEXT,
        DataType.LONG,
        DataType.INTEGER,
        DataType.DOUBLE,
        DataType.BOOLEAN,
        DataType.DATETIME,
        DataType.UNSIGNED_LONG
    );

    public static void validate(DatasetMapping mapping) {
        if (mapping == null) {
            return;
        }
        DatasetMapping.Mappings mappings = mapping.mappings();
        if (mappings != null) {
            // Physical-name uniqueness: a column's physical (file) name is its `source`, or its logical name when no
            // rename. Two columns resolving to the same physical name break the 1:1 rename the whole read path assumes
            // (PhysicalNames.inverse collapses, the non-strict overlay silently drops one, the reader emits it twice),
            // so reject at PUT rather than corrupt results. Resolve-or-reject at create, like the type check.
            Map<String, String> physicalToLogical = new HashMap<>();
            for (Map.Entry<String, DatasetFieldMapping> e : mappings.properties().entrySet()) {
                validateType(e.getKey(), e.getValue().type());
                String logical = e.getKey();
                String physical = e.getValue().source() != null ? e.getValue().source() : logical;
                String clash = physicalToLogical.putIfAbsent(physical, logical);
                if (clash != null) {
                    throw new IllegalArgumentException(
                        "columns [" + clash + "] and [" + logical + "] both resolve to the physical column [" + physical + "]"
                    );
                }
            }
            boolean strict = mappings.dynamic() == DatasetMapping.Dynamic.FALSE;
            validateRole("id_field", mapping.idField(), mappings, strict);
        }
    }

    private static void validateType(String column, String type) {
        DataType resolved = DataType.fromNameOrAlias(type);
        if (resolved == DataType.UNSUPPORTED || DECLARABLE_TYPES.contains(resolved) == false) {
            throw new IllegalArgumentException(
                "unsupported declared type [" + type + "] for column [" + column + "]; supported types are " + supportedTypeNames()
            );
        }
    }

    private static void validateRole(String role, String column, DatasetMapping.Mappings mappings, boolean strict) {
        if (column == null) {
            return;
        }
        DatasetFieldMapping declared = mappings.properties().get(column);
        if (declared == null && strict) {
            // Not declared: under strict mode there is nothing to infer it from, so it must be declared.
            // Under non-strict mode it may come from inference — defer the existence check to first query.
            throw new IllegalArgumentException(
                "[" + role + "] references column [" + column + "] which is not declared, and dynamic is [false]"
            );
        }
    }

    private static List<String> supportedTypeNames() {
        Set<String> names = new TreeSet<>();
        for (DataType t : DECLARABLE_TYPES) {
            names.add(t.typeName().toLowerCase(Locale.ROOT));
        }
        return List.copyOf(names);
    }
}
