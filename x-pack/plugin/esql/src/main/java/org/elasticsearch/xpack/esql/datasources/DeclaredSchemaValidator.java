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
import java.util.HashSet;
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
 * a declared {@code path}/type matches the physical file; per-format narrowing (e.g. {@code unsigned_long} is
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
            // Physical-name uniqueness for the read (move) columns: a column's physical name is its `path`, or its
            // logical name. Two columns resolving to one physical break the 1:1 read-path rename, so reject. (A COPY is
            // NOT a shared physical: `copy_to` is materialized as an EVAL above the relation, so it never touches the
            // read path — only the OUTPUT name must be unique, checked below.)
            Set<String> outputNames = new HashSet<>();
            Map<String, String> physicalToLogical = new HashMap<>();
            for (Map.Entry<String, DatasetFieldMapping> e : mappings.properties().entrySet()) {
                validateType(e.getKey(), e.getValue().type());
                outputNames.add(e.getKey()); // property keys are unique by JSON-object semantics
                String logical = e.getKey();
                String physical = e.getValue().path() != null ? e.getValue().path() : logical;
                String clash = physicalToLogical.putIfAbsent(physical, logical);
                if (clash != null) {
                    throw new IllegalArgumentException(
                        "columns [" + clash + "] and [" + logical + "] both resolve to the physical column [" + physical + "]"
                    );
                }
            }
            // Every copy_to target is a NEW output column — it must not collide with a declared column or another target.
            for (Map.Entry<String, DatasetFieldMapping> e : mappings.properties().entrySet()) {
                for (String copyTo : e.getValue().copyTo()) {
                    if (outputNames.add(copyTo) == false) {
                        throw new IllegalArgumentException(
                            "copy_to target [" + copyTo + "] on column [" + e.getKey() + "] collides with another declared column"
                        );
                    }
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
