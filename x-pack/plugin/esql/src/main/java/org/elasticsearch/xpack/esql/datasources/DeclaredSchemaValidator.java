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
 *   <li>a declared {@code timestamp_field} that points at a <i>declared</i> column requires that column to be
 *       a date type;</li>
 *   <li>under strict mode ({@code dynamic: false}) the {@code timestamp_field}/{@code id_field} must point at a
 *       declared column — nothing is inferred to satisfy them.</li>
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

    private static final Set<DataType> DATE_TYPES = Set.of(DataType.DATETIME, DataType.DATE_NANOS);

    public static void validate(DatasetMapping mapping) {
        if (mapping == null) {
            return;
        }
        DatasetMapping.Mappings mappings = mapping.mappings();
        if (mappings != null) {
            for (Map.Entry<String, DatasetFieldMapping> e : mappings.properties().entrySet()) {
                validateType(e.getKey(), e.getValue().type());
            }
            boolean strict = mappings.dynamic() == DatasetMapping.Dynamic.FALSE;
            validateRole("timestamp_field", mapping.timestampField(), mappings, strict, true);
            validateRole("id_field", mapping.idField(), mappings, strict, false);
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

    private static void validateRole(String role, String column, DatasetMapping.Mappings mappings, boolean strict, boolean mustBeDate) {
        if (column == null) {
            return;
        }
        DatasetFieldMapping declared = mappings.properties().get(column);
        if (declared == null) {
            // Not declared: under strict mode there is nothing to infer it from, so it must be declared.
            // Under non-strict mode it may come from inference — defer the existence check to first query.
            if (strict) {
                throw new IllegalArgumentException(
                    "[" + role + "] references column [" + column + "] which is not declared, and dynamic is [false]"
                );
            }
            return;
        }
        if (mustBeDate) {
            DataType resolved = DataType.fromNameOrAlias(declared.type());
            if (DATE_TYPES.contains(resolved) == false) {
                throw new IllegalArgumentException(
                    "[" + role + "] references column [" + column + "] of type [" + declared.type() + "]; it must be a date type"
                );
            }
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
