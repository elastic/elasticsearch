/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.cluster.metadata.DatasetFieldMapping;
import org.elasticsearch.cluster.metadata.DatasetMapping;
import org.elasticsearch.common.time.DateFormatter;
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
 *       (the {@link #DECLARABLE_TYPES} whitelist — declaring {@code geo_point}/{@code version}/etc. is rejected
 *       until the readers grow them);</li>
 *   <li>under strict mode ({@code dynamic: false}) the {@code _id.path} column must be declared —
 *       nothing is inferred to satisfy it.</li>
 * </ul>
 *
 * <p>What is deliberately <b>not</b> checked here (deferred to first-query mapping resolution, because PUT does no
 * I/O and the files may not exist yet): that the {@code _id.path} column exists when it is inferred rather than declared; that
 * a declared {@code path}/type matches the physical file; per-format narrowing, should a format ever be unable to
 * read a declarable type — the producing format is authoritative at read time.
 */
public final class DeclaredSchemaValidator {

    private DeclaredSchemaValidator() {}

    /**
     * ES|QL types the external readers (CSV/NDJSON/Parquet/ORC) can currently produce, hence the declarable set.
     * {@code ip} is produced by parsing a string column (the mapper-style coercion in
     * {@code DeclaredTypeCoercions}: CSV parses it directly, Parquet/ORC coerce a physical string column);
     * per-format narrowing stays deferred to read time like the rest of the set (see the class Javadoc).
     */
    static final Set<DataType> DECLARABLE_TYPES = Set.of(
        DataType.KEYWORD,
        DataType.TEXT,
        DataType.LONG,
        DataType.INTEGER,
        DataType.DOUBLE,
        DataType.BOOLEAN,
        DataType.DATETIME,
        DataType.UNSIGNED_LONG,
        DataType.IP
    );

    /**
     * The types a user may declare on a dataset mapping. Exposed so a format reader's tests can pin that the reader
     * actually builds every declarable type with the shape {@link
     * org.elasticsearch.xpack.esql.datasources.spi.DeclaredTypeCoercions#elementTypeFor} prescribes — the drift
     * that let a declared {@code unsigned_long} pass validation and then fail at read.
     */
    public static Set<DataType> declarableTypes() {
        return DECLARABLE_TYPES;
    }

    public static void validate(DatasetMapping mapping) {
        if (mapping == null) {
            return;
        }
        DatasetMapping.Mappings mappings = mapping.mappings();
        if (mappings != null) {
            // Strict mode means "the declaration IS the schema" — with no declared columns there is no schema, and the
            // zero-column relation the resolver would build is not a queryable thing. Reject rather than let it fail
            // downstream. (An _id.path-only strict block is not a legitimate shape either: strict already requires the
            // id column to be declared.)
            if (mappings.dynamic() == DatasetMapping.Dynamic.FALSE && mappings.properties().isEmpty()) {
                throw new IllegalArgumentException("[dynamic: false] requires at least one declared column under [properties]");
            }
            // Physical-name uniqueness for the read (move) columns: a column's physical name is its `path`, or its
            // logical name. Two columns resolving to one physical break the 1:1 read-path rename, so reject.
            Map<String, String> physicalToLogical = new HashMap<>();
            for (Map.Entry<String, DatasetFieldMapping> e : mappings.properties().entrySet()) {
                requireNonBlank(e.getKey(), "column name");
                requireNonBlank(e.getValue().path(), "path of column [" + e.getKey() + "]");
                validateType(e.getKey(), e.getValue().type());
                validateFormat(e.getKey(), e.getValue().type(), e.getValue().format());
                String logical = e.getKey();
                String physical = e.getValue().path() != null ? e.getValue().path() : logical;
                String clash = physicalToLogical.putIfAbsent(physical, logical);
                if (clash != null) {
                    throw new IllegalArgumentException(
                        "columns [" + clash + "] and [" + logical + "] both resolve to the physical column [" + physical + "]"
                    );
                }
            }
            requireNonBlank(mappings.idPath(), "[_id] path");
            boolean strict = mappings.dynamic() == DatasetMapping.Dynamic.FALSE;
            validateIdPath(mappings, strict);
        }
    }

    /** Rejects blank names (the index-mapping precedent: field names must be non-empty). {@code null} means "unset" and passes. */
    private static void requireNonBlank(String value, String what) {
        if (value != null && value.isBlank()) {
            throw new IllegalArgumentException(what + " must not be empty");
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

    /**
     * A declared {@code format} is a date-parse pattern, so it is only accepted on a column whose type resolves to
     * {@code datetime} ({@code date_nanos} is not a declarable type). The pattern is validated here with the ES
     * {@link DateFormatter#forPattern} — the same class the text readers use to parse a <b>declared</b> date column
     * (CSV/TSV via a per-column {@code DateFormatter}, NDJSON likewise), so a bad pattern fails the PUT rather than the
     * first query and a PUT-accepted pattern is read-parseable. (The readers' <em>file-level</em> {@code datetime_format}
     * on CSV uses a JDK formatter; that is a separate, unaffected path.)
     */
    private static void validateFormat(String column, String type, String format) {
        if (format == null) {
            return;
        }
        if (DataType.fromNameOrAlias(type) != DataType.DATETIME) {
            throw new IllegalArgumentException("[format] on column [" + column + "] is only supported on [date] columns");
        }
        try {
            DateFormatter.forPattern(format);
        } catch (Exception e) {
            throw new IllegalArgumentException("invalid [format] [" + format + "] on column [" + column + "]", e);
        }
    }

    private static void validateIdPath(DatasetMapping.Mappings mappings, boolean strict) {
        String column = mappings.idPath();
        if (column == null) {
            return;
        }
        DatasetFieldMapping declared = mappings.properties().get(column);
        if (declared == null && strict) {
            // Not declared: under strict mode there is nothing to infer it from, so it must be declared.
            // Under non-strict mode it may come from inference — defer the existence check to first query.
            throw new IllegalArgumentException("[_id] references column [" + column + "] which is not declared, and dynamic is [false]");
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
