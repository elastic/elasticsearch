/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.fixtures;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * Builds a declared-schema {@code mappings} object for a dataset from its canonical CSV header.
 *
 * <p>{@code schema_mode} is the one dimension whose value cannot be a constant in the contract: a
 * declared schema IS the dataset's columns. So it is derived here, from the same canonical CSVs every
 * fixture is generated from -- one source, so a declaration cannot drift from the bytes it describes.
 *
 * <p><b>Not every column can be declared.</b> {@code DeclaredSchemaValidator.DECLARABLE_TYPES} admits
 * ten types; the corpus also uses {@code byte}, {@code short}, {@code float}, {@code half_float},
 * {@code scaled_float} and {@code version}. Measured: only 4 of 10 datasets are fully declarable. That
 * splits the dimension rather than blocking it -- {@code declared_open} declares what it can and lets
 * the reader infer the rest, while {@code declared_closed} demands every column and therefore only
 * applies where every column is declarable.
 *
 * <p>Pure JDK: fixture-common sits on the ORC and Parquet generator classpaths, which isolate Hadoop.
 */
public final class DeclaredSchemas {

    /**
     * The types a dataset mapping may declare, mirroring {@code DeclaredSchemaValidator.DECLARABLE_TYPES}.
     *
     * <p>Duplicated deliberately and narrowly: fixture-common cannot depend on the ESQL plugin, and a
     * declared type the validator rejects fails the registration loudly rather than silently, so drift
     * here surfaces as a test failure naming the type rather than as wrong coverage.
     */
    private static final Set<String> DECLARABLE = Set.of(
        "keyword",
        "text",
        "long",
        "integer",
        "double",
        "boolean",
        "date",
        "datetime",
        "date_nanos",
        "unsigned_long",
        "ip"
    );

    private DeclaredSchemas() {}

    /** Whether every column of this dataset can be declared, which is what closed mode requires. */
    public static boolean fullyDeclarable(List<CsvFixtureParser.ColumnSpec> schema) {
        for (CsvFixtureParser.ColumnSpec column : schema) {
            if (DECLARABLE.contains(column.type()) == false) {
                return false;
            }
        }
        return true;
    }

    /**
     * The {@code mappings} JSON for a dataset, or null when nothing declarable is left to declare.
     *
     * @param dynamic false for closed mode, which requires every column to be declared
     */
    public static String mappingsJson(List<CsvFixtureParser.ColumnSpec> schema, boolean dynamic) {
        List<String> properties = new ArrayList<>();
        for (CsvFixtureParser.ColumnSpec column : schema) {
            if (DECLARABLE.contains(column.type()) == false) {
                if (dynamic == false) {
                    throw new IllegalArgumentException(
                        "column ["
                            + column.name()
                            + "] has undeclarable type ["
                            + column.type()
                            + "], so this dataset cannot be read in closed mode; the crossing must filter it"
                    );
                }
                continue;
            }
            // Declared under its own name and path: a rename would be a different test, and the point
            // here is to declare the schema the data already has.
            properties.add("\"" + column.name() + "\": {\"type\": \"" + declaredType(column.type()) + "\"}");
        }
        if (properties.isEmpty()) {
            return null;
        }
        return "{\"dynamic\": \"" + dynamic + "\", \"properties\": {" + String.join(", ", properties) + "}}";
    }

    /** The canonical header spells datetime as {@code date}; the mapping wants the declarable name. */
    private static String declaredType(String headerType) {
        return "date".equals(headerType) ? "datetime" : headerType;
    }

    /**
     * The declared columns of a dataset, read from its canonical CSV header on the classpath.
     *
     * <p>Only the HEADER is read. The schema is the header, so extracting the whole file to a temporary
     * path was both wasteful and the reason forbiddenApis rejected an earlier version of this class --
     * it wanted java.io.File and an unlocated temp file for data that is one line long.
     *
     * <p>Returns null when the dataset has no canonical CSV, which is not an error: parquet-only
     * datasets exist and simply cannot be declared from one.
     */
    public static List<CsvFixtureParser.ColumnSpec> headerSchema(ClassLoader loader, String dataset) {
        String resource = "data/" + dataset + ".csv";
        try (InputStream in = loader.getResourceAsStream(resource)) {
            if (in == null) {
                return null;
            }
            String header = new String(in.readAllBytes(), StandardCharsets.UTF_8).lines().findFirst().orElse(null);
            if (header == null || header.isBlank()) {
                return null;
            }
            List<CsvFixtureParser.ColumnSpec> columns = new ArrayList<>();
            for (String field : header.split(",")) {
                String trimmed = field.trim();
                int colon = trimmed.lastIndexOf(':');
                // No colon means the header names a column without a type, which the parser reads as
                // keyword; mirroring that here keeps the declaration consistent with what is read.
                columns.add(
                    colon < 0
                        ? new CsvFixtureParser.ColumnSpec(trimmed, "keyword")
                        : new CsvFixtureParser.ColumnSpec(trimmed.substring(0, colon), trimmed.substring(colon + 1))
                );
            }
            return columns;
        } catch (IOException e) {
            throw new UncheckedIOException("could not read [" + resource + "]", e);
        }
    }

}
