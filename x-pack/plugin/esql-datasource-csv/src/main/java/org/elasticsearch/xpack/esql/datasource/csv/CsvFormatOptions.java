/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.csv;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.time.format.DateTimeFormatter;

/**
 * Configurable options for CSV/TSV parsing.
 *
 * @param delimiter          field separator character (default: comma)
 * @param quoteChar          character used to quote fields (default: double-quote)
 * @param escapeChar         character used to escape special characters (default: backslash)
 * @param commentPrefix      prefix for comment lines to skip (default: "//")
 * @param nullValue          string representation of null in the data (default: empty string)
 * @param encoding           character encoding of the input (default: UTF-8)
 * @param datetimeFormatter  custom datetime format pattern, or null for ISO-8601/epoch
 * @param maxFieldSize       maximum size in bytes for a single field value; 0 means no limit
 *                           (default: 10MB). Provides OOM protection against malformed files.
 * @param multiValueSyntax   syntax for multi-value fields: BRACKETS (default, [a,b,c]) or NONE
 * @param headerRow          when {@code true} (default), the first non-comment line is read as the
 *                           schema header; when {@code false}, no header is read and column names
 *                           are synthesized from {@link #columnPrefix}.
 * @param columnPrefix       prefix used to synthesize column names when {@link #headerRow} is
 *                           {@code false}. Counters are appended starting at 0 (e.g.
 *                           {@code col0, col1, col2, ...}). Default: {@code "col"}.
 */
public record CsvFormatOptions(
    char delimiter,
    char quoteChar,
    char escapeChar,
    String commentPrefix,
    String nullValue,
    Charset encoding,
    DateTimeFormatter datetimeFormatter,
    int maxFieldSize,
    MultiValueSyntax multiValueSyntax,
    boolean headerRow,
    String columnPrefix
) {

    public enum MultiValueSyntax {
        NONE,
        BRACKETS
    }

    /** 10 MB default field size limit — generous for real-world data, prevents OOM on corrupt files. */
    static final int DEFAULT_MAX_FIELD_SIZE = 10 * 1024 * 1024;

    /** Default prefix for synthesized column names when {@link #headerRow} is {@code false}. */
    public static final String DEFAULT_COLUMN_PREFIX = "col";

    public static final CsvFormatOptions DEFAULT = new CsvFormatOptions(
        ',',
        '"',
        '\\',
        "//",
        "",
        StandardCharsets.UTF_8,
        null,
        DEFAULT_MAX_FIELD_SIZE,
        MultiValueSyntax.BRACKETS,
        true,
        DEFAULT_COLUMN_PREFIX
    );

    public static final CsvFormatOptions TSV = new CsvFormatOptions(
        '\t',
        '"',
        '\\',
        "//",
        "",
        StandardCharsets.UTF_8,
        null,
        DEFAULT_MAX_FIELD_SIZE,
        MultiValueSyntax.BRACKETS,
        true,
        DEFAULT_COLUMN_PREFIX
    );

    public CsvFormatOptions {
        if (delimiter > 127) {
            throw new IllegalArgumentException("delimiter must be an ASCII character, got: " + (int) delimiter);
        }
        if (quoteChar > 127) {
            throw new IllegalArgumentException("quoteChar must be an ASCII character, got: " + (int) quoteChar);
        }
        if (escapeChar > 127) {
            throw new IllegalArgumentException("escapeChar must be an ASCII character, got: " + (int) escapeChar);
        }
        if (maxFieldSize < 0) {
            throw new IllegalArgumentException("maxFieldSize must be non-negative, got: " + maxFieldSize);
        }
        if (multiValueSyntax == null) {
            throw new IllegalArgumentException("multiValueSyntax must not be null");
        }
        if (columnPrefix == null) {
            throw new IllegalArgumentException("columnPrefix must not be null");
        }
    }
}
