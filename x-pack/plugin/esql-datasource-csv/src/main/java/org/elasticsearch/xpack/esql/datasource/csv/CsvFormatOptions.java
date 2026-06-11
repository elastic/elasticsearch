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
import java.util.Locale;

/**
 * Configurable options for CSV/TSV parsing.
 *
 * @param delimiter          field separator character (default: comma)
 * @param quoteChar          character used to quote fields (default: double-quote); only consulted
 *                           when {@link #dialect} is {@link Dialect#QUOTED}
 * @param escapeChar         character used to escape special characters (default: backslash); only
 *                           consulted when {@link #dialect} uses escaping ({@link Dialect#QUOTED}
 *                           inside quoted fields, {@link Dialect#ESCAPED} at value decode)
 * @param commentPrefix      prefix for comment lines to skip (default: "//")
 * @param nullValue          string representation of null in the data (default: empty string)
 * @param encoding           character encoding of the input (default: UTF-8)
 * @param datetimeFormatter  custom datetime format pattern, or null for ISO-8601/epoch
 * @param maxFieldSize       maximum size in bytes for a single field value; 0 means no limit
 *                           (default: 10MB). Provides OOM protection against malformed files.
 * @param multiValueSyntax   syntax for multi-value fields: NONE (default — standard CSV, no array
 *                           parsing) or BRACKETS ([a,b,c] read as a multi-value)
 * @param headerRow          when {@code true} (default), the first non-comment line is read as the
 *                           schema header; when {@code false}, no header is read and column names
 *                           are synthesized from {@link #columnPrefix}.
 * @param columnPrefix       prefix used to synthesize column names when {@link #headerRow} is
 *                           {@code false}. Counters are appended starting at 0 (e.g.
 *                           {@code col0, col1, col2, ...}). Default: {@code "col"}. Ignored when
 *                           {@code headerRow} is {@code true}. An empty prefix yields purely numeric
 *                           names ({@code 0, 1, 2, ...}) which must be backtick-quoted in ES|QL.
 * @param dialect            how a separator-that-is-data is represented (see {@link Dialect}). The
 *                           dialect is authoritative for the <em>mechanism</em>; {@link #quoteChar}
 *                           and {@link #escapeChar} only carry the <em>character</em> and are inert
 *                           under a dialect that does not use them.
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
    String columnPrefix,
    Dialect dialect
) {

    public enum MultiValueSyntax {
        NONE,
        BRACKETS
    }

    /**
     * How a field value that contains the delimiter (or a newline) is represented on disk. There is
     * no single TSV/CSV convention — real producers split three ways, so the dialect is explicit:
     * <ul>
     *   <li>{@link #QUOTED} — fields may be wrapped in {@link #quoteChar}; an embedded quote is
     *       doubled (RFC 4180). Excel, Python, R, spreadsheet CSV. The only dialect where a quote
     *       character is meaningful.</li>
     *   <li>{@link #ESCAPED} — no quoting; specials are escaped with {@link #escapeChar}
     *       ({@code \t}, {@code \n}, {@code \\}) and null is {@code \N}. ClickHouse
     *       {@code TabSeparated}, MySQL {@code LOAD DATA}, PostgreSQL {@code COPY} text.</li>
     *   <li>{@link #PLAIN} — no quoting, no escaping; every byte is literal, so a field cannot
     *       contain the delimiter or a newline. Unix tools, bioinformatics formats, IANA
     *       tab-separated-values.</li>
     * </ul>
     * In {@link #ESCAPED} and {@link #PLAIN} a raw newline is always a record boundary and a raw
     * delimiter always a field boundary, which is what allows the no-quote fast scan.
     */
    public enum Dialect {
        QUOTED,
        ESCAPED,
        PLAIN;

        /** Case-insensitive parse; returns {@code null} for null/empty input (option absent). */
        public static Dialect parse(String value) {
            if (value == null || value.isEmpty()) {
                return null;
            }
            return switch (value.trim().toLowerCase(Locale.ROOT)) {
                case "quoted" -> QUOTED;
                case "escaped" -> ESCAPED;
                case "plain" -> PLAIN;
                default -> throw new IllegalArgumentException(
                    "Invalid dialect [" + value + "]. Accepted values: \"quoted\", \"escaped\", \"plain\""
                );
            };
        }

        /** Whether {@link CsvFormatOptions#quoteChar} is consulted under this dialect. */
        public boolean usesQuote() {
            return this == QUOTED;
        }

        /** Whether {@link CsvFormatOptions#escapeChar} is consulted under this dialect. */
        public boolean usesEscape() {
            return this != PLAIN;
        }
    }

    /** 10 MB default field size limit — generous for real-world data, prevents OOM on corrupt files. */
    static final int DEFAULT_MAX_FIELD_SIZE = 10 * 1024 * 1024;

    /** Default prefix for synthesized column names when {@link #headerRow} is {@code false}. */
    static final String DEFAULT_COLUMN_PREFIX = "col";

    public static final CsvFormatOptions DEFAULT = new CsvFormatOptions(
        ',',
        '"',
        '\\',
        "//",
        "",
        StandardCharsets.UTF_8,
        null,
        DEFAULT_MAX_FIELD_SIZE,
        MultiValueSyntax.NONE,
        true,
        DEFAULT_COLUMN_PREFIX,
        Dialect.QUOTED
    );

    /**
     * The {@code .tsv} baseline is {@link Dialect#PLAIN}: bulk TSV at rest (DB exports, Unix
     * tooling, bioinformatics) does not quote, and a quoting reader glues records on a stray
     * {@code "}. PLAIN never silently corrupts any input; ClickHouse-style files opt into full
     * escape fidelity with {@code "dialect": "escaped"}. CSV keeps QUOTED — that ecosystem quotes.
     */
    public static final CsvFormatOptions TSV = new CsvFormatOptions(
        '\t',
        '"',
        '\\',
        "//",
        "",
        StandardCharsets.UTF_8,
        null,
        DEFAULT_MAX_FIELD_SIZE,
        MultiValueSyntax.NONE,
        true,
        DEFAULT_COLUMN_PREFIX,
        Dialect.PLAIN
    );

    /**
     * Pre-dialect constructor: callers that don't say otherwise get {@link Dialect#QUOTED}, which is
     * the only behavior that existed before the dialect axis.
     */
    public CsvFormatOptions(
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
        this(
            delimiter,
            quoteChar,
            escapeChar,
            commentPrefix,
            nullValue,
            encoding,
            datetimeFormatter,
            maxFieldSize,
            multiValueSyntax,
            headerRow,
            columnPrefix,
            Dialect.QUOTED
        );
    }

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
        if (dialect == null) {
            throw new IllegalArgumentException("dialect must not be null");
        }
        // The dialect's ACTIVE special characters must be pairwise-distinct and none of them a line
        // terminator: each byte in the hot scan has exactly one meaning. Inert characters (e.g. the
        // quote under PLAIN/ESCAPED) are never consulted, so they are not constrained.
        if (delimiter == '\n' || delimiter == '\r') {
            throw new IllegalArgumentException("delimiter must not be a line terminator (\\n / \\r)");
        }
        if (dialect.usesQuote()) {
            if (quoteChar == '\n' || quoteChar == '\r') {
                throw new IllegalArgumentException("quote must not be a line terminator (\\n / \\r)");
            }
            if (quoteChar == delimiter) {
                throw new IllegalArgumentException(
                    "quote [" + printable(quoteChar) + "] must differ from delimiter [" + printable(delimiter) + "]"
                );
            }
        }
        if (dialect.usesEscape()) {
            if (escapeChar == '\n' || escapeChar == '\r') {
                throw new IllegalArgumentException("escape must not be a line terminator (\\n / \\r)");
            }
            if (escapeChar == delimiter) {
                throw new IllegalArgumentException(
                    "escape [" + printable(escapeChar) + "] must differ from delimiter [" + printable(delimiter) + "]"
                );
            }
            if (dialect.usesQuote() && escapeChar == quoteChar) {
                throw new IllegalArgumentException("quote and escape must differ, both are [" + printable(quoteChar) + "]");
            }
        }
    }

    /** Renders control characters readably in error messages ({@code \t}, {@code \n}, {@code \r}). */
    static String printable(char c) {
        return switch (c) {
            case '\t' -> "\\t";
            case '\n' -> "\\n";
            case '\r' -> "\\r";
            default -> String.valueOf(c);
        };
    }
}
