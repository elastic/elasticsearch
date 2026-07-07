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
 * <p>
 * Quoting and escaping are two independent knobs. {@link #quoting} decides whether a field may be
 * wrapped in {@link #quoteChar} (RFC&nbsp;4180 doubling); {@link #escaping} decides whether
 * {@link #escapeChar} is consulted — inside quoted fields when {@link #quoting} is also on, or as a
 * C-style value decode ({@code \t}, {@code \n}, {@code \N}) when it is off. The four combinations are
 * all reachable and all meaningful; the user-facing {@code mode} keyword ({@link Mode}) is just a
 * named preset over the pair, and explicit {@code quote}/{@code escape} keys override whatever the
 * preset chose.
 *
 * @param delimiter          field separator character (default: comma)
 * @param quoteChar          character used to quote fields (default: double-quote); consulted only
 *                           when {@link #quoting} is {@code true}
 * @param escapeChar         character used to escape special characters (default: backslash);
 *                           consulted only when {@link #escaping} is {@code true} — inside quoted
 *                           fields when {@link #quoting} is also on, otherwise at value decode
 * @param commentPrefix      prefix for comment lines to skip (default: "//")
 * @param nullValue          token whose exact match reads as null (default: empty string, which installs
 *                           no null token, so an empty field is a present empty value rather than null)
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
 * @param quoting            whether fields may be quoted with {@link #quoteChar}. When {@code false}
 *                           a raw newline is always a record boundary and a raw delimiter always a
 *                           field boundary, which is what allows the no-quote fast scan.
 * @param escaping           whether {@link #escapeChar} is consulted (see {@link #escapeChar}).
 * @param trimSpaces         whether surrounding ASCII whitespace is trimmed from field values
 *                           (default: {@code false}). Applies to string content only — typed
 *                           (non-string) parses always tolerate padding (they trim before parsing,
 *                           mirroring schema inference). Jackson only ever
 *                           trimmed unquoted values; with this off an unquoted padded value is preserved,
 *                           matching RFC 4180 and the byte-fidelity posture of {@link Mode#PLAIN}. Known
 *                           limitation: {@code mode: escaped} (the one dialect that stays on Jackson under
 *                           no-trim) still loses the FIRST column's leading whitespace (Jackson skips it at
 *                           record start regardless of the feature); every other column and all trailing
 *                           whitespace is preserved. PLAIN/QUOTED no-trim reads — including when the
 *                           direct-block path is disabled — use the house grammar, which preserves it.
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
    boolean quoting,
    boolean escaping,
    boolean trimSpaces
) {

    public enum MultiValueSyntax {
        NONE,
        BRACKETS
    }

    /**
     * Named presets over the {@code (quoting, escaping)} pair. There is no single TSV/CSV convention —
     * real producers split three ways — so the {@code mode} keyword names the common ones:
     * <ul>
     *   <li>{@link #QUOTED} — {@code (quoting, escaping) = (true, true)}. Fields may be wrapped in
     *       {@link CsvFormatOptions#quoteChar}; an embedded quote is doubled (RFC 4180) and a
     *       backslash escapes inside a quoted field. The RFC 4180 spreadsheet/dataframe ecosystem.</li>
     *   <li>{@link #ESCAPED} — {@code (false, true)}. No quoting; specials are escaped with
     *       {@link CsvFormatOptions#escapeChar} ({@code \t}, {@code \n}, {@code \\}) and null is
     *       {@code \N}. Common database text exports (tab-separated, backslash-escaped).</li>
     *   <li>{@link #PLAIN} — {@code (false, false)}. No quoting, no escaping; every byte is literal,
     *       so a field cannot contain the delimiter or a newline. Unix tooling and scientific text
     *       formats; the IANA tab-separated-values registration.</li>
     * </ul>
     * The fourth combination {@code (true, false)} — pure RFC 4180 quoting with no backslash escape —
     * has no preset name but is reachable via {@code mode: quoted, escape: none}.
     */
    public enum Mode {
        QUOTED,
        ESCAPED,
        PLAIN;

        /** Case-insensitive parse; returns {@code null} for null/empty input (option absent). */
        public static Mode parse(String value) {
            if (value == null || value.isEmpty()) {
                return null;
            }
            return switch (value.trim().toLowerCase(Locale.ROOT)) {
                case "quoted" -> QUOTED;
                case "escaped" -> ESCAPED;
                case "plain" -> PLAIN;
                default -> throw new IllegalArgumentException(
                    "Invalid mode [" + value + "]. Accepted values: \"quoted\", \"escaped\", \"plain\""
                );
            };
        }

        /** Whether this preset turns quoting on. */
        public boolean usesQuote() {
            return this == QUOTED;
        }

        /** Whether this preset consults the escape character. */
        public boolean usesEscape() {
            return this != PLAIN;
        }
    }

    /** 10 MB default field size limit — generous for real-world data, prevents OOM on corrupt files. */
    static final int DEFAULT_MAX_FIELD_SIZE = 10 * 1024 * 1024;

    /** Default prefix for synthesized column names when {@link #headerRow} is {@code false}. */
    static final String DEFAULT_COLUMN_PREFIX = "col";

    /** Canonical quote / escape characters a preset installs before any explicit override. */
    static final char DEFAULT_QUOTE = '"';
    static final char DEFAULT_ESCAPE = '\\';

    public static final CsvFormatOptions DEFAULT = new CsvFormatOptions(
        ',',
        DEFAULT_QUOTE,
        DEFAULT_ESCAPE,
        "//",
        "",
        StandardCharsets.UTF_8,
        null,
        DEFAULT_MAX_FIELD_SIZE,
        MultiValueSyntax.NONE,
        true,
        DEFAULT_COLUMN_PREFIX,
        true,
        true,
        false // trimSpaces (default; see the record javadoc)
    );

    /**
     * The {@code .tsv} baseline is {@link Mode#PLAIN} — {@code (quoting, escaping) = (false, false)}:
     * bulk TSV at rest (DB exports, Unix tooling, scientific data) does not quote, and a quoting reader
     * glues records on a stray {@code "}. PLAIN never silently corrupts any input; backslash-escaped
     * exports opt into full escape fidelity with {@code "mode": "escaped"}. CSV keeps QUOTED — that
     * ecosystem quotes.
     */
    public static final CsvFormatOptions TSV = new CsvFormatOptions(
        '\t',
        DEFAULT_QUOTE,
        DEFAULT_ESCAPE,
        "//",
        "",
        StandardCharsets.UTF_8,
        null,
        DEFAULT_MAX_FIELD_SIZE,
        MultiValueSyntax.NONE,
        true,
        DEFAULT_COLUMN_PREFIX,
        false,
        false,
        false // trimSpaces (default; see the record javadoc)
    );

    /**
     * Pre-mode constructor: callers that don't say otherwise get fully-quoted parsing
     * ({@code (quoting, escaping) = (true, true)}, i.e. {@link Mode#QUOTED}), which is the only
     * behavior that existed before the quoting/escaping axes were split out.
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
            true,
            true,
            false // trimSpaces (default; see the record javadoc)
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
        // The ACTIVE special characters must be pairwise-distinct and none of them a line terminator:
        // each byte in the hot scan has exactly one meaning. Inactive characters (the quote when
        // quoting is off, the escape when escaping is off) are never consulted, so they are not
        // constrained — this is the only validation that survives; the mode/override coherence rules
        // are gone, because explicit quote/escape always win (silent errors are the user's to own).
        if (delimiter == '\n' || delimiter == '\r') {
            throw new IllegalArgumentException("delimiter must not be a line terminator (\\n / \\r)");
        }
        if (quoting) {
            if (quoteChar == '\n' || quoteChar == '\r') {
                throw new IllegalArgumentException("quote must not be a line terminator (\\n / \\r)");
            }
            if (quoteChar == delimiter) {
                throw new IllegalArgumentException(
                    "quote [" + printable(quoteChar) + "] must differ from delimiter [" + printable(delimiter) + "]"
                );
            }
        }
        if (escaping) {
            if (escapeChar == '\n' || escapeChar == '\r') {
                throw new IllegalArgumentException("escape must not be a line terminator (\\n / \\r)");
            }
            if (escapeChar == delimiter) {
                throw new IllegalArgumentException(
                    "escape [" + printable(escapeChar) + "] must differ from delimiter [" + printable(delimiter) + "]"
                );
            }
            if (quoting && escapeChar == quoteChar) {
                throw new IllegalArgumentException("quote and escape must differ, both are [" + printable(quoteChar) + "]");
            }
        }
    }

    /** Whether field values are run through the C-style escape decoder ({@code escaped} mode). */
    public boolean decodesEscapes() {
        return escaping && quoting == false;
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
