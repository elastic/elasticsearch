/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.fixtures;

import java.util.List;

/**
 * Re-renders parsed rows into one text dialect.
 *
 * <p>Re-render, not line copy. The hive generator copies source lines because it only redistributes
 * them; a dialect has to re-encode every cell, so the typed values are the input and the bytes are the
 * output.
 *
 * <p>Pure JDK: fixture-common is on the ORC and Parquet generator classpaths, which deliberately isolate
 * their Hadoop jars, so a dependency here would land on both.
 *
 * <p><b>It throws rather than adapts.</b> A value the dialect cannot carry -- a comma inside a PLAIN
 * cell, a bracket list in ESCAPED -- is a generation-time failure. Silently re-quoting it would produce
 * bytes the announced reader parses differently from what was intended, and the damage would be
 * invisible: a corrupted cell in row 60 changes neither the row count nor an aggregate, so probes that
 * read five rows and a SUM stay green over it. Whether a given dataset may be skipped in a dialect is
 * the generator's decision from the declaration; the renderer only ever refuses.
 */
public final class TextRowRenderer {

    /** How cells are made unambiguous, matching the reader's {@code mode} setting. */
    public enum Dialect {
        /** RFC 4180: wrap in quotes when needed, double an internal quote. */
        QUOTED,
        /** No quoting; a backslash escapes the delimiter, quote, backslash, tab and newline. */
        ESCAPED,
        /** Neither. Any value needing either is unrepresentable. */
        PLAIN
    }

    private final char delimiter;
    private final Dialect dialect;
    private final boolean headerRow;

    public TextRowRenderer(char delimiter, Dialect dialect, boolean headerRow) {
        this.delimiter = delimiter;
        this.dialect = dialect;
        this.headerRow = headerRow;
    }

    /**
     * Renders a parsed fixture, or throws naming the cell this dialect cannot carry.
     *
     * @throws IllegalArgumentException when any value is unrepresentable in this dialect
     */
    public String render(CsvFixtureParser.CsvFixtureResult parsed) {
        StringBuilder out = new StringBuilder();
        if (headerRow) {
            for (int i = 0; i < parsed.schema().size(); i++) {
                CsvFixtureParser.ColumnSpec column = parsed.schema().get(i);
                out.append(i == 0 ? "" : delimiter).append(column.name()).append(':').append(column.type());
            }
            out.append('\n');
        }
        for (int row = 0; row < parsed.rows().size(); row++) {
            Object[] values = parsed.rows().get(row);
            for (int column = 0; column < values.length; column++) {
                out.append(column == 0 ? "" : delimiter).append(cell(values[column], row, column));
            }
            out.append('\n');
        }
        return out.toString();
    }

    private String cell(Object value, int row, int column) {
        if (value == null) {
            // \N in ESCAPED, empty elsewhere: an escaped empty field is indistinguishable from an empty
            // string, and the two mean different things to the reader.
            return dialect == Dialect.ESCAPED ? "\\N" : "";
        }
        if (value instanceof List<?> list) {
            if (dialect != Dialect.QUOTED) {
                throw new IllegalArgumentException(
                    "row "
                        + row
                        + " column "
                        + column
                        + " is a multi-value cell, which only QUOTED can carry: the bracket scanner needs quote-aware "
                        + "tokenisation, so the reader refuses brackets in any other mode"
                );
            }
            StringBuilder bracketed = new StringBuilder("[");
            for (int i = 0; i < list.size(); i++) {
                bracketed.append(i == 0 ? "" : ",").append(list.get(i));
            }
            return quote(bracketed.append(']').toString());
        }
        String text = String.valueOf(value);
        return switch (dialect) {
            case QUOTED -> quote(text);
            case ESCAPED -> escape(text);
            case PLAIN -> plain(text, row, column);
        };
    }

    private String quote(String text) {
        if (text.indexOf(delimiter) < 0 && text.indexOf('"') < 0 && text.indexOf('\n') < 0 && text.indexOf('\r') < 0) {
            return text;
        }
        return '"' + text.replace("\"", "\"\"") + '"';
    }

    /**
     * The delimiter is in the escape set on purpose. The reader decodes any {@code \c} to {@code c}, so
     * {@code \,} round-trips -- and leaving it out would misalign every row containing a comma while
     * every other row stayed correct, which is the kind of damage a row-count check cannot see.
     */
    private String escape(String text) {
        StringBuilder out = new StringBuilder(text.length());
        for (int i = 0; i < text.length(); i++) {
            char c = text.charAt(i);
            switch (c) {
                case '\\' -> out.append("\\\\");
                case '\n' -> out.append("\\n");
                case '\r' -> out.append("\\r");
                case '\t' -> out.append("\\t");
                case '"' -> out.append("\\\"");
                default -> {
                    if (c == delimiter) {
                        out.append('\\');
                    }
                    out.append(c);
                }
            }
        }
        return out.toString();
    }

    private String plain(String text, int row, int column) {
        if (text.indexOf(delimiter) >= 0 || text.indexOf('\n') >= 0 || text.indexOf('\r') >= 0 || text.startsWith("\"")) {
            throw new IllegalArgumentException(
                "row "
                    + row
                    + " column "
                    + column
                    + " value ["
                    + text
                    + "] is unrepresentable in PLAIN: it contains the delimiter, a newline, or a leading quote, and PLAIN has "
                    + "neither quoting nor escaping to disambiguate it"
            );
        }
        return text;
    }
}
