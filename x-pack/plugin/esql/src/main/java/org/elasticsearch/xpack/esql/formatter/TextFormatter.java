/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.formatter;

import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.xpack.core.esql.action.ColumnInfo;
import org.elasticsearch.xpack.esql.action.EsqlQueryResponse;

import java.io.IOException;
import java.io.Writer;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;

/**
 * Formats {@link EsqlQueryResponse} for the textual representation.
 */
public class TextFormatter {
    /**
     * The minimum width for any column in the formatted results.
     */
    private static final int MIN_COLUMN_WIDTH = 15;

    private final List<? extends ColumnInfo> columns;
    private final Iterator<Iterator<Object>> values;
    private final int[] width;
    private static final Function<Object, String> FORMATTER = Objects::toString;
    private final boolean includeHeader;
    private final boolean[] dropColumns;

    /**
     * Create a new {@linkplain TextFormatter} for formatting responses
     */
    public TextFormatter(EsqlQueryResponse response, boolean includeHeader, boolean dropNullColumns) {
        this(
            response.columns(),
            response.values(),
            widths(response.columns(), response.values()),
            includeHeader,
            dropNullColumns ? response.nullColumns() : new boolean[response.columns().size()]
        );
    }

    /**
     * A formatter over rows sized beforehand: a response whose pages are in a run sizes its columns over a cursor
     * first, and then formats one page's rows at a time.
     *
     * @param values the rows to format; a page's, or the whole response's
     * @param width the columns' widths, from {@link #widths}
     */
    public TextFormatter(
        List<? extends ColumnInfo> columns,
        Iterator<Iterator<Object>> values,
        int[] width,
        boolean includeHeader,
        boolean[] dropColumns
    ) {
        this.columns = columns;
        this.values = values;
        this.width = width;
        this.includeHeader = includeHeader;
        this.dropColumns = dropColumns;
    }

    /**
     * The widths of the columns: their names' or the minimum, widened by {@link #widen} to the largest value.
     */
    public static int[] widths(List<? extends ColumnInfo> columns, Iterator<Iterator<Object>> values) {
        // Figure out the column widths:
        // 1. Start with the widths of the column names
        int[] width = new int[columns.size()];
        for (int i = 0; i < width.length; i++) {
            // TODO read the width from the data type?
            width[i] = Math.max(MIN_COLUMN_WIDTH, columns.get(i).name().length());
        }
        // 2. Expand columns to fit the largest value
        widen(width, values);
        return width;
    }

    /**
     * Expands the columns to fit the largest of {@code values}: once over a resident response, or page by page.
     */
    public static void widen(int[] width, Iterator<Iterator<Object>> values) {
        while (values.hasNext()) {
            var row = values.next();
            for (int i = 0; i < width.length; i++) {
                assert row.hasNext();
                width[i] = Math.max(width[i], FORMATTER.apply(row.next()).length());
            }
            assert row.hasNext() == false;
        }
    }

    /**
     * Format the provided {@linkplain EsqlQueryResponse}
     */
    public Iterator<CheckedConsumer<Writer, IOException>> format() {
        return Iterators.concat(header(), formatResults());
    }

    /**
     * The header lines, if asked for.
     */
    public Iterator<CheckedConsumer<Writer, IOException>> header() {
        return includeHeader && columns.isEmpty() == false ? Iterators.single(this::formatHeader) : Collections.emptyIterator();
    }

    private void formatHeader(Writer writer) throws IOException {
        for (int i = 0; i < width.length; i++) {
            if (dropColumns[i]) {
                continue;
            }
            if (i > 0) {
                writer.append('|');
            }

            String name = columns.get(i).name();
            // left padding
            int leftPadding = (width[i] - name.length()) / 2;
            writePadding(leftPadding, writer);
            writer.append(name);
            // right padding
            writePadding(width[i] - name.length() - leftPadding, writer);
        }
        writer.append('\n');

        for (int i = 0; i < width.length; i++) {
            if (dropColumns[i]) {
                continue;
            }
            if (i > 0) {
                writer.append('+');
            }
            writer.append("-".repeat(Math.max(0, width[i]))); // emdash creates issues
        }
        writer.append('\n');
    }

    /**
     * One chunk per row of the values this formatter was given.
     */
    public Iterator<CheckedConsumer<Writer, IOException>> formatResults() {
        return Iterators.map(values, row -> writer -> {
            for (int i = 0; i < width.length; i++) {
                assert row.hasNext();
                if (dropColumns[i]) {
                    row.next();
                    continue;
                }
                if (i > 0) {
                    writer.append('|');
                }
                String string = FORMATTER.apply(row.next());
                if (string.length() <= width[i]) {
                    // Pad
                    writer.append(string);
                    writePadding(width[i] - string.length(), writer);
                } else {
                    // Trim
                    writer.append(string, 0, width[i] - 1);
                    writer.append('~');
                }
            }
            assert row.hasNext() == false;
            writer.append('\n');
        });
    }

    private static final String PADDING_64 = " ".repeat(64);

    private static void writePadding(int padding, Writer writer) throws IOException {
        while (padding > PADDING_64.length()) {
            writer.append(PADDING_64);
            padding -= PADDING_64.length();
        }
        if (padding > 0) {
            writer.append(PADDING_64, 0, padding);
        }
    }
}
