/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.formatter;

import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.xpack.esql.action.EsqlQueryResponse;

import java.io.IOException;
import java.io.Writer;
import java.util.Collections;
import java.util.Iterator;
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

    private final EsqlQueryResponse response;
    private final int[] width;
    private final Function<Object, String> FORMATTER = Objects::toString;
    private final boolean includeHeader;
    private final boolean[] dropColumns;

    /**
     * Create a new {@linkplain TextFormatter} for formatting responses
     */
    public TextFormatter(EsqlQueryResponse response, boolean includeHeader, boolean dropNullColumns) {
        this.response = response;
        var columns = response.columns();
        this.includeHeader = includeHeader;
        this.dropColumns = dropNullColumns ? response.nullColumns() : new boolean[columns.size()];
        // Figure out the column widths:
        // 1. Start with the widths of the column names
        width = new int[columns.size()];
        for (int i = 0; i < width.length; i++) {
            // TODO read the width from the data type?
            width[i] = Math.max(MIN_COLUMN_WIDTH, columns.get(i).name().length());
        }

        // 2. Expand columns to fit the largest value
        var iterator = response.values();
        while (iterator.hasNext()) {
            var row = iterator.next();
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
        return Iterators.concat(
            // The header lines
            includeHeader && response.columns().isEmpty() == false ? Iterators.single(this::formatHeader) : Collections.emptyIterator(),
            // Now format the results.
            formatResults()
        );
    }

    private void formatHeader(Writer writer) throws IOException {
        for (int i = 0; i < width.length; i++) {
            if (dropColumns[i]) {
                continue;
            }
            if (i > 0) {
                writer.append('|');
            }

            String name = response.columns().get(i).name();
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

    private Iterator<CheckedConsumer<Writer, IOException>> formatResults() {
        return Iterators.map(response.values(), row -> writer -> {
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
