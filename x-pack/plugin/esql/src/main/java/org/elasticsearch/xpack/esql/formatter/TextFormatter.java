/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.formatter;

import org.elasticsearch.xpack.esql.action.EsqlQueryResponse;

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

    /**
     * Create a new {@linkplain TextFormatter} for formatting responses.
     */
    public TextFormatter(EsqlQueryResponse response) {
        this.response = response;
        var columns = response.columns();
        // Figure out the column widths:
        // 1. Start with the widths of the column names
        width = new int[columns.size()];
        for (int i = 0; i < width.length; i++) {
            // TODO read the width from the data type?
            width[i] = Math.max(MIN_COLUMN_WIDTH, columns.get(i).name().length());
        }

        // 2. Expand columns to fit the largest value
        for (var row : response.values()) {
            for (int i = 0; i < width.length; i++) {
                width[i] = Math.max(width[i], FORMATTER.apply(row.get(i)).length());
            }
        }
    }

    /**
     * Format the provided {@linkplain EsqlQueryResponse} optionally including the header lines.
     */
    public String format(boolean includeHeader) {
        StringBuilder sb = new StringBuilder(estimateSize(response.values().size() + 2));

        // The header lines
        if (includeHeader && response.columns().size() > 0) {
            formatHeader(sb);
        }
        // Now format the results.
        formatResults(sb);

        return sb.toString();
    }

    private void formatHeader(StringBuilder sb) {
        for (int i = 0; i < width.length; i++) {
            if (i > 0) {
                sb.append('|');
            }

            String name = response.columns().get(i).name();
            // left padding
            int leftPadding = (width[i] - name.length()) / 2;
            sb.append(" ".repeat(Math.max(0, leftPadding)));
            sb.append(name);
            // right padding
            sb.append(" ".repeat(Math.max(0, width[i] - name.length() - leftPadding)));
        }
        sb.append('\n');

        for (int i = 0; i < width.length; i++) {
            if (i > 0) {
                sb.append('+');
            }
            sb.append("-".repeat(Math.max(0, width[i]))); // emdash creates issues
        }
        sb.append('\n');
    }

    private void formatResults(StringBuilder sb) {
        for (var row : response.values()) {
            for (int i = 0; i < width.length; i++) {
                if (i > 0) {
                    sb.append('|');
                }
                String string = FORMATTER.apply(row.get(i));
                if (string.length() <= width[i]) {
                    // Pad
                    sb.append(string);
                    sb.append(" ".repeat(Math.max(0, width[i] - string.length())));
                } else {
                    // Trim
                    sb.append(string, 0, width[i] - 1);
                    sb.append('~');
                }
            }
            sb.append('\n');
        }
    }

    /**
     * Pick a good estimate of the buffer size needed to contain the rows.
     */
    int estimateSize(int rows) {
        /* Each column has either a '|' or a '\n' after it
         * so initialize size to number of columns then add
         * up the actual widths of each column. */
        int rowWidthEstimate = width.length;
        for (int w : width) {
            rowWidthEstimate += w;
        }
        return rowWidthEstimate * rows;
    }
}
