/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.action;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xpack.sql.proto.ColumnInfo;
import org.elasticsearch.xpack.sql.proto.StringUtils;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;

/**
 * Formats {@link SqlQueryResponse} for the CLI and for the TEXT format. {@linkplain Writeable} so
 * that its state can be saved between pages of results.
 */
public class BasicFormatter implements Writeable {
    /**
     * The minimum width for any column in the formatted results.
     */
    private static final int MIN_COLUMN_WIDTH = 15;

    private int[] width;
    
    public enum FormatOption {
        CLI(Objects::toString),
        TEXT(StringUtils::toString);

        private final Function<Object, String> apply;

        FormatOption(Function<Object, String> apply) {
            this.apply = apply;
        }

        public final String apply(Object l) {
            return apply.apply(l);
        }
    }
    
    private final FormatOption formatOption;

    /**
     * Create a new {@linkplain BasicFormatter} for formatting responses similar
     * to the provided columns and rows.
     */
    public BasicFormatter(List<ColumnInfo> columns, List<List<Object>> rows, FormatOption formatOption) {
        // Figure out the column widths:
        // 1. Start with the widths of the column names
        this.formatOption = formatOption;
        width = new int[columns.size()];
        for (int i = 0; i < width.length; i++) {
            // TODO read the width from the data type?
            width[i] = Math.max(MIN_COLUMN_WIDTH, columns.get(i).name().length());
        }

        // 2. Expand columns to fit the largest value
        for (List<Object> row : rows) {
            for (int i = 0; i < width.length; i++) {
                width[i] = Math.max(width[i], formatOption.apply(row.get(i)).length());
            }
        }
    }

    public BasicFormatter(StreamInput in) throws IOException {
        width = in.readIntArray();
        formatOption = in.readEnum(FormatOption.class);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeIntArray(width);
        out.writeEnum(formatOption);
    }
    
    /**
     * Format the provided {@linkplain SqlQueryResponse} for the set format
     * including the header lines.
     */
    public String formatWithHeader(List<ColumnInfo> columns, List<List<Object>> rows) {
        // The header lines
        StringBuilder sb = new StringBuilder(estimateSize(rows.size() + 2));
        for (int i = 0; i < width.length; i++) {
            if (i > 0) {
                sb.append('|');
            }

            String name = columns.get(i).name();
            // left padding
            int leftPadding = (width[i] - name.length()) / 2;
            for (int j = 0; j < leftPadding; j++) {
                sb.append(' ');
            }
            sb.append(name);
            // right padding
            for (int j = 0; j < width[i] - name.length() - leftPadding; j++) {
                sb.append(' ');
            }
        }
        sb.append('\n');

        for (int i = 0; i < width.length; i++) {
            if (i > 0) {
                sb.append('+');
            }
            for (int j = 0; j < width[i]; j++) {
                sb.append('-'); // emdash creates issues
            }
        }
        sb.append('\n');


        /* Now format the results. Sadly, this means that column
         * widths are entirely determined by the first batch of
         * results. */
        return formatWithoutHeader(sb, rows);
    }

    /**
     * Format the provided {@linkplain SqlQueryResponse} for the set format
     * without the header lines.
     */
    public String formatWithoutHeader(List<List<Object>> rows) {
        return formatWithoutHeader(new StringBuilder(estimateSize(rows.size())), rows);
    }

    private String formatWithoutHeader(StringBuilder sb, List<List<Object>> rows) {
        for (List<Object> row : rows) {
            for (int i = 0; i < width.length; i++) {
                if (i > 0) {
                    sb.append('|');
                }
                String string = formatOption.apply(row.get(i));
                if (string.length() <= width[i]) {
                    // Pad
                    sb.append(string);
                    int padding = width[i] - string.length();
                    for (int p = 0; p < padding; p++) {
                        sb.append(' ');
                    }
                } else {
                    // Trim
                    sb.append(string.substring(0, width[i] - 1));
                    sb.append('~');
                }
            }
            sb.append('\n');
        }
        return sb.toString();
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

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        BasicFormatter that = (BasicFormatter) o;
        return Arrays.equals(width, that.width) && formatOption == that.formatOption;
    }

    @Override
    public int hashCode() {
        return Objects.hash(width, formatOption);
    }
}
