/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.csv;

/**
 * Helpers for building short, user-facing CSV error messages.
 *
 * <p>CSV parsing errors must include enough context for the user to locate and fix the
 * offending row in their input file, but rows or values can be megabyte-sized in
 * pathological inputs. These helpers cap excerpts at small, fixed budgets so the
 * resulting HTTP response stays small even when many warnings accumulate.
 */
final class CsvErrorMessages {

    /** Per-value/row character cap. Picked to comfortably show typical URL-bearing rows
     *  (median ~200 chars in ClickBench-style data) without spilling into KB territory. */
    static final int MAX_EXCERPT_CHARS = 256;

    /** Maximum total characters of free-form text appended to a single error message
     *  (sum of the row excerpt plus any embedded cause excerpts). */
    static final int MAX_MESSAGE_CHARS = 1024;

    private CsvErrorMessages() {}

    /**
     * Returns {@code value} unchanged if it fits within {@link #MAX_EXCERPT_CHARS}, otherwise
     * returns a head/tail summary of the form
     * {@code "<first>… (truncated, N chars total) …<last>"} so both ends of the offending
     * value remain visible. {@code null} maps to the literal string {@code "null"}.
     */
    static String summarize(String value) {
        return summarize(value, MAX_EXCERPT_CHARS);
    }

    /** {@link #summarize(String)} with an explicit cap; visible for tests. */
    static String summarize(String value, int maxChars) {
        if (value == null) {
            return "null";
        }
        if (value.length() <= maxChars) {
            return value;
        }
        // Reserve room for the "(truncated, N chars total)" marker; split the rest evenly.
        String marker = "… (truncated, " + value.length() + " chars total) …";
        int remaining = Math.max(0, maxChars - marker.length());
        int head = remaining / 2;
        int tail = remaining - head;
        return value.substring(0, head) + marker + value.substring(value.length() - tail);
    }

    /**
     * Builds a short row excerpt of the form {@code col0=[…], col1=[…], …}. Empty rows
     * (sentinel for rows that could not be tokenised) return the literal {@code "<unparsed>"}.
     * The total excerpt is capped at {@link #MAX_EXCERPT_CHARS} characters.
     */
    static String summarizeRow(String[] row) {
        if (row == null || row.length == 0) {
            return "<unparsed>";
        }
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < row.length; i++) {
            if (i > 0) {
                sb.append(", ");
            }
            sb.append("col").append(i).append("=[").append(row[i]).append("]");
            if (sb.length() >= MAX_EXCERPT_CHARS) {
                break;
            }
        }
        return summarize(sb.toString());
    }
}
