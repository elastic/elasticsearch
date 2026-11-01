/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.spi;

/**
 * Bounds user-supplied text before a datasource reader embeds it in an error message.
 * <p>
 * Reader error messages name the offending value so the user can locate it, but a value can be as long as a
 * record (megabytes, in pathological inputs), and the same message travels to the client as a {@code Warning}
 * response header (see {@link SkipWarnings}) or as a fail-fast exception. Neither channel shortens it, so the
 * value must be cut where it enters the message. Cutting the value rather than the assembled message keeps the
 * frame -- row, column, target type -- which is what the user needs to act on.
 */
public final class ErrorExcerpts {

    /** Per-value character cap. Picked to comfortably show typical URL-bearing values
     *  (median ~200 chars in ClickBench-style data) without spilling into KB territory. */
    public static final int MAX_EXCERPT_CHARS = 256;

    private ErrorExcerpts() {}

    /**
     * Returns {@code value} unchanged if it fits within {@link #MAX_EXCERPT_CHARS}, otherwise
     * returns a head/tail summary of the form
     * {@code "<first>… (truncated, N chars total) …<last>"} so both ends of the offending
     * value remain visible. {@code null} maps to the literal string {@code "null"}.
     */
    public static String summarize(String value) {
        if (value == null) {
            return "null";
        }
        if (value.length() <= MAX_EXCERPT_CHARS) {
            return value;
        }
        // Reserve room for the "(truncated, N chars total)" marker; split the rest evenly.
        String marker = "… (truncated, " + value.length() + " chars total) …";
        int remaining = Math.max(0, MAX_EXCERPT_CHARS - marker.length());
        int head = remaining / 2;
        int tail = remaining - head;
        return value.substring(0, head) + marker + value.substring(value.length() - tail);
    }
}
