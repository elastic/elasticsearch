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

    /** Characters of context to keep before the fault offset in {@link #summarizeAround}. Small enough
     *  that most of the {@link #MAX_EXCERPT_CHARS} budget is spent on the characters <em>at and after</em>
     *  the fault, where the unmatched delimiter actually lives. */
    static final int OFFSET_LOOKBACK_CHARS = 32;

    /**
     * Like {@link #summarize}, but anchored on the character index where the parser detected the
     * fault. Short values are returned unchanged regardless of {@code offset}. Long values are
     * reduced to a window anchored slightly before {@code offset} (see {@link #OFFSET_LOOKBACK_CHARS})
     * and labelled with the offset and total length so the operator can locate the fault in the
     * source file.
     *
     * <p>This is the right shape for parse-fault excerpts: the parser knows where it failed (the
     * index where {@code inQuotes} or {@code bracketDepth} flipped to a state that was never
     * closed), so we keep the characters <em>at and after</em> that index rather than a symmetric
     * head/tail snapshot that hides the middle of long rows.
     *
     * <p>If {@code offset} is negative (caller does not know where the fault is) the head of the
     * value is returned with a {@code "(truncated, N chars total)"} suffix \u2014 same shape as
     * {@link #summarize} but without the tail, since for parse errors a head-only excerpt is the
     * least-misleading fallback. Offsets that fall outside the multi-line gluing seam (e.g.
     * computed against a {@code logicalLine} that joined continuations with {@code \n}) are
     * relative to the glued logical line, not absolute file positions.
     *
     * @param value  the full row (or other parsed input); {@code null} maps to {@code "null"}.
     * @param offset zero-based char index into {@code value} where the parser flagged the fault;
     *               clamped to {@code [0, value.length()]}; negative means "unknown".
     */
    static String summarizeAround(String value, int offset) {
        if (value == null) {
            return "null";
        }
        if (value.length() <= MAX_EXCERPT_CHARS) {
            return value;
        }
        if (offset < 0) {
            String marker = "… (truncated, " + value.length() + " chars total)";
            int head = Math.max(0, MAX_EXCERPT_CHARS - marker.length());
            return value.substring(0, head) + marker;
        }
        int clampedOffset = Math.min(offset, value.length());
        int start = Math.max(0, clampedOffset - OFFSET_LOOKBACK_CHARS);
        // Always emit the offset/total annotation so operators can locate the fault in the source file,
        // even when the window happens to start at index 0. Bracket-style ellipses match the existing
        // marker idiom in summarize().
        String prefix = "(offset " + clampedOffset + " of " + value.length() + " chars) ";
        if (start > 0) {
            prefix = "… " + prefix + "… ";
        }
        String suffix = "…";
        int budget = Math.max(0, MAX_EXCERPT_CHARS - prefix.length() - suffix.length());
        int end = Math.min(value.length(), start + budget);
        if (end >= value.length()) {
            suffix = "";
        }
        return prefix + value.substring(start, end) + suffix;
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
