/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.csv;

import org.elasticsearch.xpack.esql.datasources.spi.ErrorExcerpts;
import org.elasticsearch.xpack.esql.datasources.spi.RowLocation;

/**
 * Helpers for building short, user-facing CSV error messages.
 *
 * <p>CSV parsing errors must include enough context for the user to locate and fix the
 * offending row in their input file, but rows or values can be megabyte-sized in
 * pathological inputs. These helpers cap excerpts at small, fixed budgets so the
 * resulting HTTP response stays small even when many warnings accumulate. Single values
 * are cut by {@link ErrorExcerpts#summarize}, which the other text readers share; the
 * row- and fault-shaped excerpts here are specific to CSV.
 */
final class CsvErrorMessages {

    /** Maximum total characters of free-form text appended to a single error message
     *  (sum of the row excerpt plus any embedded cause excerpts). */
    static final int MAX_MESSAGE_CHARS = 1024;

    private CsvErrorMessages() {}

    /** Characters of context to keep before the fault offset in {@link #summarizeEscapedAround}. Small enough
     *  that most of the {@link ErrorExcerpts#MAX_EXCERPT_CHARS} budget is spent on the characters <em>at and after</em>
     *  the fault, where the unmatched delimiter actually lives. */
    static final int OFFSET_LOOKBACK_CHARS = 32;

    /**
     * Like {@link ErrorExcerpts#summarize}, but anchored on the character index where the parser detected the fault,
     * and escaped by {@link RowLocation#escapedForDisplay} because the excerpt is printed inside a bracketed clause.
     * A value that fits {@code cap} escaped is returned whole regardless of {@code offset}. A longer one is reduced to
     * a window that starts slightly before {@code offset} (see {@link #OFFSET_LOOKBACK_CHARS}) and is labelled with
     * the offset and the total length, so the user can find the fault in the file: the parser knows where it failed,
     * so the characters at and after that index are kept rather than a head and tail that hide the middle of a long row.
     * <p>
     * The window is chosen with the escape already counted. Escaping a finished window and cutting it again once the
     * escapes had widened it took from both ends, and with enough brackets or tabs before an unmatched quote the cut
     * took the quote -- the one character the window is anchored to keep. {@code cap} is the room the caller's own
     * wording leaves, so a second cap over the whole message never cuts inside an escape.
     * <p>
     * A negative {@code offset} means the fault's position is unknown, and the head of the value is kept with a
     * {@code "(truncated, N chars total)"} suffix: for a parse error a head-only excerpt is the least misleading.
     *
     * @param value  the full row; {@code null} maps to {@code "null"}
     * @param offset zero-based index into {@code value} where the parser flagged the fault; clamped to the value
     */
    static String summarizeEscapedAround(String value, int offset, int cap) {
        if (value == null) {
            return "null";
        }
        if (value.length() <= cap) {
            String escaped = RowLocation.escapedForDisplay(value);
            if (escaped.length() <= cap) {
                return escaped;
            }
        }
        if (offset < 0) {
            String marker = "… (truncated, " + value.length() + " chars total)";
            int budget = Math.max(0, cap - marker.length());
            String window = value.substring(0, Math.min(value.length(), budget));
            int head = ErrorExcerpts.charsEscapingWithin(window, budget, true);
            return RowLocation.escapedForDisplay(value.substring(0, head)) + marker;
        }
        int clampedOffset = Math.min(offset, value.length());
        // Each kept character costs at least one, so no window can hold more characters than its budget: slicing to
        // the budget first bounds the walk without changing what it keeps.
        String lookback = value.substring(Math.max(0, clampedOffset - OFFSET_LOOKBACK_CHARS), clampedOffset);
        int start = clampedOffset - ErrorExcerpts.charsEscapingWithin(lookback, OFFSET_LOOKBACK_CHARS, false);
        String prefix = "(offset " + clampedOffset + " of " + value.length() + " chars) ";
        if (start > 0) {
            prefix = "… " + prefix + "… ";
        }
        String suffix = "…";
        int budget = Math.max(0, cap - prefix.length() - suffix.length());
        String forward = value.substring(start, Math.min(value.length(), start + budget));
        int end = start + ErrorExcerpts.charsEscapingWithin(forward, budget, true);
        if (end >= value.length()) {
            suffix = "";
        }
        return prefix + RowLocation.escapedForDisplay(value.substring(start, end)) + suffix;
    }

    /**
     * Builds a short row excerpt of the form {@code col0=[…], col1=[…], …}. Empty rows
     * (sentinel for rows that could not be tokenised) return the literal {@code "<unparsed>"}.
     * The total excerpt is capped at {@link ErrorExcerpts#MAX_EXCERPT_CHARS} characters.
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
            if (sb.length() >= ErrorExcerpts.MAX_EXCERPT_CHARS) {
                break;
            }
        }
        return ErrorExcerpts.summarize(sb.toString());
    }
}
