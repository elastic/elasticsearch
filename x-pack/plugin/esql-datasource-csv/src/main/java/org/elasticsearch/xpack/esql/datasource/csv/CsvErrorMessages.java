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

    /** Characters of context to keep before the fault offset in {@link #summarizeAround}. Small enough
     *  that most of the {@link ErrorExcerpts#MAX_EXCERPT_CHARS} budget is spent on the characters <em>at and after</em>
     *  the fault, where the unmatched delimiter actually lives. */
    static final int OFFSET_LOOKBACK_CHARS = 32;

    /**
     * Like {@link ErrorExcerpts#summarize}, but anchored on the character index where the parser detected the
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
     * {@link ErrorExcerpts#summarize} but without the tail, since for parse errors a head-only excerpt is the
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
        if (value.length() <= ErrorExcerpts.MAX_EXCERPT_CHARS) {
            return value;
        }
        if (offset < 0) {
            String marker = "… (truncated, " + value.length() + " chars total)";
            int head = Math.max(0, ErrorExcerpts.MAX_EXCERPT_CHARS - marker.length());
            return value.substring(0, head) + marker;
        }
        int clampedOffset = Math.min(offset, value.length());
        int start = Math.max(0, clampedOffset - OFFSET_LOOKBACK_CHARS);
        // Always emit the offset/total annotation so operators can locate the fault in the source file,
        // even when the window happens to start at index 0. Bracket-style ellipses match the existing
        // marker idiom in ErrorExcerpts.summarize().
        String prefix = "(offset " + clampedOffset + " of " + value.length() + " chars) ";
        if (start > 0) {
            prefix = "… " + prefix + "… ";
        }
        String suffix = "…";
        int budget = Math.max(0, ErrorExcerpts.MAX_EXCERPT_CHARS - prefix.length() - suffix.length());
        int end = Math.min(value.length(), start + budget);
        if (end >= value.length()) {
            suffix = "";
        }
        return prefix + value.substring(start, end) + suffix;
    }

    /**
     * {@link #summarizeAround}, escaped by {@link RowLocation#escapedForDisplay}, for an excerpt printed where a
     * bracket or a newline in it would be read structurally. The window is chosen with the escape already counted:
     * escaping a finished window and cutting it again once the escapes had widened it took from both ends, and with
     * enough brackets or tabs before an unmatched quote the cut took the quote -- the one character the window is
     * anchored to keep. Here the lookback is measured in what the reader sees, and the fault is the first thing the
     * forward walk spends on after it. A value with nothing to escape comes out exactly as {@link #summarizeAround}
     * renders it.
     */
    static String summarizeEscapedAround(String value, int offset) {
        if (value == null) {
            return "null";
        }
        if (value.length() <= ErrorExcerpts.MAX_EXCERPT_CHARS) {
            String escaped = RowLocation.escapedForDisplay(value);
            if (escaped.length() <= ErrorExcerpts.MAX_EXCERPT_CHARS) {
                return escaped;
            }
        }
        if (offset < 0) {
            String marker = "… (truncated, " + value.length() + " chars total)";
            int budget = Math.max(0, ErrorExcerpts.MAX_EXCERPT_CHARS - marker.length());
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
        int budget = Math.max(0, ErrorExcerpts.MAX_EXCERPT_CHARS - prefix.length() - suffix.length());
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
