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

    /**
     * {@link #summarize} of {@code value} escaped by {@link RowLocation#escapedForDisplay}, for a message that
     * prints store-supplied text inside a bracketed clause and so needs the escape as well as the cap.
     * <p>
     * The cap bounds the escaped text, so it bounds what the reader sees, but the cut is taken on the unescaped
     * value and only the two kept windows are escaped. Escaping first and cutting second is what this avoids: a
     * value here is a field, which a CSV dialect lets reach {@code max_field_size} -- ten megabytes by default,
     * unbounded when the option is zero -- while the escape spends six characters on each one it rewrites.
     * <p>
     * Cutting first also means a cut can never land inside one of those six-character escapes and leave a stump
     * that spells nothing. The count in the marker is the value's own length, so it names something the reader
     * can find in the object rather than a width this message invented.
     */
    public static String summarizeEscaped(String value) {
        if (value == null) {
            return "null";
        }
        if (value.length() <= MAX_EXCERPT_CHARS) {
            String escaped = RowLocation.escapedForDisplay(value);
            if (escaped.length() <= MAX_EXCERPT_CHARS) {
                return escaped;
            }
        }
        String marker = "… (truncated, " + value.length() + " chars total) …";
        int remaining = Math.max(0, MAX_EXCERPT_CHARS - marker.length());
        int headBudget = remaining / 2;
        int head = charsEscapingWithin(value, headBudget, true);
        int tail = charsEscapingWithin(value, remaining - headBudget, false);
        return RowLocation.escapedForDisplay(value.substring(0, head)) + marker + RowLocation.escapedForDisplay(
            value.substring(value.length() - tail)
        );
    }

    /**
     * How many characters from one end of {@code value} escape to no more than {@code budget} characters. The
     * walk is over the value, so nothing past the two windows is ever escaped, and a window that would end
     * between the halves of a surrogate pair gives the orphan up: a lone surrogate encodes to no UTF-8 sequence,
     * so keeping it spends a position in the cap to show the reader whatever each channel substitutes for it.
     */
    private static int charsEscapingWithin(String value, int budget, boolean fromStart) {
        int taken = 0;
        int spent = 0;
        while (taken < value.length()) {
            char c = fromStart ? value.charAt(taken) : value.charAt(value.length() - 1 - taken);
            int width = RowLocation.escapedWidth(c);
            if (spent + width > budget) {
                break;
            }
            spent += width;
            taken++;
        }
        if (fromStart) {
            if (taken > 0 && Character.isHighSurrogate(value.charAt(taken - 1))) {
                taken--;
            }
        } else if (taken > 0 && Character.isLowSurrogate(value.charAt(value.length() - taken))) {
            taken--;
        }
        return taken;
    }
}
