/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action.suggestions;

import org.elasticsearch.xpack.esql.core.tree.Location;
import org.elasticsearch.xpack.esql.core.tree.Source;

import java.util.ArrayList;
import java.util.List;

/**
 * Maps between absolute character offsets into a query string and the {@code (line, column)}
 * coordinates that ESQL's {@link Source}/{@link Location} carry.
 *
 * <p>{@link Source} does not store an absolute offset — only a 1-based line and a 0-based
 * "char position in line" plus the source text fragment. The cursor, however, arrives as an
 * absolute offset. This helper builds a line-offset table from the query once, then converts in
 * either direction so that "does the cursor sit inside this node?" reduces to an integer range
 * check against {@link #range(Source)}.
 *
 * <p><b>UTF-16 vs. code points:</b> the ESQL parser feeds ANTLR a {@code CodePointCharStream}
 * (see {@code EsqlParser}), so every {@link Location}'s {@code charPositionInLine} is a Unicode
 * <i>code point</i> count from the start of the line. The cursor supplied by callers (and every
 * Java {@code String} API such as {@code length()}/{@code charAt}) works in UTF-16 <i>code
 * units</i> instead. These two only coincide for text confined to the Basic Multilingual Plane;
 * any supplementary-plane character (most emoji, some CJK extensions, mathematical alphanumeric
 * symbols) occupies one code point but two UTF-16 units. This class is therefore code-point-aware
 * internally: {@link #toOffset} converts an ANTLR {@code (line, code-point column)} into a UTF-16
 * offset via {@link Character#offsetByCodePoints}, and {@link #toLocation} converts a UTF-16
 * offset into an ANTLR-style code-point column via {@link Character#codePointCount}. Callers only
 * ever pass/receive UTF-16 offsets (matching {@code EsqlSuggestionsRequest.cursor}); the code-point
 * conversion is entirely internal.
 */
public final class CursorLocation {

    /** A half-open {@code [start, end)} absolute-offset range into the query. */
    public record OffsetRange(int start, int end) {
        public boolean contains(int offset) {
            return offset >= start && offset < end;
        }

        /** Like {@link #contains(int)} but treats the end boundary as inside — for caret placement. */
        public boolean containsInclusive(int offset) {
            return offset >= start && offset <= end;
        }
    }

    private final String query;
    /** {@code lineStart[i]} is the absolute offset of the first character of line {@code i} (1-based; index 0 unused). */
    private final int[] lineStart;

    public CursorLocation(String query) {
        this.query = query;
        List<Integer> starts = new ArrayList<>();
        starts.add(0); // dummy for index 0 so lines are 1-based
        starts.add(0); // line 1 starts at offset 0
        for (int i = 0; i < query.length(); i++) {
            if (query.charAt(i) == '\n') {
                starts.add(i + 1);
            }
        }
        this.lineStart = starts.stream().mapToInt(Integer::intValue).toArray();
    }

    /**
     * Convert an absolute UTF-16 character offset into a 1-based line and a 0-based, <i>code
     * point</i> column, matching how ANTLR (and therefore {@link Location}) number positions.
     */
    public Location toLocation(int offset) {
        if (offset < 0 || offset > query.length()) {
            throw new IllegalArgumentException("offset [" + offset + "] out of bounds for query of length [" + query.length() + "]");
        }
        // Find the greatest line whose start is <= offset.
        int line = 1;
        for (int candidate = 1; candidate < lineStart.length; candidate++) {
            if (lineStart[candidate] <= offset) {
                line = candidate;
            } else {
                break;
            }
        }
        int column = Character.codePointCount(query, lineStart[line], offset);
        return new Location(line, column);
    }

    /**
     * Absolute UTF-16 offset of the start of a 1-based {@code (line, code-point column)} position.
     */
    public int toOffset(int line, int columnZeroBased) {
        if (line < 1 || line >= lineStart.length) {
            throw new IllegalArgumentException("line [" + line + "] out of bounds");
        }
        return Character.offsetByCodePoints(query, lineStart[line], columnZeroBased);
    }

    /**
     * The absolute {@code [start, end)} range that a node's {@link Source} occupies in the query.
     * The start comes from the source's {@link Location}; the length from the captured source text.
     */
    public OffsetRange range(Source source) {
        Location location = source.source();
        int start = toOffset(location.getLineNumber(), location.getColumnNumber() - 1);
        int end = start + source.text().length();
        return new OffsetRange(start, end);
    }
}
