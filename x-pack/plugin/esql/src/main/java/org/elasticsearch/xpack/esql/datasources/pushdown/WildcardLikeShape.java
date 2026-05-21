/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.pushdown;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.core.Nullable;

/**
 * Shape decomposition for {@code WildcardLike} patterns of the affix-contains family:
 * an optional literal prefix, an optional literal substring in the middle, and an optional
 * literal suffix. The pattern is represented as
 * <pre>{@code prefix * literal * suffix}</pre>
 * where any of the three literal segments may be empty (rendered as {@code null} on the result).
 *
 * <p>Recognized shapes — adjacent {@code *}s are collapsed during parsing, so {@code **foo**bar*}
 * is treated as {@code *foo*bar*} (and rejected; see below):
 * <ul>
 *   <li>{@code prefix*} — one wildcard, only a prefix</li>
 *   <li>{@code *suffix} — one wildcard, only a suffix</li>
 *   <li>{@code prefix*suffix} — one wildcard, both affixes</li>
 *   <li>{@code *literal*} — two wildcards, only a middle literal</li>
 *   <li>{@code prefix*literal*} — two wildcards, prefix + middle</li>
 *   <li>{@code *literal*suffix} — two wildcards, middle + suffix</li>
 *   <li>{@code prefix*literal*suffix} — two wildcards, all three segments</li>
 * </ul>
 *
 * <p>Patterns that fall outside the family stay on the automaton path and produce {@code null}
 * from {@link #of(String)}:
 * <ul>
 *   <li>any {@code ?} — single-codepoint wildcard, awkward on UTF-8 byte boundaries</li>
 *   <li>any {@code \\} backslash anywhere in the pattern — escape handling would need to be
 *       consistent with Lucene's wildcard un-escaping per segment; rejecting outright keeps the
 *       parser bit-identical with {@code ByteRunAutomaton} and avoids divergence risk</li>
 *   <li>three or more unescaped, non-adjacent {@code *} — would require multi-segment ordered
 *       contains, more complex than the affix-contains shape can express</li>
 *   <li>an empty pattern — degenerate; {@code WildcardPattern} guards against it upstream</li>
 *   <li>a pattern with no {@code *} — equivalent to {@code Equals}, which the logical optimizer
 *       already decomposes; not the responsibility of this parser</li>
 * </ul>
 *
 * <p>Bytes are committed to UTF-8 by way of {@link BytesRef#BytesRef(CharSequence)}, matching
 * what KEYWORD values are encoded as on the read path.
 *
 * @param prefix  the literal prefix, or {@code null} if the pattern starts with {@code *}
 * @param literal the literal middle substring, or {@code null} if the pattern has fewer than two
 *                {@code *} runs or the middle segment is empty
 * @param suffix  the literal suffix, or {@code null} if the pattern ends with {@code *}
 */
public record WildcardLikeShape(@Nullable BytesRef prefix, @Nullable BytesRef literal, @Nullable BytesRef suffix) {

    /**
     * Returns the shape of the given case-sensitive {@code WildcardLike} pattern, or {@code null}
     * if the pattern is outside the affix-contains family (see the class-level docs).
     *
     * <p>Caller is responsible for the case-sensitivity check — this parser is intentionally
     * blind to it so the rule is local.
     */
    @Nullable
    public static WildcardLikeShape of(String pattern) {
        int len = pattern.length();
        if (len == 0) {
            return null;
        }
        // Walk once. Each unescaped non-collapsed '*' closes the current literal segment and
        // opens the next one. We track at most three segments (prefix, middle, suffix); a fourth
        // segment means a third star, which is outside the affix-contains shape — bail out.
        // Adjacent '*'s collapse so '**foo**bar*' is parsed as '*foo*bar*'. '?' or '\\' anywhere
        // forces the automaton path.
        int segCount = 0;
        int[] segStart = new int[3];
        int[] segEnd = new int[3];
        int currentStart = 0;
        boolean lastWasStar = false;
        for (int i = 0; i < len; i++) {
            char c = pattern.charAt(i);
            if (c == '?' || c == '\\') {
                return null;
            }
            if (c == '*') {
                if (lastWasStar) {
                    currentStart = i + 1;
                    continue;
                }
                if (segCount == 3) {
                    return null;
                }
                segStart[segCount] = currentStart;
                segEnd[segCount] = i;
                segCount++;
                currentStart = i + 1;
                lastWasStar = true;
            } else {
                lastWasStar = false;
            }
        }
        if (segCount == 0) {
            // No '*' at all — equivalent to Equals, already decomposed by the logical optimizer.
            return null;
        }
        // The trailing segment (after the final star, possibly empty) is also a valid segment.
        if (segCount == 3) {
            return null;
        }
        segStart[segCount] = currentStart;
        segEnd[segCount] = len;
        segCount++;
        // segCount is now 2 (one '*') or 3 (two '*'s).
        BytesRef prefix = sliceOrNull(pattern, segStart[0], segEnd[0]);
        if (segCount == 2) {
            // 'prefix*suffix', 'prefix*', '*suffix', or '*' (collapsed forms included).
            BytesRef suffix = sliceOrNull(pattern, segStart[1], segEnd[1]);
            return new WildcardLikeShape(prefix, null, suffix);
        }
        BytesRef middle = sliceOrNull(pattern, segStart[1], segEnd[1]);
        BytesRef suffix = sliceOrNull(pattern, segStart[2], segEnd[2]);
        return new WildcardLikeShape(prefix, middle, suffix);
    }

    @Nullable
    private static BytesRef sliceOrNull(String pattern, int start, int end) {
        if (end <= start) {
            return null;
        }
        return new BytesRef(pattern.substring(start, end));
    }

    /**
     * Returns {@code true} when this shape selects every value (every component is absent).
     * Equivalent to a {@code LIKE *} pattern; callers typically have an upstream {@code matchesAll}
     * fast path that handles this without dispatching to {@link ByteMatchers#affixContains}.
     */
    public boolean matchesAll() {
        return prefix == null && literal == null && suffix == null;
    }
}
