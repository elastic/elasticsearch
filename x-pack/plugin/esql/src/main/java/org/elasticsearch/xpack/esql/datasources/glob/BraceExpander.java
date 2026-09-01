/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.glob;

import org.elasticsearch.core.Nullable;

/**
 * Expands the body of one brace group into its alternatives: {@code a,b,c} into three spellings, and a numeric
 * range such as {@code 1..3} or {@code 01..03} into its members, zero-padded when either endpoint is written
 * padded and wider than one digit.
 *
 * <p>Sole caller is {@link GlobMatcher}, which parses brace groups with this so that the two ways a pattern can be
 * resolved cannot disagree about what a group means. They used to: a brace-only pattern was expanded here and
 * probed with {@code exists()}, while any other pattern was matched by {@code GlobMatcher}, and only one of them
 * understood numeric ranges. Deciding which strategy a pattern takes is now derived from the parsed pattern
 * ({@link GlobMatcher#enumerateKeys}), not from a second scan of the raw string.
 *
 * <p>Nested braces are not supported. {@code GlobMatcher} rejects them before calling here.
 */
final class BraceExpander {

    private BraceExpander() {}

    /**
     * Package-private so {@link GlobMatcher} parses brace bodies with the very same code the brace-only fast path
     * uses. The two used to disagree: this expanded {@code {1..3}} numerically while the matcher read the literal
     * text, so one construct meant two different things depending on which engine a pattern happened to reach.
     */
    static String[] expandBraceContent(String content, int maxExpansion) {
        int dotDot = content.indexOf("..");
        if (dotDot > 0 && dotDot < content.length() - 2 && content.indexOf(',') < 0) {
            String startStr = content.substring(0, dotDot);
            String endStr = content.substring(dotDot + 2);
            if (isNumeric(startStr) && isNumeric(endStr)) {
                String[] rangeResult = expandNumericRange(startStr, endStr, maxExpansion);
                if (rangeResult != null) {
                    return rangeResult;
                }
                return null;
            }
        }
        return content.split(",", -1);
    }

    private static boolean isNumeric(String s) {
        if (s.isEmpty()) {
            return false;
        }
        for (int i = 0; i < s.length(); i++) {
            if (Character.isDigit(s.charAt(i)) == false) {
                return false;
            }
        }
        return true;
    }

    @Nullable
    private static String[] expandNumericRange(String startStr, String endStr, int maxExpansion) {
        long start;
        long end;
        try {
            start = Long.parseLong(startStr);
            end = Long.parseLong(endStr);
        } catch (NumberFormatException e) {
            return null;
        }

        int width = Math.max(startStr.length(), endStr.length());
        boolean zeroPad = (startStr.length() > 1 && startStr.charAt(0) == '0') || (endStr.length() > 1 && endStr.charAt(0) == '0');

        long count;
        if (start <= end) {
            count = end - start + 1;
        } else {
            count = start - end + 1;
        }
        if (count < 0 || count > maxExpansion) {
            return null;
        }

        String[] result = new String[(int) count];
        long step = start <= end ? 1 : -1;
        long current = start;
        for (int i = 0; i < count; i++) {
            result[i] = zeroPad ? padWithZeros(current, width) : Long.toString(current);
            current += step;
        }
        return result;
    }

    private static String padWithZeros(long value, int width) {
        String str = Long.toString(value);
        if (str.length() >= width) {
            return str;
        }
        StringBuilder sb = new StringBuilder(width);
        for (int i = str.length(); i < width; i++) {
            sb.append('0');
        }
        sb.append(str);
        return sb.toString();
    }
}
