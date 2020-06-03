/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.function.scalar.string;

import static org.elasticsearch.common.Strings.hasLength;
import static org.elasticsearch.xpack.ql.util.StringUtils.EMPTY;

final class StringFunctionUtils {

    private StringFunctionUtils() {}

    /**
     * Extract a substring from the given string, using start index and length of the extracted substring.
     *
     * @param s       the original String
     * @param start   starting position for the substring within the original string. 0-based index position
     * @param length  length in characters of the subtracted substring
     * @return the resulting String
     */
    static String substring(String s, int start, int length) {
        if (!hasLength(s)) {
            return s;
        }

        if (start < 0) {
            start = 0;
        }

        if (start + 1 > s.length() || length < 0) {
            return "";
        }

        return (start + length > s.length()) ? s.substring(start) : s.substring(start, start + length);
    }

    /**
     * Trims the trailing whitespace characters from the given String. Uses {@link Character#isWhitespace(char)}
     * to determine if a character is whitespace or not.
     *
     * @param s       the original String
     * @return the resulting String
     */
    static String trimTrailingWhitespaces(String s) {
        if (s == null || s.isEmpty()) {
            return s;
        }

        int endIdx = -1;
        for (int i = s.length() - 1; (i >= 0) && (endIdx < 0); i--) {
            if (Character.isWhitespace(s.charAt(i)) == false) {
                endIdx = i;
            }
        }
        if (endIdx < 0) {
            return EMPTY;
        }
        return s.substring(0, endIdx + 1);
    }

    /**
     * Trims the leading whitespace characters from the given String. Uses {@link Character#isWhitespace(char)}
     * to determine if a character is whitespace or not.
     *
     * @param s       the original String
     * @return the resulting String
     */
    static String trimLeadingWhitespaces(String s) {
        if (s == null || s.isEmpty()) {
            return s;
        }

        int startIdx = -1;
        for (int i = 0; (i < s.length()) && (startIdx < 0); i++) {
            if (Character.isWhitespace(s.charAt(i)) == false) {
                startIdx = i;
            }
        }
        if (startIdx < 0) {
            return EMPTY;
        }
        return s.substring(startIdx);
    }
}
