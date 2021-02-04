/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.sql.expression.function.scalar.string;

import static org.elasticsearch.common.Strings.hasLength;

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
        if (hasLength(s) == false) {
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
        if (hasLength(s) == false) {
            return s;
        }

        int i = s.length() - 1;
        while (i >= 0 && Character.isWhitespace(s.charAt(i))) {
            i--;
        }
        return s.substring(0, i + 1);
    }

    /**
     * Trims the leading whitespace characters from the given String. Uses {@link Character#isWhitespace(char)}
     * to determine if a character is whitespace or not.
     *
     * @param s       the original String
     * @return the resulting String
     */
    static String trimLeadingWhitespaces(String s) {
        if (hasLength(s) == false) {
            return s;
        }

        int i = 0;
        while (i < s.length() && Character.isWhitespace(s.charAt(i))) {
            i++;
        }
        return s.substring(i);
    }
}
