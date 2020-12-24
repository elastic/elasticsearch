/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.eql.expression.function.scalar.string;

import org.elasticsearch.common.Strings;

import java.util.Locale;

import static org.elasticsearch.common.Strings.hasLength;
import static org.elasticsearch.xpack.ql.util.StringUtils.EMPTY;

final class StringUtils {

    private StringUtils() {}

    /**
     * Extracts a substring from string between left and right strings.
     * Port of "between" function from the original EQL python implementation.
     *
     * @param string        string to search.
     * @param left          left bounding substring to search for.
     * @param right         right bounding substring to search for.
     * @param greedy        match the longest substring if true.
     * @param caseSensitive match case when searching for {@code left} and {@code right} strings.
     * @return the substring in between {@code left} and {@code right} strings.
     */
    static String between(String string, String left, String right, boolean greedy, boolean caseSensitive) {
        if (hasLength(string) == false || hasLength(left) == false || hasLength(right) == false) {
            return string;
        }

        String matchString = string;
        if (caseSensitive == false) {
            matchString = matchString.toLowerCase(Locale.ROOT);
            left = left.toLowerCase(Locale.ROOT);
            right = right.toLowerCase(Locale.ROOT);
        }

        int idx = matchString.indexOf(left);
        if (idx == -1) {
            return EMPTY;
        }

        int start = idx + left.length();

        if (greedy) {
            idx = matchString.lastIndexOf(right);
        } else {
            idx = matchString.indexOf(right, start);
        }

        if (idx == -1) {
            return EMPTY;
        }

        return string.substring(start, idx);
    }

    /**
     * Checks if {@code string} contains {@code substring} string.
     *
     * @param string    string to search through.
     * @param substring string to search for.
     * @param isCaseSensitive toggle for case sensitivity.
     * @return {@code true} if {@code string} string contains {@code substring} string.
     */
    static boolean stringContains(String string, String substring, boolean isCaseSensitive) {
        if (hasLength(string) == false || hasLength(substring) == false) {
            return false;
        }

        if (isCaseSensitive == false) {
            string = string.toLowerCase(Locale.ROOT);
            substring = substring.toLowerCase(Locale.ROOT);
        }

        return string.contains(substring);
    }

    /**
     * Returns a substring using the Python slice semantics, meaning
     * start and end can be negative
     */
    static String substringSlice(String string, int start, int end) {
        if (hasLength(string) == false) {
            return string;
        }

        int length = string.length();

        // handle first negative values
        if (start < 0) {
            start += length;
        }
        if (start < 0) {
            start = 0;
        }
        if (end < 0) {
            end += length;
        }
        if (end < 0) {
            end = 0;
        } else if (end > length) {
            end = length;
        }

        if (start >= end) {
            return org.elasticsearch.xpack.ql.util.StringUtils.EMPTY;
        }

        return Strings.substring(string, start, end);
    }
}
