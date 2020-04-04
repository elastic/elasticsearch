/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.eql.expression.function.scalar.string;

import org.elasticsearch.common.Strings;

import java.util.Locale;

import static org.elasticsearch.common.Strings.hasLength;

final class StringUtils {

    private StringUtils() {}

    /**
     * Extracts a substring from string between left and right strings.
     * Port of "stringContains" function from the original EQL python implementation.
     *
     * @param string    string to search through.
     * @param substring string to search for.
     * @return {@code true} if {@code string} string contains {@code substring} string.
     */
    static boolean stringContains(String string, String substring) {
        if (hasLength(string) == false || hasLength(substring) == false) {
            return false;
        }

        string = string.toLowerCase(Locale.ROOT);
        substring = substring.toLowerCase(Locale.ROOT);
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
