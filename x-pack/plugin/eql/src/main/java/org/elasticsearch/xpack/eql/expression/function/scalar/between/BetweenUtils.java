/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.eql.expression.function.scalar.between;

import java.util.Locale;

import static org.elasticsearch.common.Strings.hasLength;

final class BetweenUtils {

    private BetweenUtils() {
    }

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
            return "";
        }

        int start = idx + left.length();

        if (greedy) {
            idx = matchString.lastIndexOf(right);
        } else {
            idx = matchString.indexOf(right, start);
        }

        if (idx == -1) {
            return "";
        }

        return string.substring(start, idx);
    }
}
