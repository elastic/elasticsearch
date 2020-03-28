/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.eql.expression.function.scalar.stringcontains;

import java.util.Locale;

import static org.elasticsearch.common.Strings.hasLength;

final class StringContainsUtils {

    private StringContainsUtils() {
    }

    /**
     * Extracts a substring from string between left and right strings.
     * Port of "between" function from the original EQL python implementation.
     *
     * @param haystack      string to search through.
     * @param needle        string to search for.
     * @param caseSensitive flag for case-sensitive matching.
     * @return              {@code true} if {@code search} string contains {@code find} string.
     */
    static boolean stringContains(String haystack, String needle, boolean caseSensitive) {
        if (hasLength(haystack) == false || hasLength(needle) == false) {
            return false;
        }

        String search = haystack;
        String find = needle;
        if (caseSensitive == false) {
            search = search.toLowerCase(Locale.ROOT);
            find = find.toLowerCase(Locale.ROOT);
        }
        return search.contains(find);
    }
}
