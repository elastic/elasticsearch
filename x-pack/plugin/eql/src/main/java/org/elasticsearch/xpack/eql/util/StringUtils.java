/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.eql.util;

import org.elasticsearch.common.Strings;
import org.elasticsearch.xpack.eql.EqlIllegalArgumentException;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.predicate.regex.LikePattern;
import org.elasticsearch.xpack.ql.type.DataTypes;

import java.util.Locale;

import static org.elasticsearch.common.Strings.hasLength;
import static org.elasticsearch.xpack.ql.util.StringUtils.EMPTY;

public final class StringUtils {

    private StringUtils() {}

    /**
     * Convert an EQL wildcard string to a LikePattern.
     */
    public static LikePattern toLikePattern(String s) {
        // pick a character that is guaranteed not to be in the string, because it isn't allowed to escape itself
        char escape = 1;
        String escapeString = Character.toString(escape);

        // replace wildcards with % and escape special characters
        String likeString = s.replace("%", escapeString + "%").replace("_", escapeString + "_").replace("*", "%").replace("?", "_");

        return new LikePattern(likeString, escape);
    }

    public static LikePattern toLikePattern(Expression expression) {
        if (expression.foldable() == false || DataTypes.isString(expression.dataType()) == false) {
            throw new EqlIllegalArgumentException("Invalid like pattern received {}", expression);
        }
        return toLikePattern(expression.fold().toString());
    }

    /**
     * Extracts a substring from string between left and right strings.
     * Port of "between" function from the original EQL python implementation.
     *
     * @param string          string to search.
     * @param left            left bounding substring to search for.
     * @param right           right bounding substring to search for.
     * @param greedy          match the longest substring if true.
     * @param caseInsensitive match case when searching for {@code left} and {@code right} strings.
     * @return the substring in between {@code left} and {@code right} strings.
     */
    public static String between(String string, String left, String right, boolean greedy, boolean caseInsensitive) {
        if (hasLength(string) == false || hasLength(left) == false || hasLength(right) == false) {
            return string;
        }

        String matchString = string;
        if (caseInsensitive) {
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
     * @param string          string to search through.
     * @param substring       string to search for.
     * @param caseInsensitive toggle for case sensitivity.
     * @return {@code true} if {@code string} string contains {@code substring} string.
     */
    public static boolean stringContains(String string, String substring, boolean caseInsensitive) {
        if (hasLength(string) == false || hasLength(substring) == false) {
            return false;
        }

        if (caseInsensitive) {
            string = string.toLowerCase(Locale.ROOT);
            substring = substring.toLowerCase(Locale.ROOT);
        }

        return string.contains(substring);
    }

    /**
     * Returns a substring using the Python slice semantics, meaning
     * start and end can be negative
     */
    public static String substringSlice(String string, int start, int end) {
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
