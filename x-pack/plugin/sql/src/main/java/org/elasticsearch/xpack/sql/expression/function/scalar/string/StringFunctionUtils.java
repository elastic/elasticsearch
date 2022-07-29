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
}
