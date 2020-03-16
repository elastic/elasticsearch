/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.eql.expression.function.scalar.string;

import org.elasticsearch.common.Strings;

import static org.elasticsearch.common.Strings.hasLength;

final class StringUtils {

    private StringUtils() {}

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
        while (start < 0) {
            start += length;
        }
        while (end < 0) {
            end += length;
        }
        int validEndIndex = length;

        if (start > validEndIndex) {
            start = validEndIndex;
        }
        if (end > validEndIndex) {
            end = validEndIndex;
        }

        if (start >= end) {
            return org.elasticsearch.xpack.ql.util.StringUtils.EMPTY;
        }

        return Strings.substring(string, start, end);
    }
}
