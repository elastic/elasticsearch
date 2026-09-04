/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.pushdown;

import org.apache.lucene.util.BytesRef;

/**
 * Utility for computing the exclusive upper bound of a UTF-8 string prefix range.
 * Used by Parquet and ORC pushdown to translate {@code StartsWith(col, prefix)} into
 * {@code col >= prefix AND col < nextPrefixUpperBound(prefix)} for row-group/stripe skipping.
 */
public final class StringPrefixUtils {

    private StringPrefixUtils() {}

    public static BytesRef nextPrefixUpperBound(BytesRef prefix) {
        if (prefix == null || prefix.length == 0) {
            return null;
        }

        String s = prefix.utf8ToString();
        int len = s.length();
        while (len > 0) {
            int cp = s.codePointBefore(len);
            int cpLen = Character.charCount(cp);

            int nextCp = cp + 1;
            if (nextCp >= Character.MIN_SURROGATE && nextCp <= Character.MAX_SURROGATE) {
                nextCp = Character.MAX_SURROGATE + 1;
            }

            if (nextCp > Character.MAX_CODE_POINT) {
                len -= cpLen;
                continue;
            }

            StringBuilder sb = new StringBuilder(len - cpLen + 2);
            sb.append(s, 0, len - cpLen);
            sb.appendCodePoint(nextCp);
            return new BytesRef(sb.toString());
        }

        return null;
    }
}
