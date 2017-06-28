/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.jdbc.net.protocol;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.StringTokenizer;

abstract class StringUtils {

    static final String EMPTY = "";

    static String nullAsEmpty(String string) {
        return string == null ? EMPTY : string;
    }

    static boolean hasText(CharSequence sequence) {
        if (!hasLength(sequence)) {
            return false;
        }
        int length = sequence.length();
        for (int i = 0; i < length; i++) {
            if (!Character.isWhitespace(sequence.charAt(i))) {
                return true;
            }
        }
        return false;
    }

    static boolean hasLength(CharSequence sequence) {
        return (sequence != null && sequence.length() > 0);
    }

    static String[] splitToIndexAndType(String pattern) {
        List<String> tokens = tokenize(pattern, ".");

        String[] results = new String[2];
        if (tokens.size() == 2) {
            results[0] = tokens.get(0);
            results[1] = tokens.get(1);
        }
        else {
            results[0] = nullAsEmpty(pattern);
            results[1] = EMPTY;
        }

        return results;
    }

    static List<String> tokenize(String string, String delimiters) {
        return tokenize(string, delimiters, true, true);
    }

    static List<String> tokenize(String string, String delimiters, boolean trimTokens, boolean ignoreEmptyTokens) {
        if (!hasText(string)) {
            return Collections.emptyList();
        }
        StringTokenizer st = new StringTokenizer(string, delimiters);
        List<String> tokens = new ArrayList<String>();
        while (st.hasMoreTokens()) {
            String token = st.nextToken();
            if (trimTokens) {
                token = token.trim();
            }
            if (!ignoreEmptyTokens || token.length() > 0) {
                tokens.add(token);
            }
        }
        return tokens;
    }

}