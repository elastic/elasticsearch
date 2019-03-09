/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.utils;

import org.elasticsearch.cluster.metadata.MetaData;

import java.util.regex.Pattern;

/**
 * Another String utilities class. Class name is prefixed with Ml to avoid confusion
 * with one of the myriad String utility classes out there.
 */
public final class MlStrings {

    private static final Pattern NEEDS_QUOTING = Pattern.compile("\\W");

    /**
     * Valid user id pattern.
     * Matches a string that contains lower case characters, digits, hyphens, underscores or dots.
     * The string may start and end only in lower case characters or digits.
     * Note that '.' is allowed but not documented.
     */
    private static final Pattern VALID_ID_CHAR_PATTERN = Pattern.compile("[a-z0-9](?:[a-z0-9_\\-\\.]*[a-z0-9])?");

    public static final int ID_LENGTH_LIMIT = 64;

    private MlStrings() {
    }

    /**
     * Surrounds with double quotes the given {@code input} if it contains
     * any non-word characters. Any double quotes contained in {@code input}
     * will be escaped.
     *
     * @param input any non null string
     * @return {@code input} when it does not contain non-word characters, or a new string
     * that contains {@code input} surrounded by double quotes otherwise
     */
    public static String doubleQuoteIfNotAlphaNumeric(String input) {
        if (!NEEDS_QUOTING.matcher(input).find()) {
            return input;
        }

        StringBuilder quoted = new StringBuilder();
        quoted.append('\"');

        for (int i = 0; i < input.length(); ++i) {
            char c = input.charAt(i);
            if (c == '\"' || c == '\\') {
                quoted.append('\\');
            }
            quoted.append(c);
        }

        quoted.append('\"');
        return quoted.toString();
    }

    public static boolean isValidId(String id) {
        return id != null && VALID_ID_CHAR_PATTERN.matcher(id).matches() && !MetaData.ALL.equals(id);
    }

    /**
     * Checks if the given {@code id} has a valid length.
     * We keep IDs in a length shorter or equal than {@link #ID_LENGTH_LIMIT}
     * in order to avoid unfriendly errors when storing docs with
     * more than 512 bytes.
     *
     * @param id the id
     * @return {@code true} if the id has a valid length
     */
    public static boolean hasValidLengthForId(String id) {
        return id.length() <= ID_LENGTH_LIMIT;
    }

    /**
     * Returns the path to the parent field if {@code fieldPath} is nested
     * or {@code fieldPath} itself.
     *
     * @param fieldPath a field path
     * @return the path to the parent field if {code fieldPath} is nested
     * or {@code} fieldPath itself
     */
    public static String getParentField(String fieldPath) {
        if (fieldPath == null) {
            return fieldPath;
        }
        int lastIndexOfDot = fieldPath.lastIndexOf('.');
        if (lastIndexOfDot < 0) {
            return fieldPath;
        }
        return fieldPath.substring(0, lastIndexOfDot);
    }
}
