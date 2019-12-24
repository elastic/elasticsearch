/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.transform.utils;

import org.elasticsearch.cluster.metadata.MetaData;

import java.util.regex.Pattern;

/**
 * Yet Another String utilities class.
 */
public final class TransformStrings {

    /**
     * Valid user id pattern.
     * Matches a string that contains lowercase characters, digits, hyphens, underscores or dots.
     * The string may start and end only in characters or digits.
     * Note that '.' is allowed but not documented.
     */
    private static final Pattern VALID_ID_CHAR_PATTERN = Pattern.compile("[a-z0-9](?:[a-z0-9_\\-\\.]*[a-z0-9])?");

    public static final int ID_LENGTH_LIMIT = 64;

    private TransformStrings() {
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
}
