/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.eql.util;

import org.elasticsearch.xpack.ql.expression.predicate.regex.LikePattern;

public final class StringUtils {

    private StringUtils() {}

    /**
     * Convert an EQL wildcard string to a LikePattern.
     */
    public static LikePattern toLikePattern(String s) {
        // pick a character that is guaranteed not to be in the string, because it isn't allowed to escape itself
        char escape = 1;

        // replace wildcards with % and escape special characters
        String likeString = s.replace("%", escape + "%")
            .replace("_", escape + "_")
            .replace("*", "%");

        return new LikePattern(likeString, escape);
    }
}
