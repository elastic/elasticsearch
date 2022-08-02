/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.eql.util;

import org.elasticsearch.xpack.eql.EqlIllegalArgumentException;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.predicate.regex.LikePattern;
import org.elasticsearch.xpack.ql.type.DataTypes;

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
}
