/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.core.expression.predicate.regex;

import java.util.regex.Pattern;

public class RegexOperation {

    public static Boolean match(Object value, Pattern pattern) {
        if (pattern == null) {
            return Boolean.TRUE;
        }

        if (value == null) {
            return null;
        }

        return pattern.matcher(value.toString()).matches();
    }

    public static Boolean match(Object value, String pattern) {
        return match(value, pattern, Boolean.FALSE);
    }

    public static Boolean match(Object value, String pattern, Boolean caseInsensitive) {
        if (pattern == null) {
            return Boolean.TRUE;
        }

        if (value == null) {
            return null;
        }

        int flags = 0;
        if (Boolean.TRUE.equals(caseInsensitive)) {
            flags |= Pattern.CASE_INSENSITIVE;
        }
        return Pattern.compile(pattern, flags).matcher(value.toString()).matches();
    }
}
