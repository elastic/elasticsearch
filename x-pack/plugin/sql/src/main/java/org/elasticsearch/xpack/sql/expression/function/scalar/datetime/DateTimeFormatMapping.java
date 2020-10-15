/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.sql.expression.function.scalar.datetime;

import java.util.function.Function;

class DateTimeFormatMapping {

    private final String pattern;
    private final String javaPattern;
    private final Function<String, String> additionalMapper;

    DateTimeFormatMapping(String pattern, String javaPattern, Function<String, String> additionalMapper) {
        this.pattern = pattern;
        this.javaPattern = javaPattern;
        this.additionalMapper = additionalMapper;
    }

    DateTimeFormatMapping(String pattern, String javaPattern) {
        this(pattern, javaPattern, null);
    }

    public String getPattern() {
        return pattern;
    }

    public String getJavaPattern() {
        return javaPattern;
    }

    public Function<String, String> getAdditionalMapper() {
        if (additionalMapper == null) {
            return Function.identity();
        }

        return additionalMapper;
    }
}
