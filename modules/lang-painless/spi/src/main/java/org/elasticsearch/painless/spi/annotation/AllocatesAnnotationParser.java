/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.painless.spi.annotation;

import java.util.Map;

/**
 * Parses {@code @allocates[class="fully.qualified.Class", method="estimate"]}. Only the shape is validated here; the estimator
 * is resolved at allowlist load time so a missing one fails loudly.
 */
public class AllocatesAnnotationParser implements WhitelistAnnotationParser {

    public static final String CLASS = "class";
    public static final String METHOD = "method";

    public static final AllocatesAnnotationParser INSTANCE = new AllocatesAnnotationParser();

    private AllocatesAnnotationParser() {}

    @Override
    public Object parse(Map<String, String> arguments) {
        if (arguments.size() != 2 || arguments.containsKey(CLASS) == false || arguments.containsKey(METHOD) == false) {
            throw new IllegalArgumentException(
                "[@" + AllocatesAnnotation.NAME + "] requires [" + CLASS + "] and [" + METHOD + "] arguments"
            );
        }

        String className = arguments.get(CLASS).trim();
        String methodName = arguments.get(METHOD).trim();

        if (className.isEmpty()) {
            throw new IllegalArgumentException("[@" + AllocatesAnnotation.NAME + "] [" + CLASS + "] must not be empty");
        }
        if (methodName.isEmpty()) {
            throw new IllegalArgumentException("[@" + AllocatesAnnotation.NAME + "] [" + METHOD + "] must not be empty");
        }

        return new AllocatesAnnotation(className, methodName);
    }
}
