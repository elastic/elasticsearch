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

public class DeprecatedAnnotationParser implements WhitelistAnnotationParser {

    public static final DeprecatedAnnotationParser INSTANCE = new DeprecatedAnnotationParser();

    public static final String MESSAGE = "message";

    private DeprecatedAnnotationParser() {

    }

    @Override
    public Object parse(Map<String, String> arguments) {
        String message = arguments.getOrDefault(MESSAGE, "");

        if ((arguments.isEmpty() || arguments.size() == 1 && arguments.containsKey(MESSAGE)) == false) {
            throw new IllegalArgumentException("unexpected parameters for [@deprecation] annotation, found " + arguments);
        }

        return new DeprecatedAnnotation(message);
    }
}
