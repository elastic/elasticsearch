/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.painless.spi.annotation;

import java.util.Map;

public class DynamicTypeAnnotationParser implements WhitelistAnnotationParser {

    public static final DynamicTypeAnnotationParser INSTANCE = new DynamicTypeAnnotationParser();

    private DynamicTypeAnnotationParser() {}

    @Override
    public Object parse(Map<String, String> arguments) {
        if (arguments.isEmpty() == false) {
            throw new IllegalArgumentException(
                "unexpected parameters for [@" + DynamicTypeAnnotation.NAME + "] annotation, found " + arguments
            );
        }

        return DynamicTypeAnnotation.INSTANCE;
    }
}
