/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.painless.spi.annotation;

import java.util.Map;

/**
 * Methods annotated with {@link CompileTimeOnlyAnnotation} must be run at
 * compile time so their arguments must all be constant and they produce a
 * constant.
 */
public class CompileTimeOnlyAnnotationParser implements WhitelistAnnotationParser {

    public static final CompileTimeOnlyAnnotationParser INSTANCE = new CompileTimeOnlyAnnotationParser();

    private CompileTimeOnlyAnnotationParser() {}

    @Override
    public Object parse(Map<String, String> arguments) {
        if (arguments.isEmpty() == false) {
            throw new IllegalArgumentException(
                "unexpected parameters for [@" + CompileTimeOnlyAnnotation.NAME + "] annotation, found " + arguments
            );
        }

        return CompileTimeOnlyAnnotation.INSTANCE;
    }
}
