/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.painless.spi.annotation;

import java.util.Map;

public class AugmentedAnnotationParser implements WhitelistAnnotationParser {

    public static final AugmentedAnnotationParser INSTANCE = new AugmentedAnnotationParser();

    public static final String AUGMENTED_CANONICAL_CLASS_NAME = "augmented_canonical_class_name";

    private AugmentedAnnotationParser() {

    }

    @Override
    public Object parse(Map<String, String> arguments) {
        String javaClassName = arguments.get(AUGMENTED_CANONICAL_CLASS_NAME);

        if (javaClassName == null) {
            throw new IllegalArgumentException("augmented_canonical_class_name cannot be null for [@augmented] annotation");
        }

        if ((arguments.isEmpty() || arguments.size() == 1 && arguments.containsKey(AUGMENTED_CANONICAL_CLASS_NAME)) == false) {
            throw new IllegalArgumentException("unexpected parameters for [@augmented] annotation, found " + arguments);
        }

        return new AugmentedAnnotation(javaClassName);
    }
}
