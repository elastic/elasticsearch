/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.painless.spi.annotation;

import org.elasticsearch.queryableexpression.QueryableExpressionBuilder;

import java.util.Map;

/**
 * Collect the expression in the argument to this method into a method
 * returning a {@link QueryableExpressionBuilder}.
 */
public class CollectArgumentAnnotationParser implements WhitelistAnnotationParser {

    public static final CollectArgumentAnnotationParser INSTANCE = new CollectArgumentAnnotationParser();

    private CollectArgumentAnnotationParser() {}

    @Override
    public Object parse(Map<String, String> arguments) {
        String target = arguments.get("target");
        if (target == null || arguments.size() != 1) {
            throw new IllegalArgumentException(
                "expected only the [target] parameter for [@" + CollectArgumentAnnotation.NAME + "] annotation, found " + arguments
            );
        }
        return new CollectArgumentAnnotation(target);
    }
}
