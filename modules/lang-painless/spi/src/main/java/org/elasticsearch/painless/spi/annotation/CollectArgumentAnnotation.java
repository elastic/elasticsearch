/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.painless.spi.annotation;

import org.elasticsearch.queryableexpression.QueryableExpressionBuilder;

/**
 * Collect the expression in the argument to this method into a method
 * returning a {@link QueryableExpressionBuilder}.
 */
public class CollectArgumentAnnotation {
    public static final String NAME = "collect_argument";
    public final String target;

    public CollectArgumentAnnotation(String target) {
        this.target = target;
    }
}
