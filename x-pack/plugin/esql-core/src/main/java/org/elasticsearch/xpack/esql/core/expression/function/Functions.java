/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.core.expression.function;

import org.elasticsearch.xpack.esql.core.expression.Expression;

/**
 * @deprecated for removal
 */
@Deprecated
public abstract class Functions {

    /**
     * @deprecated for removal
     */
    @Deprecated
    public static boolean isAggregate(Expression e) {
        throw new IllegalStateException("Should never reach this code");
    }
}
