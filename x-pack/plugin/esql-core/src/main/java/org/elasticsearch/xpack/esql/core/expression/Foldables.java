/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.core.expression;

import org.elasticsearch.xpack.esql.core.QlIllegalArgumentException;

public abstract class Foldables {

    public static Object valueOf(FoldContext ctx, Expression e) {
        if (e.foldable()) {
            return e.fold(ctx);
        }
        throw new QlIllegalArgumentException("Cannot determine value for {}", e);
    }
}
