/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules;

import org.elasticsearch.xpack.esql.core.expression.Expression;

public final class BooleanSimplification extends OptimizerRules.BooleanSimplification {

    public BooleanSimplification() {
        super();
    }

    @Override
    protected Expression maybeSimplifyNegatable(Expression e) {
        return null;
    }

}
