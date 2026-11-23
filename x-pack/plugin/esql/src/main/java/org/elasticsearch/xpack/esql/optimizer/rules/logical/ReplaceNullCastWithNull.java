/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical;

import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.expression.NullCast;
import org.elasticsearch.xpack.esql.optimizer.LogicalOptimizerContext;

/**
 * Replaces a {@link NullCast} with a {@code null} literal of its type. This runs after the substitutions have moved casts over
 * aggregate functions out of the aggregations, so the aggregation they were over can be pruned.
 */
public final class ReplaceNullCastWithNull extends OptimizerRules.OptimizerExpressionRule<NullCast> {

    public ReplaceNullCastWithNull() {
        super(OptimizerRules.TransformDirection.UP);
    }

    @Override
    protected Expression rule(NullCast nullCast, LogicalOptimizerContext ctx) {
        return new Literal(nullCast.source(), null, nullCast.dataType());
    }
}
