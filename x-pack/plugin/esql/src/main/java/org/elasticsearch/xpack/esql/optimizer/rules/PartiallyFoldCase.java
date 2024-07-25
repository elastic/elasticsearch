/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules;

import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.expression.function.scalar.conditional.Case;

import static org.elasticsearch.xpack.esql.optimizer.rules.OptimizerRules.TransformDirection.DOWN;

/**
 * Fold the arms of {@code CASE} statements.
 * <pre>{@code
 * EVAL c=CASE(true, foo, bar)
 * }</pre>
 * becomes
 * <pre>{@code
 * EVAL c=foo
 * }</pre>
 */
public final class PartiallyFoldCase extends OptimizerRules.OptimizerExpressionRule<Case> {
    public PartiallyFoldCase() {
        super(DOWN);
    }

    @Override
    protected Expression rule(Case c) {
        return c.partiallyFold();
    }
}
