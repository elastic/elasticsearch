/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules;

import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.AbstractConvertFunction;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;

/**
 * Replace type converting eval with aliasing eval when type change does not occur.
 * A following {@link ReplaceAliasingEvalWithProject} will effectively convert {@link ReferenceAttribute} into {@link FieldAttribute},
 * something very useful in local physical planning.
 */
public final class ReplaceTrivialTypeConversions extends OptimizerRules.OptimizerRule<Eval> {
    @Override
    protected LogicalPlan rule(Eval eval) {
        return eval.transformExpressionsOnly(AbstractConvertFunction.class, convert -> {
            if (convert.field() instanceof FieldAttribute fa && fa.dataType() == convert.dataType()) {
                return fa;
            }
            return convert;
        });
    }
}
