/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules;

import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Expressions;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.predicate.logical.And;
import org.elasticsearch.xpack.esql.core.expression.predicate.logical.BinaryLogic;
import org.elasticsearch.xpack.esql.core.expression.predicate.logical.Or;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.optimizer.LogicalPlanOptimizer;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;

import static org.elasticsearch.xpack.esql.core.expression.Literal.FALSE;
import static org.elasticsearch.xpack.esql.core.expression.Literal.TRUE;

public final class PruneFilters extends OptimizerRules.OptimizerRule<Filter> {
    @Override
    protected LogicalPlan rule(Filter filter) {
        Expression condition = filter.condition().transformUp(BinaryLogic.class, PruneFilters::foldBinaryLogic);

        if (condition instanceof Literal) {
            if (TRUE.equals(condition)) {
                return filter.child();
            }
            if (FALSE.equals(condition) || Expressions.isNull(condition)) {
                return LogicalPlanOptimizer.skipPlan(filter);
            }
        }

        if (condition.equals(filter.condition()) == false) {
            return new Filter(filter.source(), filter.child(), condition);
        }
        return filter;
    }

    private static Expression foldBinaryLogic(BinaryLogic binaryLogic) {
        if (binaryLogic instanceof Or or) {
            boolean nullLeft = Expressions.isNull(or.left());
            boolean nullRight = Expressions.isNull(or.right());
            if (nullLeft && nullRight) {
                return new Literal(binaryLogic.source(), null, DataType.NULL);
            }
            if (nullLeft) {
                return or.right();
            }
            if (nullRight) {
                return or.left();
            }
        }
        if (binaryLogic instanceof And and) {
            if (Expressions.isNull(and.left()) || Expressions.isNull(and.right())) {
                return new Literal(binaryLogic.source(), null, DataType.NULL);
            }
        }
        return binaryLogic;
    }

}
