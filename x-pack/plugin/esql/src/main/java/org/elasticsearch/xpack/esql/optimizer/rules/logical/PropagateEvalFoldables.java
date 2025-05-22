/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical;

import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.AttributeMap;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.optimizer.LogicalOptimizerContext;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.rule.ParameterizedRule;

/**
 * Replace any reference attribute with its source, if it does not affect the result.
 * This avoids ulterior look-ups between attributes and its source across nodes.
 */
public final class PropagateEvalFoldables extends ParameterizedRule<LogicalPlan, LogicalPlan, LogicalOptimizerContext> {

    @Override
    public LogicalPlan apply(LogicalPlan plan, LogicalOptimizerContext ctx) {
        AttributeMap.Builder<Expression> collectRefsBuilder = AttributeMap.builder();

        java.util.function.Function<ReferenceAttribute, Expression> replaceReference = r -> collectRefsBuilder.build().resolve(r, r);

        // collect aliases bottom-up
        plan.forEachExpressionUp(Alias.class, a -> {
            var c = a.child();
            boolean shouldCollect = c.foldable();
            // try to resolve the expression based on an existing foldables
            if (shouldCollect == false) {
                c = c.transformUp(ReferenceAttribute.class, replaceReference);
                shouldCollect = c.foldable();
            }
            if (shouldCollect) {
                collectRefsBuilder.put(a.toAttribute(), Literal.of(ctx.foldCtx(), c));
            }
        });
        if (collectRefsBuilder.isEmpty()) {
            return plan;
        }

        plan = plan.transformUp(p -> {
            // Apply the replacement inside Filter and Eval (which shouldn't make a difference)
            // TODO: also allow aggregates once aggs on constants are supported.
            // C.f. https://github.com/elastic/elasticsearch/issues/100634
            if (p instanceof Filter || p instanceof Eval) {
                p = p.transformExpressionsOnly(ReferenceAttribute.class, replaceReference);
            }
            return p;
        });

        return plan;
    }
}
