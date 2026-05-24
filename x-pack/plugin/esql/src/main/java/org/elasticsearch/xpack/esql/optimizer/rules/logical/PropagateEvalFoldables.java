/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical;

import org.elasticsearch.xpack.esql.core.expression.AttributeMap;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.optimizer.LogicalOptimizerContext;
import org.elasticsearch.xpack.esql.optimizer.rules.RuleUtils;
import org.elasticsearch.xpack.esql.plan.logical.ChangePoint;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.LimitBy;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.MMR;
import org.elasticsearch.xpack.esql.plan.logical.Row;
import org.elasticsearch.xpack.esql.rule.ParameterizedRule;

import java.util.List;

/**
 * Replace any reference attribute with its source, if it does not affect the result.
 * This avoids ulterior look-ups between attributes and its source across nodes.
 */
public final class PropagateEvalFoldables extends ParameterizedRule<LogicalPlan, LogicalPlan, LogicalOptimizerContext> {

    @Override
    public LogicalPlan apply(LogicalPlan plan, LogicalOptimizerContext ctx) {
        AttributeMap<Expression> collectRefs = RuleUtils.foldableReferencesSkipMVGroupings(plan, ctx);
        if (collectRefs.isEmpty()) {
            return plan;
        }

        // Apply the replacement inside Filter, Eval, Row and LimitBy (groupings).
        // TODO: also allow aggregates once aggs on constants are supported.
        // C.f. https://github.com/elastic/elasticsearch/issues/100634
        plan = plan.transformUp(p -> {
            if (p instanceof Filter || p instanceof Eval || p instanceof Row || p instanceof LimitBy || p instanceof MMR) {
                p = p.transformExpressionsOnly(ReferenceAttribute.class, r -> collectRefs.resolve(r, r));
            } else if (p instanceof ChangePoint cp) {
                // Among ChangePoint's fields, only `groupings` accepts arbitrary expressions;
                // Applying replacement to `groupings` only
                List<Expression> newGroupings = cp.groupings()
                    .stream()
                    .map(g -> g.transformUp(ReferenceAttribute.class, r -> collectRefs.resolve(r, r)))
                    .toList();
                if (newGroupings.equals(cp.groupings()) == false) {
                    p = new ChangePoint(cp.source(), cp.child(), cp.value(), cp.key(), cp.targetType(), cp.targetPvalue(), newGroupings);
                }
            }
            return p;
        });

        return plan;
    }
}
