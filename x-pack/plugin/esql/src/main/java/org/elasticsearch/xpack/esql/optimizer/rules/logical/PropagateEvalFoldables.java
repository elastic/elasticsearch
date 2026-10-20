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
import org.elasticsearch.xpack.esql.plan.logical.join.IndependentSubqueryJoin;
import org.elasticsearch.xpack.esql.rule.ParameterizedRule;

import java.util.ArrayList;
import java.util.List;

/**
 * Replace any reference attribute with its source, if it does not affect the result.
 * This avoids ulterior look-ups between attributes and its source across nodes.
 */
public final class PropagateEvalFoldables extends ParameterizedRule<LogicalPlan, LogicalPlan, LogicalOptimizerContext> {

    @Override
    public LogicalPlan apply(LogicalPlan plan, LogicalOptimizerContext ctx) {
        return propagateInScope(plan, ctx);
    }

    private static LogicalPlan propagateInScope(LogicalPlan plan, LogicalOptimizerContext ctx) {
        AttributeMap<Expression> collectRefs = RuleUtils.foldableReferencesSkipMVGroupings(plan, ctx);
        return propagateInScope(plan, collectRefs, ctx);
    }

    private static LogicalPlan propagateInScope(LogicalPlan plan, AttributeMap<Expression> collectRefs, LogicalOptimizerContext ctx) {
        List<LogicalPlan> children = plan.children();
        List<LogicalPlan> transformedChildren = null;
        for (int i = 0; i < children.size(); i++) {
            LogicalPlan child = children.get(i);
            LogicalPlan transformedChild = plan instanceof IndependentSubqueryJoin && i == 1
                ? propagateInScope(child, ctx)
                : propagateInScope(child, collectRefs, ctx);
            if (child.equals(transformedChild) == false) {
                if (transformedChildren == null) {
                    transformedChildren = new ArrayList<>(children);
                }
                transformedChildren.set(i, transformedChild);
            }
        }
        if (transformedChildren != null) {
            plan = plan.replaceChildren(transformedChildren);
        }

        if (collectRefs.isEmpty()) {
            return plan;
        }
        // Apply the replacement inside Filter, Eval, Row and LimitBy (groupings).
        // TODO: also allow aggregates once aggs on constants are supported.
        // C.f. https://github.com/elastic/elasticsearch/issues/100634
        if (plan instanceof Filter || plan instanceof Eval || plan instanceof Row || plan instanceof LimitBy || plan instanceof MMR) {
            plan = plan.transformExpressionsOnly(ReferenceAttribute.class, r -> collectRefs.resolve(r, r));
        } else if (plan instanceof ChangePoint cp) {
            // Among ChangePoint's fields, only `groupings` accepts arbitrary expressions;
            // Applying replacement to `groupings` only
            List<Expression> newGroupings = cp.groupings()
                .stream()
                .map(g -> g.transformUp(ReferenceAttribute.class, r -> collectRefs.resolve(r, r)))
                .toList();
            if (newGroupings.equals(cp.groupings()) == false) {
                plan = new ChangePoint(cp.source(), cp.child(), cp.value(), cp.key(), cp.targetType(), cp.targetPvalue(), newGroupings);
            }
        }
        return plan;
    }
}
