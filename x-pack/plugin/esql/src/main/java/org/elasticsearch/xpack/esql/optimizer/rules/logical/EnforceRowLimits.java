/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical;

import org.elasticsearch.common.logging.HeaderWarning;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.expression.Foldables;
import org.elasticsearch.xpack.esql.plan.logical.Limit;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.RowLimited;
import org.elasticsearch.xpack.esql.plan.logical.Streaming;
import org.elasticsearch.xpack.esql.plan.logical.UnaryPlan;

/**
 * Optimizer rule that enforces row limits for plans implementing {@link RowLimited}.
 * <p>
 * This rule ensures that plans with row limits have a LIMIT operator applied to their children,
 * either by adding a new LIMIT or by adjusting an existing LIMIT if it exceeds the plan's maximum rows.
 * Warnings are added when limits are added or adjusted.
 */
public final class EnforceRowLimits extends OptimizerRules.OptimizerRule<LogicalPlan> {

    public EnforceRowLimits() {
        super(OptimizerRules.TransformDirection.UP);
    }

    @Override
    protected LogicalPlan rule(LogicalPlan plan) {
        if (plan instanceof RowLimited rowLimited && plan instanceof UnaryPlan unaryPlan) {
            int maxRows = rowLimited.maxRows();
            LogicalPlan child = unaryPlan.child();
            boolean hadDirectLimit = child instanceof Limit;
            LogicalPlan newChild = applyLimitToChildPlan(child, maxRows);

            if (newChild != child) {
                if (hadDirectLimit == false) {
                    HeaderWarning.addWarning("No limit defined, adding default limit of [{}]", maxRows);
                } else {
                    HeaderWarning.addWarning("Limit adjusted to [{}] to enforce row limit", maxRows);
                }
                return unaryPlan.replaceChild(newChild);
            }
        }
        return plan;
    }

    /**
     * Check if the plan tree contains a Limit operator.
     */
    private static boolean hasLimit(LogicalPlan plan) {
        if (plan instanceof Limit) {
            return true;
        }
        for (LogicalPlan child : plan.children()) {
            if (hasLimit(child)) {
                return true;
            }
        }
        return false;
    }

    private static LogicalPlan applyLimitToChildPlan(LogicalPlan child, int rowLimit) {
        if (child instanceof UnaryPlan unaryPlan) {
            return adjustUnaryPlanLimit(unaryPlan, rowLimit);
        }

        // For other plans (including leaf plans like EsRelation), wrap them with a Limit
        return new Limit(child.source(), Literal.integer(Source.EMPTY, rowLimit), child, false, true);
    }

    private static LogicalPlan adjustUnaryPlanLimit(UnaryPlan plan, int rowLimit) {
        if (plan instanceof Limit limitPlan) {
            return adjustLimitValue(limitPlan, rowLimit);
        }

        if (plan instanceof Streaming) {
            // For streaming plans, push the limit down recursively since they don't change row count
            LogicalPlan newChild = applyLimitToChildPlan(plan.child(), rowLimit);
            return newChild != plan.child() ? plan.replaceChild(newChild) : plan;
        }

        // For non-streaming UnaryPlans (like MvExpand, Aggregate, etc.),
        // add a new limit wrapping the plan's child, preserving any existing limits
        // This ensures the limit is applied AFTER the operation
        return new Limit(plan.source(), Literal.integer(Source.EMPTY, rowLimit), plan, false, true);
    }

    private static Limit adjustLimitValue(Limit limit, int newLimit) {
        Object limitValue = Foldables.literalValueOf(limit.limit());
        if (limitValue instanceof Integer existingLimit && existingLimit <= newLimit) {
            return limit;
        }
        return limit.withLimit(Literal.integer(Source.EMPTY, newLimit));
    }
}
