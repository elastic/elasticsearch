/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical;

import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.NameId;
import org.elasticsearch.xpack.esql.optimizer.LogicalOptimizerContext;
import org.elasticsearch.xpack.esql.plan.logical.CompoundOutputEval;
import org.elasticsearch.xpack.esql.plan.logical.Enrich;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.LimitBy;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.MvExpand;
import org.elasticsearch.xpack.esql.plan.logical.Project;
import org.elasticsearch.xpack.esql.plan.logical.RegexExtract;
import org.elasticsearch.xpack.esql.plan.logical.UnaryPlan;
import org.elasticsearch.xpack.esql.plan.logical.inference.InferencePlan;
import org.elasticsearch.xpack.esql.plan.logical.join.InlineJoin;
import org.elasticsearch.xpack.esql.plan.logical.join.Join;
import org.elasticsearch.xpack.esql.plan.logical.join.JoinTypes;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.elasticsearch.xpack.esql.core.expression.Expressions.listSemanticEqualsIgnoreOrder;

/**
 * Push-down and combine rules specific to {@link LimitBy} (LIMIT N BY groupings).
 */
public final class PushDownAndCombineLimitBy extends OptimizerRules.ParameterizedOptimizerRule<LimitBy, LogicalOptimizerContext> {

    public PushDownAndCombineLimitBy() {
        super(OptimizerRules.TransformDirection.DOWN);
    }

    @Override
    public LogicalPlan rule(LimitBy limitBy, LogicalOptimizerContext ctx) {
        if (limitBy.child() instanceof LimitBy childLimitBy
            && listSemanticEqualsIgnoreOrder(childLimitBy.groupings(), limitBy.groupings())) {
            return combineLimitBys(limitBy, childLimitBy, ctx.foldCtx());
        } else if (limitBy.child() instanceof UnaryPlan unary) {
            if (unary instanceof Eval
                || unary instanceof Project
                || unary instanceof RegexExtract
                || unary instanceof CompoundOutputEval<?>
                || unary instanceof InferencePlan<?>) {
                if (groupingsReferenceAttributeDefinedByChild(limitBy, unary)) {
                    return limitBy;
                } else {
                    return unary.replaceChild(limitBy.replaceChild(unary.child()));
                }
            } else if (unary instanceof MvExpand) {
                return duplicateLimitByAsFirstGrandchild(limitBy);
            } else if (unary instanceof Enrich enrich) {
                if (groupingsReferenceAttributeDefinedByChild(limitBy, enrich)) {
                    return limitBy;
                }
                if (enrich.mode() == Enrich.Mode.REMOTE) {
                    return duplicateLimitByAsFirstGrandchild(limitBy);
                } else {
                    return enrich.replaceChild(limitBy.replaceChild(enrich.child()));
                }
            }
        } else if (limitBy.child() instanceof Join join && join.config().type() == JoinTypes.LEFT && join instanceof InlineJoin == false) {
            if (groupingsReferenceAttributeNotInOutput(limitBy, join.left())) {
                return limitBy;
            }
            return duplicateLimitByAsFirstGrandchild(limitBy);
        }
        return limitBy;
    }

    private static LimitBy combineLimitBys(LimitBy upper, LimitBy lower, FoldContext ctx) {
        var upperLimitValue = (int) upper.limitPerGroup().fold(ctx);
        var lowerLimitValue = (int) lower.limitPerGroup().fold(ctx);
        if (lowerLimitValue <= upperLimitValue) {
            return lower;
        } else {
            return new LimitBy(upper.source(), upper.limitPerGroup(), lower.child(), upper.groupings(), upper.duplicated());
        }
    }

    /**
     * Returns {@code true} if any attribute referenced by the LimitBy's groupings is defined by the child node
     * (i.e. present in the child's output but absent from the grandchild's output). Pushing a grouped limit
     * past such a child would leave the grouping attribute unresolved.
     */
    private static boolean groupingsReferenceAttributeDefinedByChild(LimitBy limitBy, UnaryPlan child) {
        return groupingsReferenceAttributeNotInOutput(limitBy, child.child());
    }

    /**
     * Returns {@code true} if any attribute referenced by the LimitBy's groupings is absent from the given plan's output.
     * Duplicating the LimitBy below such a plan would leave the grouping attribute unresolved.
     */
    private static boolean groupingsReferenceAttributeNotInOutput(LimitBy limitBy, LogicalPlan plan) {
        Set<NameId> outputIds = new HashSet<>();
        for (Attribute a : plan.output()) {
            outputIds.add(a.id());
        }
        for (Expression g : limitBy.groupings()) {
            if (g instanceof Attribute a && outputIds.contains(a.id()) == false) {
                return true;
            }
        }
        return false;
    }

    /**
     * Duplicate the LimitBy past its child if it wasn't duplicated yet.
     */
    private static LimitBy duplicateLimitByAsFirstGrandchild(LimitBy limitBy) {
        if (limitBy.duplicated()) {
            return limitBy;
        }

        List<LogicalPlan> grandChildren = limitBy.child().children();
        LogicalPlan firstGrandChild = grandChildren.getFirst();
        LogicalPlan newFirstGrandChild = limitBy.replaceChild(firstGrandChild);

        List<LogicalPlan> newGrandChildren = new ArrayList<>();
        newGrandChildren.add(newFirstGrandChild);
        for (int i = 1; i < grandChildren.size(); i++) {
            newGrandChildren.add(grandChildren.get(i));
        }

        LogicalPlan newChild = limitBy.child().replaceChildren(newGrandChildren);
        return limitBy.replaceChild(newChild).withDuplicated(true);
    }
}
