/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.physical.local;

import org.elasticsearch.xpack.esql.core.expression.AttributeSet;
import org.elasticsearch.xpack.esql.core.expression.Expressions;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.optimizer.PhysicalOptimizerRules;
import org.elasticsearch.xpack.esql.plan.physical.EvalExec;
import org.elasticsearch.xpack.esql.plan.physical.HighlightExec;
import org.elasticsearch.xpack.esql.plan.physical.LimitExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.plan.physical.TopNExec;
import org.elasticsearch.xpack.esql.plan.physical.UnaryExec;

import java.util.HashSet;
import java.util.Set;

/**
 * Moves {@link HighlightExec} after limits and sorts so it processes only the rows they return.
 * <p>
 * The rule swaps the highlight with one parent at a time. Repeated optimizer passes can therefore move it past both the
 * {@link EvalExec} and {@link TopNExec} produced for a sort expression.
 * <p>
 * {@link HighlightExec} appends columns without changing row count or order. It cannot move past an operator that reads one
 * of those columns.
 * <p>
 * Moving the highlight can also expose the limit to {@link PushLimitToSource} or {@link PushTopNToSource}. If Lucene pushdown
 * is unavailable, the local {@link TopNExec} still reduces the rows passed to the highlight.
 * <p>
 * This is a local physical rule. Applying the rewrite to the logical plan would move highlighting to the coordinator and
 * require data nodes to send the raw {@code ON} field across the exchange.
 */
public final class PushHighlightPastNonDependents extends PhysicalOptimizerRules.OptimizerRule<UnaryExec> {

    @Override
    protected PhysicalPlan rule(UnaryExec plan) {
        if (plan.child() instanceof HighlightExec highlight
            && hoistablePast(plan, highlight)
            && plan.references().intersect(AttributeSet.of(highlight.generatedFields())).isEmpty()) {
            return highlight.replaceChild(plan.replaceChild(highlight.child()));
        }
        return plan;
    }

    /**
     * Keep this list explicit. Other unary operators may change row count, drop the {@code ON} field, or move execution to the
     * coordinator.
     * <p>
     * The logical optimizer already moves filters below the highlight.
     */
    private static boolean hoistablePast(UnaryExec plan, HighlightExec highlight) {
        if (plan instanceof LimitExec || plan instanceof TopNExec) {
            return true;
        }
        return plan instanceof EvalExec eval && evalShadowsHighlightName(eval, highlight) == false;
    }

    /**
     * {@link EvalExec} and {@link HighlightExec} replace child attributes with the same name. Swapping them would change which
     * attribute survives when an eval field has the name of a generated column. It would also hide the highlight input when an
     * eval field has the name of an {@code ON} field. The ID-based {@code references()} check does not catch name collisions.
     */
    private static boolean evalShadowsHighlightName(EvalExec eval, HighlightExec highlight) {
        Set<String> highlightNames = new HashSet<>(Expressions.names(highlight.generatedFields()));
        highlightNames.addAll(highlight.references().names());
        return eval.fields().stream().map(NamedExpression::name).anyMatch(highlightNames::contains);
    }
}
