/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.physical;

import org.elasticsearch.xpack.esql.optimizer.PhysicalOptimizerContext;
import org.elasticsearch.xpack.esql.plan.physical.EvalExec;
import org.elasticsearch.xpack.esql.plan.physical.ExchangeDistribution;
import org.elasticsearch.xpack.esql.plan.physical.FilterExec;
import org.elasticsearch.xpack.esql.plan.physical.FragmentExec;
import org.elasticsearch.xpack.esql.plan.physical.LimitExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.plan.physical.ProjectExec;
import org.elasticsearch.xpack.esql.plan.physical.ShuffleExchangeExec;
import org.elasticsearch.xpack.esql.plan.physical.TopNExec;
import org.elasticsearch.xpack.esql.plan.physical.UnaryExec;
import org.elasticsearch.xpack.esql.plugin.ShuffleGate;
import org.elasticsearch.xpack.esql.rule.ParameterizedRule;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

/** Pushes safe row operations below gather boundaries and duplicates limits for worker-side pruning. */
public final class PushDownPastShuffleGather extends ParameterizedRule<PhysicalPlan, PhysicalPlan, PhysicalOptimizerContext> {
    @Override
    public PhysicalPlan apply(PhysicalPlan plan, PhysicalOptimizerContext context) {
        if (ShuffleGate.allowed(context.configuration(), plan) == false) {
            return plan;
        }
        if (plan.anyMatch(ShuffleExchangeExec.class::isInstance) == false) {
            return plan;
        }

        PhysicalPlan current = plan;
        while (true) {
            AtomicBoolean changed = new AtomicBoolean();
            PhysicalPlan rewritten = current.transformUp(PhysicalPlan.class, node -> {
                PhysicalPlan replacement = pushBelowGather(node);
                if (replacement != node) {
                    changed.set(true);
                }
                return replacement;
            });
            if (changed.get() == false) {
                return rewritten;
            }
            current = rewritten;
        }
    }

    private static PhysicalPlan pushBelowGather(PhysicalPlan node) {
        if (!(node instanceof UnaryExec upper)) {
            return node;
        }
        if (!(upper.child() instanceof ShuffleExchangeExec gather)) {
            return node;
        }
        if (gather.distribution() != ExchangeDistribution.GATHER || gather.inBetweenAggs()) {
            return node;
        }

        PhysicalPlan child = gather.child();
        if (upper instanceof EvalExec || upper instanceof FilterExec || upper instanceof ProjectExec) {
            return gatherWithChild(gather, upper.replaceChild(child));
        }

        if (upper instanceof LimitExec || upper instanceof TopNExec) {
            if (child instanceof FragmentExec || isCopyOf(upper, child)) {
                return node;
            }
            return upper.replaceChild(gatherWithChild(gather, upper.replaceChild(child)));
        }
        return node;
    }

    private static ShuffleExchangeExec gatherWithChild(ShuffleExchangeExec gather, PhysicalPlan child) {
        return new ShuffleExchangeExec(gather.source(), List.of(), false, ExchangeDistribution.GATHER, List.of(), child);
    }

    private static boolean isCopyOf(UnaryExec upper, PhysicalPlan child) {
        if (upper.getClass() != child.getClass()) {
            return false;
        }
        if (upper instanceof LimitExec limit && child instanceof LimitExec childLimit) {
            return Objects.equals(limit.limit(), childLimit.limit());
        }
        if (upper instanceof TopNExec topN && child instanceof TopNExec childTopN) {
            return Objects.equals(topN.limit(), childTopN.limit()) && Objects.equals(topN.order(), childTopN.order());
        }
        return false;
    }
}
