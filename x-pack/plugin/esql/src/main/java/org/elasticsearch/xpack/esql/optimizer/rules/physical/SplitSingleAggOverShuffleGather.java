/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.physical;

import org.elasticsearch.compute.aggregation.AggregatorMode;
import org.elasticsearch.xpack.esql.optimizer.PhysicalOptimizerContext;
import org.elasticsearch.xpack.esql.plan.physical.AggregateExec;
import org.elasticsearch.xpack.esql.plan.physical.ExchangeDistribution;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.plan.physical.ShuffleExchangeExec;
import org.elasticsearch.xpack.esql.plugin.ShuffleGate;
import org.elasticsearch.xpack.esql.rule.ParameterizedRule;

import java.util.List;

/** Splits eligible single-stage aggregations across worker gather boundaries. */
public final class SplitSingleAggOverShuffleGather extends ParameterizedRule<PhysicalPlan, PhysicalPlan, PhysicalOptimizerContext> {
    @Override
    public PhysicalPlan apply(PhysicalPlan plan, PhysicalOptimizerContext context) {
        if (ShuffleGate.allowed(context.configuration(), plan) == false) {
            return plan;
        }
        if (context.configuration().pragmas().shuffleAgg() == false) {
            return plan;
        }
        if (plan.anyMatch(ShuffleExchangeExec.class::isInstance) == false) {
            return plan;
        }
        return plan.transformUp(AggregateExec.class, aggregate -> {
            if (aggregate.getClass() != AggregateExec.class
                || aggregate.getMode() != AggregatorMode.SINGLE
                || ShuffleRuleUtils.hasSupportedGroupKeyTypes(aggregate.groupings()) == false) {
                return aggregate;
            }
            if (!(aggregate.child() instanceof ShuffleExchangeExec gather)) {
                return aggregate;
            }
            if (gather.distribution() != ExchangeDistribution.GATHER || gather.inBetweenAggs()) {
                return aggregate;
            }

            var intermediate = aggregate.intermediateAttributes();
            var initial = new AggregateExec(
                aggregate.source(),
                gather.child(),
                aggregate.groupings(),
                aggregate.aggregates(),
                AggregatorMode.INITIAL,
                intermediate,
                aggregate.estimatedRowSize()
            );
            var intermediateGather = new ShuffleExchangeExec(
                gather.source(),
                intermediate,
                true,
                ExchangeDistribution.GATHER,
                List.of(),
                initial
            );
            return new AggregateExec(
                aggregate.source(),
                intermediateGather,
                aggregate.groupings(),
                aggregate.aggregates(),
                AggregatorMode.FINAL,
                intermediate,
                aggregate.estimatedRowSize()
            );
        });
    }
}
