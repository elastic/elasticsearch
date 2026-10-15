/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.physical;

import org.elasticsearch.compute.aggregation.AggregatorMode;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.optimizer.PhysicalOptimizerContext;
import org.elasticsearch.xpack.esql.plan.physical.AggregateExec;
import org.elasticsearch.xpack.esql.plan.physical.ExchangeDistribution;
import org.elasticsearch.xpack.esql.plan.physical.ExchangeExec;
import org.elasticsearch.xpack.esql.plan.physical.FragmentExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.plan.physical.ShuffleExchangeExec;
import org.elasticsearch.xpack.esql.plugin.ShuffleGate;
import org.elasticsearch.xpack.esql.rule.ParameterizedRule;

import java.util.List;

/** Replaces a final aggregation exchange with hash partitioning and a gather boundary. */
public final class ShuffleFinalAggregate extends ParameterizedRule<PhysicalPlan, PhysicalPlan, PhysicalOptimizerContext> {
    @Override
    public PhysicalPlan apply(PhysicalPlan plan, PhysicalOptimizerContext context) {
        if (ShuffleGate.allowed(context.configuration(), plan) == false) {
            return plan;
        }
        if (context.configuration().pragmas().shuffleAgg() == false) {
            return plan;
        }
        return plan.transformUp(AggregateExec.class, aggregate -> {
            if (aggregate.getClass() != AggregateExec.class
                || aggregate.getMode() != AggregatorMode.FINAL
                || ShuffleRuleUtils.hasSupportedGroupings(aggregate.groupings()) == false) {
                return aggregate;
            }
            if (!(aggregate.child() instanceof ExchangeExec exchange)) {
                return aggregate;
            }
            if (exchange.inBetweenAggs() == false) {
                return aggregate;
            }
            if (!(exchange.child() instanceof FragmentExec fragment)) {
                return aggregate;
            }

            List<Attribute> intermediate = exchange.output();
            if (intermediate.size() < aggregate.groupings().size()) {
                return aggregate;
            }
            var hash = new ShuffleExchangeExec(
                exchange.source(),
                intermediate,
                true,
                ExchangeDistribution.HASH,
                intermediate.subList(0, aggregate.groupings().size()),
                fragment
            );
            return new ShuffleExchangeExec(
                aggregate.source(),
                List.of(),
                false,
                ExchangeDistribution.GATHER,
                List.of(),
                aggregate.replaceChild(hash)
            );
        });
    }
}
