/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.physical;

import org.elasticsearch.xpack.esql.optimizer.PhysicalOptimizerContext;
import org.elasticsearch.xpack.esql.plan.physical.ExchangeDistribution;
import org.elasticsearch.xpack.esql.plan.physical.ExchangeExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.plan.physical.ShuffleExchangeExec;
import org.elasticsearch.xpack.esql.plugin.ShuffleGate;
import org.elasticsearch.xpack.esql.rule.ParameterizedRule;

import java.util.List;

/** Replaces legacy coordinator exchanges with explicit gather stage boundaries. */
public final class NormalizeExchangesForShuffle extends ParameterizedRule<PhysicalPlan, PhysicalPlan, PhysicalOptimizerContext> {
    @Override
    public PhysicalPlan apply(PhysicalPlan plan, PhysicalOptimizerContext context) {
        if (ShuffleGate.allowed(context.configuration(), plan) == false) {
            return plan;
        }
        return plan.transformUp(
            ExchangeExec.class,
            exchange -> new ShuffleExchangeExec(
                exchange.source(),
                exchange.inBetweenAggs() ? exchange.output() : List.of(),
                exchange.inBetweenAggs(),
                ExchangeDistribution.GATHER,
                List.of(),
                exchange.child()
            )
        );
    }
}
