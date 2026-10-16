/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.physical;

import org.elasticsearch.xpack.esql.plan.physical.ExchangeDistribution;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.plan.physical.ShuffleExchangeExec;
import org.elasticsearch.xpack.esql.rule.Rule;

/** Removes redundant gather boundaries directly below another shuffle exchange. */
public final class CollapseStackedShuffleExchanges extends Rule<PhysicalPlan, PhysicalPlan> {
    @Override
    public PhysicalPlan apply(PhysicalPlan plan) {
        return plan.transformUp(ShuffleExchangeExec.class, exchange -> {
            if (exchange.inBetweenAggs() == false
                && exchange.child() instanceof ShuffleExchangeExec gather
                && gather.distribution() == ExchangeDistribution.GATHER
                && gather.inBetweenAggs() == false) {
                return exchange.replaceChild(gather.child());
            }
            return exchange;
        });
    }
}
