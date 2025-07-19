/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.physical;

import org.elasticsearch.compute.aggregation.AggregatorMode;
import org.elasticsearch.xpack.esql.expression.function.grouping.GroupingFunction;
import org.elasticsearch.xpack.esql.optimizer.PhysicalOptimizerRules;
import org.elasticsearch.xpack.esql.plan.physical.AggregateExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;

/**
 * Collapses two-phase aggregation into a single phase when possible.
 * For example, in FROM .. | STATS first | STATS second, the STATS second aggregation
 * can be executed in a single phase on the coordinator instead of two phases.
 */
public class SinglePhaseAggregate extends PhysicalOptimizerRules.OptimizerRule<AggregateExec> {
    @Override
    protected PhysicalPlan rule(AggregateExec plan) {
        if (plan instanceof AggregateExec parent
            && parent.getMode() == AggregatorMode.FINAL
            && parent.child() instanceof AggregateExec child
            && child.getMode() == AggregatorMode.INITIAL) {
            if (parent.groupings()
                .stream()
                .noneMatch(group -> group.anyMatch(expr -> expr instanceof GroupingFunction.NonEvaluatableGroupingFunction))) {
                return child.withMode(AggregatorMode.SINGLE);
            }
        }
        return plan;
    }
}
