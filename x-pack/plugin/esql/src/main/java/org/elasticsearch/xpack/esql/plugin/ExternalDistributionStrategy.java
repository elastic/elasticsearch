/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.xpack.esql.plan.physical.AggregateExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.plan.physical.TopNExec;

/**
 * Decides whether an external source query should be distributed across data nodes
 * or executed locally on the coordinator.
 */
public interface ExternalDistributionStrategy {

    ExternalDistributionPlan planDistribution(ExternalDistributionContext context);

    /**
     * Whether the plan contains an operator that cannot simply be replicated across parallel scan drivers, because
     * each driver would run it over its own slice of the input and emit its own result: an aggregation would produce
     * one row per driver instead of one merged row, and a {@code TopN} its own top-N per driver. Such a plan needs a
     * gather boundary above the scan.
     *
     * <p>A {@code LIMIT} is deliberately not in this set: {@code LimitOperator.Factory} builds a single
     * {@code Limiter} and hands that same instance to every driver it creates, so a limit is already enforced
     * across all of them and stays correct without a gather.
     *
     * <p>Single home for the rule, shared by {@link AdaptiveStrategy} (deciding whether the scan is worth
     * distributing) and {@link ComputeService} (deciding whether a scan staying local must still keep its exchange).
     * If the two diverged, a plan could be collapsed onto parallel drivers by one and assumed gathered by the other.
     */
    static boolean needsGatherBoundary(PhysicalPlan plan) {
        return plan.anyMatch(n -> n instanceof AggregateExec || n instanceof TopNExec);
    }
}
