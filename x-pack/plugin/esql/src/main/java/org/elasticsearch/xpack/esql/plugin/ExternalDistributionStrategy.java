/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.LimitBy;
import org.elasticsearch.xpack.esql.plan.logical.TopN;
import org.elasticsearch.xpack.esql.plan.logical.TopNBy;
import org.elasticsearch.xpack.esql.plan.physical.AggregateExec;
import org.elasticsearch.xpack.esql.plan.physical.FragmentExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.plan.physical.TopNExec;

/**
 * Decides whether an external source query should be distributed across eligible
 * remote workers or executed locally on the coordinator. Index-role nodes are never
 * selected as remote workers; an empty eligible-worker set is a {@code LOCAL} fallback,
 * not a failure.
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
     * <p>Single home for the gather-correctness rule used by {@link ComputeService} (whether a scan staying
     * local must still keep its exchange). Whether a hop is worth it is a separate question and lives on
     * {@link #hasReducingOperator(PhysicalPlan)}.
     */
    static boolean needsGatherBoundary(PhysicalPlan plan) {
        return plan.anyMatch(n -> n instanceof AggregateExec || n instanceof TopNExec);
    }

    /**
     * Whether distributing this read would have a data node reduce rows before shipping them back.
     * Broader than {@link #needsGatherBoundary}: that rule is a correctness check for a local read's
     * operators, this one only decides whether a hop pays. A UNION child still holds its pushed-down
     * aggregation as a logical {@link Aggregate} inside {@link FragmentExec}, so the physical tree
     * alone would report no reduction.
     *
     * <p>A plain {@code Limit} is absent on purpose: it does reduce rows, but a limit-only read is
     * cheapest where the limit is applied once.
     */
    static boolean hasReducingOperator(PhysicalPlan plan) {
        return needsGatherBoundary(plan)
            || plan.anyMatch(node -> node instanceof FragmentExec fragment && fragmentHoldsReducingLogical(fragment));
    }

    private static boolean fragmentHoldsReducingLogical(FragmentExec fragment) {
        return fragment.fragment()
            .anyMatch(n -> n instanceof Aggregate || n instanceof TopN || n instanceof TopNBy || n instanceof LimitBy);
    }
}
