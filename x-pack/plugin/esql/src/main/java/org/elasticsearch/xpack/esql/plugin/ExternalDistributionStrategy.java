/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.LimitBy;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.TopN;
import org.elasticsearch.xpack.esql.plan.logical.TopNBy;
import org.elasticsearch.xpack.esql.plan.physical.AggregateExec;
import org.elasticsearch.xpack.esql.plan.physical.FragmentExec;
import org.elasticsearch.xpack.esql.plan.physical.LimitByExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.plan.physical.TopNByExec;
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
     * one row per driver instead of one merged row, and a {@code TopN} / {@code TopNBy} / {@code LimitBy} its own
     * slice per driver. Such a plan needs a gather boundary above the scan.
     *
     * <p>Looks at both physical nodes and the logical plan inside a {@link FragmentExec}: a UNION leaf still holds
     * a pushed-down aggregation as a logical {@link Aggregate} in the fragment, and collapsing that leaf's exchange
     * would drop the gather the same way collapsing an {@link AggregateExec} would.
     *
     * <p>A plain {@code LIMIT} is deliberately not in this set: {@code LimitOperator.Factory} builds a single
     * {@code Limiter} and hands that same instance to every driver it creates, so a limit is already enforced
     * across all of them and stays correct without a gather.
     *
     * <p>{@link #hasReducingOperator(PhysicalPlan)} asks a different question (whether a hop pays) of the same
     * operator set: a per-driver-unsafe operator both needs a gather when the scan stays local and is worth
     * shipping after a remote reduce. The two methods must not diverge.
     */
    static boolean needsGatherBoundary(PhysicalPlan plan) {
        return plan.anyMatch(
            n -> isPerDriverUnsafePhysical(n) || (n instanceof FragmentExec fragment && fragmentHoldsPerDriverUnsafeLogical(fragment))
        );
    }

    /**
     * Whether distributing this read would have a data node reduce rows before shipping them back.
     * Same operators as {@link #needsGatherBoundary}: a UNION child still holds its pushed-down
     * aggregation as a logical {@link Aggregate} inside {@link FragmentExec}, so the physical tree
     * alone would report no reduction.
     *
     * <p>A plain {@code Limit} is absent on purpose: it does reduce rows, but a limit-only read is
     * cheapest where the limit is applied once, and the shared {@code Limiter} already makes a local
     * collapse correct.
     */
    static boolean hasReducingOperator(PhysicalPlan plan) {
        return needsGatherBoundary(plan);
    }

    private static boolean isPerDriverUnsafePhysical(PhysicalPlan node) {
        return node instanceof AggregateExec || node instanceof TopNExec || node instanceof TopNByExec || node instanceof LimitByExec;
    }

    private static boolean fragmentHoldsPerDriverUnsafeLogical(FragmentExec fragment) {
        return fragment.fragment().anyMatch(ExternalDistributionStrategy::isPerDriverUnsafeLogical);
    }

    private static boolean isPerDriverUnsafeLogical(LogicalPlan node) {
        return node instanceof Aggregate || node instanceof TopN || node instanceof TopNBy || node instanceof LimitBy;
    }
}
