/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.approximation;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Count;
import org.elasticsearch.xpack.esql.expression.function.aggregate.CountApproximate;
import org.elasticsearch.xpack.esql.expression.function.aggregate.StdDev;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Sum;
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.Fork;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.SampledAggregate;

import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;

public class ForkApproximationTests extends ApproximationTestCase {

    public void testPlans_statsAfterFork() throws Exception {
        assumeTrue("needs approximation fork", EsqlCapabilities.Cap.APPROXIMATION_FORK.isEnabled());

        // This test simulates a source count of 10^10, and a filtered count of 10^10.

        LogicalPlan originalPlan = getLogicalPlan("FROM test | FORK (WHERE emp_no > 42) (WHERE emp_no <= 42) | STATS SUM(emp_no)");
        LogicalPlan mainPlan = ApproximationPlan.get(
            originalPlan,
            ApproximationVerifier.verifyPlanOrThrow(originalPlan, TransportVersion.current()),
            ApproximationSettings.DEFAULT
        );
        ApproximationDriver approximation = ApproximationDriver.create(mainPlan, ApproximationSettings.DEFAULT);

        // The first subplan should be the source count.
        LogicalPlan subplan = approximation.firstSubPlan();
        assertThat(subplan, not(hasPlan(Filter.class)));
        assertThat(subplan, not(hasPlan(SampledAggregate.class)));
        assertThat(subplan, hasPlan(Aggregate.class, withAggs(Count.class)));

        // Source count of 10^10.
        approximation.newMainPlan(mainPlan, newCountResult(10_000_000_000L));
        // There's filtering, so start with a count plan with sampling with probability 10^4 / 10^10.
        subplan = approximation.firstSubPlan();
        assertThat(subplan, hasPlan(Filter.class));
        assertThat(subplan, not(hasPlan(Aggregate.class)));
        assertThat(subplan, hasPlan(SampledAggregate.class, withProbability(1e-6), withAggs(CountApproximate.class)));

        // Sampled-corrected filtered count of 10^10 (so actual count of 10,000), so no more subplans.
        mainPlan = approximation.newMainPlan(mainPlan, newCountResult(10_000_000_000L));
        subplan = approximation.firstSubPlan();
        assertThat(subplan, nullValue());

        // The main plan should have sampling with probability 10^5 / 10^10.
        assertThat(mainPlan, hasPlan(Filter.class));
        assertThat(mainPlan, not(hasPlan(Aggregate.class)));
        assertThat(mainPlan, hasPlan(SampledAggregate.class, withProbability(1e-5), withAggs(Sum.class)));
    }

    public void testPlans_statsInsideFork() throws Exception {
        assumeTrue("needs approximation fork", EsqlCapabilities.Cap.APPROXIMATION_FORK.isEnabled());

        // This test simulates a source count of 10^10. Both branches have filtering,
        // so they need count subplans. The source count is shared (run once), and the
        // count subplans for both branches are combined into a single FORK plan.

        LogicalPlan originalPlan = getLogicalPlan(
            "FROM test | FORK (WHERE emp_no > 42 | STATS SUM(emp_no)) (WHERE emp_no <= 42 | STATS STD_DEV(emp_no))"
        );
        LogicalPlan mainPlan = ApproximationPlan.get(
            originalPlan,
            ApproximationVerifier.verifyPlanOrThrow(originalPlan, TransportVersion.current()),
            ApproximationSettings.DEFAULT
        );
        ApproximationDriver approximation = ApproximationDriver.create(mainPlan, ApproximationSettings.DEFAULT);

        // The first subplan should be the shared source count (run once, not per branch).
        LogicalPlan subplan = approximation.firstSubPlan();
        assertThat(subplan, not(hasPlan(Fork.class)));
        assertThat(subplan, not(hasPlan(Filter.class)));
        assertThat(subplan, not(hasPlan(SampledAggregate.class)));
        assertThat(subplan, hasPlan(Aggregate.class, withAggs(Count.class)));

        // Source count of 10^10. Both branches have filtering, so both need count subplans.
        mainPlan = approximation.newMainPlan(mainPlan, newCountResult(10_000_000_000L));

        // The next subplan should be a FORK combining both branches' count subplans.
        subplan = approximation.firstSubPlan();
        assertThat(subplan, hasPlan(Fork.class));
        assertThat(subplan, hasPlan(Filter.class));
        assertThat(subplan, not(hasPlan(Aggregate.class)));
        assertThat(subplan, hasPlan(SampledAggregate.class, withProbability(1e-6), withAggs(CountApproximate.class)));

        // Process the FORK count result: both branches converge in one iteration.
        // Branch 0: sample-corrected count 10^10 (actual 10,000) -> p = 1e-5
        // Branch 1: sample-corrected count 8*10^9 (actual 8,000) -> p = 1.25e-5
        mainPlan = approximation.newMainPlan(mainPlan, newForkCountResult(10_000_000_000L, 8_000_000_000L));
        subplan = approximation.firstSubPlan();
        assertThat(subplan, nullValue());

        // The main plan should have per-branch sampling probabilities.
        assertThat(mainPlan, hasPlan(Filter.class));
        assertThat(mainPlan, not(hasPlan(Aggregate.class)));
        assertThat(mainPlan, hasPlan(SampledAggregate.class, withProbability(1e-5), withAggs(Sum.class)));
        assertThat(mainPlan, hasPlan(SampledAggregate.class, withProbability(1.25e-5), withAggs(StdDev.class)));
    }
}
