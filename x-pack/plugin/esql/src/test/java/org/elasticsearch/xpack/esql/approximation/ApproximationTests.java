/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.approximation;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Count;
import org.elasticsearch.xpack.esql.expression.function.aggregate.CountApproximate;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Sum;
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.MvExpand;
import org.elasticsearch.xpack.esql.plan.logical.SampledAggregate;

import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;

public class ApproximationTests extends ApproximationTestCase {

    public void testPlans_noData() {
        // This test simulates a source count of 0.

        LogicalPlan originalPlan = getLogicalPlan("FROM test | STATS SUM(emp_no)");
        LogicalPlan mainPlan = ApproximationPlan.get(
            originalPlan,
            ApproximationVerifier.verifyPlanOrThrow(originalPlan, TransportVersion.current()),
            ApproximationSettings.DEFAULT
        );
        ApproximationDriver approximation = ApproximationDriver.create(mainPlan, ApproximationSettings.DEFAULT);

        // The first subplan should be the source count.
        LogicalPlan subplan = approximation.firstSubPlan();
        assertThat(subplan, not(hasPlan(SampledAggregate.class)));
        assertThat(subplan, hasPlan(Aggregate.class, withAggs(Count.class)));

        // Source count of 0, so no more subplans.
        mainPlan = approximation.newMainPlan(mainPlan, newCountResult(0));
        subplan = approximation.firstSubPlan();
        assertThat(subplan, nullValue());

        // The main plan shouldn't have any sampling.
        assertThat(mainPlan, not(hasPlan(SampledAggregate.class)));
        assertThat(mainPlan, hasPlan(Aggregate.class, withAggs(Sum.class)));
    }

    public void testPlans_smallDataNoFilters() {
        // This test simulates a source count of 1_000.

        LogicalPlan originalPlan = getLogicalPlan("FROM test | STATS SUM(emp_no)");
        LogicalPlan mainPlan = ApproximationPlan.get(
            originalPlan,
            ApproximationVerifier.verifyPlanOrThrow(originalPlan, TransportVersion.current()),
            ApproximationSettings.DEFAULT
        );
        ApproximationDriver approximation = ApproximationDriver.create(mainPlan, ApproximationSettings.DEFAULT);

        // The first subplan should be the source count.
        LogicalPlan subplan = approximation.firstSubPlan();
        assertThat(subplan, not(hasPlan(SampledAggregate.class)));
        assertThat(subplan, hasPlan(Aggregate.class, withAggs(Count.class)));

        // Source count of 1000, so no more subplans.
        mainPlan = approximation.newMainPlan(mainPlan, newCountResult(1_000));
        subplan = approximation.firstSubPlan();
        assertThat(subplan, nullValue());

        // The main plan shouldn't have any sampling.
        assertThat(mainPlan, not(hasPlan(SampledAggregate.class)));
        assertThat(mainPlan, hasPlan(Aggregate.class, withAggs(Sum.class)));
    }

    public void testPlans_largeDataNoFilters() {
        // This test simulates a source count of 10^9.

        LogicalPlan originalPlan = getLogicalPlan("FROM test | STATS SUM(emp_no)");
        LogicalPlan mainPlan = ApproximationPlan.get(
            originalPlan,
            ApproximationVerifier.verifyPlanOrThrow(originalPlan, TransportVersion.current()),
            ApproximationSettings.DEFAULT
        );
        ApproximationDriver approximation = ApproximationDriver.create(mainPlan, ApproximationSettings.DEFAULT);

        // The first subplan should be the source count.
        LogicalPlan subplan = approximation.firstSubPlan();
        assertThat(subplan, not(hasPlan(SampledAggregate.class)));
        assertThat(subplan, hasPlan(Aggregate.class, withAggs(Count.class)));

        // Source count of 10^9.
        mainPlan = approximation.newMainPlan(mainPlan, newCountResult(1_000_000_000));
        // No filtering, so no more subplans.
        subplan = approximation.firstSubPlan();
        assertThat(subplan, nullValue());

        // The main plan should have sampling with probability 10^5 / 10^9.
        assertThat(mainPlan, not(hasPlan(Aggregate.class)));
        assertThat(mainPlan, hasPlan(SampledAggregate.class, withProbability(1e-4), withAggs(Sum.class)));
    }

    public void testPlans_largeDataAfterFiltering() {
        // This test simulates a source count of 10^12, and a filtered count of 10^9.

        LogicalPlan originalPlan = getLogicalPlan("FROM test | WHERE emp_no < 1 | STATS SUM(emp_no)");
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

        // Source count of 10^12.
        approximation.newMainPlan(mainPlan, newCountResult(1_000_000_000_000L));
        // There's filtering, so start with a count plan with sampling with probability 10^4 / 10^12.
        subplan = approximation.firstSubPlan();
        assertThat(subplan, hasPlan(Filter.class));
        assertThat(subplan, not(hasPlan(Aggregate.class)));
        assertThat(subplan, hasPlan(SampledAggregate.class, withProbability(1e-8), withAggs(CountApproximate.class)));

        // Sampled-corrected filtered count of 10^9 (so actual count of 10), so increase the sample probability.
        approximation.newMainPlan(mainPlan, newCountResult(1_000_000_000));
        subplan = approximation.firstSubPlan();
        assertThat(subplan, hasPlan(Filter.class));
        assertThat(subplan, not(hasPlan(Aggregate.class)));
        assertThat(subplan, hasPlan(SampledAggregate.class, withProbability(1e-5), withAggs(CountApproximate.class)));

        // Sampled-corrected filtered count of 10^9 (so actual count of 10_000), so no more subplans.
        mainPlan = approximation.newMainPlan(mainPlan, newCountResult(1_000_000_000));
        subplan = approximation.firstSubPlan();
        assertThat(subplan, nullValue());

        // The main plan should have sampling with probability 10^5 / 10^9.
        assertThat(mainPlan, hasPlan(Filter.class));
        assertThat(mainPlan, not(hasPlan(Aggregate.class)));
        assertThat(mainPlan, hasPlan(SampledAggregate.class, withProbability(1e-4), withAggs(Sum.class)));
    }

    public void testPlans_smallDataAfterFiltering() {
        // This test simulates a source count of 10^18, and a filtered count of 100.

        LogicalPlan originalPlan = getLogicalPlan("FROM test | WHERE emp_no < 1 | STATS SUM(emp_no)");
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

        // Source count of 10^18.
        approximation.newMainPlan(mainPlan, newCountResult(1_000_000_000_000_000_000L));
        // There's filtering, so start with a count plan with sampling with probability 10^4 / 10^18.
        subplan = approximation.firstSubPlan();
        assertThat(subplan, hasPlan(Filter.class));
        assertThat(subplan, not(hasPlan(Aggregate.class)));
        assertThat(subplan, hasPlan(SampledAggregate.class, withProbability(1e-14), withAggs(CountApproximate.class)));

        // Filtered count of 0, so increase the sample probability.
        approximation.newMainPlan(mainPlan, newCountResult(0));
        subplan = approximation.firstSubPlan();
        assertThat(subplan, hasPlan(Filter.class));
        assertThat(subplan, not(hasPlan(Aggregate.class)));
        assertThat(subplan, hasPlan(SampledAggregate.class, withProbability(1e-10), withAggs(CountApproximate.class)));

        // Filtered count of 0, so increase the sample probability.
        approximation.newMainPlan(mainPlan, newCountResult(0));
        subplan = approximation.firstSubPlan();
        assertThat(subplan, hasPlan(Filter.class));
        assertThat(subplan, not(hasPlan(Aggregate.class)));
        assertThat(subplan, hasPlan(SampledAggregate.class, withProbability(1e-6), withAggs(CountApproximate.class)));

        // Filtered count of 0, so increase the sample probability.
        approximation.newMainPlan(mainPlan, newCountResult(0));
        subplan = approximation.firstSubPlan();
        assertThat(subplan, hasPlan(Filter.class));
        assertThat(subplan, not(hasPlan(Aggregate.class)));
        assertThat(subplan, hasPlan(SampledAggregate.class, withProbability(1e-2), withAggs(CountApproximate.class)));

        // Filtered count of 0, so no more subplans.
        mainPlan = approximation.newMainPlan(mainPlan, newCountResult(0));
        subplan = approximation.firstSubPlan();
        assertThat(subplan, nullValue());

        // The main plan shouldn't have any sampling.
        assertThat(mainPlan, hasPlan(Filter.class));
        assertThat(mainPlan, not(hasPlan(SampledAggregate.class)));
        assertThat(mainPlan, hasPlan(Aggregate.class, withAggs(Sum.class)));
    }

    public void testPlans_smallDataBeforeFiltering() {
        // This test simulates a source count of 1_000, and a filtered count of 10.

        LogicalPlan originalPlan = getLogicalPlan("FROM test | WHERE gender == \"X\" | STATS SUM(emp_no)");
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

        // Source count of 1000, so no more subplans.
        mainPlan = approximation.newMainPlan(mainPlan, newCountResult(1_000));
        subplan = approximation.firstSubPlan();
        assertThat(subplan, nullValue());

        // The main plan shouldn't have any sampling.
        assertThat(mainPlan, hasPlan(Filter.class));
        assertThat(mainPlan, not(hasPlan(SampledAggregate.class)));
        assertThat(mainPlan, hasPlan(Aggregate.class, withAggs(Sum.class)));
    }

    public void testPlans_smallDataBeforeMvExpanding() {
        // This test simulates a source count of 1_000, and an irrelevant mv_expanded count

        LogicalPlan originalPlan = getLogicalPlan("FROM test | MV_EXPAND emp_no | STATS SUM(emp_no)");
        LogicalPlan mainPlan = ApproximationPlan.get(
            originalPlan,
            ApproximationVerifier.verifyPlanOrThrow(originalPlan, TransportVersion.current()),
            ApproximationSettings.DEFAULT
        );
        ApproximationDriver approximation = ApproximationDriver.create(mainPlan, ApproximationSettings.DEFAULT);

        // The first subplan should be the source count.
        LogicalPlan subplan = approximation.firstSubPlan();
        assertThat(subplan, not(hasPlan(MvExpand.class)));
        assertThat(subplan, not(hasPlan(SampledAggregate.class)));
        assertThat(subplan, hasPlan(Aggregate.class, withAggs(Count.class)));

        // Source count of 1000, so no more subplans.
        mainPlan = approximation.newMainPlan(mainPlan, newCountResult(1_000));
        subplan = approximation.firstSubPlan();
        assertThat(subplan, nullValue());

        // The main plan shouldn't have any sampling.
        assertThat(mainPlan, hasPlan(MvExpand.class));
        assertThat(mainPlan, not(hasPlan(SampledAggregate.class)));
        assertThat(mainPlan, hasPlan(Aggregate.class, withAggs(Sum.class)));
    }

    public void testPlans_largeDataAfterMvExpandAndFilter() {
        // This test simulates a source count of 10^9, and an mv_expanded/filtered count of 10^12.

        LogicalPlan originalPlan = getLogicalPlan("FROM test | MV_EXPAND emp_no | WHERE emp_no > 1 | STATS SUM(emp_no)");
        LogicalPlan mainPlan = ApproximationPlan.get(
            originalPlan,
            ApproximationVerifier.verifyPlanOrThrow(originalPlan, TransportVersion.current()),
            ApproximationSettings.DEFAULT
        );
        ApproximationDriver approximation = ApproximationDriver.create(mainPlan, ApproximationSettings.DEFAULT);

        // The first subplan should be the source count.
        LogicalPlan subplan = approximation.firstSubPlan();
        assertThat(subplan, not(hasPlan(MvExpand.class)));
        assertThat(subplan, not(hasPlan(Filter.class)));
        assertThat(subplan, not(hasPlan(SampledAggregate.class)));
        assertThat(subplan, hasPlan(Aggregate.class, withAggs(Count.class)));

        // Source count of 10^9.
        approximation.newMainPlan(mainPlan, newCountResult(1_000_000_000));
        subplan = approximation.firstSubPlan();
        assertThat(subplan, hasPlan(MvExpand.class));
        assertThat(subplan, hasPlan(Filter.class));
        assertThat(subplan, hasPlan(SampledAggregate.class, withProbability(1e-5), withAggs(CountApproximate.class)));

        // Sampled-corrected filtered count of 10^12, so use the minimum sample probability.
        mainPlan = approximation.newMainPlan(mainPlan, newCountResult(1_000_000_000_000L));

        // The main plan should have sampling with probability 10^5/10^9.
        assertThat(mainPlan, hasPlan(MvExpand.class));
        assertThat(mainPlan, not(hasPlan(Aggregate.class)));
        assertThat(mainPlan, hasPlan(SampledAggregate.class, withProbability(1e-4), withAggs(Sum.class)));
    }

    public void testPlans_sampleProbabilityThreshold_noFilter() {
        // This test simulates a source count of 500_000, which leads to a sample probability of 0.5,
        // which is above the threshold, so no sampling should be applied.

        LogicalPlan originalPlan = getLogicalPlan("FROM test | STATS SUM(emp_no)");
        LogicalPlan mainPlan = ApproximationPlan.get(
            originalPlan,
            ApproximationVerifier.verifyPlanOrThrow(originalPlan, TransportVersion.current()),
            ApproximationSettings.DEFAULT
        );
        ApproximationDriver approximation = ApproximationDriver.create(mainPlan, ApproximationSettings.DEFAULT);

        // The first subplan should be the source count.
        LogicalPlan subplan = approximation.firstSubPlan();
        assertThat(subplan, not(hasPlan(SampledAggregate.class)));
        assertThat(subplan, hasPlan(Aggregate.class, withAggs(Count.class)));

        // Source count of 500_000, so no more subplans.
        mainPlan = approximation.newMainPlan(mainPlan, newCountResult(500_000));
        subplan = approximation.firstSubPlan();
        assertThat(subplan, nullValue());

        // The main plan shouldn't have any sampling.
        assertThat(mainPlan, not(hasPlan(SampledAggregate.class)));
        assertThat(mainPlan, hasPlan(Aggregate.class, withAggs(Sum.class)));
    }

    public void testCountPlan_sampleProbabilityThreshold_withFilter() {
        // This test simulates a source count of 10^12, and a filtered count of 200_000.
        // This leads to a sample probability of 0.2, which is above the threshold,
        // so no sampling should be applied.

        LogicalPlan originalPlan = getLogicalPlan("FROM test | WHERE emp_no > 1 | STATS SUM(emp_no)");
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

        // Source count of 10^12.
        approximation.newMainPlan(mainPlan, newCountResult(1_000_000_000_000L));
        // There's filtering, so start with a count plan with sampling with probability 10^4 / 10^12.
        subplan = approximation.firstSubPlan();
        assertThat(subplan, hasPlan(Filter.class));
        assertThat(subplan, not(hasPlan(Aggregate.class)));
        assertThat(subplan, hasPlan(SampledAggregate.class, withProbability(1e-8), withAggs(CountApproximate.class)));

        // Sampled filtered count of 0, so increase the sample probability.
        approximation.newMainPlan(mainPlan, newCountResult(0));
        // There's filtering, so start with a count plan with sampling with probability 10^4 / 10^12.
        subplan = approximation.firstSubPlan();
        assertThat(subplan, hasPlan(Filter.class));
        assertThat(subplan, not(hasPlan(Aggregate.class)));
        assertThat(subplan, hasPlan(SampledAggregate.class, withProbability(1e-4), withAggs(CountApproximate.class)));

        // Sampled filtered count of 20, which would next to a sample probability of 0.2,
        // which is above the threshold, so no more subplans.
        mainPlan = approximation.newMainPlan(mainPlan, newCountResult(20));
        // There's filtering, so start with a count plan with sampling with probability 10^4 / 10^12.
        subplan = approximation.firstSubPlan();
        assertThat(subplan, nullValue());

        // The main plan shouldn't have any sampling.
        assertThat(mainPlan, hasPlan(Filter.class));
        assertThat(mainPlan, not(hasPlan(SampledAggregate.class)));
        assertThat(mainPlan, hasPlan(Aggregate.class, withAggs(Sum.class)));
    }
}
