/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.approximation;

import org.elasticsearch.xpack.esql.expression.function.aggregate.Count;
import org.elasticsearch.xpack.esql.expression.function.aggregate.CountApproximate;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Sum;
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.MvExpand;
import org.elasticsearch.xpack.esql.plan.logical.SampledAggregate;

import java.util.List;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.withDefaultLimitWarning;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;

public class ApproximationTests extends ApproximationTestCase {

    @Override
    protected List<String> filteredWarnings() {
        return withDefaultLimitWarning(super.filteredWarnings());
    }

    public void testVerify_validQuery() throws Exception {
        verify("FROM test | WHERE emp_no<99 | SORT last_name | MV_EXPAND salary | STATS COUNT() BY gender");
        verify("FROM test | CHANGE_POINT salary ON emp_no | EVAL x=1 | DROP emp_no | STATS SUM(salary) BY x");
        verify("FROM test | KEEP gender, emp_no | RENAME gender AS whatever | STATS MEDIAN(emp_no) | LIMIT 1000");
        verify("FROM test | EVAL blah=1 | GROK last_name \"%{IP:x}\" | SAMPLE 0.1 | STATS a=COUNT() | LIMIT 100 | SORT a");
        verify("ROW i=[1,2,3] | EVAL x=TO_STRING(i) | DISSECT x \"%{x}\" | STATS i=10*POW(PERCENTILE(i, 0.5), 2) | LIMIT 10");
        verify("FROM test | URI_PARTS parts = last_name | STATS scheme_count = COUNT() BY parts.scheme | LIMIT 10");
        verify("FROM test | REGISTERED_DOMAIN rd = last_name | STATS c = COUNT() BY rd.registered_domain | LIMIT 10");
    }

    public void testVerify_validQuery_queryProperties() throws Exception {
        assertThat(
            verify("FROM test | SORT last_name | RENAME gender AS whatever | EVAL blah=1 | STATS COUNT() BY emp_no"),
            equalTo(new Approximation.QueryProperties(true, false, false))
        );
        assertThat(
            verify("FROM test | STATS COUNT() BY emp_no | WHERE emp_no > 10 | LIMIT 3 | MV_EXPAND emp_no"),
            equalTo(new Approximation.QueryProperties(true, false, false))
        );
        assertThat(
            verify("FROM test | WHERE emp_no > 10 | STATS SUM(emp_no)"),
            equalTo(new Approximation.QueryProperties(false, true, false))
        );
        assertThat(
            verify("FROM test | SAMPLE 0.3 | STATS COUNT() BY emp_no"),
            equalTo(new Approximation.QueryProperties(true, true, false))
        );
        assertThat(
            verify("FROM test | MV_EXPAND gender | STATS COUNT() BY emp_no"),
            equalTo(new Approximation.QueryProperties(true, false, true))
        );
        assertThat(
            verify("FROM test | MV_EXPAND gender | WHERE emp_no < 3 | STATS COUNT()"),
            equalTo(new Approximation.QueryProperties(false, true, true))
        );
    }

    public void testVerify_exactlyOneStats() {
        assertError(
            "FROM test | EVAL x = 1 | SORT emp_no | LIMIT 100 | MV_EXPAND x",
            equalTo("line 1:1: approximation not supported: query must have [STATS] with aggregation function(s) that can be approximated")
        );
        assertError(
            "FROM test | STATS COUNT() BY emp_no | STATS COUNT()",
            equalTo("line 1:39: approximation not supported: query with multiple [STATS] cannot be approximated")
        );
    }

    public void testVerify_incompatibleSourceCommand() {
        assertError(
            "SHOW INFO | STATS COUNT()",
            equalTo("line 1:1: approximation not supported: query with [SHOW INFO] cannot be approximated")
        );
        assertError(
            "TS k8s | STATS COUNT(network.cost)",
            equalTo("line 1:1: approximation not supported: query with [TS k8s] cannot be approximated")
        );
    }

    public void testVerify_incompatibleProcessingCommand() {
        assertError(
            "FROM test | FORK (EVAL x=1) (EVAL y=1) | STATS COUNT()",
            equalTo("line 1:13: approximation not supported: query with [FORK (EVAL x=1) (EVAL y=1)] cannot be approximated")
        );
        assertError(
            "FROM test | STATS COUNT() | FORK (EVAL x=1) (EVAL y=1)",
            equalTo("line 1:29: approximation not supported: query with [FORK (EVAL x=1) (EVAL y=1)] cannot be approximated")
        );
        assertError(
            "FROM test | INLINE STATS COUNT() | STATS COUNT()",
            equalTo("line 1:13: approximation not supported: query with [INLINE STATS COUNT()] cannot be approximated")
        );
        assertError(
            "FROM test | STATS COUNT() | INLINE STATS COUNT()",
            equalTo("line 1:29: approximation not supported: query with [INLINE STATS COUNT()] cannot be approximated")
        );
        assertError(
            "FROM test | LOOKUP JOIN test_lookup ON emp_no | FORK (EVAL x=1) (EVAL y=1) | STATS COUNT()",
            equalTo("line 1:13: approximation not supported: query with [LOOKUP JOIN test_lookup ON emp_no] cannot be approximated")
        );
        assertError(
            "FROM test | STATS emp_no=COUNT() | LOOKUP JOIN test_lookup ON emp_no | FORK (EVAL x=1) (EVAL y=1)",
            equalTo("line 1:36: approximation not supported: query with [LOOKUP JOIN test_lookup ON emp_no] cannot be approximated")
        );
    }

    public void testVerify_incompatibleProcessingCommandBeforeStats() {
        assertError(
            "FROM test | LIMIT 1000 | STATS COUNT()",
            equalTo("line 1:13: approximation not supported: query with [LIMIT 1000] before [STATS] cannot be approximated")
        );
    }

    public void testVerify_incompatibleAggregation() {
        assertError(
            "FROM test | SORT emp_no | STATS MIN(emp_no) | LIMIT 100",
            equalTo("line 1:33: approximation not supported: aggregation function [MIN(emp_no)] cannot be approximated")
        );
        assertError(
            "FROM test | STATS SUM(emp_no), VALUES(emp_no), TOP(emp_no, 2, \"ASC\"), COUNT()",
            equalTo("line 1:32: approximation not supported: aggregation function [VALUES(emp_no)] cannot be approximated")
        );
        assertError(
            "FROM test | STATS 5+10*POW(MAX(emp_no), 2) BY gender",
            equalTo("line 1:28: approximation not supported: aggregation function [MAX(emp_no)] cannot be approximated")
        );
        assertError(
            "ROW x=[1,2]::DENSE_VECTOR | STATS SUM(x)",
            equalTo("line 1:35: approximation not supported: aggregation function [SUM(x)] must return a numeric value; got [DENSE_VECTOR]")
        );
    }

    public void testPlans_noData() throws Exception {
        // This test simulates a source count of 0.

        LogicalPlan originalPlan = getLogicalPlan("FROM test | STATS SUM(emp_no)");
        LogicalPlan mainPlan = ApproximationPlan.get(originalPlan, ApproximationSettings.DEFAULT);
        Approximation approximation = new Approximation(mainPlan, ApproximationSettings.DEFAULT);

        // The first subplan should be the source count.
        LogicalPlan subplan = approximation.firstSubPlan();
        assertThat(subplan, not(hasPlan(SampledAggregate.class)));
        assertThat(subplan, hasPlan(Aggregate.class, withAggs(Count.class)));

        // Source count of 0, so no more subplans.
        mainPlan = approximation.newMainPlan(newCountResult(0));
        subplan = approximation.firstSubPlan();
        assertThat(subplan, nullValue());

        // The main plan shouldn't have any sampling.
        assertThat(mainPlan, not(hasPlan(SampledAggregate.class)));
        assertThat(mainPlan, hasPlan(Aggregate.class, withAggs(Sum.class)));
    }

    public void testPlans_smallDataNoFilters() throws Exception {
        // This test simulates a source count of 1_000.

        LogicalPlan originalPlan = getLogicalPlan("FROM test | STATS SUM(emp_no)");
        LogicalPlan mainPlan = ApproximationPlan.get(originalPlan, ApproximationSettings.DEFAULT);
        Approximation approximation = new Approximation(mainPlan, ApproximationSettings.DEFAULT);

        // The first subplan should be the source count.
        LogicalPlan subplan = approximation.firstSubPlan();
        assertThat(subplan, not(hasPlan(SampledAggregate.class)));
        assertThat(subplan, hasPlan(Aggregate.class, withAggs(Count.class)));

        // Source count of 1000, so no more subplans.
        mainPlan = approximation.newMainPlan(newCountResult(1_000));
        subplan = approximation.firstSubPlan();
        assertThat(subplan, nullValue());

        // The main plan shouldn't have any sampling.
        assertThat(mainPlan, not(hasPlan(SampledAggregate.class)));
        assertThat(mainPlan, hasPlan(Aggregate.class, withAggs(Sum.class)));
    }

    public void testPlans_largeDataNoFilters() throws Exception {
        // This test simulates a source count of 10^9.

        LogicalPlan originalPlan = getLogicalPlan("FROM test | STATS SUM(emp_no)");
        LogicalPlan mainPlan = ApproximationPlan.get(originalPlan, ApproximationSettings.DEFAULT);
        Approximation approximation = new Approximation(mainPlan, ApproximationSettings.DEFAULT);

        // The first subplan should be the source count.
        LogicalPlan subplan = approximation.firstSubPlan();
        assertThat(subplan, not(hasPlan(SampledAggregate.class)));
        assertThat(subplan, hasPlan(Aggregate.class, withAggs(Count.class)));

        // Source count of 10^9.
        mainPlan = approximation.newMainPlan(newCountResult(1_000_000_000));
        // No filtering, so no more subplans.
        subplan = approximation.firstSubPlan();
        assertThat(subplan, nullValue());

        // The main plan should have sampling with probability 10^5 / 10^9.
        assertThat(mainPlan, not(hasPlan(Aggregate.class)));
        assertThat(mainPlan, hasPlan(SampledAggregate.class, withProbability(1e-4), withAggs(Sum.class)));
    }

    public void testPlans_largeDataAfterFiltering() throws Exception {
        // This test simulates a source count of 10^12, and a filtered count of 10^9.

        LogicalPlan originalPlan = getLogicalPlan("FROM test | WHERE emp_no < 1 | STATS SUM(emp_no)");
        LogicalPlan mainPlan = ApproximationPlan.get(originalPlan, ApproximationSettings.DEFAULT);
        Approximation approximation = new Approximation(mainPlan, ApproximationSettings.DEFAULT);

        // The first subplan should be the source count.
        LogicalPlan subplan = approximation.firstSubPlan();
        assertThat(subplan, not(hasPlan(Filter.class)));
        assertThat(subplan, not(hasPlan(SampledAggregate.class)));
        assertThat(subplan, hasPlan(Aggregate.class, withAggs(Count.class)));

        // Source count of 10^12.
        approximation.newMainPlan(newCountResult(1_000_000_000_000L));
        // There's filtering, so start with a count plan with sampling with probability 10^4 / 10^12.
        subplan = approximation.firstSubPlan();
        assertThat(subplan, hasPlan(Filter.class));
        assertThat(subplan, not(hasPlan(Aggregate.class)));
        assertThat(subplan, hasPlan(SampledAggregate.class, withProbability(1e-8), withAggs(CountApproximate.class)));

        // Sampled-corrected filtered count of 10^9 (so actual count of 10), so increase the sample probability.
        approximation.newMainPlan(newCountResult(1_000_000_000));
        subplan = approximation.firstSubPlan();
        assertThat(subplan, hasPlan(Filter.class));
        assertThat(subplan, not(hasPlan(Aggregate.class)));
        assertThat(subplan, hasPlan(SampledAggregate.class, withProbability(1e-5), withAggs(CountApproximate.class)));

        // Sampled-corrected filtered count of 10^9 (so actual count of 10_000), so no more subplans.
        mainPlan = approximation.newMainPlan(newCountResult(1_000_000_000));
        subplan = approximation.firstSubPlan();
        assertThat(subplan, nullValue());

        // The main plan should have sampling with probability 10^5 / 10^9.
        assertThat(mainPlan, hasPlan(Filter.class));
        assertThat(mainPlan, not(hasPlan(Aggregate.class)));
        assertThat(mainPlan, hasPlan(SampledAggregate.class, withProbability(1e-4), withAggs(Sum.class)));
    }

    public void testPlans_smallDataAfterFiltering() throws Exception {
        // This test simulates a source count of 10^18, and a filtered count of 100.

        LogicalPlan originalPlan = getLogicalPlan("FROM test | WHERE emp_no < 1 | STATS SUM(emp_no)");
        LogicalPlan mainPlan = ApproximationPlan.get(originalPlan, ApproximationSettings.DEFAULT);
        Approximation approximation = new Approximation(mainPlan, ApproximationSettings.DEFAULT);

        // The first subplan should be the source count.
        LogicalPlan subplan = approximation.firstSubPlan();
        assertThat(subplan, not(hasPlan(Filter.class)));
        assertThat(subplan, not(hasPlan(SampledAggregate.class)));
        assertThat(subplan, hasPlan(Aggregate.class, withAggs(Count.class)));

        // Source count of 10^18.
        approximation.newMainPlan(newCountResult(1_000_000_000_000_000_000L));
        // There's filtering, so start with a count plan with sampling with probability 10^4 / 10^18.
        subplan = approximation.firstSubPlan();
        assertThat(subplan, hasPlan(Filter.class));
        assertThat(subplan, not(hasPlan(Aggregate.class)));
        assertThat(subplan, hasPlan(SampledAggregate.class, withProbability(1e-14), withAggs(CountApproximate.class)));

        // Filtered count of 0, so increase the sample probability.
        approximation.newMainPlan(newCountResult(0));
        subplan = approximation.firstSubPlan();
        assertThat(subplan, hasPlan(Filter.class));
        assertThat(subplan, not(hasPlan(Aggregate.class)));
        assertThat(subplan, hasPlan(SampledAggregate.class, withProbability(1e-10), withAggs(CountApproximate.class)));

        // Filtered count of 0, so increase the sample probability.
        approximation.newMainPlan(newCountResult(0));
        subplan = approximation.firstSubPlan();
        assertThat(subplan, hasPlan(Filter.class));
        assertThat(subplan, not(hasPlan(Aggregate.class)));
        assertThat(subplan, hasPlan(SampledAggregate.class, withProbability(1e-6), withAggs(CountApproximate.class)));

        // Filtered count of 0, so no more subplans.
        mainPlan = approximation.newMainPlan(newCountResult(0));
        subplan = approximation.firstSubPlan();
        assertThat(subplan, nullValue());

        // The main plan shouldn't have any sampling.
        assertThat(mainPlan, hasPlan(Filter.class));
        assertThat(mainPlan, not(hasPlan(SampledAggregate.class)));
        assertThat(mainPlan, hasPlan(Aggregate.class, withAggs(Sum.class)));
    }

    public void testPlans_smallDataBeforeFiltering() throws Exception {
        // This test simulates a source count of 1_000, and a filtered count of 10.

        LogicalPlan originalPlan = getLogicalPlan("FROM test | WHERE gender == \"X\" | STATS SUM(emp_no)");
        LogicalPlan mainPlan = ApproximationPlan.get(originalPlan, ApproximationSettings.DEFAULT);
        Approximation approximation = new Approximation(mainPlan, ApproximationSettings.DEFAULT);

        // The first subplan should be the source count.
        LogicalPlan subplan = approximation.firstSubPlan();
        assertThat(subplan, not(hasPlan(Filter.class)));
        assertThat(subplan, not(hasPlan(SampledAggregate.class)));
        assertThat(subplan, hasPlan(Aggregate.class, withAggs(Count.class)));

        // Source count of 1000, so no more subplans.
        mainPlan = approximation.newMainPlan(newCountResult(1_000));
        subplan = approximation.firstSubPlan();
        assertThat(subplan, nullValue());

        // The main plan shouldn't have any sampling.
        assertThat(mainPlan, hasPlan(Filter.class));
        assertThat(mainPlan, not(hasPlan(SampledAggregate.class)));
        assertThat(mainPlan, hasPlan(Aggregate.class, withAggs(Sum.class)));
    }

    public void testPlans_smallDataAfterMvExpanding() throws Exception {
        // This test simulates a source count of 1_000, and a mv_expanded count of 10_000.

        LogicalPlan originalPlan = getLogicalPlan("FROM test | MV_EXPAND emp_no | STATS SUM(emp_no)");
        LogicalPlan mainPlan = ApproximationPlan.get(originalPlan, ApproximationSettings.DEFAULT);
        Approximation approximation = new Approximation(mainPlan, ApproximationSettings.DEFAULT);

        // The first subplan should be the source count.
        LogicalPlan subplan = approximation.firstSubPlan();
        assertThat(subplan, not(hasPlan(MvExpand.class)));
        assertThat(subplan, not(hasPlan(SampledAggregate.class)));
        assertThat(subplan, hasPlan(Aggregate.class, withAggs(Count.class)));

        // Source count of 1000.
        approximation.newMainPlan(newCountResult(1_000));
        // The next subplan should be the mv_expanded count,
        // without sampling, because source count is small.
        subplan = approximation.firstSubPlan();
        assertThat(subplan, hasPlan(MvExpand.class));
        assertThat(subplan, not(hasPlan(SampledAggregate.class)));
        assertThat(subplan, hasPlan(Aggregate.class, withAggs(Count.class)));

        // mv_expanded count of 10_000, so no more subplans.
        mainPlan = approximation.newMainPlan(newCountResult(1_000));
        subplan = approximation.firstSubPlan();
        assertThat(subplan, nullValue());

        // The main plan shouldn't have any sampling.
        assertThat(mainPlan, hasPlan(MvExpand.class));
        assertThat(mainPlan, not(hasPlan(SampledAggregate.class)));
        assertThat(mainPlan, hasPlan(Aggregate.class, withAggs(Sum.class)));
    }

    public void testPlans_largeDataAfterMvExpanding() throws Exception {
        // This test simulates a source count of 1_000, and a mv_expanded count of 10^9

        LogicalPlan originalPlan = getLogicalPlan("FROM test | MV_EXPAND emp_no | STATS SUM(emp_no)");
        LogicalPlan mainPlan = ApproximationPlan.get(originalPlan, ApproximationSettings.DEFAULT);
        Approximation approximation = new Approximation(mainPlan, ApproximationSettings.DEFAULT);

        // The first subplan should be the source count.
        LogicalPlan subplan = approximation.firstSubPlan();
        assertThat(subplan, not(hasPlan(MvExpand.class)));
        assertThat(subplan, not(hasPlan(SampledAggregate.class)));
        assertThat(subplan, hasPlan(Aggregate.class, withAggs(Count.class)));

        // Source count of 1000.
        approximation.newMainPlan(newCountResult(1_000));
        // The next subplan should be the mv_expanded count,
        // without no sampling because source count is small.
        subplan = approximation.firstSubPlan();
        assertThat(subplan, hasPlan(MvExpand.class));
        assertThat(subplan, not(hasPlan(SampledAggregate.class)));
        assertThat(subplan, hasPlan(Aggregate.class, withAggs(Count.class)));

        // mv_expanded count of 10^9, so no more subplans.
        mainPlan = approximation.newMainPlan(newCountResult(1_000_000_000));
        subplan = approximation.firstSubPlan();
        assertThat(subplan, nullValue());

        // The main plan should have any sampling with probability 10^5/10^9.
        assertThat(mainPlan, hasPlan(MvExpand.class));
        assertThat(mainPlan, not(hasPlan(Aggregate.class)));
        assertThat(mainPlan, hasPlan(SampledAggregate.class, withProbability(1e-4), withAggs(Sum.class)));
    }

    public void testPlans_largeDataBeforeMvExpanding() throws Exception {
        // This test simulates a source count of 10^9, and a mv_expanded count of 10^12

        LogicalPlan originalPlan = getLogicalPlan("FROM test | MV_EXPAND emp_no | STATS SUM(emp_no)");
        LogicalPlan mainPlan = ApproximationPlan.get(originalPlan, ApproximationSettings.DEFAULT);
        Approximation approximation = new Approximation(mainPlan, ApproximationSettings.DEFAULT);

        // The first subplan should be the source count.
        LogicalPlan subplan = approximation.firstSubPlan();
        assertThat(subplan, not(hasPlan(MvExpand.class)));
        assertThat(subplan, not(hasPlan(SampledAggregate.class)));
        assertThat(subplan, hasPlan(Aggregate.class, withAggs(Count.class)));

        // Source count of 10^9.
        approximation.newMainPlan(newCountResult(1_000_000_000));
        // The next subplan should be the mv_expanded count,
        // with sampling because source count is large.
        subplan = approximation.firstSubPlan();
        assertThat(subplan, hasPlan(MvExpand.class));
        assertThat(subplan, not(hasPlan(Aggregate.class)));
        assertThat(subplan, hasPlan(SampledAggregate.class, withProbability(1e-5), withAggs(CountApproximate.class)));

        // Sample-corrected mv_expanded count of 10^12 (so actual of 10^7), so no more subplans.
        mainPlan = approximation.newMainPlan(newCountResult(1_000_000_000_000L));
        subplan = approximation.firstSubPlan();
        assertThat(subplan, nullValue());

        // The main plan should have any sampling with probability 10^5/10^12.
        assertThat(mainPlan, hasPlan(MvExpand.class));
        assertThat(mainPlan, not(hasPlan(Aggregate.class)));
        assertThat(mainPlan, hasPlan(SampledAggregate.class, withProbability(1e-7), withAggs(Sum.class)));
    }

    public void testPlans_sampleProbabilityThreshold_noFilter() throws Exception {
        // This test simulates a source count of 500_000, which leads to a sample probability of 0.5,
        // which is above the threshold, so no sampling should be applied.

        LogicalPlan originalPlan = getLogicalPlan("FROM test | STATS SUM(emp_no)");
        LogicalPlan mainPlan = ApproximationPlan.get(originalPlan, ApproximationSettings.DEFAULT);
        Approximation approximation = new Approximation(mainPlan, ApproximationSettings.DEFAULT);

        // The first subplan should be the source count.
        LogicalPlan subplan = approximation.firstSubPlan();
        assertThat(subplan, not(hasPlan(SampledAggregate.class)));
        assertThat(subplan, hasPlan(Aggregate.class, withAggs(Count.class)));

        // Source count of 500_000, so no more subplans.
        mainPlan = approximation.newMainPlan(newCountResult(500_000));
        subplan = approximation.firstSubPlan();
        assertThat(subplan, nullValue());

        // The main plan shouldn't have any sampling.
        assertThat(mainPlan, not(hasPlan(SampledAggregate.class)));
        assertThat(mainPlan, hasPlan(Aggregate.class, withAggs(Sum.class)));
    }

    public void testCountPlan_sampleProbabilityThreshold_withFilter() throws Exception {
        // This test simulates a source count of 10^12, and a filtered count of 200_000.
        // This leads to a sample probability of 0.2, which is above the threshold,
        // so no sampling should be applied.

        LogicalPlan originalPlan = getLogicalPlan("FROM test | WHERE emp_no > 1 | STATS SUM(emp_no)");
        LogicalPlan mainPlan = ApproximationPlan.get(originalPlan, ApproximationSettings.DEFAULT);
        Approximation approximation = new Approximation(mainPlan, ApproximationSettings.DEFAULT);

        // The first subplan should be the source count.
        LogicalPlan subplan = approximation.firstSubPlan();
        assertThat(subplan, not(hasPlan(Filter.class)));
        assertThat(subplan, not(hasPlan(SampledAggregate.class)));
        assertThat(subplan, hasPlan(Aggregate.class, withAggs(Count.class)));

        // Source count of 10^12.
        approximation.newMainPlan(newCountResult(1_000_000_000_000L));
        // There's filtering, so start with a count plan with sampling with probability 10^4 / 10^12.
        subplan = approximation.firstSubPlan();
        assertThat(subplan, hasPlan(Filter.class));
        assertThat(subplan, not(hasPlan(Aggregate.class)));
        assertThat(subplan, hasPlan(SampledAggregate.class, withProbability(1e-8), withAggs(CountApproximate.class)));

        // Sampled filtered count of 0, so increase the sample probability.
        approximation.newMainPlan(newCountResult(0));
        // There's filtering, so start with a count plan with sampling with probability 10^4 / 10^12.
        subplan = approximation.firstSubPlan();
        assertThat(subplan, hasPlan(Filter.class));
        assertThat(subplan, not(hasPlan(Aggregate.class)));
        assertThat(subplan, hasPlan(SampledAggregate.class, withProbability(1e-4), withAggs(CountApproximate.class)));

        // Sampled filtered count of 20, which would next to a sample probability of 0.2,
        // which is above the threshold, so no more subplans.
        mainPlan = approximation.newMainPlan(newCountResult(20));
        // There's filtering, so start with a count plan with sampling with probability 10^4 / 10^12.
        subplan = approximation.firstSubPlan();
        assertThat(subplan, nullValue());

        // The main plan shouldn't have any sampling.
        assertThat(mainPlan, hasPlan(Filter.class));
        assertThat(mainPlan, not(hasPlan(SampledAggregate.class)));
        assertThat(mainPlan, hasPlan(Aggregate.class, withAggs(Sum.class)));
    }
}
