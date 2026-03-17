/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.approximation;

import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.SampledAggregate;

import java.util.List;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.withDefaultLimitWarning;
import static org.hamcrest.Matchers.not;

public class ApproximationPlanTests extends ApproximationTestCase {

    @Override
    protected List<String> filteredWarnings() {
        return withDefaultLimitWarning(super.filteredWarnings());
    }

    public void testApproximationPlan_createsConfidenceInterval_withoutGrouping() throws Exception {
        LogicalPlan approximationPlan = ApproximationPlan.get(
            ApproximationTests.getLogicalPlan("FROM test | STATS COUNT(), SUM(emp_no)"),
            ApproximationSettings.DEFAULT
        );

        assertThat(approximationPlan, hasPlan(SampledAggregate.class));
        assertThat(approximationPlan, hasPlan(Eval.class, withField("CONFIDENCE_INTERVAL(COUNT())")));
        assertThat(approximationPlan, hasPlan(Eval.class, withField(("CERTIFIED(COUNT())"))));
        assertThat(approximationPlan, hasPlan(Eval.class, withField(("CONFIDENCE_INTERVAL(SUM(emp_no))"))));
        assertThat(approximationPlan, hasPlan(Eval.class, withField(("CERTIFIED(SUM(emp_no))"))));
    }

    public void testApproximationPlan_createsConfidenceInterval_withGrouping() throws Exception {
        LogicalPlan approximationPlan = ApproximationPlan.get(
            ApproximationTests.getLogicalPlan("FROM test | STATS COUNT(), SUM(emp_no) BY emp_no"),
            ApproximationSettings.DEFAULT
        );

        assertThat(approximationPlan, hasPlan(SampledAggregate.class));
        assertThat(approximationPlan, hasPlan(Eval.class, withField(("CONFIDENCE_INTERVAL(COUNT())"))));
        assertThat(approximationPlan, hasPlan(Eval.class, withField(("CERTIFIED(COUNT())"))));
        assertThat(approximationPlan, hasPlan(Eval.class, withField(("CONFIDENCE_INTERVAL(SUM(emp_no))"))));
        assertThat(approximationPlan, hasPlan(Eval.class, withField(("CERTIFIED(SUM(emp_no))"))));
    }

    public void testApproximationPlan_dependentConfidenceIntervals() throws Exception {
        LogicalPlan approximationPlan = ApproximationPlan.get(
            ApproximationTests.getLogicalPlan(
                "FROM test | STATS x=SUM(emp_no) | EVAL a=x*x, b=7, c=TO_STRING(x), d=MV_APPEND(x, 1::LONG), e=a+POW(b, 2)"
            ),
            ApproximationSettings.DEFAULT
        );

        assertThat(approximationPlan, hasPlan(SampledAggregate.class));
        assertThat(approximationPlan, hasPlan(Eval.class, withField(("CONFIDENCE_INTERVAL(x)"))));
        assertThat(approximationPlan, hasPlan(Eval.class, withField(("CERTIFIED(x)"))));
        assertThat(approximationPlan, hasPlan(Eval.class, withField(("CONFIDENCE_INTERVAL(a)"))));
        assertThat(approximationPlan, hasPlan(Eval.class, withField(("CERTIFIED(a)"))));
        assertThat(approximationPlan, not(hasPlan(Eval.class, withField(("CONFIDENCE_INTERVAL(b)")))));
        assertThat(approximationPlan, not(hasPlan(Eval.class, withField(("CERTIFIED(b)")))));
        assertThat(approximationPlan, not(hasPlan(Eval.class, withField(("CONFIDENCE_INTERVAL(c)")))));
        assertThat(approximationPlan, not(hasPlan(Eval.class, withField(("CERTIFIED(c)")))));
        assertThat(approximationPlan, not(hasPlan(Eval.class, withField(("CONFIDENCE_INTERVAL(d)")))));
        assertThat(approximationPlan, not(hasPlan(Eval.class, withField(("CERTIFIED(d)")))));
        assertThat(approximationPlan, hasPlan(Eval.class, withField(("CONFIDENCE_INTERVAL(e)"))));
        assertThat(approximationPlan, hasPlan(Eval.class, withField(("CERTIFIED(e)"))));
    }
}
