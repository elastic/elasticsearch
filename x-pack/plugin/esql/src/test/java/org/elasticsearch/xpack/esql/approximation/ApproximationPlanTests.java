/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.approximation;

import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.SampledAggregate;

import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.withDefaultLimitWarning;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;

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
        assertThat(approximationPlan, hasPlan(Eval.class, withField("_approximation_confidence_interval(COUNT())")));
        assertThat(approximationPlan, hasPlan(Eval.class, withField(("_approximation_certified(COUNT())"))));
        assertThat(approximationPlan, hasPlan(Eval.class, withField(("_approximation_confidence_interval(SUM(emp_no))"))));
        assertThat(approximationPlan, hasPlan(Eval.class, withField(("_approximation_certified(SUM(emp_no))"))));
    }

    public void testApproximationPlan_createsConfidenceInterval_withGrouping() throws Exception {
        LogicalPlan approximationPlan = ApproximationPlan.get(
            ApproximationTests.getLogicalPlan("FROM test | STATS COUNT(), SUM(emp_no) BY emp_no"),
            ApproximationSettings.DEFAULT
        );

        assertThat(approximationPlan, hasPlan(SampledAggregate.class));
        assertThat(approximationPlan, hasPlan(Eval.class, withField(("_approximation_confidence_interval(COUNT())"))));
        assertThat(approximationPlan, hasPlan(Eval.class, withField(("_approximation_certified(COUNT())"))));
        assertThat(approximationPlan, hasPlan(Eval.class, withField(("_approximation_confidence_interval(SUM(emp_no))"))));
        assertThat(approximationPlan, hasPlan(Eval.class, withField(("_approximation_certified(SUM(emp_no))"))));
    }

    public void testApproximationPlan_dependentConfidenceIntervals() throws Exception {
        LogicalPlan approximationPlan = ApproximationPlan.get(
            ApproximationTests.getLogicalPlan(
                "FROM test | STATS x=SUM(emp_no) | EVAL a=x*x, b=7, c=TO_STRING(x), d=MV_APPEND(x, 1::LONG), e=a+POW(b, 2)"
            ),
            ApproximationSettings.DEFAULT
        );

        assertThat(approximationPlan, hasPlan(SampledAggregate.class));
        assertThat(approximationPlan, hasPlan(Eval.class, withField(("_approximation_confidence_interval(x)"))));
        assertThat(approximationPlan, hasPlan(Eval.class, withField(("_approximation_certified(x)"))));
        assertThat(approximationPlan, hasPlan(Eval.class, withField(("_approximation_confidence_interval(a)"))));
        assertThat(approximationPlan, hasPlan(Eval.class, withField(("_approximation_certified(a)"))));
        assertThat(approximationPlan, not(hasPlan(Eval.class, withField(("_approximation_confidence_interval(b)")))));
        assertThat(approximationPlan, not(hasPlan(Eval.class, withField(("_approximation_certified(b)")))));
        assertThat(approximationPlan, not(hasPlan(Eval.class, withField(("_approximation_confidence_interval(c)")))));
        assertThat(approximationPlan, not(hasPlan(Eval.class, withField(("_approximation_certified(c)")))));
        assertThat(approximationPlan, not(hasPlan(Eval.class, withField(("_approximation_confidence_interval(d)")))));
        assertThat(approximationPlan, not(hasPlan(Eval.class, withField(("_approximation_certified(d)")))));
        assertThat(approximationPlan, hasPlan(Eval.class, withField(("_approximation_confidence_interval(e)"))));
        assertThat(approximationPlan, hasPlan(Eval.class, withField(("_approximation_certified(e)"))));
    }

    public void testColumnMetadata() throws Exception {
        LogicalPlan approximationPlan = ApproximationPlan.get(
            ApproximationTests.getLogicalPlan("FROM test | STATS count=COUNT(), sum=SUM(emp_no)"),
            ApproximationSettings.DEFAULT
        );

        for (Attribute attr : approximationPlan.output()) {
            Map<String, Object> metadata = ApproximationPlan.columnMetadata(attr);
            switch (attr.name()) {
                case "count", "sum":
                    assertThat(attr.synthetic(), equalTo(false));
                    assertThat(metadata, nullValue());
                    break;
                case "_approximation_confidence_interval(count)":
                    assertThat(attr.synthetic(), equalTo(true));
                    assertThat(metadata, equalTo(Map.of("approximation", Map.of("type", "confidence_interval", "column", "count"))));
                    break;
                case "_approximation_certified(count)":
                    assertThat(attr.synthetic(), equalTo(true));
                    assertThat(metadata, equalTo(Map.of("approximation", Map.of("type", "certified", "column", "count"))));
                    break;
                case "_approximation_confidence_interval(sum)":
                    assertThat(attr.synthetic(), equalTo(true));
                    assertThat(metadata, equalTo(Map.of("approximation", Map.of("type", "confidence_interval", "column", "sum"))));
                    break;
                case "_approximation_certified(sum)":
                    assertThat(attr.synthetic(), equalTo(true));
                    assertThat(metadata, equalTo(Map.of("approximation", Map.of("type", "certified", "column", "sum"))));
                    break;
                default:
                    fail("Unexpected attribute: " + attr);
            }
        }
    }
}
