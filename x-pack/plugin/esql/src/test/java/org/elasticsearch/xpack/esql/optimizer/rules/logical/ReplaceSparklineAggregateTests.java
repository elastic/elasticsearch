/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical;

import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Expressions;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.expression.function.aggregate.FromPartial;
import org.elasticsearch.xpack.esql.expression.function.aggregate.ToPartial;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Top;
import org.elasticsearch.xpack.esql.optimizer.AbstractLogicalPlanOptimizerTests;
import org.elasticsearch.xpack.esql.parser.ParsingException;
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.SparklineGenerateEmptyBuckets;
import org.elasticsearch.xpack.esql.plan.logical.UnaryPlan;

import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.as;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.not;

public class ReplaceSparklineAggregateTests extends AbstractLogicalPlanOptimizerTests {

    private static final String SPARKLINE_EXPR = "sparkline(count(*), hire_date, 10, \"2024-01-01\", \"2024-12-31\")";

    public void testNoSparklines() {
        LogicalPlan current = plan("from test | stats s = sum(salary) by last_name");
        while (current != null) {
            assertThat(current, not(instanceOf(SparklineGenerateEmptyBuckets.class)));
            if (current instanceof UnaryPlan unary) {
                current = unary.child();
            } else {
                break;
            }
        }
    }

    public void testSingleSparkline() {
        UnaryPlan plan = as(plan("from test | stats s = " + SPARKLINE_EXPR), UnaryPlan.class);
        List<String> sparklineAggregateNames = List.of("s");
        SparklineGenerateEmptyBuckets sparkline = as(plan.child(), SparklineGenerateEmptyBuckets.class);
        validateSparklineGenerateEmptyBuckets(sparkline, sparklineAggregateNames);

        Aggregate secondPhase = as(sparkline.child(), Aggregate.class);
        Aggregate firstPhase = as(secondPhase.child(), Aggregate.class);
        validateAggregates(secondPhase, firstPhase, sparklineAggregateNames, List.of(), List.of());
    }

    public void testSingleSparklineWithGrouping() {
        UnaryPlan plan = as(plan("from test | stats s = " + SPARKLINE_EXPR + " by last_name"), UnaryPlan.class);
        List<String> sparklineAggregateNames = List.of("s");
        SparklineGenerateEmptyBuckets sparkline = as(plan.child(), SparklineGenerateEmptyBuckets.class);
        validateSparklineGenerateEmptyBuckets(sparkline, sparklineAggregateNames);

        Aggregate secondPhase = as(sparkline.child(), Aggregate.class);
        Aggregate firstPhase = as(secondPhase.child(), Aggregate.class);
        validateAggregates(secondPhase, firstPhase, sparklineAggregateNames, List.of(), List.of("last_name"));
    }

    public void testSparklineWithNonSparklineAgg() {
        UnaryPlan plan = as(plan("from test | stats s = " + SPARKLINE_EXPR + ", c = count(*)"), UnaryPlan.class);
        List<String> sparklineAggregateNames = List.of("s");
        SparklineGenerateEmptyBuckets sparkline = as(plan.child(), SparklineGenerateEmptyBuckets.class);
        validateSparklineGenerateEmptyBuckets(sparkline, sparklineAggregateNames);

        Aggregate secondPhase = as(sparkline.child(), Aggregate.class);
        Aggregate firstPhase = as(secondPhase.child(), Aggregate.class);
        validateAggregates(secondPhase, firstPhase, sparklineAggregateNames, List.of("c"), List.of());
        // TODO: Add topartial and frompartial checks
    }

    public void testSparklineWithNonSparklineAggAndGrouping() {
        UnaryPlan plan = as(plan("from test | stats s = " + SPARKLINE_EXPR + ", c = count(*) by last_name"), UnaryPlan.class);
        List<String> sparklineAggregateNames = List.of("s");
        SparklineGenerateEmptyBuckets sparkline = as(plan.child(), SparklineGenerateEmptyBuckets.class);
        validateSparklineGenerateEmptyBuckets(sparkline, sparklineAggregateNames);

        Aggregate secondPhase = as(sparkline.child(), Aggregate.class);
        Aggregate firstPhase = as(secondPhase.child(), Aggregate.class);
        validateAggregates(secondPhase, firstPhase, sparklineAggregateNames, List.of("c"), List.of("last_name"));
    }

    public void testMultipleSparklines() {
        UnaryPlan plan = as(plan("""
             from test
             | stats s1 = sparkline(max(salary), hire_date, 10, \"2024-01-01\", \"2024-12-31\"),
                 s2 = sparkline(min(salary), hire_date, 10, \"2024-01-01\", \"2024-12-31\")
            """), UnaryPlan.class);
        List<String> sparklineAggregateNames = List.of("s1", "s2");
        SparklineGenerateEmptyBuckets sparkline = as(plan.child(), SparklineGenerateEmptyBuckets.class);
        validateSparklineGenerateEmptyBuckets(sparkline, sparklineAggregateNames);

        Aggregate secondPhase = as(sparkline.child(), Aggregate.class);
        Aggregate firstPhase = as(secondPhase.child(), Aggregate.class);
        validateAggregates(secondPhase, firstPhase, sparklineAggregateNames, List.of(), List.of());
    }

    public void testMultipleSparklinesWithGrouping() {
        UnaryPlan plan = as(plan("""
            from test
            | stats s1 = sparkline(max(salary), hire_date, 10, \"2024-01-01\", \"2024-12-31\"),
                s2 = sparkline(min(salary), hire_date, 10, \"2024-01-01\", \"2024-12-31\") by last_name"""), UnaryPlan.class);
        List<String> sparklineAggregateNames = List.of("s1", "s2");
        SparklineGenerateEmptyBuckets sparkline = as(plan.child(), SparklineGenerateEmptyBuckets.class);
        validateSparklineGenerateEmptyBuckets(sparkline, sparklineAggregateNames);

        Aggregate secondPhase = as(sparkline.child(), Aggregate.class);
        Aggregate firstPhase = as(secondPhase.child(), Aggregate.class);
        validateAggregates(secondPhase, firstPhase, sparklineAggregateNames, List.of(), List.of("last_name"));
    }

    public void testMultipleSparklineAggregatesWithMultipleMultipleNonSparklineAggregatesAndGroupings() {
        UnaryPlan plan = as(plan("""
            from test
            | stats s1 = sparkline(min(salary), hire_date, 10, "2024-01-01", "2024-12-31"),
                    s2 = sparkline(max(salary), hire_date, 10, "2024-01-01", "2024-12-31"),
                    c = count(salary),
                    total = sum(salary),
                    maximum = max(salary)
                    by last_name, first_name
            """), UnaryPlan.class);
        List<String> sparklineAggregateNames = List.of("s1", "s2");
        SparklineGenerateEmptyBuckets sparkline = as(plan.child(), SparklineGenerateEmptyBuckets.class);
        validateSparklineGenerateEmptyBuckets(sparkline, sparklineAggregateNames);

        Aggregate secondPhase = as(sparkline.child(), Aggregate.class);
        Aggregate firstPhase = as(secondPhase.child(), Aggregate.class);
        validateAggregates(
            secondPhase,
            firstPhase,
            sparklineAggregateNames,
            List.of("c", "total", "maximum"),
            List.of("last_name", "first_name")
        );
    }

    public void testMultipleSparklinesDifferentBucketCount() {
        var e = expectThrows(ParsingException.class, () -> plan("""
            from test
            | stats s1 = sparkline(min(salary), hire_date, 10, "2024-01-01", "2024-12-31"),
                    s2 = sparkline(max(salary), hire_date, 5, "2024-01-01", "2024-12-31")
            """));
        assertThat(e.getMessage(), containsString("All SPARKLINE functions in a single STATS command must share the same"));
    }

    public void testMultipleSparklinesDifferentFrom() {
        var e = expectThrows(ParsingException.class, () -> plan("""
            from test
            | stats s1 = sparkline(min(salary), hire_date, 10, "2024-01-01", "2024-12-31"),
                    s2 = sparkline(max(salary), hire_date, 10, "2024-06-01", "2024-12-31")
            """));
        assertThat(e.getMessage(), containsString("All SPARKLINE functions in a single STATS command must share the same"));
    }

    public void testMultipleSparklinesDifferentTo() {
        var e = expectThrows(ParsingException.class, () -> plan("""
            from test
            | stats s1 = sparkline(min(salary), hire_date, 10, "2024-01-01", "2024-12-31"),
                    s2 = sparkline(max(salary), hire_date, 10, "2024-01-01", "2024-06-30")
            """));
        assertThat(e.getMessage(), containsString("All SPARKLINE functions in a single STATS command must share the same"));
    }

    private void validateSparklineGenerateEmptyBuckets(SparklineGenerateEmptyBuckets sparkline, List<String> sparklineAggregateNames) {
        assertNotNull(sparkline);
        assertThat(sparkline.values(), hasSize(sparklineAggregateNames.size()));
        for (Attribute attribute : sparkline.values()) {
            assertThat(sparklineAggregateNames, hasItem(attribute.name()));
        }
        assertNotNull(sparkline.dateBucketRounding());
        assertTrue(sparkline.minDate() > 0); // TODO: Update this to take in the date to check
        assertTrue(sparkline.maxDate() > sparkline.minDate());
    }

    private void validateAggregates(
        Aggregate secondPhaseAggregate,
        Aggregate firstPhaseAggregate,
        List<String> sparklineAggregateNames,
        List<String> nonSparklineAggregateNames,
        List<String> groupings
    ) {
        validateFirstPhaseAggregate(firstPhaseAggregate, sparklineAggregateNames, nonSparklineAggregateNames, groupings);
        validateSecondPhaseAggregate(secondPhaseAggregate, sparklineAggregateNames, nonSparklineAggregateNames, groupings);
    }

    private void validateFirstPhaseAggregate(
        Aggregate aggregate,
        List<String> sparklineAggregateNames,
        List<String> nonSparklineAggregateNames,
        List<String> groupings
    ) {
        assertNotNull(aggregate);
        assertEquals(
            sparklineAggregateNames.size() + nonSparklineAggregateNames.size() + groupings.size() + 1,
            aggregate.aggregates().size()
        );
        List<String> expectedAggregates = new ArrayList<>();
        expectedAggregates.add("$$timestamp");
        expectedAggregates.addAll(sparklineAggregateNames);
        nonSparklineAggregateNames.forEach(agg -> { expectedAggregates.add("$$" + agg); });
        expectedAggregates.addAll(groupings);
        for (NamedExpression agg : aggregate.aggregates()) {
            assertThat(expectedAggregates, hasItem(agg.name()));
            if (sparklineAggregateNames.contains(agg.name())) {
                assertThat(agg, instanceOf(Alias.class));
            } else if (agg.name().equals("$$timestamp")) {
                // No need to check the exact class here, but it should be some kind of attribute representing the timestamp for bucketing
                assertThat(agg, instanceOf(ReferenceAttribute.class));
            } else if (groupings.contains(agg.name())) {
                assertThat(agg, instanceOf(FieldAttribute.class));
                // TODO: Handle cases where this is a grouping function instead of a direct field reference
            } else {
                // Non-sparkline aggregates should be ToPartial in the first phase
                assertThat(agg, instanceOf(Alias.class));
                Alias alias = (Alias) agg;
                assertThat(alias.child(), instanceOf(ToPartial.class));
            }
            expectedAggregates.remove(agg.name());
        }
        if (expectedAggregates.isEmpty() == false) {
            fail("First phase aggregate is missing expected aggregates: " + expectedAggregates);
        }

        assertEquals(aggregate.groupings().size(), groupings.size() + 1);
        List<String> expectedGroupings = new ArrayList<>(groupings);
        expectedGroupings.add("$$timestamp");
        for (Expression grouping : aggregate.groupings()) {
            assertThat(expectedGroupings, hasItem(Expressions.name(grouping)));
            expectedGroupings.remove(Expressions.name(grouping));
        }
        if (expectedGroupings.isEmpty() == false) {
            fail("First phase aggregate is missing expected groupings: " + expectedGroupings);
        }
    }

    private void validateSecondPhaseAggregate(
        Aggregate aggregate,
        List<String> sparklineAggregateNames,
        List<String> nonSparklineAggregateNames,
        List<String> groupings
    ) {
        assertNotNull(aggregate);
        assertEquals(
            sparklineAggregateNames.size() + nonSparklineAggregateNames.size() + groupings.size() + 1,
            aggregate.aggregates().size()
        );
        List<String> expectedAggregates = new ArrayList<>();
        expectedAggregates.add("$$timestamp");
        expectedAggregates.addAll(sparklineAggregateNames);
        expectedAggregates.addAll(nonSparklineAggregateNames);
        expectedAggregates.addAll(groupings);
        for (NamedExpression agg : aggregate.aggregates()) {
            assertThat(expectedAggregates, hasItem(agg.name()));
            if (sparklineAggregateNames.contains(agg.name()) || agg.name().equals("$$timestamp")) {
                assertThat(agg, instanceOf(Alias.class));
                Alias alias = (Alias) agg;
                assertThat(alias.child(), instanceOf(Top.class));
            } else if (groupings.contains(agg.name())) {
                assertThat(agg, instanceOf(FieldAttribute.class));
            } else {
                // Non-sparkline aggregates should be FromPartial in the second phase
                assertThat(agg, instanceOf(Alias.class));
                Alias alias = (Alias) agg;
                assertThat(alias.child(), instanceOf(FromPartial.class));
            }
            expectedAggregates.remove(agg.name());
        }
        if (expectedAggregates.isEmpty() == false) {
            fail("Second phase aggregate is missing expected aggregates: " + expectedAggregates);
        }

        assertEquals(aggregate.groupings().size(), groupings.size());
        List<String> expectedGroupings = new ArrayList<>(groupings);
        for (Expression grouping : aggregate.groupings()) {
            assertThat(expectedGroupings, hasItem(Expressions.name(grouping)));
            expectedGroupings.remove(Expressions.name(grouping));
        }
        if (expectedGroupings.isEmpty() == false) {
            fail("Second phase aggregate is missing expected groupings: " + expectedGroupings);
        }
    }

    // TODO: Add test with different key. Currently there is no test mapping with 2 dates values
    /*public void testMultipleSparklinesDifferentKey() {
        var e = expectThrows(ParsingException.class, () -> plan("""
            from test
            | stats s1 = sparkline(min(salary), hire_date, 10, "2024-01-01", "2024-12-31"),
                    s2 = sparkline(max(salary), first_name, 10, "2024-01-01", "2024-12-31")
            """));
        assertThat(e.getMessage(), containsString("All SPARKLINE functions in a single STATS command must share the same"));
    }*/
}
