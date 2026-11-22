/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer;

import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Count;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Sum;
import org.elasticsearch.xpack.esql.expression.function.aggregate.SumLongAsDouble;
import org.elasticsearch.xpack.esql.optimizer.rules.logical.local.ReplaceSumOfToDoubleWithSumLongAsDouble;
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.SampledAggregate;
import org.elasticsearch.xpack.esql.planner.AggregateMapper;

import java.util.List;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.TEST_SEARCH_STATS;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.as;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.sameInstance;

/**
 * Tests {@link org.elasticsearch.xpack.esql.optimizer.rules.logical.local.ReplaceSumOfToDoubleWithSumLongAsDouble}, which runs as
 * part of the {@link LocalLogicalPlanOptimizer}, the data node's optimizer. {@code long_noidx} is the only {@code long} in the
 * test mapping. It isn't indexed, so {@code InferNonNullAggConstraint} doesn't add a filter below the aggregate.
 */
public class ReplaceSumOfToDoubleWithSumLongAsDoubleTests extends AbstractLocalLogicalPlanOptimizerTests {

    /**
     * {@code AVG(long)} is {@code SUM(TO_DOUBLE(long)) / COUNT(long)}. Locally the sum reads the {@code long} itself, and the
     * {@code TO_DOUBLE} that nothing else uses any more is gone.
     * <pre>{@code
     * Aggregate[[],[SUMLONGASDOUBLE(long_noidx) AS $$SUM$a$0, COUNT(long_noidx) AS $$COUNT$a$1]]
     * \_EsRelation[test][...]
     * }</pre>
     * The test stats consider every field indexed, so there is also a {@code Filter[ISNOTNULL(long_noidx)]} on top of the relation.
     */
    public void testAvgOfLong() {
        var plan = testAnalyzer().coordinatorPlan("FROM test | STATS a = AVG(long_noidx)");
        assertThat("the coordinator plans the plain sum", sumsOf(aggregate(plan)), hasSize(1));

        var local = localPlan(plan, TEST_SEARCH_STATS);

        Aggregate aggregate = aggregate(local);
        assertThat(sumsOf(aggregate), hasSize(0));
        List<SumLongAsDouble> sums = aggregate.aggregates()
            .stream()
            .map(Alias::unwrap)
            .filter(SumLongAsDouble.class::isInstance)
            .map(SumLongAsDouble.class::cast)
            .toList();
        assertThat(sums, hasSize(1));
        assertThat(as(sums.get(0).field(), Attribute.class).name(), equalTo("long_noidx"));
        assertThat(aggregate.aggregates().stream().map(Alias::unwrap).filter(Count.class::isInstance).count(), equalTo(1L));
        assertThat("the TO_DOUBLE nothing reads is pruned", aggregate.child().collect(Eval.class::isInstance), hasSize(0));
        assertThat(aggregate.child().collect(EsRelation.class::isInstance), hasSize(1));
    }

    /**
     * What the coordinator merges must be what the data node sends, so the replacement has to describe the very same
     * intermediate state, for every aggregate of the plan.
     */
    public void testIntermediateStateIsUnchanged() {
        var plan = testAnalyzer().coordinatorPlan("FROM test | STATS a = AVG(long_noidx), b = AVG(long_noidx) + 1, c = COUNT(*)");
        var local = localPlan(plan, TEST_SEARCH_STATS);

        Aggregate original = aggregate(plan);
        Aggregate replaced = aggregate(local);
        assertThat(replaced.aggregates().stream().map(Alias::unwrap).filter(SumLongAsDouble.class::isInstance).count(), equalTo(1L));

        List<NamedExpression> expected = AggregateMapper.mapNonGrouping(original.aggregates());
        List<NamedExpression> actual = AggregateMapper.mapNonGrouping(replaced.aggregates());
        assertThat(actual.stream().map(NamedExpression::name).toList(), equalTo(expected.stream().map(NamedExpression::name).toList()));
        assertThat(
            actual.stream().map(ne -> ne.toAttribute().dataType()).toList(),
            equalTo(expected.stream().map(ne -> ne.toAttribute().dataType()).toList())
        );
    }

    /** A conversion the user spelled out has to keep rounding every value, so the sum stays as it is. */
    public void testExplicitToDoubleIsUnchanged() {
        assertNotReplaced("FROM test | STATS s = SUM(TO_DOUBLE(long_noidx))");
    }

    /** Same for a conversion in an {@code EVAL}, which is inlined into the aggregate on the way. */
    public void testExplicitToDoubleInEvalIsUnchanged() {
        assertNotReplaced("FROM test | EVAL d = TO_DOUBLE(long_noidx) | STATS s = SUM(d)");
    }

    /**
     * The sums of a sampled aggregate are scaled up by the sample probability, and that only knows a {@link Sum}, so a sampled
     * aggregate keeps its sums, while the same aggregate, not sampled, gets the replacement.
     */
    public void testSampledAggregateIsUnchanged() {
        var plan = testAnalyzer().coordinatorPlan("FROM test | STATS a = AVG(long_noidx)");
        Aggregate aggregate = aggregate(plan);
        var rule = new ReplaceSumOfToDoubleWithSumLongAsDouble();

        Aggregate replaced = as(rule.apply(aggregate), Aggregate.class);
        assertThat(replaced.aggregates().stream().map(Alias::unwrap).filter(SumLongAsDouble.class::isInstance).count(), equalTo(1L));

        SampledAggregate sampled = new SampledAggregate(
            aggregate.source(),
            aggregate.child(),
            aggregate.groupings(),
            aggregate.aggregates(),
            aggregate.aggregates(),
            Literal.fromDouble(Source.EMPTY, 0.1)
        );
        assertThat(rule.apply(sampled), sameInstance(sampled));
    }

    public void testAvgOfIntegerIsUnchanged() {
        assertNotReplaced("FROM test | STATS a = AVG(salary)");
    }

    public void testSumOfToDoubleOfIntegerIsUnchanged() {
        assertNotReplaced("FROM test | STATS s = SUM(TO_DOUBLE(salary))");
    }

    public void testSumOfLongIsUnchanged() {
        assertNotReplaced("FROM test | STATS s = SUM(long_noidx)");
    }

    /** The aggregator function has no grouping variant. */
    public void testGroupedAvgOfLongIsUnchanged() {
        assertNotReplaced("FROM test | STATS a = AVG(long_noidx) BY emp_no");
    }

    /** Aggregates with different filters can't share a {@code Filter} below the aggregate, so each keeps its own. */
    public void testFilteredAvgsOfLongAreUnchanged() {
        assertNotReplaced("FROM test | STATS a = AVG(long_noidx) WHERE salary > 1000, b = AVG(long_noidx) WHERE salary < 500", 2);
    }

    /**
     * A filter that all the aggregates share is hoisted into a {@code Filter} below the aggregate, which keeps applying to
     * the rows, so the sum itself is a plain one and gets replaced.
     */
    public void testAvgOfLongWithTheFilterHoistedIsReplaced() {
        var plan = testAnalyzer().coordinatorPlan("FROM test | STATS a = AVG(long_noidx) WHERE salary > 1000");
        var local = localPlan(plan, TEST_SEARCH_STATS);

        Aggregate aggregate = aggregate(local);
        assertThat(aggregate.aggregates().stream().map(Alias::unwrap).filter(SumLongAsDouble.class::isInstance).count(), equalTo(1L));
        assertThat(aggregate.child().collect(Filter.class::isInstance).size() > 0, equalTo(true));
    }

    public void testToDoubleOfAnExpressionIsUnchanged() {
        assertNotReplaced("FROM test | STATS s = SUM(TO_DOUBLE(long_noidx + 1))");
    }

    private void assertNotReplaced(String query) {
        assertNotReplaced(query, 1);
    }

    private void assertNotReplaced(String query, int expectedSums) {
        var plan = testAnalyzer().coordinatorPlan(query);
        var local = localPlan(plan, TEST_SEARCH_STATS);

        Aggregate aggregate = aggregate(local);
        assertThat(aggregate.aggregates().stream().map(Alias::unwrap).filter(SumLongAsDouble.class::isInstance).count(), equalTo(0L));
        assertThat(sumsOf(aggregate), hasSize(expectedSums));
    }

    private static List<Sum> sumsOf(Aggregate aggregate) {
        return aggregate.aggregates().stream().map(Alias::unwrap).filter(Sum.class::isInstance).map(Sum.class::cast).toList();
    }

    private static Aggregate aggregate(LogicalPlan plan) {
        List<LogicalPlan> aggregates = plan.collect(Aggregate.class::isInstance);
        assertThat(aggregates, hasSize(1));
        return as(aggregates.get(0), Aggregate.class);
    }
}
