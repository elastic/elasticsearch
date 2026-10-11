/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical;

import org.elasticsearch.xpack.esql.core.expression.Expressions;
import org.elasticsearch.xpack.esql.core.util.Holder;
import org.elasticsearch.xpack.esql.optimizer.AbstractLogicalPlanOptimizerTests;
import org.elasticsearch.xpack.esql.plan.logical.Fork;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.TimeSeriesAggregate;

import java.util.List;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.not;

public class TranslateTimeSeriesAggregateTests extends AbstractLogicalPlanOptimizerTests {

    /**
     * Regression test: a constant literal aggregate (e.g. {@code metric_type = "mongodb"}) must land in
     * the outer {@code Aggregate} produced by {@link TranslateTimeSeriesAggregate}, not in the inner
     * {@code TimeSeriesAggregate}. Before the fix, it was placed only in the inner phase, causing
     * {@code CombineProjections} to drop it and the plan verifier to report missing references.
     */
    public void testLiteralAggregateGoesToOuterAggregate() {
        var plan = planMetrics("""
            TS k8s
            | STATS metric_type = "network.cost", cnt = COUNT(COUNT_OVER_TIME(network.cost)) BY cluster
            | SORT cluster
            """);

        assertThat(Expressions.names(plan.output()), containsInAnyOrder("metric_type", "cnt", "cluster"));
        assertLiteralNotInInnerTsAgg(plan, "metric_type");
    }

    /** Multiple literals in a single STATS — all must reach the outer Aggregate. */
    public void testMultipleLiteralAggregatesGoToOuterAggregate() {
        var plan = planMetrics("""
            TS k8s
            | STATS source = "k8s", metric = "cost", cnt = COUNT(COUNT_OVER_TIME(network.cost)) BY cluster
            """);

        assertThat(Expressions.names(plan.output()), containsInAnyOrder("source", "metric", "cnt", "cluster"));
        assertLiteralNotInInnerTsAgg(plan, "source");
        assertLiteralNotInInnerTsAgg(plan, "metric");
    }

    /** Literal appearing last in the STATS clause (not first) must still reach the outer Aggregate. */
    public void testLiteralAggregateLastPositionGoesToOuterAggregate() {
        var plan = planMetrics("""
            TS k8s
            | STATS cnt = COUNT(COUNT_OVER_TIME(network.cost)), metric_type = "cost" BY cluster
            """);

        assertThat(Expressions.names(plan.output()), containsInAnyOrder("cnt", "metric_type", "cluster"));
        assertLiteralNotInInnerTsAgg(plan, "metric_type");
    }

    /** Integer literal — type is irrelevant; the fix applies to any Literal child. */
    public void testIntegerLiteralAggregateGoesToOuterAggregate() {
        var plan = planMetrics("""
            TS k8s
            | STATS version = 2, cnt = COUNT(COUNT_OVER_TIME(network.cost)) BY cluster
            """);

        assertThat(Expressions.names(plan.output()), containsInAnyOrder("version", "cnt", "cluster"));
        assertLiteralNotInInnerTsAgg(plan, "version");
    }

    /** Multiple grouping keys — literals must still be routed to the outer Aggregate. */
    public void testLiteralAggregateWithMultipleGroupingsGoesToOuterAggregate() {
        var plan = planMetrics("""
            TS k8s
            | STATS metric_type = "cost", cnt = COUNT(COUNT_OVER_TIME(network.cost)) BY cluster, pod
            """);

        assertThat(Expressions.names(plan.output()), containsInAnyOrder("metric_type", "cnt", "cluster", "pod"));
        assertLiteralNotInInnerTsAgg(plan, "metric_type");
    }

    /**
     * Regression test for the FORK variant: each branch has a TS STATS with a per-branch constant
     * literal label (metric_type = "cost" / "cost_max") plus a time-series aggregate. The optimizer
     * must not drop the literal column when the alignment Project is merged by CombineProjections.
     */
    public void testLiteralAggregateInForkBranchSurvivesOptimizer() {
        var plan = planMetrics("""
            TS k8s
            | FORK
                (STATS metric_type = "cost", cnt = COUNT(COUNT_OVER_TIME(network.cost)) BY cluster)
                (STATS metric_type = "cost_max", cnt = COUNT(MAX_OVER_TIME(network.cost)) BY cluster)
            | KEEP _fork, cluster, metric_type, cnt
            | SORT _fork, cluster
            """);

        assertThat(Expressions.names(plan.output()), containsInAnyOrder("_fork", "cluster", "metric_type", "cnt"));

        List<Fork> forks = plan.collectFirstChildren(p -> p instanceof Fork).stream().map(p -> (Fork) p).toList();
        assertThat(forks, hasSize(1));
        for (LogicalPlan branch : forks.get(0).children()) {
            assertThat(Expressions.names(branch.output()), hasItem("metric_type"));
        }
    }

    /**
     * Asserts that the inner {@link TimeSeriesAggregate} in the plan does not expose {@code name}
     * as an output — confirming the literal was not incorrectly placed in the first pass.
     */
    private static void assertLiteralNotInInnerTsAgg(LogicalPlan plan, String name) {
        Holder<TimeSeriesAggregate> tsHolder = new Holder<>();
        // forEachDown overwrites the holder on each match; safe here because each tested plan
        // contains exactly one TimeSeriesAggregate.
        plan.forEachDown(TimeSeriesAggregate.class, tsHolder::set);
        assertNotNull("expected a TimeSeriesAggregate in the plan", tsHolder.get());
        assertThat(
            "literal '" + name + "' must not appear in the inner TimeSeriesAggregate",
            Expressions.names(tsHolder.get().output()),
            not(hasItem(name))
        );
    }
}
