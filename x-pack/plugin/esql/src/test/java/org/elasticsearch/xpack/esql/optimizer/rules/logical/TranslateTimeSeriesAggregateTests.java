/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical;

import org.elasticsearch.xpack.esql.core.expression.Expressions;
import org.elasticsearch.xpack.esql.optimizer.AbstractLogicalPlanOptimizerTests;
import org.elasticsearch.xpack.esql.plan.logical.Fork;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;

import java.util.List;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasSize;

public class TranslateTimeSeriesAggregateTests extends AbstractLogicalPlanOptimizerTests {

    /**
     * Regression test: a constant literal aggregate (e.g. {@code metric_type = "mongodb"}) must land in
     * the outer {@code Aggregate} produced by {@link TranslateTimeSeriesAggregate}, not in the inner
     * {@code TimeSeriesAggregate}. Before the fix, it was placed only in the inner phase, causing
     * {@code CombineProjections} to drop it and the plan verifier to report missing references.
     */
    public void testLiteralAggregateGoesToOuterAggregate() {
        var query = """
            TS k8s
            | STATS metric_type = "network.cost", cnt = COUNT(COUNT_OVER_TIME(network.cost)) BY cluster
            | SORT cluster
            """;

        var plan = planMetrics(query);

        assertThat(Expressions.names(plan.output()), containsInAnyOrder("metric_type", "cnt", "cluster"));
    }

    /**
     * Regression test for the FORK variant: each branch has a TS STATS with a per-branch constant
     * literal label (metric_type = "cost" / "cost_max") plus a time-series aggregate. The optimizer
     * must not drop the literal column when the alignment Project is merged by CombineProjections.
     */
    public void testLiteralAggregateInForkBranchSurvivesOptimizer() {
        var query = """
            TS k8s
            | FORK
                (STATS metric_type = "cost", cnt = COUNT(COUNT_OVER_TIME(network.cost)) BY cluster)
                (STATS metric_type = "cost_max", cnt = COUNT(MAX_OVER_TIME(network.cost)) BY cluster)
            | KEEP _fork, cluster, metric_type, cnt
            | SORT _fork, cluster
            """;

        var plan = planMetrics(query);

        assertThat(Expressions.names(plan.output()), containsInAnyOrder("_fork", "cluster", "metric_type", "cnt"));

        List<Fork> forks = plan.collectFirstChildren(p -> p instanceof Fork).stream().map(p -> (Fork) p).toList();
        assertThat(forks, hasSize(1));
        for (LogicalPlan branch : forks.get(0).children()) {
            assertThat(Expressions.names(branch.output()), hasItem("metric_type"));
        }
    }
}
