/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.promql;

import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.PackDims;
import org.elasticsearch.xpack.esql.plan.logical.TimeSeriesAggregate;
import org.elasticsearch.xpack.esql.plan.logical.join.HashJoin;

import java.util.List;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.as;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;

/** PromQL arithmetic and comparison operators translated over a label container. */
public class PromqlPlanVectorBinaryOperatorV2Tests extends AbstractPromqlPlanOptimizerTests {

    public PromqlPlanVectorBinaryOperatorV2Tests(VersionMode versionMode) {
        super(versionMode);
    }

    /** Operands over the same series share a frame and fold into one plan without a join. */
    public void testOperandsOverTheSameSeriesFold() {
        LogicalPlan plan = planPromqlV2("PROMQL index=k8s step=1h network.bytes_in + network.cost", false);

        assertThat(plan.collect(HashJoin.class), empty());
        assertThat(plan.collect(TimeSeriesAggregate.class), hasSize(1));
        assertThat(names(plan.output()), hasItem("_timeseries"));
    }

    /** Aggregates grouped the same way share a frame too. */
    public void testAggregatesOfTheSameGrainFold() {
        LogicalPlan plan = planPromqlV2(
            "PROMQL index=k8s step=1h sum by (cluster) (network.bytes_in) / sum by (cluster) (network.cost)",
            false
        );

        assertThat(plan.collect(HashJoin.class), empty());
        assertThat(names(plan.output()), hasItem("cluster"));
    }

    /** {@code on(...)} joins the operands on the step and the listed labels of each series. */
    public void testOnJoinsByTheListedLabels() {
        LogicalPlan plan = planPromqlV2(
            "PROMQL index=k8s step=1h sum by (pod) (network.bytes_in) / on (pod) sum by (pod) (network.cost)",
            false
        );

        HashJoin join = plan.collect(HashJoin.class).getFirst();
        assertThat(join.leftFields(), hasSize(2));
        assertThat(names(as(join.left(), PackDims.class).dims()), equalTo(List.of("pod")));
    }

    /** {@code group_left(...)} writes the included labels of the one side into the many side's labels. */
    public void testGroupLeftWritesTheIncludedLabels() {
        LogicalPlan plan = planPromqlV2(
            "PROMQL index=k8s step=1h sum by (pod, cluster) (network.bytes_in)"
                + " * on (pod) group_left (region) sum by (pod, region) (network.cost)",
            false
        );

        HashJoin join = plan.collect(HashJoin.class).getFirst();
        assertThat(names(as(join.left(), PackDims.class).dims()), equalTo(List.of("pod")));
        assertThat(names(join.addedFields()).stream().anyMatch(name -> name.endsWith("region")), is(true));
        assertThat(names(plan.output()), hasItem("region"));
    }

    /** A source-backed scalar broadcasts over the other operand by joining on the step alone. */
    public void testScalarBroadcastsOnTheStep() {
        LogicalPlan plan = planPromqlV2("PROMQL index=k8s step=1h scalar(sum(network.bytes_in)) * time()", false);

        HashJoin join = plan.collect(HashJoin.class).getFirst();
        assertThat(join.leftFields(), hasSize(1));
    }

    /** A comparison against a literal filters the series. */
    public void testComparisonFiltersTheSeries() {
        LogicalPlan plan = planPromqlV2("PROMQL index=k8s step=1h network.bytes_in > 10", false);
        assertThat(plan.collect(Filter.class), not(empty()));
        assertThat(plan.collect(HashJoin.class), empty());
    }

    /** The translated operators plan through the optimizer. */
    public void testOperatorsOptimize() {
        for (String query : List.of(
            "network.bytes_in + network.cost",
            "sum by (cluster) (network.bytes_in) / sum by (cluster) (network.cost)",
            "sum by (pod) (network.bytes_in) / on (pod) sum by (pod) (network.cost)",
            "sum by (pod, cluster) (network.bytes_in) * on (pod) group_left (region) sum by (pod, region) (network.cost)",
            "scalar(sum(network.bytes_in)) * time()",
            "network.bytes_in > 10"
        )) {
            LogicalPlan plan = planPromqlV2("PROMQL index=k8s step=1h " + query, true);
            assertTrue(query, plan.resolved());
        }
    }

    /** The label lists of the signatures two series match on. */
    private static List<String> names(List<Attribute> attributes) {
        return attributes.stream().map(Attribute::name).toList();
    }
}
