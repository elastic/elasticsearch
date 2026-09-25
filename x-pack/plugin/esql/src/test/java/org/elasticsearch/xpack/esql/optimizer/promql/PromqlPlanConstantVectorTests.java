/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.promql;

import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.NameId;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.MvExpand;
import org.elasticsearch.xpack.esql.plan.logical.TimeSeriesAggregate;
import org.elasticsearch.xpack.esql.plan.logical.UnionAll;
import org.elasticsearch.xpack.esql.plan.logical.join.InnerJoin;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.as;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

/**
 * {@code vector(s)} is a table of one label-less row per step of the query, independent of the source. These tests pin how
 * that table aggregates, pairs, unions and compares.
 */
public class PromqlPlanConstantVectorTests extends AbstractPromqlPlanOptimizerTests {

    private static final String RANGE = "PROMQL index=k8s start=\"2024-05-10T00:00:00Z\" end=\"2024-05-10T00:10:00Z\" step=1m v=(";
    private static final String INSTANT = "PROMQL index=k8s time=\"2024-05-10T00:10:00Z\" v=(";

    public PromqlPlanConstantVectorTests(VersionMode versionMode) {
        super(versionMode);
    }

    /** An aggregate over a constant vector regroups its table by step; the source is never read, let alone collapsed. */
    public void testAggregateOverAConstantVectorRegroupsItsTable() {
        for (String promql : List.of("sum(vector(1))", "count(vector(5))", "sum by (pod) (vector(1))", "sum(abs(vector(-1)))")) {
            LogicalPlan plan = translated(RANGE, promql);
            assertThat(promql, plan.collect(EsRelation.class), empty());
            assertThat(promql, plan.collect(TimeSeriesAggregate.class), empty());
            Aggregate regroup = plan.collect(Aggregate.class).getFirst();
            assertThat(promql, as(regroup.groupings().getFirst(), NamedExpression.class).name(), equalTo("step"));
            assertThat(promql, regroup.collect(MvExpand.class), hasSize(1));
        }
    }

    /**
     * The constant relation's step list is its own column. Defined under the command's step id - which the final projection
     * defines again - the optimizer folded the whole list into the range filter above: every union with a constant branch
     * came back empty, and under an instant query the filter vanished instead, letting a sample one lookback before the
     * query time reach the collapse out of range.
     */
    public void testConstantRelationDefinesItsOwnStepColumn() {
        for (String prefix : List.of(RANGE, INSTANT)) {
            for (String promql : List.of("network.bytes_in or vector(0)", "vector(1) or vector(2)", "sum(network.bytes_in) or vector(0)")) {
                LogicalPlan plan = translated(prefix, promql);
                Map<NameId, Set<Alias>> literalColumns = new HashMap<>();
                plan.forEachExpressionDown(Alias.class, alias -> {
                    if (alias.child() instanceof Literal) {
                        literalColumns.computeIfAbsent(alias.id(), id -> new HashSet<>()).add(alias);
                    }
                });
                plan.forEachExpressionDown(Alias.class, alias -> {
                    Set<Alias> definitions = literalColumns.get(alias.id());
                    if (definitions != null) {
                        assertThat(promql + ": a literal column is defined once\n" + plan, definitions.contains(alias), equalTo(true));
                    }
                });
                LogicalPlan optimized = logicalOptimizer.optimize(plan);
                assertThat(promql + ": the union survives optimization\n" + optimized, optimized.collect(UnionAll.class), hasSize(1));
                boolean filtersStep = optimized.anyMatch(
                    p -> p instanceof Filter f && f.condition().references().stream().anyMatch(a -> a.name().equals("step"))
                );
                assertTrue(promql + ": the step range filter survives optimization\n" + optimized, filtersStep);
            }
        }
    }

    /** A filter-mode comparison applies to a constant vector as to any other: {@code vector(1) > 2} is the empty vector. */
    public void testFilterComparisonOverAConstantVector() {
        assertThat(optimized(RANGE, "vector(1) > 2").collect(MvExpand.class), empty());
        assertThat(optimized(RANGE, "vector(3) > 2").collect(MvExpand.class), hasSize(1));
    }

    /**
     * A constant vector against another vector pairs through the join - {@code {}} matches {@code {}} alone, so
     * {@code sum(m) + vector(1)} has a value and {@code sum by (k) (m) + vector(1)} is empty; against a scalar it stays a
     * constant table and never touches the source.
     */
    public void testConstantVectorAgainstAVectorPairsThroughTheJoin() {
        for (String promql : List.of(
            "sum(network.bytes_in) + vector(1)",
            "vector(1) + sum(network.bytes_in)",
            "network.bytes_in * vector(2)",
            "vector(1) + vector(2)"
        )) {
            assertThat(promql, translated(RANGE, promql).collect(InnerJoin.class), hasSize(1));
        }
        for (String promql : List.of("vector(1) + 2", "abs(vector(-1)) * 3", "vector(1) + time()", "scalar(vector(5)) + 1")) {
            LogicalPlan plan = translated(RANGE, promql);
            assertThat(promql, plan.collect(InnerJoin.class), empty());
            assertThat(promql, plan.collect(EsRelation.class), empty());
        }
    }

    private LogicalPlan translated(String prefix, String promql) {
        return planPromql(prefix + promql + ")", false, false);
    }

    private LogicalPlan optimized(String prefix, String promql) {
        return planPromql(prefix + promql + ")", false, true);
    }
}
