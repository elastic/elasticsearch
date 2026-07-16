/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.promql;

import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.expression.Order;
import org.elasticsearch.xpack.esql.expression.predicate.nulls.IsNotNull;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.TopNBy;
import org.elasticsearch.xpack.esql.plan.logical.UnionAll;

import java.util.List;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.as;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasSize;

/**
 * Golden plan tests for the top-level PromQL {@code or} (UNION) set operator. They assert the
 * {@code UnionAll} + {@code TopNBy} shape produced for the supported fallback idioms.
 */
public class PromqlPlanSetOperatorTests extends AbstractPromqlPlanOptimizerTests {

    public void testSimpleUnionShape() {
        LogicalPlan plan = planPromql("PROMQL index=k8s step=1m network.bytes_in or network.cost", false);
        assertTrue(plan.resolved());
        assertUnionDedupShape(plan, 2);
    }

    public void testRateOrIrateShape() {
        LogicalPlan plan = planPromql(
            "PROMQL index=k8s step=1m rate(network.total_bytes_in[5m]) or irate(network.total_bytes_in[5m])",
            false
        );
        assertTrue(plan.resolved());
        assertUnionDedupShape(plan, 2);
    }

    public void testExprOrVectorZeroShape() {
        LogicalPlan plan = planPromql("PROMQL index=k8s step=1m sum(rate(network.total_bytes_in[5m])) or vector(0)", false);
        assertTrue(plan.resolved());
        assertUnionDedupShape(plan, 2);
    }

    public void testParenthesizedExprOrVectorZeroShape() {
        LogicalPlan plan = planPromql("PROMQL index=k8s step=1m (sum(rate(network.total_bytes_in[5m]))) or (vector(0))", false);
        assertTrue(plan.resolved());
        assertUnionDedupShape(plan, 2);
    }

    public void testLeftAssociativeChainFlattensToOneUnion() {
        LogicalPlan plan = planPromql("PROMQL index=k8s step=1m network.bytes_in or network.cost or network.total_bytes_in", false);
        assertTrue(plan.resolved());
        // A left-associative chain `(a or b) or c` flattens into a single 3-branch UnionAll, not nested unions.
        assertUnionDedupShape(plan, 3);
    }

    /**
     * Each union branch must drop rows whose value is null before the dedup, so an absent left side does not
     * shadow a present right side (the {@code rate(...) or irate(...)} fallback idiom). This pins the filter to
     * the value column - it is not the first output column of an un-projected branch, so a positional filter
     * would silently target a label/step column instead.
     */
    public void testEachBranchDropsNullValues() {
        LogicalPlan plan = planPromql("PROMQL index=k8s step=1m v=(network.bytes_in or network.cost)", false);
        assertTrue(plan.resolved());
        List<UnionAll> unions = plan.collect(UnionAll.class);
        assertThat(unions, hasSize(1));
        for (LogicalPlan branch : unions.getFirst().children()) {
            boolean dropsNullValues = branch.collect(Filter.class)
                .stream()
                .map(Filter::condition)
                .anyMatch(c -> c instanceof IsNotNull notNull && notNull.field() instanceof Attribute a && a.name().equals("v"));
            assertTrue("each branch must drop null values on the value column [v]: " + branch, dropsNullValues);
        }
    }

    /**
     * Asserts that the plan contains exactly one {@link UnionAll} with {@code expectedBranches} branches, fed into a
     * single {@link TopNBy} that keeps one row per group ordered by the synthetic branch tag ascending, grouped by
     * the step column (plus any label columns).
     */
    private static void assertUnionDedupShape(LogicalPlan plan, int expectedBranches) {
        List<UnionAll> unions = plan.collect(UnionAll.class);
        assertThat("expected a single UnionAll", unions, hasSize(1));
        assertThat(unions.getFirst().children(), hasSize(expectedBranches));

        List<TopNBy> topNs = plan.collect(TopNBy.class);
        assertThat("expected a single TopNBy for the union dedup", topNs, hasSize(1));
        TopNBy topN = topNs.getFirst();

        // Keep a single row per group.
        assertThat(as(topN.limitPerGroup().fold(FoldContext.small()), Integer.class), equalTo(1));

        // Ordered by the synthetic branch tag ascending, so the leftmost branch wins.
        assertThat(topN.order(), hasSize(1));
        Order order = topN.order().getFirst();
        assertThat(order.direction(), equalTo(Order.OrderDirection.ASC));
        assertThat(as(order.child(), Attribute.class).name(), equalTo("_branch"));

        // Grouped by the step column (and any labels), never by the value or branch tag.
        List<String> groupingNames = topN.groupings().stream().map(e -> as(e, NamedExpression.class).name()).toList();
        assertThat(groupingNames, hasItem("step"));
        assertFalse(groupingNames.contains("_branch"));

        // The synthetic branch tag must not leak into the command output.
        List<String> outputNames = plan.output().stream().map(Attribute::name).toList();
        assertFalse("the synthetic _branch column must be projected away", outputNames.contains("_branch"));
    }
}
