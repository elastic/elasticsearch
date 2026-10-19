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
import org.elasticsearch.xpack.esql.plan.logical.join.HashJoin;

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

    public PromqlPlanSetOperatorTests(VersionMode versionMode) {
        super(versionMode);
    }

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

    /** Nested {@code or} under an aggregation flattens to UnionAll + TopNBy like the top-level chain. */
    public void testNestedUnionUnderSum() {
        LogicalPlan plan = planPromql("PROMQL index=k8s step=1m sum(network.bytes_in or network.cost)", false);
        assertTrue(plan.resolved());
        assertUnionDedupShape(plan, 2);
    }

    public void testNestedUnionRateOrIrate() {
        LogicalPlan plan = planPromql(
            "PROMQL index=k8s step=1m max(rate(network.total_bytes_in[5m]) or irate(network.total_bytes_in[5m]))",
            false
        );
        assertTrue(plan.resolved());
        assertUnionDedupShape(plan, 2);
    }

    /** Nested {@code or} as an arithmetic operand joins rather than folding UnionAll into one Aggregate. */
    public void testNestedUnionInArithmetic() {
        assumeTrue("joining union results needs typed metadata regrouping", versionMode == VersionMode.CURRENT);
        LogicalPlan plan = planPromql(
            "PROMQL index=k8s step=1m (rate(network.total_bytes_in[5m]) or irate(network.total_bytes_in[5m])) "
                + "/ (rate(network.bytes_in[5m]) or irate(network.bytes_in[5m]))",
            false
        );
        assertTrue(plan.resolved());
        assertFalse(plan.collect(HashJoin.class).isEmpty());
    }

    /**
     * {@code and} matches many-to-many: several left series may share one right series under {@code ignoring(pod)}.
     * The join must not demand one-to-one matching, and the build side is cut to one row per key so each left row
     * matches at most once.
     */
    public void testAndIgnoringJoinsManyToMany() {
        LogicalPlan plan = planPromql(
            "PROMQL index=k8s step=1m sum by (cluster, pod) (network.bytes_in) and ignoring (pod) sum by (cluster) (network.cost)",
            false
        );
        assertTrue(plan.resolved());
        List<HashJoin> joins = plan.collect(HashJoin.class);
        assertThat(joins, hasSize(1));
        HashJoin join = joins.getFirst();
        assertFalse("set operators match many-to-many", join.unique());
        List<TopNBy> dedup = join.right().collect(TopNBy.class);
        assertThat(dedup, hasSize(1));
        assertThat(dedup.getFirst().limitPerGroup().fold(FoldContext.small()), equalTo(1));
        assertThat(dedup.getFirst().groupings(), hasSize(join.rightFields().size()));
    }

    /**
     * Empty-guard {@code sum(...) or on () vector(1)}: both sides are labelless, so left-preferring TopNBy BY step
     * alone matches Prometheus (keep left when present, else the fill).
     */
    public void testOrOnEmptyVectorFillShape() {
        LogicalPlan plan = planPromql("PROMQL index=k8s step=1m sum(rate(network.total_bytes_in[5m])) or on () vector(1)", false);
        assertTrue(plan.resolved());
        assertUnionDedupShape(plan, 2);
    }

    /** Nested under division — the Grafana istio empty-denominator idiom. */
    public void testOrOnEmptyNestedUnderDivision() {
        LogicalPlan plan = planPromql(
            "PROMQL index=k8s step=1m sum(rate(network.total_bytes_in[5m])) "
                + "/ (sum(rate(network.total_bytes_in[5m])) or on () vector(1))",
            false
        );
        assertTrue(plan.resolved());
        assertFalse(plan.collect(UnionAll.class).isEmpty());
    }

    /**
     * grafanalabs baseline-2 cases 2017/2018/4687/4688: {@code (agg * 0 + 1) and (ratio >= thresh)} must optimize.
     * Set-op folds null the right value where its filter fails (rather than Filter-dropping the row) so the left
     * value stays live for {@code CASE(IS NOT NULL(right), left)}.
     */
    public void testAndWithArithmeticMaskOptimizes() {
        assumeTrue("requires typed _timeseries", typedTimeSeriesMetadata());
        var plan = planPromql("""
            PROMQL index=k8s step=1m result=(
              ((max by (cluster) (network.bytes_in) * 0 + 1)
                and
               ((max by (cluster) (network.bytes_in) / max by (cluster) (network.cost)) >= 0.8))
            )
            """, true);
        assertTrue(plan.resolved());
    }

    /**
     * grafanalabs baseline-2 cases 2357/2358: {@code (rate or irate)/(rate or irate)} is admitted and plans, but e2e
     * fails with "Cannot project block". Plan-only coverage is insufficient; keep this as a typed-path shape pin until
     * an e2e {@code @AwaitsFix} (FunctionalTimeSeriesIT / csv-spec) owns the runtime assertion.
     */
    public void testOrFallbackRatioPlans() {
        assumeTrue("requires typed _timeseries", typedTimeSeriesMetadata());
        var plan = planPromql("""
            PROMQL index=k8s step=1m result=(
              (rate(network.total_bytes_in[5m]) or irate(network.total_bytes_in[5m]))
              /
              (rate(network.total_bytes_out[5m]) or irate(network.total_bytes_out[5m]))
            )
            """, true);
        assertTrue(plan.resolved());
    }
}
