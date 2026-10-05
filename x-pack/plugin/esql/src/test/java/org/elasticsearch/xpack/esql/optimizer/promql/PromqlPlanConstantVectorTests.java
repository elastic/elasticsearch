/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.promql;

import org.elasticsearch.xpack.esql.VerificationException;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.GreaterThan;
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.MvExpand;
import org.elasticsearch.xpack.esql.plan.logical.TimeSeriesAggregate;
import org.elasticsearch.xpack.esql.plan.logical.join.InnerJoin;

import java.util.List;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.as;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

/**
 * {@code vector(s)} is one series with no labels whose value is {@code s} at every step of the query, independent of the
 * source. These tests pin how it aggregates, compares and pairs.
 */
public class PromqlPlanConstantVectorTests extends AbstractPromqlPlanOptimizerTests {

    private static final String RANGE = "PROMQL index=k8s start=\"2024-05-10T00:00:00Z\" end=\"2024-05-10T00:10:00Z\" step=1m v=(";
    private static final String INSTANT = "PROMQL index=k8s time=\"2024-05-10T00:10:00Z\" v=(";

    public PromqlPlanConstantVectorTests(VersionMode versionMode) {
        super(versionMode);
    }

    /**
     * An aggregate over a constant vector regroups a table of one row per step; the source is never read, let alone
     * collapsed into one copy of the constant per stored series. What composes over it stays such a table.
     */
    public void testAggregateOverAConstantVectorRegroupsItsTable() {
        for (String prefix : List.of(RANGE, INSTANT)) {
            for (String promql : List.of(
                "sum(vector(1))",
                "count(vector(5))",
                "sum by (pod) (vector(1))",
                "sum(abs(vector(-1)))",
                "abs(sum(vector(-1)))",
                "count(vector(1)) * 2",
                "sum(vector(1)) + vector(2)",
                "vector(2) - sum(vector(time()))"
            )) {
                LogicalPlan plan = translated(prefix, promql);
                assertThat(promql, plan.collect(EsRelation.class), empty());
                assertThat(promql, plan.collect(TimeSeriesAggregate.class), empty());
                Aggregate regroup = plan.collect(Aggregate.class).getFirst();
                assertThat(promql, as(regroup.groupings().getFirst(), NamedExpression.class).name(), equalTo("step"));
                assertThat(promql, regroup.collect(MvExpand.class), hasSize(1));
            }
        }
    }

    /** A filter-mode comparison applies to a constant vector as to any other: {@code vector(1) > 2} is the empty vector. */
    public void testFilterComparisonOverAConstantVector() {
        for (String prefix : List.of(RANGE, INSTANT)) {
            assertThat(optimized(prefix, "vector(1) > 2").collect(MvExpand.class), empty());
            assertThat(optimized(prefix, "vector(3) > 2").collect(MvExpand.class), hasSize(1));
            LogicalPlan aggregated = translated(prefix, "count(vector(1)) > 1");
            assertTrue(
                aggregated.anyMatch(
                    p -> p instanceof Filter f && f.condition() instanceof GreaterThan && f.child().anyMatch(Aggregate.class::isInstance)
                )
            );
        }
    }

    /**
     * A constant vector pairs only with series that have no labels either: next to an aggregate without grouping it
     * has a value, next to a closed label set it pairs through the join (where {@code {}} matches only absent labels),
     * and against a scalar it stays a constant table that never touches the source.
     */
    public void testConstantVectorAgainstAVector() {
        for (String promql : List.of("sum(network.bytes_in) + vector(1)", "vector(1) + sum(network.bytes_in)")) {
            LogicalPlan plan = translated(RANGE, promql);
            assertThat(promql, plan.collect(InnerJoin.class), empty());
            assertThat(promql, plan.collect(MvExpand.class), empty());
        }
        assertThat(translated(RANGE, "sum by (cluster) (network.bytes_in) + vector(1)").collect(InnerJoin.class), hasSize(1));
        for (String promql : List.of("vector(1) + vector(2)", "vector(1) + 2", "abs(vector(-1)) * 3", "vector(1) + time()")) {
            LogicalPlan plan = translated(RANGE, promql);
            assertThat(promql, plan.collect(InnerJoin.class), empty());
            assertThat(promql, plan.collect(EsRelation.class), empty());
        }
    }

    /**
     * Pairing a constant vector with a vector whose label set is packed would need the series that have no labels told
     * apart from the others, which main cannot do yet: the verifier rejects it rather than broadcasting the constant to
     * every series ({@code x * vector(2)} is empty in Prometheus).
     */
    public void testConstantVectorAgainstAVectorWithoutConcreteLabelsIsRejected() {
        for (String promql : List.of(
            "network.bytes_in * vector(2)",
            "vector(2) * network.bytes_in",
            "vector(time()) - network.bytes_in",
            "rate(network.total_bytes_in[5m]) + abs(vector(1))"
        )) {
            for (String prefix : List.of(RANGE, INSTANT)) {
                VerificationException e = expectThrows(VerificationException.class, () -> translated(prefix, promql));
                assertThat(
                    promql,
                    e.getMessage(),
                    containsString("binary operations between vector() and a vector without a concrete label set are not supported")
                );
            }
        }
    }

    /**
     * An aggregate over a constant vector is a relation of its own; combining it with another relation - an aggregate over
     * the source, or over a constant vector again - would need a join the shared aggregate cannot express.
     */
    public void testAggregateOverAConstantVectorAgainstAnotherTableIsRejected() {
        for (String promql : List.of(
            "sum(vector(1)) + sum(network.bytes_in)",
            "sum(network.bytes_in) / count(vector(1))",
            "sum(vector(1)) + sum(vector(2))"
        )) {
            VerificationException e = expectThrows(VerificationException.class, () -> translated(RANGE, promql));
            assertThat(promql, e.getMessage(), containsString("are not supported at this time"));
        }
    }

    private LogicalPlan translated(String prefix, String promql) {
        return planPromql(prefix + promql + ")", false, false);
    }

    private LogicalPlan optimized(String prefix, String promql) {
        return planPromql(prefix + promql + ")", false, true);
    }
}
