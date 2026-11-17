/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.promql;

import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.OrderBy;
import org.elasticsearch.xpack.esql.plan.logical.TimeSeriesAggregate;
import org.elasticsearch.xpack.esql.plan.logical.TimeSeriesCollapse;
import org.junit.Before;

import java.util.List;

public class PromqlPlanSortTests extends AbstractPromqlPlanOptimizerTests {

    public PromqlPlanSortTests(VersionMode versionMode) {
        super(versionMode);
    }

    @Before
    public void assumeSortEnabled() {
        assumeTrue("Requires PROMQL_SORT capability", EsqlCapabilities.Cap.PROMQL_SORT.isEnabled());
    }

    public void testSortIsIdentityWithBareSelector() {
        assertIdentity("sort(network.bytes_in)", "network.bytes_in");
    }

    public void testSortDescIsIdentityWithBareSelector() {
        assertIdentity("sort_desc(network.bytes_in)", "network.bytes_in");
    }

    public void testSortIsIdentityWithAggregation() {
        assertIdentity("sort(avg by (cluster) (network.bytes_in))", "avg by (cluster) (network.bytes_in)");
    }

    public void testInstantSortInjectsOrderByOnBareCommand() {
        LogicalPlan plan = planPromql("PROMQL index=k8s time=\"2024-05-10T00:03:00.000Z\" result=(sort(network.bytes_in))", false);
        assertEquals(1, plan.collect(OrderBy.class).size());
        assertTrue(plan.collect(TimeSeriesCollapse.class).isEmpty());
    }

    public void testRangeSortDoesNotInjectOrderBy() {
        LogicalPlan plan = planPromql("PROMQL index=k8s step=1h result=(sort(network.bytes_in))", false);
        assertTrue(plan.collect(OrderBy.class).isEmpty());
        assertWarnings("sort: ordering is discarded for range queries");
    }

    /**
     * Prometheus keeps an instant query's order through steps that pass a sort's values through unchanged, so the result is
     * ordered by its own value.
     */
    public void testInstantSortBelowValuePreservingStepsInjectsOrderBy() {
        for (String query : List.of(
            "label_replace(sort(sum by (cluster) (network.bytes_in)), \"dst\", \"$1\", \"cluster\", \"(.*)\")",
            "sort(network.bytes_in) > 100",
            "100 < sort_desc(network.bytes_in)",
            "sort(network.bytes_in) > network.cost"
        )) {
            assertEquals(query, 1, planInstant(query).collect(OrderBy.class).size());
        }
    }

    /** Below a step that changes the values or the series, ordering by the result's value would not give the sort's order. */
    public void testInstantSortBelowOtherStepsDoesNotInjectOrderBy() {
        for (String query : List.of(
            "abs(sort(network.bytes_in))",
            "sort(network.bytes_in) * 2",
            "sum(sort(network.bytes_in))",
            "topk(1, sort_desc(network.bytes_in))",
            "sort(network.bytes_in) or network.cost"
        )) {
            assertTrue(query, planInstant(query).collect(OrderBy.class).isEmpty());
        }
    }

    private LogicalPlan planInstant(String query) {
        return planPromql("PROMQL index=k8s time=\"2024-05-10T00:03:00.000Z\" result=(" + query + ")", false);
    }

    private void assertIdentity(String sorted, String bare) {
        LogicalPlan withSort = planPromql("PROMQL index=k8s step=1h result=(" + sorted + ")");
        LogicalPlan without = planPromql("PROMQL index=k8s step=1h result=(" + bare + ")");
        assertEquals(outputColumns(without), outputColumns(withSort));
        assertEquals(without.collect(TimeSeriesAggregate.class).size(), withSort.collect(TimeSeriesAggregate.class).size());
        String functionName = sorted.startsWith("sort_desc") ? "sort_desc" : "sort";
        assertWarnings(functionName + ": ordering is discarded for range queries");
    }

    private static List<String> outputColumns(LogicalPlan plan) {
        return plan.output().stream().map(Attribute::name).toList();
    }
}
