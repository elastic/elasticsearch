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

    @Before
    public void assumeSortEnabled() {
        assumeTrue("Requires PROMQL_SORT capability", EsqlCapabilities.Cap.PROMQL_SORT.isEnabled());
        assumeTrue("Requires PROMQL_SORT_DESC capability", EsqlCapabilities.Cap.PROMQL_SORT_DESC.isEnabled());
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
