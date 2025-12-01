/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer;

import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.elasticsearch.xpack.esql.core.tree.NodeUtils;
import org.elasticsearch.xpack.esql.plan.physical.EsQueryExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.session.Configuration;

import static org.elasticsearch.index.query.QueryBuilders.existsQuery;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.assertEqualsIgnoringIds;
import static org.elasticsearch.xpack.esql.core.querydsl.query.Query.unscore;
import static org.hamcrest.Matchers.is;

/**
 * Tests for the {@link org.elasticsearch.xpack.esql.optimizer.rules.logical.local.IgnoreNullMetrics} planner rule, to
 * verify that the filters are being pushed to Lucene.
 */
public class IgnoreNullMetricsPhysicalPlannerTests extends AbstractLocalPhysicalPlanOptimizerTests {
    public IgnoreNullMetricsPhysicalPlannerTests(String name, Configuration config) {
        super(name, config);
    }

    /**
     * This tests that we get the same end result plan with an explicit isNotNull and the implicit one added by the rule
     */
    public void testSamePhysicalPlans() {
        assumeTrue("requires metrics command", EsqlCapabilities.Cap.TS_COMMAND_V0.isEnabled());
        String testQuery = """
            TS k8s
            | STATS max(rate(network.total_bytes_in)) BY Bucket(@timestamp, 1 hour)
            | LIMIT 10
            """;
        PhysicalPlan actualPlan = plannerOptimizerTimeSeries.plan(testQuery);

        String controlQuery = """
            TS k8s
            | WHERE network.total_bytes_in IS NOT NULL
            | STATS max(rate(network.total_bytes_in)) BY Bucket(@timestamp, 1 hour)
            | LIMIT 10
            """;
        PhysicalPlan expectedPlan = plannerOptimizerTimeSeries.plan(controlQuery);

        assertEqualsIgnoringIds(NodeUtils.diffString(expectedPlan, actualPlan), expectedPlan, actualPlan);
    }

    public void testPushdownOfSimpleCounterQuery() {
        assumeTrue("requires metrics command", EsqlCapabilities.Cap.TS_COMMAND_V0.isEnabled());
        String query = """
            TS k8s
            | STATS max(rate(network.total_bytes_in)) BY Bucket(@timestamp, 1 hour)
            | LIMIT 10
            """;
        PhysicalPlan actualPlan = plannerOptimizerTimeSeries.plan(query);
        EsQueryExec queryExec = (EsQueryExec) actualPlan.collect(node -> node instanceof EsQueryExec).get(0);

        QueryBuilder expected = unscore(existsQuery("network.total_bytes_in"));
        assertThat(queryExec.query().toString(), is(expected.toString()));
    }

    public void testPushdownOfSimpleGagueQuery() {
        assumeTrue("requires metrics command", EsqlCapabilities.Cap.TS_COMMAND_V0.isEnabled());
        String query = """
            TS k8s
            | STATS max(max_over_time(network.eth0.tx)) BY Bucket(@timestamp, 1 hour)
            | LIMIT 10
            """;
        PhysicalPlan actualPlan = plannerOptimizerTimeSeries.plan(query);
        EsQueryExec queryExec = (EsQueryExec) actualPlan.collect(node -> node instanceof EsQueryExec).get(0);

        QueryBuilder expected = unscore(existsQuery("network.eth0.tx"));
        assertThat(queryExec.query().toString(), is(expected.toString()));
    }
}
