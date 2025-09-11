/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer;

import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.tree.NodeUtils;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.physical.EsQueryExec;
import org.elasticsearch.xpack.esql.plan.physical.ExchangeExec;
import org.elasticsearch.xpack.esql.plan.physical.FieldExtractExec;
import org.elasticsearch.xpack.esql.plan.physical.LimitExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.plan.physical.ProjectExec;
import org.elasticsearch.xpack.esql.session.Configuration;

import static org.elasticsearch.index.query.QueryBuilders.existsQuery;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.as;
import static org.elasticsearch.xpack.esql.core.querydsl.query.Query.unscore;
import static org.hamcrest.Matchers.is;

/**
 * Tests for the {@link org.elasticsearch.xpack.esql.optimizer.rules.logical.local.IgnoreNullMetrics} planner rule, to
 * verify that the filters are being pushed to Lucene.
 */
public class IgnoreNullMetricsPhysicalPlannerTests extends LocalPhysicalPlanOptimizerTests {
    public IgnoreNullMetricsPhysicalPlannerTests(String name, Configuration config) {
        super(name, config);
    }

    /**
     * This tests that we get the same end result plan with an explicit isNotNull and the implicit one added by the rule
     */
    public void testSamePhysicalPlans() {
        String testQuery = """
            TS k8s
            | STATS max(rate(network.total_bytes_in))
            | LIMIT 10
            """;
        String controlQuery = """
            TS k8s
            | WHERE network.cost IS NOT NULL
            | STATS max(rate(network.total_bytes_in))
            | LIMIT 10
            """;

        // first, make sure we got the same logical plans
        LogicalPlan actualLogicalPlan = plannerOptimizerTimeSeries.logicalPlan(testQuery);
        LogicalPlan expectedLogicalPlan = plannerOptimizerTimeSeries.logicalPlan(controlQuery);
        assertEquals("Logical plans differ, this isn't a physical planner issue", expectedLogicalPlan, actualLogicalPlan);

        PhysicalPlan actualPlan = plannerOptimizerTimeSeries.plan(testQuery);
        PhysicalPlan expectedPlan = plannerOptimizerTimeSeries.plan(controlQuery);
        assertEquals(NodeUtils.diffString(expectedPlan, actualPlan), expectedPlan, actualPlan);
    }

}
