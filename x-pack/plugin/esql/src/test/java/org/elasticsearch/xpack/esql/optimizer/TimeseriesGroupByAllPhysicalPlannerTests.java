/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer;

import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Count;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Values;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.plan.physical.TimeSeriesAggregateExec;
import org.elasticsearch.xpack.esql.session.Configuration;

import java.util.List;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.as;

public class TimeseriesGroupByAllPhysicalPlannerTests extends LocalPhysicalPlanOptimizerTests {
    public TimeseriesGroupByAllPhysicalPlannerTests(String name, Configuration config) {
        super(name, config);
    }

    public void testSimple() {
        assumeTrue("requires metrics command", EsqlCapabilities.Cap.METRICS_COMMAND.isEnabled());
        assumeTrue("requires metrics group by all", EsqlCapabilities.Cap.METRICS_GROUP_BY_ALL.isEnabled());

        String testQuery = """
            TS k8s
            | STATS count = count_over_time(network.cost)
            | LIMIT 10
            """;
        PhysicalPlan actualPlan = plannerOptimizerTimeSeries.plan(testQuery);

        List<PhysicalPlan> aggs = actualPlan.collect(node -> node instanceof TimeSeriesAggregateExec);
        for (PhysicalPlan agg : aggs) {
            if (agg instanceof TimeSeriesAggregateExec tsAgg) {
                assertEquals(3, tsAgg.aggregates().size());
                // Check the first child to unwrap the alias
                Count countFn = as(tsAgg.aggregates().get(0).children().get(0), Count.class);
                Values clusterFn = as(tsAgg.aggregates().get(1).children().get(0), Values.class);
                Values podFn = as(tsAgg.aggregates().get(2).children().get(0), Values.class);
            }
        }
    }
}
