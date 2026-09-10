/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.parser;

import org.elasticsearch.index.IndexMode;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.TimeSeriesAggregate;
import org.elasticsearch.xpack.esql.plan.logical.TimeSeriesExemplars;
import org.elasticsearch.xpack.esql.plan.logical.UnresolvedRelation;
import org.elasticsearch.xpack.esql.plan.logical.promql.PromqlCommand;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.as;

/**
 * {@code TS_EXEMPLARS} parses into a node wrapping the metrics query; the exemplar data streams are only derived once the metrics
 * index pattern is resolved, so the parser does not produce a relation for them.
 */
public class TimeSeriesExemplarsParserTests extends AbstractStatementParserTests {

    public void testBasicCommand() {
        TimeSeriesExemplars exemplars = as(
            query("TS_EXEMPLARS (TS metrics-otel-* | STATS AVG(metrics.cpu_time))"),
            TimeSeriesExemplars.class
        );

        TimeSeriesAggregate aggregate = as(exemplars.metricsQuery(), TimeSeriesAggregate.class);
        assertRelation(aggregate.child(), "metrics-otel-*");
        assertEquals("metrics-otel-*", exemplars.metricsIndexPattern().indexPattern());
        assertTrue(exemplars.output().isEmpty());
    }

    public void testDimensionFilterAndFollowingCommands() {
        Filter outerFilter = as(query("""
            TS_EXEMPLARS (
              TS metrics-generic.otel-default
              | WHERE attributes.host_name == "host-a"
              | STATS MAX(metrics.request_duration)
            )
            | WHERE trace_id IS NOT NULL
            """), Filter.class);
        TimeSeriesExemplars exemplars = as(outerFilter.child(), TimeSeriesExemplars.class);
        TimeSeriesAggregate aggregate = as(exemplars.metricsQuery(), TimeSeriesAggregate.class);
        Filter metricsFilter = as(aggregate.child(), Filter.class);
        assertRelation(metricsFilter.child(), "metrics-generic.otel-default");
    }

    public void testArbitraryProcessingCommandsInMetricsQuery() {
        TimeSeriesExemplars exemplars = as(query("""
            TS_EXEMPLARS (
              TS metrics-generic.otel-default
              | EVAL host = attributes.host_name
              | WHERE host == "host-a"
              | STATS MAX(metrics.request_duration) BY host
              | WHERE host IS NOT NULL
            )
            """), TimeSeriesExemplars.class);
        Filter postAggregateFilter = as(exemplars.metricsQuery(), Filter.class);
        TimeSeriesAggregate aggregate = as(postAggregateFilter.child(), TimeSeriesAggregate.class);
        Filter metricsFilter = as(aggregate.child(), Filter.class);
        Eval eval = as(metricsFilter.child(), Eval.class);
        assertRelation(eval.child(), "metrics-generic.otel-default");
    }

    public void testPromqlMetricsQuery() {
        TimeSeriesExemplars exemplars = as(query("""
            TS_EXEMPLARS (
              PROMQL index=metrics-generic.otel-default step=5m start="2024-05-10T00:20:00.000Z" end="2024-05-10T00:25:00.000Z"
                (avg(rate(metrics.request_duration{attributes.host_name="host-a"}[5m])))
            )
            """), TimeSeriesExemplars.class);
        PromqlCommand promql = as(exemplars.metricsQuery(), PromqlCommand.class);
        assertRelation(promql.child(), "metrics-generic.otel-default");
        assertEquals("metrics-generic.otel-default", exemplars.metricsIndexPattern().indexPattern());
    }

    public void testMultipleAndRemoteIndexPatterns() {
        TimeSeriesExemplars exemplars = as(
            query("TS_EXEMPLARS (TS remote:metrics-a.otel-*::data,metrics-b.otel-* | STATS AVG(metrics.cpu_time))"),
            TimeSeriesExemplars.class
        );
        assertEquals("remote:metrics-a.otel-*::data,metrics-b.otel-*", exemplars.metricsIndexPattern().indexPattern());
    }

    private static void assertRelation(LogicalPlan plan, String pattern) {
        UnresolvedRelation relation = as(plan, UnresolvedRelation.class);
        assertEquals(pattern, relation.indexPattern().indexPattern());
        assertEquals(IndexMode.TIME_SERIES, relation.indexMode());
    }
}
