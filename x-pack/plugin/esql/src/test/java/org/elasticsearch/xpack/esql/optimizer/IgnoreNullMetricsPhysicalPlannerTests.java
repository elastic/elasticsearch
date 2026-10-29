/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.xpack.esql.EsqlTestUtils;
import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.elasticsearch.xpack.esql.analysis.Analyzer;
import org.elasticsearch.xpack.esql.analysis.EnrichResolution;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.tree.NodeUtils;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.core.type.InvalidMappedField;
import org.elasticsearch.xpack.esql.index.EsIndex;
import org.elasticsearch.xpack.esql.index.EsIndexGenerator;
import org.elasticsearch.xpack.esql.index.IndexProperties;
import org.elasticsearch.xpack.esql.index.IndexResolution;
import org.elasticsearch.xpack.esql.plan.physical.EsQueryExec;
import org.elasticsearch.xpack.esql.plan.physical.FilterExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.session.Configuration;

import java.util.Map;
import java.util.Set;

import static org.elasticsearch.index.query.QueryBuilders.boolQuery;
import static org.elasticsearch.index.query.QueryBuilders.existsQuery;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.TEST_FUNCTION_REGISTRY;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.TEST_VERIFIER;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.assertEqualsIgnoringIds;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.emptyInferenceResolution;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.loadMapping;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.logicalOptimizerContext;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.testAnalyzerContext;
import static org.elasticsearch.xpack.esql.analysis.AnalyzerTestUtils.indexResolutions;
import static org.elasticsearch.xpack.esql.core.querydsl.query.Query.unscore;
import static org.hamcrest.Matchers.empty;
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

    public void testPushdownOfMixedAggregateMetricDoubleAndDoubleMetric() {
        assumeTrue("requires metrics command", EsqlCapabilities.Cap.TS_COMMAND_V0.isEnabled());
        assumeTrue("requires implicit aggregate_metric_double casting", EsqlCapabilities.Cap.AGGREGATE_METRIC_DOUBLE_V0.isEnabled());
        String query = """
            TS k8s,k8s-downsampled
            | STATS avg(avg_over_time(network.eth0.rx)) BY Bucket(@timestamp, 1 hour)
            | LIMIT 10
            """;
        PhysicalPlan actualPlan = mixedDownsampledPlanner().plan(query);
        EsQueryExec queryExec = (EsQueryExec) actualPlan.collect(node -> node instanceof EsQueryExec).get(0);
        QueryBuilder expected = boolQuery().should(unscore(existsQuery("network.eth0.rx"))).should(unscore(existsQuery("network.eth0.rx")));
        assertThat(queryExec.query().toString(), is(expected.toString()));
        assertThat(actualPlan.collect(node -> node instanceof FilterExec), empty());
    }

    public void testPushdownOfAggregateMetricDoubleMetric() {
        assumeTrue("requires metrics command", EsqlCapabilities.Cap.TS_COMMAND_V0.isEnabled());
        assumeTrue("requires implicit aggregate_metric_double casting", EsqlCapabilities.Cap.AGGREGATE_METRIC_DOUBLE_V0.isEnabled());
        String query = """
            TS k8s-downsampled
            | STATS avg(avg_over_time(network.eth0.rx)) BY Bucket(@timestamp, 1 hour)
            | LIMIT 10
            """;
        EsIndex esIndex = EsIndexGenerator.esIndex(
            "k8s-downsampled",
            loadMapping("k8s-downsampled-mappings.json"),
            Map.of("k8s-downsampled", IndexMode.TIME_SERIES)
        );
        PhysicalPlan actualPlan = planner(esIndex).plan(query);
        EsQueryExec queryExec = (EsQueryExec) actualPlan.collect(node -> node instanceof EsQueryExec).get(0);

        QueryBuilder expected = unscore(existsQuery("network.eth0.rx"));
        assertThat(queryExec.query().toString(), is(expected.toString()));
        assertThat(actualPlan.collect(node -> node instanceof FilterExec), empty());
    }

    private TestPlannerOptimizer mixedDownsampledPlanner() {
        Map<String, EsField> mapping = Map.of(
            "@timestamp",
            new EsField("@timestamp", DataType.DATETIME, Map.of(), true, EsField.TimeSeriesFieldType.NONE),
            "pod",
            new EsField("pod", DataType.KEYWORD, Map.of(), true, EsField.TimeSeriesFieldType.DIMENSION),
            "network.eth0.rx",
            new InvalidMappedField(
                "network.eth0.rx",
                Map.of("aggregate_metric_double", Set.of("k8s-downsampled"), "double", Set.of("k8s")),
                EsField.TimeSeriesFieldType.METRIC
            )
        );
        return planner(
            new EsIndex(
                "k8s,k8s-downsampled",
                mapping,
                Map.of(
                    "k8s",
                    new IndexProperties(IndexMode.TIME_SERIES, 0),
                    "k8s-downsampled",
                    new IndexProperties(IndexMode.TIME_SERIES, 0)
                ),
                Map.of(),
                Map.of()
            )
        );
    }

    private TestPlannerOptimizer planner(EsIndex esIndex) {
        Analyzer analyzer = new Analyzer(
            testAnalyzerContext(
                EsqlTestUtils.TEST_CFG,
                TEST_FUNCTION_REGISTRY,
                indexResolutions(IndexResolution.valid(esIndex)),
                new EnrichResolution(),
                emptyInferenceResolution()
            ),
            TEST_VERIFIER
        );
        return new TestPlannerOptimizer(
            config,
            analyzer,
            new LogicalPlanOptimizer(logicalOptimizerContext(config, FoldContext.small(), TransportVersion.current()))
        );
    }
}
