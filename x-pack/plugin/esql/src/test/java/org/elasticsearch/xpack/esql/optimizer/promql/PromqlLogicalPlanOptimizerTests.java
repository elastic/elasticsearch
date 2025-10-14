/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.promql;

import org.elasticsearch.index.IndexMode;
import org.elasticsearch.xpack.esql.EsqlTestUtils;
import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.elasticsearch.xpack.esql.analysis.Analyzer;
import org.elasticsearch.xpack.esql.analysis.AnalyzerContext;
import org.elasticsearch.xpack.esql.expression.function.EsqlFunctionRegistry;
import org.elasticsearch.xpack.esql.index.EsIndex;
import org.elasticsearch.xpack.esql.index.IndexResolution;
import org.elasticsearch.xpack.esql.optimizer.AbstractLogicalPlanOptimizerTests;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.junit.BeforeClass;
import org.junit.Ignore;

import java.util.Map;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.TEST_VERIFIER;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.emptyInferenceResolution;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.loadMapping;

//@TestLogging(value="org.elasticsearch.xpack.esql:TRACE", reason="debug tests")
@Ignore("Proper assertions need to be added")
public class PromqlLogicalPlanOptimizerTests extends AbstractLogicalPlanOptimizerTests {

    private static final String PARAM_FORMATTING = "%1$s";

    private static Analyzer tsAnalyzer;

    @BeforeClass
    public static void initTest() {
        assumeTrue("requires metrics command", EsqlCapabilities.Cap.TS_COMMAND_V0.isEnabled());

        var timeSeriesMapping = loadMapping("k8s-mappings.json");
        var timeSeriesIndex = IndexResolution.valid(new EsIndex("k8s", timeSeriesMapping, Map.of("k8s", IndexMode.TIME_SERIES)));
        tsAnalyzer = new Analyzer(
            new AnalyzerContext(
                EsqlTestUtils.TEST_CFG,
                new EsqlFunctionRegistry(),
                timeSeriesIndex,
                enrichResolution,
                emptyInferenceResolution()
            ),
            TEST_VERIFIER
        );
    }

    public void testExplainPromql() {
        // TS metrics-hostmetricsreceiver.otel-default
        // | WHERE @timestamp >= \"{{from | minus .benchmark.duration}}\" AND @timestamp <=\"{{from}}\"
        // | STATS AVG(AVG_OVER_TIME(`metrics.system.memory.utilization`)) BY host.name, TBUCKET(1h) | LIMIT 10000"
        var plan = planPromql("""
            EXPLAIN (
            TS k8s
            | promql avg by (pod) (avg_over_time(network.bytes_in{pod=~"host-0|host-1|host-2"}[1h]))
            | LIMIT 1000
            )
            """);

        System.out.println(plan);
    }

    public void testExplainPromqlSimple() {
        // TS metrics-hostmetricsreceiver.otel-default
        // | WHERE @timestamp >= \"{{from | minus .benchmark.duration}}\" AND @timestamp <=\"{{from}}\"
        // | STATS AVG(AVG_OVER_TIME(`metrics.system.memory.utilization`)) BY host.name, TBUCKET(1h) | LIMIT 10000"
        var plan = planPromql("""
            EXPLAIN (
            TS k8s
            | STATS AVG(AVG_OVER_TIME(network.bytes_in)) BY pod, TBUCKET(1h)
            | LIMIT 1000
            )
            """);

        System.out.println(plan);
    }
    public void testAvgAvgOverTimeOutput() {
        // TS metrics-hostmetricsreceiver.otel-default
        // | WHERE @timestamp >= \"{{from | minus .benchmark.duration}}\" AND @timestamp <=\"{{from}}\"
        // | STATS AVG(AVG_OVER_TIME(`metrics.system.memory.utilization`)) BY host.name, TBUCKET(1h) | LIMIT 10000"
        var plan = planPromql("""
            TS k8s
            | promql avg by (pod) (avg_over_time(network.bytes_in{pod=~"host-0|host-1|host-2"}[1h]))
            | LIMIT 1000
            """);

        System.out.println(plan);
    }

    public void testTSAvgAvgOverTimeOutput() {
        // TS metrics-hostmetricsreceiver.otel-default
        // | STATS AVG(AVG_OVER_TIME(`metrics.system.memory.utilization`)) BY host.name, TBUCKET(1h) | LIMIT 10000"
        var plan = planPromql("""
            TS k8s
            | STATS AVG(AVG_OVER_TIME(network.bytes_in)) BY pod, TBUCKET(1h)
            | LIMIT 1000
            """);

        System.out.println(plan);
    }

    public void testTSAvgWithoutByDimension() {
        // TS metrics-hostmetricsreceiver.otel-default
        // | STATS AVG(AVG_OVER_TIME(`metrics.system.memory.utilization`)) BY TBUCKET(1h) | LIMIT 10000"
        var plan = planPromql("""
            TS k8s
            | STATS avg(avg_over_time(network.bytes_in)) BY TBUCKET(1h)
            | LIMIT 1000
            """);

        System.out.println(plan);
    }

    public void testPromqlAvgWithoutByDimension() {
        // TS metrics-hostmetricsreceiver.otel-default
        // | STATS AVG(AVG_OVER_TIME(`metrics.system.memory.utilization`)) BY TBUCKET(1h) | LIMIT 10000"
        var plan = planPromql("""
            TS k8s
            | promql avg(avg_over_time(network.bytes_in[1h]))
            | LIMIT 1000
            """);

        System.out.println(plan);
    }

    public void testRangeSelector() {
        // TS metrics-hostmetricsreceiver.otel-default
        // | WHERE @timestamp >= \"{{from | minus .benchmark.duration}}\" AND @timestamp <=\"{{from}}\"
        // | STATS AVG(AVG_OVER_TIME(`metrics.system.memory.utilization`)) BY host.name, TBUCKET(1h) | LIMIT 10000"
        var plan = planPromql("""
            TS k8s
            | promql max by (pod) (avg_over_time(network.total_bytes_in[1h]))
            """);

        System.out.println(plan);
    }

    public void testRate() {
        // TS metrics-hostmetricsreceiver.otel-default
        // | WHERE @timestamp >= \"{{from | minus .benchmark.duration}}\" AND @timestamp <= \"{{from}}\"
        // | STATS AVG(RATE(`metrics.system.cpu.time`)) BY host.name, TBUCKET(1h) | LIMIT 10000"
        String testQuery = """
            TS k8s
            | promql
                avg by (pod) (rate(network.total_bytes_in[1h]))

            """;

        var plan = planPromql(testQuery);
        System.out.println(plan);
    }

    public void testLabelSelector() {
        // TS metrics-hostmetricsreceiver.otel-default | WHERE @timestamp >= \"{{from | minus .benchmark.duration}}\" AND @timestamp <= \"{{from}}\"
        // | WHERE host.name IN(\"host-0\", \"host-1\", \"host-2\")
        // | STATS AVG(AVG_OVER_TIME(`system.cpu.load_average.1m`)) BY host.name, TBUCKET(5m) | LIMIT 10000"
        String testQuery = """
            TS k8s
            | promql
                max by (pod)(avg_over_time(network.total_bytes_in{pod=~"host-0|host-1|host-2"}[5m]))

            """;

        var plan = planPromql(testQuery);
        System.out.println(plan);
    }

    public void testLabelSelectorPrefix() {
        // TS metrics-hostmetricsreceiver.otel-default | WHERE @timestamp >= \"{{from | minus .benchmark.duration}}\" AND @timestamp <= \"{{from}}\"
        // | WHERE host.name LIKE \"host-*\"
        // STATS AVG(AVG_OVER_TIME(`metrics.system.cpu.load_average.1m`)) BY host.name, TBUCKET(5 minutes)"
        String testQuery = """
            TS k8s
            | promql
                avg by (pod)(avg_over_time(network.total_bytes_in{pod=~"host-.*"}[5m]))

            """;

        var plan = planPromql(testQuery);
        System.out.println(plan);
    }

    public void testFsUsageTop5() {
        // TS metrics-hostmetricsreceiver.otel-default | WHERE @timestamp >= \"{{from | minus .benchmark.duration}}\" AND @timestamp <= \"{{from}}\"
        // | WHERE attributes.state IN (\"used\", \"free\")
        // | STATS sums = SUM(LAST_OVER_TIME(system.filesystem.usage)) by host.name, attributes.mountpoint
        // | STATS top = TOP(sums, 5, \"desc\") by host.name, attributes.mountpoint
        // | LIMIT 5

//                topk(5, sum by (host.name, mountpoint) (last_over_time(system.filesystem.usage{state=~"used|free"}[5m])))
        String testQuery = """
            TS k8s
            | promql
                sum by (host.name, mountpoint) (last_over_time(system.filesystem.usage{state=~"used|free"}[5m]))

            """;

        var plan = planPromql(testQuery);
        System.out.println(plan);
    }


    protected LogicalPlan planPromql(String query) {
        var analyzed = tsAnalyzer.analyze(parser.createStatement(query, EsqlTestUtils.TEST_CFG));
        System.out.println(analyzed);
        var optimized = logicalOptimizer.optimize(analyzed);
        System.out.println(optimized);
        return optimized;
    }
}
