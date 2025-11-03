/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.promql;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.xpack.esql.EsqlTestUtils;
import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.elasticsearch.xpack.esql.analysis.Analyzer;
import org.elasticsearch.xpack.esql.analysis.AnalyzerContext;
import org.elasticsearch.xpack.esql.core.expression.predicate.regex.RegexMatch;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.expression.function.EsqlFunctionRegistry;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.StartsWith;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.In;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.NotEquals;
import org.elasticsearch.xpack.esql.index.EsIndex;
import org.elasticsearch.xpack.esql.index.IndexResolution;
import org.elasticsearch.xpack.esql.optimizer.AbstractLogicalPlanOptimizerTests;
import org.elasticsearch.xpack.esql.plan.IndexPattern;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.junit.BeforeClass;

import java.util.Map;

import static java.util.Collections.emptyMap;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.TEST_VERIFIER;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.emptyInferenceResolution;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.loadMapping;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

//@TestLogging(value="org.elasticsearch.xpack.esql:TRACE", reason="debug tests")
//@Ignore("Proper assertions need to be added")
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
                Map.of(new IndexPattern(Source.EMPTY, "k8s"), timeSeriesIndex),
                emptyMap(),
                enrichResolution,
                emptyInferenceResolution(),
                TransportVersion.current()
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
            | promql step 5m ( avg by (pod) (avg_over_time(network.bytes_in{pod=~"host-0|host-1|host-2"}[1h])) )
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
            | promql step 5m ( avg by (pod) (avg_over_time(network.bytes_in{pod=~"host-0|host-1|host-2"}[1h])) )
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
            | promql step 10 ( max by (pod) (avg_over_time(network.bytes_in[1h])) )
            """);

        System.out.println(plan);
    }

    public void testRate() {
        // TS metrics-hostmetricsreceiver.otel-default
        // | WHERE @timestamp >= \"{{from | minus .benchmark.duration}}\" AND @timestamp <= \"{{from}}\"
        // | STATS AVG(RATE(`metrics.system.cpu.time`)) BY host.name, TBUCKET(1h) | LIMIT 10000"
        String testQuery = """
            TS k8s
            | promql step a (
                avg by (pod) (rate(network.bytes_in[1h]))
                )
            """;

        var plan = planPromql(testQuery);
        System.out.println(plan);
    }

    public void testLabelSelector() {
        // TS metrics-hostmetricsreceiver.otel-default | WHERE @timestamp >= \"{{from | minus .benchmark.duration}}\" AND @timestamp <=
        // \"{{from}}\"
        // | WHERE host.name IN(\"host-0\", \"host-1\", \"host-2\")
        // | STATS AVG(AVG_OVER_TIME(`system.cpu.load_average.1m`)) BY host.name, TBUCKET(5m) | LIMIT 10000"
        String testQuery = """
            TS k8s
            | promql time now (
                max by (pod) (avg_over_time(network.bytes_in{pod=~"host-0|host-1|host-2"}[5m]))
              )
            """;

        var plan = planPromql(testQuery);
        var filters = plan.collect(Filter.class::isInstance);
        assertThat(filters, hasSize(1));
        var filter = (Filter) filters.getFirst();
        assertThat(filter.condition().anyMatch(In.class::isInstance), equalTo(true));
        System.out.println(plan);
    }

    public void testLabelSelectorPrefix() {
        // TS metrics-hostmetricsreceiver.otel-default | WHERE @timestamp >= \"{{from | minus .benchmark.duration}}\" AND @timestamp <=
        // \"{{from}}\"
        // | WHERE host.name LIKE \"host-*\"
        // STATS AVG(AVG_OVER_TIME(`metrics.system.cpu.load_average.1m`)) BY host.name, TBUCKET(5 minutes)"
        String testQuery = """
            TS k8s
            | promql time now (
                avg by (pod) (avg_over_time(network.bytes_in{pod=~"host-.*"}[5m]))
                )
            """;

        var plan = planPromql(testQuery);
        var filters = plan.collect(Filter.class::isInstance);
        assertThat(filters, hasSize(1));
        var filter = (Filter) filters.getFirst();
        assertThat(filter.condition().anyMatch(StartsWith.class::isInstance), equalTo(true));
        assertThat(filter.condition().anyMatch(NotEquals.class::isInstance), equalTo(false));
        System.out.println(plan);
    }

    public void testLabelSelectorProperPrefix() {
        var plan = planPromql("""
            TS k8s
            | promql time now (
                avg(avg_over_time(network.bytes_in{pod=~"host-.+"}[1h]))
              )
            """);

        var filters = plan.collect(Filter.class::isInstance);
        assertThat(filters, hasSize(1));
        var filter = (Filter) filters.getFirst();
        assertThat(filter.condition().anyMatch(StartsWith.class::isInstance), equalTo(true));
        assertThat(filter.condition().anyMatch(NotEquals.class::isInstance), equalTo(true));
    }

    public void testLabelSelectorRegex() {
        var plan = planPromql("""
            TS k8s
            | promql time now (
                avg(avg_over_time(network.bytes_in{pod=~"[a-z]+"}[1h]))
              )
            """);

        var filters = plan.collect(Filter.class::isInstance);
        assertThat(filters, hasSize(1));
        var filter = (Filter) filters.getFirst();
        assertThat(filter.condition().anyMatch(RegexMatch.class::isInstance), equalTo(true));
    }

    public void testFsUsageTop5() {
        // TS metrics-hostmetricsreceiver.otel-default | WHERE @timestamp >= \"{{from | minus .benchmark.duration}}\" AND @timestamp <=
        // \"{{from}}\"
        // | WHERE attributes.state IN (\"used\", \"free\")
        // | STATS sums = SUM(LAST_OVER_TIME(system.filesystem.usage)) by host.name, attributes.mountpoint
        // | STATS top = TOP(sums, 5, \"desc\") by host.name, attributes.mountpoint
        // | LIMIT 5

        // topk(5, sum by (host.name, mountpoint) (last_over_time(system.filesystem.usage{state=~"used|free"}[5m])))
        String testQuery = """
            TS k8s
            | promql step 5m (
                sum by (host.name, mountpoint) (last_over_time(system.filesystem.usage{state=~"used|free"}[5m]))
                )
            """;

        var plan = planPromql(testQuery);
        System.out.println(plan);
    }

    public void testGrammar() {
        // TS metrics-hostmetricsreceiver.otel-default | WHERE @timestamp >= \"{{from | minus .benchmark.duration}}\" AND @timestamp <=
        // \"{{from}}\"
        // | WHERE attributes.state IN (\"used\", \"free\")
        // | STATS sums = SUM(LAST_OVER_TIME(system.filesystem.usage)) by host.name, attributes.mountpoint
        // | STATS top = TOP(sums, 5, \"desc\") by host.name, attributes.mountpoint
        // | LIMIT 5

        // topk(5, sum by (host.name, mountpoint) (last_over_time(system.filesystem.usage{state=~"used|free"}[5m])))
        String testQuery = """
            TS k8s
            | promql step 5m (
                foo or bar
                )
            """;

        var plan = planPromql(testQuery);
        System.out.println(plan);
    }

    // public void testPromqlArithmetricOperators() {
    // // TODO doesn't parse
    // // line 1:27: Invalid query '1+1'[ArithmeticBinaryContext] given; expected LogicalPlan but found VectorBinaryArithmetic
    // assertThat(
    // error("TS test | PROMQL step 5m (1+1)", tsdb),
    // equalTo("1:1: arithmetic operators are not supported at this time [foo]")
    // );
    // assertThat(
    // error("TS test | PROMQL step 5m ( foo and bar )", tsdb),
    // equalTo("1:1: arithmetic operators are not supported at this time [foo]")
    // );
    // assertThat(
    // error("TS test | PROMQL step 5m (1+foo)", tsdb),
    // equalTo("1:1: arithmetic operators are not supported at this time [foo]")
    // );
    // assertThat(
    // error("TS test | PROMQL step 5m (foo+bar)", tsdb),
    // equalTo("1:1: arithmetic operators are not supported at this time [foo]")
    // );
    // }

    protected LogicalPlan planPromql(String query) {
        var analyzed = tsAnalyzer.analyze(parser.createStatement(query));
        System.out.println(analyzed);
        var optimized = logicalOptimizer.optimize(analyzed);
        System.out.println(optimized);
        return optimized;
    }
}
