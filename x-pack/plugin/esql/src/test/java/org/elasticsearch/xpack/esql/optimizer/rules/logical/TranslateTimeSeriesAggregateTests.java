/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical;

import org.elasticsearch.index.IndexMode;
import org.elasticsearch.xpack.esql.EsqlTestUtils;
import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.elasticsearch.xpack.esql.analysis.Analyzer;
import org.elasticsearch.xpack.esql.analysis.AnalyzerContext;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.expression.function.EsqlFunctionRegistry;
import org.elasticsearch.xpack.esql.index.EsIndex;
import org.elasticsearch.xpack.esql.index.IndexResolution;
import org.elasticsearch.xpack.esql.optimizer.AbstractLogicalPlanOptimizerTests;
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.Drop;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.Limit;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.TimeSeriesAggregate;
import org.junit.BeforeClass;

import java.util.Map;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.TEST_VERIFIER;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.as;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.emptyInferenceResolution;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.loadMapping;
import static org.elasticsearch.xpack.esql.analysis.AnalyzerTestUtils.defaultLookupResolution;

public class TranslateTimeSeriesAggregateTests extends AbstractLogicalPlanOptimizerTests {

    private static Map<String, EsField> mappingK8s;
    private static Analyzer k8sAnalyzer;

    @BeforeClass
    public static void initK8s() {
        // Load Time Series mappings for these tests
        mappingK8s = loadMapping("k8s-mappings.json");
        EsIndex k8sIndex = new EsIndex("k8s", mappingK8s, Map.of("k8s", IndexMode.TIME_SERIES));
        IndexResolution getIndexResult = IndexResolution.valid(k8sIndex);
        k8sAnalyzer = new Analyzer(
            new AnalyzerContext(
                EsqlTestUtils.TEST_CFG,
                new EsqlFunctionRegistry(),
                getIndexResult,
                defaultLookupResolution(),
                enrichResolution,
                emptyInferenceResolution()
            ),
            TEST_VERIFIER
        );
    }

    protected LogicalPlan planK8s(String query) {
        LogicalPlan analyzed = k8sAnalyzer.analyze(parser.createStatement(query, EsqlTestUtils.TEST_CFG));
        LogicalPlan optimized = logicalOptimizer.optimize(analyzed);
        return optimized;
    }

    /**
     * Test that {@link TranslateTimeSeriesAggregate} correctly splits up a two stage aggregation with a time bucket.
     *
     * Expected plan:
     * <pre>{@code
     *Limit[10[INTEGER],false]
     * \_Aggregate[[time_bucket{r}#4],[COUNT(MAXOVERTIME_$1{r}#33,true[BOOLEAN]) AS count(max_over_time(network.cost))#6, time_bucket{r}#4]]
     *   \_TimeSeriesAggregate[[_tsid{m}#34, time_bucket{r}#4],[MAX(network.cost{f}#24,true[BOOLEAN]) AS MAXOVERTIME_$1#33,
     *                         time_bucket{r}#4 AS time_bucket#4],BUCKET(@timestamp{f}#8,PT1M[TIME_DURATION])]
     *     \_Eval[[BUCKET(@timestamp{f}#8,PT1M[TIME_DURATION]) AS time_bucket#4]]
     *       \_EsRelation[k8s][@timestamp{f}#8, client.ip{f}#12, cluster{f}#9, eve..]
     * }</pre>
     */
    public void testMaxOverTime() {
        assumeTrue("requires metrics command", EsqlCapabilities.Cap.METRICS_COMMAND.isEnabled());
        LogicalPlan plan = planK8s("""
            TS k8s
            | STATS count(max_over_time(network.cost)) BY time_bucket = BUCKET(@timestamp, 1 minute)
            | LIMIT 10
            """);
        Limit limit = as(plan, Limit.class);
        Aggregate innerStats = as(limit.child(), Aggregate.class);
        TimeSeriesAggregate outerStats = as(innerStats.child(), TimeSeriesAggregate.class);
        // TODO: Add asserts about the specific aggregation details here
        Eval eval = as(outerStats.child(), Eval.class);
        EsRelation relation = as(eval.child(), EsRelation.class);
    }

    public void testMaxOfRate() {
        assumeTrue("requires metrics command", EsqlCapabilities.Cap.METRICS_COMMAND.isEnabled());
        LogicalPlan plan = planK8s("""
            TS k8s
            | STATS max(rate(network.total_bytes_in)) BY time_bucket = BUCKET(@timestamp, 1 minute)
            | LIMIT 10
            """);
        Limit limit = as(plan, Limit.class);
        Aggregate innerStats = as(limit.child(), Aggregate.class);
        TimeSeriesAggregate outerStats = as(innerStats.child(), TimeSeriesAggregate.class);
        // TODO: Add asserts about the specific aggregation details here
        Eval eval = as(outerStats.child(), Eval.class);
        EsRelation relation = as(eval.child(), EsRelation.class);
    }

    public void testAvgOfAvgOverTime() {
        assumeTrue("requires metrics command", EsqlCapabilities.Cap.METRICS_COMMAND.isEnabled());
        LogicalPlan plan = planK8s("""
            TS k8s
            | STATS avg_cost=avg(avg_over_time(network.cost)) BY cluster, time_bucket = bucket(@timestamp,1minute)
            | SORT avg_cost DESC, time_bucket DESC, cluster
            | LIMIT 10
            """);
        Limit limit = as(plan, Limit.class);
        Aggregate innerStats = as(limit.child(), Aggregate.class);
        TimeSeriesAggregate outerStats = as(innerStats.child(), TimeSeriesAggregate.class);
        // TODO: Add asserts about the specific aggregation details here
        Eval eval = as(outerStats.child(), Eval.class);
        EsRelation relation = as(eval.child(), EsRelation.class);
    }

    /**
     * This tests the "group by all" case.  With no outer (aka vertical) aggregation, we expect to get back one bucket per time series,
     * grouped by each of the dimension fields.
     */
    public void testNoOuterAggregation() {
        assumeTrue("requires metrics command", EsqlCapabilities.Cap.METRICS_COMMAND.isEnabled());
        LogicalPlan plan = planK8s("""
            TS k8s
            | STATS max_over_time(network.cost) by time_bucket = BUCKET(@timestamp, 1 minute)
            | LIMIT 10
            """);
        Limit limit = as(plan, Limit.class);
        // Drop drop = as(limit.child(), Drop.class);
        // TimeSeriesAggregate outerStats = as(drop.child(), TimeSeriesAggregate.class);
        Aggregate outerStats = as(limit.child(), Aggregate.class);
        // TODO: Add asserts about the specific aggregation details here
        Eval eval = as(outerStats.child(), Eval.class);
        EsRelation relation = as(eval.child(), EsRelation.class);
    }

}
