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
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.expression.function.EsqlFunctionRegistry;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Max;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Rate;
import org.elasticsearch.xpack.esql.index.EsIndex;
import org.elasticsearch.xpack.esql.index.IndexResolution;
import org.elasticsearch.xpack.esql.optimizer.AbstractLogicalPlanOptimizerTests;
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.Limit;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.Project;
import org.elasticsearch.xpack.esql.plan.logical.TimeSeriesAggregate;
import org.elasticsearch.xpack.esql.plan.logical.TopN;
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
        assumeTrue("requires metrics command", EsqlCapabilities.Cap.TS_COMMAND_V0.isEnabled());
        LogicalPlan plan = planK8s("""
            TS k8s
            | STATS count(max_over_time(network.cost)) BY time_bucket = BUCKET(@timestamp, 1 minute)
            | LIMIT 10
            """);
        Limit limit = as(plan, Limit.class);
        Aggregate outerStats = as(limit.child(), Aggregate.class);
        TimeSeriesAggregate innerStats = as(outerStats.child(), TimeSeriesAggregate.class);
        // TODO: Add asserts about the specific aggregation details here
        Eval eval = as(innerStats.child(), Eval.class);
        EsRelation relation = as(eval.child(), EsRelation.class);
    }

    public void testMaxOfRate() {
        assumeTrue("requires metrics command", EsqlCapabilities.Cap.TS_COMMAND_V0.isEnabled());
        LogicalPlan plan = planK8s("""
            TS k8s
            | STATS max(rate(network.total_bytes_in)) BY time_bucket = BUCKET(@timestamp, 1 minute)
            | LIMIT 10
            """);
        Limit limit = as(plan, Limit.class);

        Aggregate outerStats = as(limit.child(), Aggregate.class);
        assertEquals(1, outerStats.groupings().size());
        Attribute timeBucketGroup = as(outerStats.groupings().get(0), Attribute.class);
        assertEquals("time_bucket", timeBucketGroup.name());
        assertEquals(2, outerStats.aggregates().size());
        assertEquals(timeBucketGroup, outerStats.aggregates().get(1));
        Alias outerAggFunction = as(outerStats.aggregates().get(0), Alias.class);
        Max outerMax = as(outerAggFunction.child(), Max.class);

        TimeSeriesAggregate innerStats = as(outerStats.child(), TimeSeriesAggregate.class);
        assertEquals(2, innerStats.groupings().size());
        assertEquals(timeBucketGroup, innerStats.groupings().get(1));
        Attribute tsidGroup = as(innerStats.groupings().get(0), Attribute.class);
        assertEquals("_tsid", tsidGroup.name());

        assertEquals(2, innerStats.aggregates().size());
        Alias innerAggFunction = as(innerStats.aggregates().get(0), Alias.class);
        Rate rateAgg = as(innerAggFunction.child(), Rate.class);
        Alias timeBucketAlias = as(innerStats.aggregates().get(1), Alias.class);
        assertEquals(timeBucketGroup, timeBucketAlias.child());

        Eval eval = as(innerStats.child(), Eval.class);
        EsRelation relation = as(eval.child(), EsRelation.class);
    }

    /**
     * <pre>{@code
     * Project[[avg_cost{r}#8, cluster{f}#14, time_bucket{r}#5]]
     * \_TopN[[Order[avg_cost{r}#8,DESC,FIRST], Order[time_bucket{r}#5,DESC,FIRST], Order[cluster{f}#14,ASC,LAST]],10[INTEGER]]
     *   \_Eval[[$$SUM$avg_cost$0{r$}#38 / $$COUNT$avg_cost$1{r$}#39 AS avg_cost#8]]
     *     \_Aggregate[[cluster{r}#14, time_bucket{r}#5],[SUM(AVGOVERTIME_$1{r}#41,true[BOOLEAN],compensated[KEYWORD])
     *                  AS $$SUM$avg_cost$0#38, COUNT(AVGOVERTIME_$1{r}#41,true[BOOLEAN]) AS $$COUNT$avg_cost$1#39,
     *                  cluster{r}#14 AS cluster#14, time_bucket{r}#5 AS time_bucket#5]]
     *       \_Eval[[$$SUM$AVGOVERTIME_$1$0{r$}#42 / $$COUNT$AVGOVERTIME_$1$1{r$}#43 AS AVGOVERTIME_$1#41]]
     *         \_TimeSeriesAggregate[[_tsid{m}#40, time_bucket{r}#5],[SUM(network.cost{f}#29,true[BOOLEAN],lossy[KEYWORD])
     *                                AS $$SUM$AVGOVERTIME_$1$0#42, COUNT(network.cost{f}#29,true[BOOLEAN]) AS $$COUNT$AVGOVERTIME_$1$1#43,
     *                                VALUES(cluster{f}#14,true[BOOLEAN]) AS cluster#14, time_bucket{r}#5],
     *                                BUCKET(@timestamp{f}#13,PT1M[TIME_DURATION])]
     *           \_Eval[[BUCKET(@timestamp{f}#13,PT1M[TIME_DURATION]) AS time_bucket#5]]
     *             \_EsRelation[k8s][@timestamp{f}#13, client.ip{f}#17, cluster{f}#14, e..]
     * }</pre>
     */
    public void testAvgOfAvgOverTime() {
        assumeTrue("requires metrics command", EsqlCapabilities.Cap.TS_COMMAND_V0.isEnabled());
        LogicalPlan plan = planK8s("""
            TS k8s
            | STATS avg_cost=avg(avg_over_time(network.cost)) BY cluster, time_bucket = bucket(@timestamp,1minute)
            | SORT avg_cost DESC, time_bucket DESC, cluster
            | LIMIT 10
            """);
        Project project = as(plan, Project.class);
        TopN topN = as(project.child(), TopN.class);
        Eval outerEval = as(topN.child(), Eval.class); // This is the surrogate average calculation for the outer average
        Aggregate outerStats = as(outerEval.child(), Aggregate.class);
        Eval innerEval = as(outerStats.child(), Eval.class); // Surrogate for the inner average
        TimeSeriesAggregate innerStats = as(innerEval.child(), TimeSeriesAggregate.class);
        Eval bucketEval = as(innerStats.child(), Eval.class); // compute the tbucket
        EsRelation relation = as(bucketEval.child(), EsRelation.class);
    }

    public void testRateWithRename() {
        assumeTrue("requires metrics command", EsqlCapabilities.Cap.TS_COMMAND_V0.isEnabled());
        LogicalPlan plan = planK8s("""
            TS k8s
            | RENAME `@timestamp` AS newTs
            | STATS maxRate = max(rate(network.total_cost))  BY time_bucket = bucket(newTs, 1hour)
            """);
        Limit limit = as(plan, Limit.class);

        Aggregate outerStats = as(limit.child(), Aggregate.class);
        assertEquals(1, outerStats.groupings().size());
        Attribute timeBucketGroup = as(outerStats.groupings().get(0), Attribute.class);
        assertEquals("time_bucket", timeBucketGroup.name());
        assertEquals(2, outerStats.aggregates().size());
        assertEquals(timeBucketGroup, outerStats.aggregates().get(1));
        Alias outerAggFunction = as(outerStats.aggregates().get(0), Alias.class);
        Max outerMax = as(outerAggFunction.child(), Max.class);

        TimeSeriesAggregate innerStats = as(outerStats.child(), TimeSeriesAggregate.class);
        assertEquals(2, innerStats.groupings().size());
        Attribute tsidGroup = as(innerStats.groupings().get(0), Attribute.class);
        assertEquals("_tsid", tsidGroup.name());
        assertEquals(timeBucketGroup, innerStats.groupings().get(1));

        assertEquals(2, innerStats.aggregates().size());
        Alias innerAggFunction = as(innerStats.aggregates().get(0), Alias.class);
        Rate rateAgg = as(innerAggFunction.child(), Rate.class);
        Alias timeBucketAlias = as(innerStats.aggregates().get(1), Alias.class);
        assertEquals(timeBucketGroup, timeBucketAlias.child());

        Eval eval = as(innerStats.child(), Eval.class);
        EsRelation relation = as(eval.child(), EsRelation.class);
    }

    public void testRateWithManyRenames() {
        assumeTrue("requires metrics command", EsqlCapabilities.Cap.TS_COMMAND_V0.isEnabled());
        LogicalPlan plan = planK8s("""
            TS k8s
            | RENAME `@timestamp` AS ts1
            | RENAME ts1 AS ts2
            | RENAME ts2 AS ts3
            | RENAME ts3 AS ts4
            | RENAME ts4 as newTs
            | STATS maxRate = max(rate(network.total_cost))  BY tbucket = bucket(newTs, 1hour)
            """);
    }

    public void testOverTimeFunctionWithRename() {
        assumeTrue("requires metrics command", EsqlCapabilities.Cap.TS_COMMAND_V0.isEnabled());
        LogicalPlan plan = planK8s("""
            TS k8s
            | RENAME `@timestamp` AS newTs
            | STATS maxRate = max(max_over_time(network.eth0.tx))  BY tbucket = bucket(newTs, 1hour)
            """);
    }

    public void testTbucketWithRename() {
        assumeTrue("requires metrics command", EsqlCapabilities.Cap.TS_COMMAND_V0.isEnabled());
        LogicalPlan plan = planK8s("""
            TS k8s
            | RENAME `@timestamp` AS newTs
            | STATS maxRate = max(max_over_time(network.eth0.tx))  BY tbucket = tbucket(1hour)
            """);
    }

    public void testTbucketWithManyRenames() {
        assumeTrue("requires metrics command", EsqlCapabilities.Cap.TS_COMMAND_V0.isEnabled());
        LogicalPlan plan = planK8s("""
            TS k8s
            | RENAME `@timestamp` AS ts1
            | RENAME ts1 AS ts2
            | RENAME ts2 AS ts3
            | RENAME ts3 AS ts4
            | STATS maxRate = max(max_over_time(network.eth0.tx))  BY tbucket = tbucket(1hour)
            """);
    }
}
