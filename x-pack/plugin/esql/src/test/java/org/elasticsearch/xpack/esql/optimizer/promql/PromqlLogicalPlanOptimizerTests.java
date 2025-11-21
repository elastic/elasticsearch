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
import org.elasticsearch.xpack.esql.action.PromqlFeatures;
import org.elasticsearch.xpack.esql.analysis.Analyzer;
import org.elasticsearch.xpack.esql.analysis.AnalyzerContext;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.predicate.regex.RegexMatch;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.expression.function.EsqlFunctionRegistry;
import org.elasticsearch.xpack.esql.expression.function.grouping.Bucket;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.StartsWith;
import org.elasticsearch.xpack.esql.expression.predicate.logical.And;
import org.elasticsearch.xpack.esql.expression.predicate.nulls.IsNotNull;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.In;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.NotEquals;
import org.elasticsearch.xpack.esql.index.EsIndex;
import org.elasticsearch.xpack.esql.index.IndexResolution;
import org.elasticsearch.xpack.esql.optimizer.AbstractLogicalPlanOptimizerTests;
import org.elasticsearch.xpack.esql.plan.IndexPattern;
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.Project;
import org.elasticsearch.xpack.esql.plan.logical.TimeSeriesAggregate;
import org.elasticsearch.xpack.esql.plan.logical.TopN;
import org.junit.BeforeClass;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Map;
import java.util.Set;

import static java.util.Collections.emptyMap;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.TEST_VERIFIER;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.as;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.emptyInferenceResolution;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.loadMapping;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;

// @TestLogging(value = "org.elasticsearch.xpack.esql:TRACE", reason = "debug tests")
public class PromqlLogicalPlanOptimizerTests extends AbstractLogicalPlanOptimizerTests {

    private static Analyzer tsAnalyzer;

    @BeforeClass
    public static void initTest() {
        assumeTrue("requires snapshot build with promql feature enabled", PromqlFeatures.isEnabled());

        var timeSeriesMapping = loadMapping("k8s-mappings.json");
        var timeSeriesIndex = IndexResolution.valid(new EsIndex("k8s", timeSeriesMapping, Map.of("k8s", IndexMode.TIME_SERIES), Set.of()));
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

    }

    public void testExplainPromqlSimple() {
        // TS metrics-hostmetricsreceiver.otel-default
        // | WHERE @timestamp >= \"{{from | minus .benchmark.duration}}\" AND @timestamp <=\"{{from}}\"
        // | STATS AVG(AVG_OVER_TIME(`metrics.system.memory.utilization`)) BY host.name, TBUCKET(1h) | LIMIT 10000"
        var plan = planPromql("""
            TS k8s
            | WHERE TRANGE($now-1h, $now)
            | STATS AVG(AVG_OVER_TIME(network.bytes_in)) BY TBUCKET(1h)
            """);

    }

    /**
     * Explain the following logical plan
     *
     * Project[[avg by (pod) (avg_over_time(network.bytes_in{pod=~"host-0|host-1|host-2"}[1h])){r}#72, pod{r}#48, TBUCKET{r}#
     * 73]]
     * \_TopN[[Order[TBUCKET{r}#73,ASC,FIRST]],1000[INTEGER],false]
     *   \_Eval[[$$SUM$avg by (pod) (avg_over_time(network.bytes_in{pod=~"host-0|host-1|host-2"}[1h]))$0{r$}#81 / $$COUNT$avg
     * by (pod) (avg_over_time(network.bytes_in{pod=~"host-0|host-1|host-2"}[1h]))$1{r$}#82 AS avg by (pod)
     *              (avg_over_time(network.bytes_in{pod=~"host-0|host-1|host-2"}[1h]))#72, UNPACKDIMENSION(grouppod_$1{r}#78) AS pod#48]]
     *     \_Aggregate[[packpod_$1{r}#77, TBUCKET{r}#73],[SUM(AVGOVERTIME_$1{r}#75,true[BOOLEAN],PT0S[TIME_DURATION],compensated[KEYWO
     * RD]) AS $$SUM$avg by (pod) (avg_over_time(network.bytes_in{pod=~"host-0|host-1|host-2"}[1h]))$0#81,
     *          COUNT(AVGOVERTIME_$1{r}#75,true[BOOLEAN],PT0S[TIME_DURATION]) AS $$COUNT$avg by (pod)
     *         (avg_over_time(network.bytes_in{pod=~"host-0|host-1|host-2"}[1h]))$1#82, packpod_$1{r}#77 AS grouppod_$1#78,
     *         TBUCKET{r}#73 AS TBUCKET#73]]
     *       \_Eval[[$$SUM$AVGOVERTIME_$1$0{r$}#79 / $$COUNT$AVGOVERTIME_$1$1{r$}#80 AS AVGOVERTIME_$1#75, PACKDIMENSION(pod{r}#76
     * ) AS packpod_$1#77]]
     *         \_TimeSeriesAggregate[[_tsid{m}#74, TBUCKET{r}#73],
     *             [SUM(network.bytes_in{f}#60,true[BOOLEAN],PT0S[TIME_DURATION],lossy[KEYWORD]) AS $
     * $SUM$AVGOVERTIME_$1$0#79, COUNT(network.bytes_in{f}#60,true[BOOLEAN],PT0S[TIME_DURATION]) AS $$COUNT$AVGOVERTIME_$1$1#80,
     * VALUES(pod{f}#48,true[BOOLEAN],PT0S[TIME_DURATION]) AS pod#76, TBUCKET{r}#73],
     * BUCKET(@timestamp{f}#46,PT1H[TIME_DURATION])]
     *           \_Eval[[BUCKET(@timestamp{f}#46,PT1H[TIME_DURATION]) AS TBUCKET#73]]
     *             \_Filter[ISNOTNULL(network.bytes_in{f}#60) AND IN(host-0[KEYWORD],host-1[KEYWORD],host-2[KEYWORD],pod{f}#48)]
     *               \_EsRelation[k8s][@timestamp{f}#46, client.ip{f}#50, cluster{f}#47, e..]
     */
    public void testAvgAvgOverTimeOutput() {
        // TS metrics-hostmetricsreceiver.otel-default
        // | WHERE @timestamp >= \"{{from | minus .benchmark.duration}}\" AND @timestamp <=\"{{from}}\"
        // | STATS AVG(AVG_OVER_TIME(`metrics.system.memory.utilization`)) BY host.name, TBUCKET(1h) | LIMIT 10000"
        var plan = planPromql("""
            TS k8s
            | promql step 1h ( avg by (pod) (avg_over_time(network.bytes_in{pod=~"host-0|host-1|host-2"}[1h])) )
            | LIMIT 1000
            """);

        var project = as(plan, Project.class);
        assertThat(project.projections(), hasSize(3));

        var topN = as(project.child(), TopN.class);
        assertThat(topN.order(), hasSize(1));

        var evalOuter = as(topN.child(), Eval.class);

        var aggregate = as(evalOuter.child(), Aggregate.class);
        assertThat(aggregate.groupings(), hasSize(2));

        var evalMiddle = as(aggregate.child(), Eval.class);

        var tsAggregate = as(evalMiddle.child(), TimeSeriesAggregate.class);
        assertThat(tsAggregate.groupings(), hasSize(2));

        // verify TBUCKET duration plus reuse
        var evalBucket = as(tsAggregate.child(), Eval.class);
        assertThat(evalBucket.fields(), hasSize(1));
        var bucketAlias = as(evalBucket.fields().get(0), Alias.class);
        var bucket = as(bucketAlias.child(), Bucket.class);

        var bucketSpan = bucket.buckets();
        assertThat(bucketSpan.fold(FoldContext.small()), equalTo(Duration.ofHours(1)));

        var tbucketId = bucketAlias.toAttribute().id();

        // Verify TBUCKET is used as timeBucket in TimeSeriesAggregate
        // FIXME: looks like we're creating multiple time buckets (one in Eval, one in TSAggregate)

        // var timeBucket = tsAggregate.timeBucket();
        // assertNotNull(timeBucket);
        // assertThat(Expressions.attribute(timeBucket).id(), equalTo(tbucketId));

        // assertThat(Expressions.attribute(tsAggregate.groupings().get(0)).id(), equalTo(tbucketId));

        // assertThat(Expressions.attribute(aggregate.groupings().get(1)).id(), equalTo(tbucketId));

        // var orderAttr = Expressions.attribute(topN.order().get(0).child());
        // assertThat(orderAttr.id(), equalTo(tbucketId));

        // assertThat(Expressions.attribute(project.projections().get(2)).id(), equalTo(tbucketId));

        // Filter should contain: ISNOTNULL(network.bytes_in) AND IN(host-0, host-1, host-2, pod)
        var filter = as(evalBucket.child(), Filter.class);
        var condition = filter.condition();
        assertThat(condition, instanceOf(And.class));
        var and = (And) condition;

        // Verify AND contains IsNotNull
        boolean hasIsNotNull = and.anyMatch(IsNotNull.class::isInstance);
        assertThat(hasIsNotNull, equalTo(true));

        // Verify AND contains In
        boolean hasIn = and.anyMatch(In.class::isInstance);
        assertThat(hasIn, equalTo(true));

        var inConditions = condition.collect(In.class::isInstance);
        assertThat(inConditions, hasSize(1));
        var in = (In) inConditions.get(0);
        assertThat(in.list(), hasSize(3));

        as(filter.child(), EsRelation.class);
    }

    /**
     * Expect the following logical plan
     *
     * Project[[AVG(AVG_OVER_TIME(network.bytes_in)){r}#86, pod{r}#90, TBUCKET(1h){r}#84]]
     * \_Eval[[UNPACKDIMENSION(grouppod_$1{r}#121) AS pod#90, $$SUM$AVG(AVG_OVER_TIME(network.bytes_in))$0{r$}#115 / $$COUNT
     * $AVG(AVG_OVER_TIME(network.bytes_in))$1{r$}#116 AS AVG(AVG_OVER_TIME(network.bytes_in))#86]]
     *   \_Limit[1000[INTEGER],false,false]
     *     \_Aggregate[[packpod_$1{r}#120, BUCKET{r}#84],[SUM(AVGOVERTIME_$1{r}#118,true[BOOLEAN],PT0S[TIME_DURATION],compensated[KEYW
     * ORD]) AS $$SUM$AVG(AVG_OVER_TIME(network.bytes_in))$0#115, COUNT(AVGOVERTIME_$1{r}#118,true[BOOLEAN],PT0S[TIME_DURATION]) AS
     *          $$COUNT$AVG(AVG_OVER_TIME(network.bytes_in))$1#116, packpod_$1{r}#120 AS grouppod_$1#121, BUCKET{r}#84 AS TBUCKET(1h)#84]]
     *       \_Eval[[$$SUM$AVGOVERTIME_$1$0{r$}#122 / $$COUNT$AVGOVERTIME_$1$1{r$}#123 AS AVGOVERTIME_$1#118, PACKDIMENSION(pod{r}
     * #119) AS packpod_$1#120]]
     *         \_TimeSeriesAggregate[[_tsid{m}#117, BUCKET{r}#84],[SUM(network.bytes_in{f}#102,true[BOOLEAN],
     *                                  PT0S[TIME_DURATION],lossy[KEYWORD]) AS
     * $$SUM$AVGOVERTIME_$1$0#122, COUNT(network.bytes_in{f}#102,true[BOOLEAN],PT0S[TIME_DURATION]) AS $$COUNT$AVGOVERTIME_$1$1#123,
     *              VALUES(pod{f}#90,true[BOOLEAN],PT0S[TIME_DURATION]) AS pod#119, BUCKET{r}#84],
     * BUCKET(@timestamp{f}#88,PT1H[TIME_DURATION])]
     *           \_Eval[[BUCKET(@timestamp{f}#88,PT1H[TIME_DURATION]) AS TBUCKET(1h)#84]]
     *             \_EsRelation[k8s][@timestamp{f}#88, client.ip{f}#92, cluster{f}#89, e..]
     */
    public void testTSAvgAvgOverTimeOutput() {
        // TS metrics-hostmetricsreceiver.otel-default
        // | STATS AVG(AVG_OVER_TIME(`metrics.system.memory.utilization`)) BY host.name, TBUCKET(1h) | LIMIT 10000"
        var plan = planPromql("""
            TS k8s
            | STATS AVG(AVG_OVER_TIME(network.bytes_in)) BY pod, TBUCKET(1h)
            | LIMIT 1000
            """);

    }

    /**
     * Expect the logical plan
     *
     * Project[[avg(avg_over_time(network.bytes_in)){r}#353, TBUCKET(1h){r}#351]]
     * \_Eval[[$$SUM$avg(avg_over_time(network.bytes_in))$0{r$}#382 / $$COUNT$avg(avg_over_time(network.bytes_in))$1{r$}#383
     *  AS avg(avg_over_time(network.bytes_in))#353]]
     *   \_Limit[1000[INTEGER],false,false]
     *     \_Aggregate[[BUCKET{r}#351],[SUM(AVGOVERTIME_$1{r}#385,true[BOOLEAN],PT0S[TIME_DURATION],compensated[KEYWORD]) AS $$SUM$avg
     * (avg_over_time(network.bytes_in))$0#382, COUNT(AVGOVERTIME_$1{r}#385,true[BOOLEAN],PT0S[TIME_DURATION]) AS
     *          $$COUNT$avg(avg_over_time(network.bytes_in))$1#383, BUCKET{r}#351 AS TBUCKET(1h)#351]]
     *       \_Eval[[$$SUM$AVGOVERTIME_$1$0{r$}#386 / $$COUNT$AVGOVERTIME_$1$1{r$}#387 AS AVGOVERTIME_$1#385]]
     *         \_TimeSeriesAggregate[[_tsid{m}#384, BUCKET{r}#351],[SUM(network.bytes_in{f}#369,true[BOOLEAN],PT0S[TIME_DURATION],
     *         lossy[KEYWORD]) AS
     *  $$SUM$AVGOVERTIME_$1$0#386, COUNT(network.bytes_in{f}#369,true[BOOLEAN],PT0S[TIME_DURATION]) AS $$COUNT$AVGOVERTIME_$1$1#387,
     *  BUCKET{r}#351],
     * BUCKET(@timestamp{f}#355,PT1H[TIME_DURATION])]
     *           \_Eval[[BUCKET(@timestamp{f}#355,PT1H[TIME_DURATION]) AS TBUCKET(1h)#351]]
     *             \_EsRelation[k8s][@timestamp{f}#355, client.ip{f}#359, cluster{f}#356, ..]
     */
    public void testTSAvgWithoutByDimension() {
        // TS metrics-hostmetricsreceiver.otel-default
        // | STATS AVG(AVG_OVER_TIME(`metrics.system.memory.utilization`)) BY TBUCKET(1h) | LIMIT 10000"
        var plan = planPromql("""
            TS k8s
            | STATS avg(avg_over_time(network.bytes_in)) BY TBUCKET(1h)
            | LIMIT 1000
            """);
    }

    /**
     * Expect the following logical plan
     *
     * Project[[avg(avg_over_time(network.bytes_in[1h])){r}#190, TBUCKET{r}#191]]
     * \_TopN[[Order[TBUCKET{r}#191,ASC,FIRST]],1000[INTEGER],false]
     *   \_Eval[[$$SUM$avg(avg_over_time(network.bytes_in[1h]))$0{r$}#196 / $$COUNT$avg(avg_over_time(network.bytes_in[1h]))$1
     * {r$}#197 AS avg(avg_over_time(network.bytes_in[1h]))#190]]
     *     \_Aggregate[[TBUCKET{r}#191],[SUM(AVGOVERTIME_$1{r}#193,true[BOOLEAN],PT0S[TIME_DURATION],compensated[KEYWORD]) AS $$SUM$av
     * g(avg_over_time(network.bytes_in[1h]))$0#196, COUNT(AVGOVERTIME_$1{r}#193,true[BOOLEAN],PT0S[TIME_DURATION]) AS
     *          $$COUNT$avg(avg_over_time(network.bytes_in[1h]))$1#197, TBUCKET{r}#191 AS TBUCKET#191]]
     *       \_Eval[[$$SUM$AVGOVERTIME_$1$0{r$}#194 / $$COUNT$AVGOVERTIME_$1$1{r$}#195 AS AVGOVERTIME_$1#193]]
     *         \_TimeSeriesAggregate[[_tsid{m}#192, TBUCKET{r}#191],
     *                      [SUM(network.bytes_in{f}#178,true[BOOLEAN],PT0S[TIME_DURATION],lossy[KEYWORD]) A
     * S $$SUM$AVGOVERTIME_$1$0#194, COUNT(network.bytes_in{f}#178,true[BOOLEAN],PT0S[TIME_DURATION]) AS
     *              $$COUNT$AVGOVERTIME_$1$1#195, TBUCKET{r}#191],
     * BUCKET(@timestamp{f}#164,PT1H[TIME_DURATION])]
     *           \_Eval[[BUCKET(@timestamp{f}#164,PT1H[TIME_DURATION]) AS TBUCKET#191]]
     *             \_Filter[ISNOTNULL(network.bytes_in{f}#178)]
     *               \_EsRelation[k8s][@timestamp{f}#164, client.ip{f}#168, cluster{f}#165, ..]
     */
    public void testPromqlAvgWithoutByDimension() {
        // TS metrics-hostmetricsreceiver.otel-default
        // | STATS AVG(AVG_OVER_TIME(`metrics.system.memory.utilization`)) BY TBUCKET(1h) | LIMIT 10000"
        var plan = planPromql("""
            TS k8s
            | promql step 1h (
                avg(avg_over_time(network.bytes_in[1h]))
              )
            | LIMIT 1000
            """);

    }

    /**
     * Expect the following logical plan
     *
     * Project[[max by (pod) (avg_over_time(network.bytes_in[1h])){r}#342, pod{r}#318, TBUCKET{r}#343]]
     * \_TopN[[Order[TBUCKET{r}#343,ASC,FIRST]],1000[INTEGER],false]
     *   \_Eval[[UNPACKDIMENSION(grouppod_$1{r}#348) AS pod#318]]
     *     \_Aggregate[[packpod_$1{r}#347, TBUCKET{r}#343],[MAX(AVGOVERTIME_$1{r}#345,true[BOOLEAN],PT0S[TIME_DURATION]) AS max by (po
     * d) (avg_over_time(network.bytes_in[1h]))#342, packpod_$1{r}#347 AS grouppod_$1#348, TBUCKET{r}#343 AS TBUCKET#343]]
     *       \_Eval[[$$SUM$AVGOVERTIME_$1$0{r$}#349 / $$COUNT$AVGOVERTIME_$1$1{r$}#350 AS AVGOVERTIME_$1#345, PACKDIMENSION(pod{r}
     * #346) AS packpod_$1#347]]
     *         \_TimeSeriesAggregate[[_tsid{m}#344, TBUCKET{r}#343],
     *          [SUM(network.bytes_in{f}#330,true[BOOLEAN],PT0S[TIME_DURATION],lossy[KEYWORD]) A
     * S $$SUM$AVGOVERTIME_$1$0#349, COUNT(network.bytes_in{f}#330,true[BOOLEAN],PT0S[TIME_DURATION]) AS $$COUNT$AVGOVERTIME_$1$1#350,
     *                          VALUES(pod{f}#318,true[BOOLEAN],PT0S[TIME_DURATION]) AS pod#346, TBUCKET{r}#343],
     * BUCKET(@timestamp{f}#316,PT1H[TIME_DURATION])]
     *           \_Eval[[BUCKET(@timestamp{f}#316,PT1H[TIME_DURATION]) AS TBUCKET#343]]
     *             \_Filter[ISNOTNULL(network.bytes_in{f}#330)]
     *               \_EsRelation[k8s][@timestamp{f}#316, client.ip{f}#320, cluster{f}#317, ..]
     */
    public void testRangeSelector() {
        // TS metrics-hostmetricsreceiver.otel-default
        // | WHERE @timestamp >= \"{{from | minus .benchmark.duration}}\" AND @timestamp <=\"{{from}}\"
        // | STATS AVG(AVG_OVER_TIME(`metrics.system.memory.utilization`)) BY host.name, TBUCKET(1h) | LIMIT 10000"
        var plan = planPromql("""
            TS k8s
            | promql step 1h ( max by (pod) (avg_over_time(network.bytes_in[1h])) )
            """);

    }

    @AwaitsFix(bugUrl = "Invalid call to dataType on an unresolved object ?RATE_$1")
    public void testRate() {
        // TS metrics-hostmetricsreceiver.otel-default
        // | WHERE @timestamp >= \"{{from | minus .benchmark.duration}}\" AND @timestamp <= \"{{from}}\"
        // | STATS AVG(RATE(`metrics.system.cpu.time`)) BY host.name, TBUCKET(1h) | LIMIT 10000"
        String testQuery = """
            TS k8s
            | promql step 1h (
                avg by (pod) (rate(network.bytes_in[1h]))
                )
            """;

        var plan = planPromql(testQuery);
    }

    /**
     * Expect the following logical plan
     *
     * Project[[avg(avg_over_time(network.bytes_in[5m])){r}#423, TBUCKET{r}#424]]
     * \_TopN[[Order[TBUCKET{r}#424,ASC,FIRST]],1000[INTEGER],false]
     *   \_Eval[[$$SUM$avg(avg_over_time(network.bytes_in[5m]))$0{r$}#429 / $$COUNT$avg(avg_over_time(network.bytes_in[5m]))$1
     * {r$}#430 AS avg(avg_over_time(network.bytes_in[5m]))#423]]
     *     \_Aggregate[[TBUCKET{r}#424],[SUM(AVGOVERTIME_$1{r}#426,true[BOOLEAN],PT0S[TIME_DURATION],compensated[KEYWORD]) AS $$SUM$av
     * g(avg_over_time(network.bytes_in[5m]))$0#429, COUNT(AVGOVERTIME_$1{r}#426,true[BOOLEAN],PT0S[TIME_DURATION]) AS
     *              $$COUNT$avg(avg_over_time(network.bytes_in[5m]))$1#430, TBUCKET{r}#424 AS TBUCKET#424]]
     *       \_Eval[[$$SUM$AVGOVERTIME_$1$0{r$}#427 / $$COUNT$AVGOVERTIME_$1$1{r$}#428 AS AVGOVERTIME_$1#426]]
     *         \_TimeSeriesAggregate[[_tsid{m}#425, TBUCKET{r}#424],
     *              [SUM(network.bytes_in{f}#411,true[BOOLEAN],PT0S[TIME_DURATION],lossy[KEYWORD]) A
     * S $$SUM$AVGOVERTIME_$1$0#427, COUNT(network.bytes_in{f}#411,true[BOOLEAN],PT0S[TIME_DURATION]) AS
     *                  $$COUNT$AVGOVERTIME_$1$1#428, TBUCKET{r}#424],
     * BUCKET(@timestamp{f}#397,PT5M[TIME_DURATION])]
     *           \_Eval[[BUCKET(@timestamp{f}#397,PT5M[TIME_DURATION]) AS TBUCKET#424]]
     *             \_Filter[ISNOTNULL(network.bytes_in{f}#411)]
     *               \_EsRelation[k8s][@timestamp{f}#397, client.ip{f}#401, cluster{f}#398, ..]
     */
    public void testStartEndStep() {
        String testQuery = """
            TS k8s
            | promql start $now-1h end $now step 5m (
                avg(avg_over_time(network.bytes_in[5m]))
                )
            """;

        var plan = planPromql(testQuery);
        var filters = plan.collect(Filter.class::isInstance);
        assertThat(filters, hasSize(1));
        var filter = (Filter) filters.getFirst();
        assertThat(filter.condition().collect(e -> e instanceof FieldAttribute a && a.name().equals("@timestamp")), hasSize(2));
    }

    /**
     * Expect the following logical plan
     *
     * Project[[max by (pod) (avg_over_time(network.bytes_in{pod=~"host-0|host-1|host-2"}[5m])){r}#33, pod{r}#9, TBUCKET{r}#3
     * 4]]
     * \_TopN[[Order[TBUCKET{r}#34,ASC,FIRST]],1000[INTEGER],false]
     *   \_Eval[[UNPACKDIMENSION(grouppod_$1{r}#39) AS pod#9]]
     *     \_Aggregate[[packpod_$1{r}#38, TBUCKET{r}#34],[MAX(AVGOVERTIME_$1{r}#36,true[BOOLEAN],PT0S[TIME_DURATION]) AS max by (pod)
     * (avg_over_time(network.bytes_in{pod=~"host-0|host-1|host-2"}[5m]))#33, packpod_$1{r}#38 AS grouppod_$1#39,
     *                              TBUCKET{r}#34 AS TBUCKET#34]]
     *       \_Eval[[$$SUM$AVGOVERTIME_$1$0{r$}#40 / $$COUNT$AVGOVERTIME_$1$1{r$}#41 AS AVGOVERTIME_$1#36, PACKDIMENSION(pod{r}#37
     * ) AS packpod_$1#38]]
     *         \_TimeSeriesAggregate[[_tsid{m}#35, TBUCKET{r}#34],
     *                              [SUM(network.bytes_in{f}#21,true[BOOLEAN],PT0S[TIME_DURATION],lossy[KEYWORD]) AS $
     * $SUM$AVGOVERTIME_$1$0#40, COUNT(network.bytes_in{f}#21,true[BOOLEAN],PT0S[TIME_DURATION]) AS $$COUNT$AVGOVERTIME_$1$1#41,
     *                                      VALUES(pod{f}#9,true[BOOLEAN],PT0S[TIME_DURATION]) AS pod#37, TBUCKET{r}#34],
     * BUCKET(@timestamp{f}#7,PT5M[TIME_DURATION])]
     *           \_Eval[[BUCKET(@timestamp{f}#7,PT5M[TIME_DURATION]) AS TBUCKET#34]]
     *             \_Filter[ISNOTNULL(network.bytes_in{f}#21) AND IN(host-0[KEYWORD],host-1[KEYWORD],host-2[KEYWORD],pod{f}#9)]
     *               \_EsRelation[k8s][@timestamp{f}#7, client.ip{f}#11, cluster{f}#8, eve..]
     */
    public void testLabelSelector() {
        // TS metrics-hostmetricsreceiver.otel-default | WHERE @timestamp >= \"{{from | minus .benchmark.duration}}\" AND @timestamp <=
        // \"{{from}}\"
        // | WHERE host.name IN(\"host-0\", \"host-1\", \"host-2\")
        // | STATS AVG(AVG_OVER_TIME(`system.cpu.load_average.1m`)) BY host.name, TBUCKET(5m) | LIMIT 10000"
        String testQuery = """
            TS k8s
            | promql time $now (
                max by (pod) (avg_over_time(network.bytes_in{pod=~"host-0|host-1|host-2"}[5m]))
              )
            """;

        var plan = planPromql(testQuery);
        var filters = plan.collect(Filter.class::isInstance);
        assertThat(filters, hasSize(1));
        var filter = (Filter) filters.getFirst();
        assertThat(filter.condition().anyMatch(In.class::isInstance), equalTo(true));
    }

    public void testLabelSelectorPrefix() {
        // TS metrics-hostmetricsreceiver.otel-default | WHERE @timestamp >= \"{{from | minus .benchmark.duration}}\" AND @timestamp <=
        // \"{{from}}\"
        // | WHERE host.name LIKE \"host-*\"
        // STATS AVG(AVG_OVER_TIME(`metrics.system.cpu.load_average.1m`)) BY host.name, TBUCKET(5 minutes)"
        String testQuery = """
            TS k8s
            | promql time $now (
                avg by (pod) (avg_over_time(network.bytes_in{pod=~"host-.*"}[5m]))
                )
            """;

        var plan = planPromql(testQuery);
        var filters = plan.collect(Filter.class::isInstance);
        assertThat(filters, hasSize(1));
        var filter = (Filter) filters.getFirst();
        assertThat(filter.condition().anyMatch(StartsWith.class::isInstance), equalTo(true));
        assertThat(filter.condition().anyMatch(NotEquals.class::isInstance), equalTo(false));
    }

    public void testLabelSelectorProperPrefix() {
        var plan = planPromql("""
            TS k8s
            | promql time $now (
                avg(avg_over_time(network.bytes_in{pod=~"host-.+"}[1h]))
              )
            """);

        var filters = plan.collect(Filter.class::isInstance);
        assertThat(filters, hasSize(1));
        var filter = (Filter) filters.getFirst();
        assertThat(filter.condition().anyMatch(StartsWith.class::isInstance), equalTo(true));
        assertThat(filter.condition().anyMatch(NotEquals.class::isInstance), equalTo(true));
    }

    /**
     * Expect the following logical plan
     *
     * Project[[avg(avg_over_time(network.bytes_in{pod=~"[a-z]+"}[1h])){r}#305, TBUCKET{r}#306]]
     * \_TopN[[Order[TBUCKET{r}#306,ASC,FIRST]],1000[INTEGER],false]
     *   \_Eval[[$$SUM$avg(avg_over_time(network.bytes_in{pod=~"[a-z]+"}[1h]))$0{r$}#311 / $$COUNT$avg(avg_over_time(network.b
     * ytes_in{pod=~"[a-z]+"}[1h]))$1{r$}#312 AS avg(avg_over_time(network.bytes_in{pod=~"[a-z]+"}[1h]))#305]]
     *     \_Aggregate[[TBUCKET{r}#306],[SUM(AVGOVERTIME_$1{r}#308,true[BOOLEAN],PT0S[TIME_DURATION],compensated[KEYWORD]) AS $$SUM$av
     * g(avg_over_time(network.bytes_in{pod=~"[a-z]+"}[1h]))$0#311, COUNT(AVGOVERTIME_$1{r}#308,true[BOOLEAN],PT0S[TIME_DURATION]) AS
     *                          $$COUNT$avg(avg_over_time(network.bytes_in{pod=~"[a-z]+"}[1h]))$1#312, TBUCKET{r}#306 AS TBUCKET#306]]
     *       \_Eval[[$$SUM$AVGOVERTIME_$1$0{r$}#309 / $$COUNT$AVGOVERTIME_$1$1{r$}#310 AS AVGOVERTIME_$1#308]]
     *         \_TimeSeriesAggregate[[_tsid{m}#307, TBUCKET{r}#306],
     *                              [SUM(network.bytes_in{f}#293,true[BOOLEAN],PT0S[TIME_DURATION],lossy[KEYWORD]) A
     * S $$SUM$AVGOVERTIME_$1$0#309, COUNT(network.bytes_in{f}#293,true[BOOLEAN],PT0S[TIME_DURATION]) AS
     *                          $$COUNT$AVGOVERTIME_$1$1#310, TBUCKET{r}#306],
     * BUCKET(@timestamp{f}#279,PT5M[TIME_DURATION])]
     *           \_Eval[[BUCKET(@timestamp{f}#279,PT5M[TIME_DURATION]) AS TBUCKET#306]]
     *             \_Filter[ISNOTNULL(network.bytes_in{f}#293) AND RLIKE(pod{f}#281, "[a-z]+", false)]
     *               \_EsRelation[k8s][@timestamp{f}#279, client.ip{f}#283, cluster{f}#280, ..]
     */
    public void testLabelSelectorRegex() {
        var plan = planPromql("""
            TS k8s
            | promql time $now (
                avg(avg_over_time(network.bytes_in{pod=~"[a-z]+"}[1h]))
              )
            """);

        var filters = plan.collect(Filter.class::isInstance);
        assertThat(filters, hasSize(1));
        var filter = (Filter) filters.getFirst();
        assertThat(filter.condition().anyMatch(RegexMatch.class::isInstance), equalTo(true));
    }

    @AwaitsFix(bugUrl = "This should never be called before the attribute is resolved")
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
    }

    @AwaitsFix(bugUrl = "only aggregations across timeseries are supported at this time (found [foo or bar])")
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
        query = query.replace("$now-1h", '"' + Instant.now().minus(1, ChronoUnit.HOURS).toString() + '"');
        query = query.replace("$now", '"' + Instant.now().toString() + '"');
        var analyzed = tsAnalyzer.analyze(parser.createStatement(query));
        logger.trace("analyzed plan:\n{}", analyzed);
        var optimized = logicalOptimizer.optimize(analyzed);
        logger.trace("optimized plan:\n{}", optimized);
        return optimized;
    }
}
