/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.promql;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.xpack.esql.EsqlTestUtils;
import org.elasticsearch.xpack.esql.action.PromqlFeatures;
import org.elasticsearch.xpack.esql.analysis.Analyzer;
import org.elasticsearch.xpack.esql.analysis.AnalyzerContext;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.AttributeSet;
import org.elasticsearch.xpack.esql.core.expression.Expressions;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.expression.predicate.regex.RegexMatch;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.EsqlFunctionRegistry;
import org.elasticsearch.xpack.esql.expression.function.aggregate.LastOverTime;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Max;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Sum;
import org.elasticsearch.xpack.esql.expression.function.grouping.Bucket;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToDouble;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.StartsWith;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.regex.RLike;
import org.elasticsearch.xpack.esql.expression.predicate.logical.And;
import org.elasticsearch.xpack.esql.expression.predicate.logical.Not;
import org.elasticsearch.xpack.esql.expression.predicate.nulls.IsNotNull;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.Add;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.Div;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.Mul;
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
import org.elasticsearch.xpack.esql.plan.logical.Limit;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.Project;
import org.elasticsearch.xpack.esql.plan.logical.TimeSeriesAggregate;
import org.elasticsearch.xpack.esql.plan.logical.TopN;
import org.hamcrest.Matcher;
import org.junit.BeforeClass;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static java.util.Collections.emptyMap;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.TEST_VERIFIER;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.as;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.emptyInferenceResolution;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.loadMapping;
import static org.elasticsearch.xpack.esql.analysis.VerifierTests.error;
import static org.elasticsearch.xpack.esql.plan.QuerySettings.UNMAPPED_FIELDS;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.not;

// @TestLogging(value = "org.elasticsearch.xpack.esql:TRACE", reason = "debug tests")
public class PromqlLogicalPlanOptimizerTests extends AbstractLogicalPlanOptimizerTests {

    private static Analyzer tsAnalyzer;

    @BeforeClass
    public static void initTest() {
        assumeTrue("requires snapshot build with promql feature enabled", PromqlFeatures.isEnabled());

        var timeSeriesMapping = loadMapping("k8s-mappings.json");
        var timeSeriesIndex = IndexResolution.valid(
            new EsIndex("k8s", timeSeriesMapping, Map.of("k8s", IndexMode.TIME_SERIES), Map.of(), Map.of(), Set.of())
        );
        tsAnalyzer = new Analyzer(
            new AnalyzerContext(
                EsqlTestUtils.TEST_CFG,
                new EsqlFunctionRegistry(),
                Map.of(new IndexPattern(Source.EMPTY, "k8s"), timeSeriesIndex),
                emptyMap(),
                enrichResolution,
                emptyInferenceResolution(),
                TransportVersion.current(),
                UNMAPPED_FIELDS.defaultValue()
            ),
            TEST_VERIFIER
        );
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
        List<Attribute> output = plan.output();

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
            PROMQL index=k8s step=1h ( avg by (pod) (avg_over_time(network.bytes_in{pod=~"host-0|host-1|host-2"}[1h])) )
            | LIMIT 1000
            """);

        var project = as(plan, Project.class);
        assertThat(project.projections(), hasSize(3));

        var evalOuter = as(project.child(), Eval.class);
        var limit = as(evalOuter.child(), Limit.class);

        var aggregate = as(limit.child(), Aggregate.class);
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
        assertThat(Expressions.attribute(tsAggregate.groupings().get(1)).id(), equalTo(tbucketId));
        assertThat(Expressions.attribute(aggregate.groupings().get(0)).id(), equalTo(tbucketId));
        assertThat(Expressions.attribute(project.projections().get(1)).id(), equalTo(tbucketId));

        // Filter should contain: IN(host-0, host-1, host-2, pod)
        var filter = as(evalBucket.child(), Filter.class);
        var in = as(filter.condition(), In.class);
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
        List<Attribute> output = plan.output();

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
            PROMQL index=k8s step=1h (
                avg(avg_over_time(network.bytes_in[1h]))
              )
            | LIMIT 1000
            """);

    }

    public void testPromqlTrailingSpaces() {
        planPromql("PROMQL index=k8s step=1h (max(network.bytes_in)) ");
        planPromql("PROMQL index=k8s step=1h (max(network.bytes_in)) | SORT step");
    }

    public void testPromqlMaxOfLongField() {
        var plan = planPromql("PROMQL index=k8s step=1h max(network.bytes_in)");
        // In PromQL, the output is always double
        assertThat(plan.output().getFirst().dataType(), equalTo(DataType.DOUBLE));
        assertThat(plan.output().getFirst().name(), equalTo("max(network.bytes_in)"));
    }

    public void testPromqlExplicitOutputName() {
        var plan = planPromql("PROMQL index=k8s step=1h max_bytes=(max(network.bytes_in))");
        assertThat(plan.output().getFirst().name(), equalTo("max_bytes"));
    }

    public void testSort() {
        var plan = planPromql("""
            PROMQL index=k8s step=1h (
                avg(network.bytes_in) by (pod)
              )
            | SORT step, pod, `avg(network.bytes_in) by (pod)`
            """);
        List<String> order = plan.collect(TopN.class)
            .getFirst()
            .order()
            .stream()
            .map(o -> as(o.child(), NamedExpression.class).name())
            .toList();
        assertThat(order, hasSize(3));
        assertThat(order, equalTo(List.of("step", "pod", "avg(network.bytes_in) by (pod)")));
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
            PROMQL index=k8s step=1h ( max by (pod) (avg_over_time(network.bytes_in[1h])) )
            """);

    }

    @AwaitsFix(bugUrl = "Invalid call to dataType on an unresolved object ?RATE_$1")
    public void testRate() {
        // TS metrics-hostmetricsreceiver.otel-default
        // | WHERE @timestamp >= \"{{from | minus .benchmark.duration}}\" AND @timestamp <= \"{{from}}\"
        // | STATS AVG(RATE(`metrics.system.cpu.time`)) BY host.name, TBUCKET(1h) | LIMIT 10000"
        String testQuery = """
            PROMQL index=k8s step=1h (
                avg by (pod) (rate(network.bytes_in[1h]))
                )
            """;

        var plan = planPromql(testQuery);
    }

    /**
     * Expect the logical plan structure:
     * Project
     * \_Eval
     *   \_Limit
     *     \_Aggregate
     *       \_Eval
     *         \_TimeSeriesAggregate[[...],[SUM(...,PT10M,...), COUNT(...,PT10M,...), ...], BUCKET(@timestamp,PT5M)]
     */
    public void testRangeSelectorWithDifferentStep() {
        var plan = planPromql("""
            PROMQL index=k8s step=5m sum by (pod) (avg_over_time(events_received[10m]))
            """);

        var tsAggregate = plan.collect(TimeSeriesAggregate.class).getFirst();

        // Verify bucket is 5 minutes
        assertThat(tsAggregate.timeBucket().buckets().fold(FoldContext.small()), equalTo(Duration.ofMinutes(5)));

        // Verify window is 10 minutes
        var sum = tsAggregate.aggregates().getFirst().collect(Sum.class).getFirst();
        assertThat(sum.window().fold(FoldContext.small()), equalTo(Duration.ofMinutes(10)));
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
            PROMQL index=k8s start=$now-1h end=$now step=5m (
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

    @AwaitsFix(bugUrl = "Instant promql queries in are not supported at the moment")
    public void testLabelSelector() {
        // TS metrics-hostmetricsreceiver.otel-default | WHERE @timestamp >= \"{{from | minus .benchmark.duration}}\" AND @timestamp <=
        // \"{{from}}\"
        // | WHERE host.name IN(\"host-0\", \"host-1\", \"host-2\")
        // | STATS AVG(AVG_OVER_TIME(`system.cpu.load_average.1m`)) BY host.name, TBUCKET(5m) | LIMIT 10000"
        String testQuery = """
            PROMQL index=k8s time=$now (
                max by (pod) (avg_over_time(network.bytes_in{pod=~"host-0|host-1|host-2"}[5m]))
              )
            """;

        var plan = planPromql(testQuery);
        var filters = plan.collect(Filter.class::isInstance);
        assertThat(filters, hasSize(1));
        var filter = (Filter) filters.getFirst();
        assertThat(filter.condition().anyMatch(In.class::isInstance), equalTo(true));
    }

    @AwaitsFix(bugUrl = "Instant promql queries in are not supported at the moment")
    public void testLabelSelectorPrefix() {
        // TS metrics-hostmetricsreceiver.otel-default | WHERE @timestamp >= \"{{from | minus .benchmark.duration}}\" AND @timestamp <=
        // \"{{from}}\"
        // | WHERE host.name LIKE \"host-*\"
        // STATS AVG(AVG_OVER_TIME(`metrics.system.cpu.load_average.1m`)) BY host.name, TBUCKET(5 minutes)"
        String testQuery = """
            PROMQL index=k8s time=$now (
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

    @AwaitsFix(bugUrl = "Instant promql queries in are not supported at the moment")
    public void testLabelSelectorProperPrefix() {
        var plan = planPromql("""
            PROMQL index=k8s time=$now (
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
    @AwaitsFix(bugUrl = "Instant promql queries in are not supported at the moment")
    public void testLabelSelectorRegex() {
        var plan = planPromql("""
            PROMQL index=k8s time=$now (
                avg(avg_over_time(network.bytes_in{pod=~"[a-z]+"}[1h]))
              )
            """);

        var filters = plan.collect(Filter.class::isInstance);
        assertThat(filters, hasSize(1));
        var filter = (Filter) filters.getFirst();
        assertThat(filter.condition().anyMatch(RegexMatch.class::isInstance), equalTo(true));
    }

    public void testLabelSelectorNotEquals() {
        var plan = planPromql("PROMQL index=k8s step=1m avg(network.bytes_in{pod!=\"foo\"})");

        var filter = plan.collect(Filter.class).getFirst();
        var not = filter.condition().collect(Not.class).getFirst();
        var in = as(not.field(), In.class);
        assertThat(as(in.value(), FieldAttribute.class).name(), equalTo("pod"));
        assertThat(in.list(), hasSize(1));
        assertThat(as(as(in.list().getFirst(), Literal.class).value(), BytesRef.class).utf8ToString(), equalTo("foo"));
    }

    public void testLabelSelectorRegexNegation() {
        var plan = planPromql("PROMQL index=k8s step=1m avg(network.bytes_in{pod!~\"f.o\"})");

        var filter = plan.collect(Filter.class).getFirst();
        var not = filter.condition().collect(Not.class).getFirst();
        var rLike = as(not.field(), RLike.class);
        assertThat(as(rLike.field(), FieldAttribute.class).name(), equalTo("pod"));
        assertThat(rLike.pattern().pattern(), equalTo("f.o"));
    }

    public void testLabelSelectors() {
        var plan = planPromql("PROMQL index=k8s step=1m avg(network.bytes_in{pod!=\"foo\",cluster=~\"bar|baz\",region!~\"us-.*\"})");

        var filter = plan.collect(Filter.class).getFirst();
        var and = as(filter.condition(), And.class);
        if (and.left() instanceof IsNotNull) {
            and = as(and.right(), And.class);
        }
        var left = as(and.left(), And.class);
        var podNotFoo = as(as(left.left(), Not.class).field(), In.class);
        assertThat(podNotFoo.list(), hasSize(1));
        assertThat(as(podNotFoo.list().getFirst(), Literal.class).value(), equalTo(new BytesRef("foo")));

        var clusterInBarBaz = as(left.right(), In.class);
        assertThat(clusterInBarBaz.list(), hasSize(2));
        assertThat(as(clusterInBarBaz.list().get(0), Literal.class).value(), equalTo(new BytesRef("bar")));
        assertThat(as(clusterInBarBaz.list().get(1), Literal.class).value(), equalTo(new BytesRef("baz")));

        var regionNotUs = as(as(and.right(), Not.class).field(), StartsWith.class);
        assertThat(as(regionNotUs.prefix(), Literal.class).value(), equalTo(new BytesRef("us-")));
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
            PROMQL index=k8s step=5m (
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
            PROMQL index=k8s step=5m (
              foo or bar
            )
            """;

        var plan = planPromql(testQuery);
    }

    public void testScalarAndInstantVectorArithmeticOperators() {
        LogicalPlan plan;
        plan = planPromql("PROMQL index=k8s step=5m max(network.bytes_in / 1024) by (pod)");
        Div div = as(plan.collect(Eval.class).get(1).fields().getLast().child(), Div.class);
        assertThat(div.left().sourceText(), equalTo("network.bytes_in"));
        assertThat(as(div.right(), Literal.class).value(), equalTo(1024.0));
    }

    public void testConstantFoldingArithmeticOperators() {
        var plan = planPromqlExpectNoReferences("PROMQL index=k8s step=5m 1 + 1");
        var eval = plan.collect(Eval.class).getFirst();
        var literal = as(eval.fields().getFirst().child(), Literal.class);
        assertThat(literal.value(), equalTo(2.0));
    }

    public void testUnsupportedBinaryOperators() {
        assertThat(
            error("PROMQL index=k8s step=5m foo or bar", tsAnalyzer),
            containsString("VectorBinarySet queries are not supported at this time [foo or bar]")
        );
        assertThat(
            error("PROMQL index=k8s step=5m foo > bar", tsAnalyzer),
            containsString("VectorBinaryComparison queries are not supported at this time [foo > bar]")
        );
    }

    public void testTopLevelBinaryArithmeticQuery() {
        var plan = planPromql("""
            PROMQL index=k8s step=1m in_n_out=(
                network.eth0.rx + network.eth0.tx
              )
            | SORT in_n_out""");
        assertThat(plan.output().stream().map(Attribute::name).toList(), equalTo(List.of("in_n_out", "step", "_timeseries")));
        Add add = as(plan.collect(Eval.class).get(1).fields().getLast().child(), Add.class);
        assertThat(add.left().sourceText(), equalTo("network.eth0.rx"));
        assertThat(add.right().sourceText(), equalTo("network.eth0.tx"));
    }

    public void testGroupByAllWithinSeriesAggregate() {
        var plan = planPromql("PROMQL index=k8s step=1m count=(count_over_time(network.bytes_in[1m]))");
        assertThat(plan.output().stream().map(Attribute::name).toList(), equalTo(List.of("count", "step", "_timeseries")));
    }

    public void testBinaryInstantSelectorAndLiteral() {
        var plan = planPromql("PROMQL index=k8s step=1m bits=(network.bytes_in * 8)");
        assertThat(plan.output().stream().map(Attribute::name).toList(), equalTo(List.of("bits", "step", "_timeseries")));

        Mul mul = as(plan.collect(Eval.class).get(1).fields().getLast().child(), Mul.class);
        assertThat(as(as(mul.left(), ToDouble.class).field(), ReferenceAttribute.class).sourceText(), equalTo("network.bytes_in"));
        assertThat(as(mul.right(), Literal.class).fold(null), equalTo(8.0));

        TimeSeriesAggregate tsAgg = plan.collect(TimeSeriesAggregate.class).getFirst();
        LastOverTime last = as(Alias.unwrap(tsAgg.aggregates().getFirst()), LastOverTime.class);
        assertThat(as(last.field(), FieldAttribute.class).sourceText(), equalTo("network.bytes_in"));
    }

    public void testBinaryAcrossSeriesAndLiteral() {
        var plan = planPromql("PROMQL index=k8s step=1m bits=(max(network.total_bytes_in) * 8)");
        assertThat(plan.output().stream().map(Attribute::name).toList(), equalTo(List.of("bits", "step")));

        Eval eval = plan.collect(Eval.class).getFirst();
        Mul mul = as(eval.fields().getFirst().child(), Mul.class);
        assertThat(mul.left().sourceText(), equalTo("max(network.total_bytes_in)"));
        assertThat(as(mul.right(), Literal.class).fold(null), equalTo(8.0));

        Aggregate agg = eval.collect(Aggregate.class).getFirst();
        Max max = as(Alias.unwrap(agg.aggregates().getFirst()), Max.class);
        assertThat(as(max.field(), ReferenceAttribute.class).sourceText(), equalTo("network.total_bytes_in"));

        TimeSeriesAggregate tsAgg = agg.collect(TimeSeriesAggregate.class).getFirst();
        assertThat(tsAgg.timeBucket().buckets().fold(null), equalTo(Duration.ofMinutes(1)));
        LastOverTime last = as(Alias.unwrap(tsAgg.aggregates().getFirst()), LastOverTime.class);
        assertThat(as(last.field(), FieldAttribute.class).sourceText(), equalTo("network.total_bytes_in"));
    }

    public void testAcrossSeriesMultiplicationLiteral() {
        var plan = planPromql("PROMQL index=k8s step=1m bits=(max(network.total_bytes_in * 8))");
        assertThat(plan.output().stream().map(Attribute::name).toList(), equalTo(List.of("bits", "step")));

        Aggregate agg = plan.collect(Aggregate.class).getFirst();
        Max max = as(Alias.unwrap(agg.aggregates().getFirst()), Max.class);
        assertThat(as(max.field(), ReferenceAttribute.class).sourceText(), equalTo("network.total_bytes_in * 8"));

        Eval eval = agg.collect(Eval.class).getFirst();
        Mul mul = as(Alias.unwrap(eval.fields().getFirst().child()), Mul.class);
        assertThat(mul.left().sourceText(), equalTo("network.total_bytes_in"));
        assertThat(as(mul.right(), Literal.class).fold(null), equalTo(8.0));

        TimeSeriesAggregate tsAgg = eval.collect(TimeSeriesAggregate.class).getFirst();
        assertThat(tsAgg.timeBucket().buckets().fold(null), equalTo(Duration.ofMinutes(1)));
        LastOverTime last = as(Alias.unwrap(tsAgg.aggregates().getFirst()), LastOverTime.class);
        assertThat(as(last.field(), FieldAttribute.class).sourceText(), equalTo("network.total_bytes_in"));
    }

    public void testGroupByAllInstantSelector() {
        var plan = planPromql("PROMQL index=k8s step=1m network.bytes_in");
        assertThat(plan.output().stream().map(Attribute::name).toList(), equalTo(List.of("network.bytes_in", "step", "_timeseries")));
    }

    public void testGroupByAllInstantSelectorRate() {
        var plan = planPromql("PROMQL index=k8s step=1m rate=(rate(network.total_bytes_in[1m]))");
        assertThat(plan.output().stream().map(Attribute::name).toList(), equalTo(List.of("rate", "step", "_timeseries")));
    }

    public void testConstantResults() {
        assertConstantResult("ceil(vector(3.14159))", equalTo(4.0));
        assertConstantResult("pi()", equalTo(Math.PI));
        assertConstantResult("abs(vector(-1))", equalTo(1.0));
    }

    public void testRound() {
        assertConstantResult("round(vector(pi()))", equalTo(3.0)); // round down to nearest integer
        assertConstantResult("round(vector(pi()), 1)", equalTo(3.0)); // same as above but with explicit argument
        assertConstantResult("round(vector(pi()), 0.01)", equalTo(3.14)); // round down 2 decimal places
        assertConstantResult("round(vector(pi()), 0.001)", equalTo(3.142)); // round up 3 decimal places
        assertConstantResult("round(vector(pi()), 0.15)", equalTo(3.15)); // rounds up to nearest
        assertConstantResult("round(vector(pi()), 0.5)", equalTo(3.0)); // rounds down to nearest
    }

    private void assertConstantResult(String query, Matcher<Double> matcher) {
        var plan = planPromqlExpectNoReferences("PROMQL index=k8s step=1m " + query);
        Eval eval = plan.collect(Eval.class).getFirst();
        Literal literal = as(eval.fields().getFirst().child(), Literal.class);
        assertThat(as(literal.value(), Double.class), matcher);

        Aggregate aggregate = eval.collect(Aggregate.class).getFirst();
        ReferenceAttribute step = as(aggregate.groupings().getFirst(), ReferenceAttribute.class);
        assertThat(step.name(), equalTo("step"));

        TimeSeriesAggregate tsAgg = aggregate.collect(TimeSeriesAggregate.class).getFirst();
        ReferenceAttribute stepInTsAgg = as(Alias.unwrap(tsAgg.aggregates().getFirst()), ReferenceAttribute.class);
        assertThat(stepInTsAgg.name(), equalTo("step"));

        Eval stepEval = tsAgg.collect(Eval.class).getFirst();
        Alias bucketAlias = as(stepEval.fields().getFirst(), Alias.class);
        assertThat(bucketAlias.id(), equalTo(stepInTsAgg.id()));
        assertThat(bucketAlias.id(), equalTo(step.id()));
    }

    protected LogicalPlan planPromql(String query) {
        return planPromql(query, false);
    }

    protected LogicalPlan planPromqlExpectNoReferences(String query) {
        return planPromql(query, true);
    }

    protected LogicalPlan planPromql(String query, boolean allowEmptyReferences) {
        query = query.replace("$now-1h", '"' + Instant.now().minus(1, ChronoUnit.HOURS).toString() + '"');
        query = query.replace("$now", '"' + Instant.now().toString() + '"');
        var analyzed = tsAnalyzer.analyze(parser.parseQuery(query));
        AttributeSet.Builder references = AttributeSet.builder();
        analyzed.forEachDown(lp -> references.addAll(lp.references()));
        if (allowEmptyReferences) {
            assertThat(references.build(), empty());
        } else {
            assertThat(references.build(), not(empty()));
        }
        logger.trace("analyzed plan:\n{}", analyzed);
        var optimized = logicalOptimizer.optimize(analyzed);
        logger.trace("optimized plan:\n{}", optimized);
        return optimized;
    }
}
