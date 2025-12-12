/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.xpack.esql.EsqlTestUtils;
import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.elasticsearch.xpack.esql.analysis.Analyzer;
import org.elasticsearch.xpack.esql.analysis.AnalyzerContext;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.MetadataAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.core.util.Holder;
import org.elasticsearch.xpack.esql.expression.function.EsqlFunctionRegistry;
import org.elasticsearch.xpack.esql.index.EsIndex;
import org.elasticsearch.xpack.esql.index.IndexResolution;
import org.elasticsearch.xpack.esql.optimizer.AbstractLogicalPlanOptimizerTests;
import org.elasticsearch.xpack.esql.plan.IndexPattern;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.TimeSeriesAggregate;
import org.junit.BeforeClass;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.TEST_VERIFIER;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.emptyInferenceResolution;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.loadMapping;
import static org.elasticsearch.xpack.esql.analysis.AnalyzerTestUtils.defaultLookupResolution;
import static org.elasticsearch.xpack.esql.plan.QuerySettings.UNMAPPED_FIELDS;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;

public class TimeSeriesBareAggregationsTests extends AbstractLogicalPlanOptimizerTests {

    private static Map<String, EsField> mappingK8s;
    private static Analyzer k8sAnalyzer;

    @BeforeClass
    public static void initK8s() {
        mappingK8s = loadMapping("k8s-mappings.json");
        EsIndex k8sIndex = new EsIndex("k8s", mappingK8s, Map.of("k8s", IndexMode.TIME_SERIES), Map.of(), Map.of(), Set.of());

        IndexResolution indexResolution = IndexResolution.valid(k8sIndex);

        Map<IndexPattern, IndexResolution> resolutions = new HashMap<>();
        resolutions.put(new IndexPattern(Source.EMPTY, indexResolution.get().name()), indexResolution);

        k8sAnalyzer = new Analyzer(
            new AnalyzerContext(
                EsqlTestUtils.TEST_CFG,
                new EsqlFunctionRegistry(),
                resolutions,
                defaultLookupResolution(),
                enrichResolution,
                emptyInferenceResolution(),
                TransportVersion.minimumCompatible(),
                UNMAPPED_FIELDS.defaultValue()
            ),
            TEST_VERIFIER
        );
    }

    protected LogicalPlan planK8s(String query) {
        LogicalPlan analyzed = k8sAnalyzer.analyze(parser.parseQuery(query));
        return logicalOptimizer.optimize(analyzed);
    }

    /**
     * Translation: TS k8s | STATS avg_over_time(field) → TS k8s | STATS VALUES(avg_over_time(field)) BY _tsid
     * <br/>
     * AVG_OVER_TIME translates into [Eval[[SUMOVERTIME(network.cost{f}#22,true[BOOLEAN]) / COUNTOVERTIME(network.cost{f}#22,true[BOOLEAN])
     * AS avg_over_time(network.cost)#4]]]
     */
    public void testBareAvgOverTime() {
        assumeTrue("requires metrics command", EsqlCapabilities.Cap.METRICS_GROUP_BY_ALL.isEnabled());
        LogicalPlan plan = planK8s("""
            TS k8s
            | STATS avg_over_time(network.cost)
            """);

        TimeSeriesAggregate tsa = findTimeSeriesAggregate(plan);
        assertThat("Should have TimeSeriesAggregate", tsa, is(instanceOf(TimeSeriesAggregate.class)));

        List<Attribute> groupings = tsa.groupings().stream().filter(g -> g instanceof Attribute).map(g -> (Attribute) g).toList();
        boolean hasTsid = groupings.stream().anyMatch(g -> g.name().equals(MetadataAttribute.TSID_FIELD));
        assertThat("Should group by _tsid", hasTsid, is(true));

        var aggregates = tsa.aggregates();
        assertThat("Should have aggregates", aggregates.isEmpty(), is(false));

        List<Attribute> output = plan.output();
        boolean hasTimeseries = output.stream().anyMatch(attr -> attr.name().equals(MetadataAttribute.TIMESERIES));
        assertThat("Should have _timeseries in output", hasTimeseries, is(true));

        Attribute timeseriesAttr = output.stream()
            .filter(attr -> attr.name().equals(MetadataAttribute.TIMESERIES))
            .findFirst()
            .orElse(null);

        assertNotNull(timeseriesAttr);
        assertThat("_timeseries attribute should exist", timeseriesAttr, is(instanceOf(Attribute.class)));
        assertThat("_timeseries should be KEYWORD type", timeseriesAttr.dataType(), is(DataType.KEYWORD));
    }

    /**
     * Translation: TS k8s | STATS sum_over_time(field) → TS k8s | STATS VALUES(sum_over_time(field)) BY _tsid
     */
    public void testBareSumOverTime() {
        assumeTrue("requires metrics command", EsqlCapabilities.Cap.METRICS_GROUP_BY_ALL.isEnabled());
        LogicalPlan plan = planK8s("""
            TS k8s
            | STATS sum_over_time(network.cost)
            """);

        TimeSeriesAggregate tsa = findTimeSeriesAggregate(plan);
        assertThat("Should have TimeSeriesAggregate", tsa, is(instanceOf(TimeSeriesAggregate.class)));

        List<Attribute> groupings = tsa.groupings().stream().filter(g -> g instanceof Attribute).map(g -> (Attribute) g).toList();
        boolean hasTsid = groupings.stream().anyMatch(g -> g.name().equals(MetadataAttribute.TSID_FIELD));
        assertThat("Should group by _tsid", hasTsid, is(true));

        var aggregates = tsa.aggregates();
        assertThat("Should have aggregates", aggregates.isEmpty(), is(false));

        List<Attribute> output = plan.output();
        boolean hasTimeseries = output.stream().anyMatch(attr -> attr.name().equals(MetadataAttribute.TIMESERIES));
        assertThat("Should have _timeseries in output", hasTimeseries, is(true));

        Attribute timeseriesAttr = output.stream()
            .filter(attr -> attr.name().equals(MetadataAttribute.TIMESERIES))
            .findFirst()
            .orElse(null);

        assertNotNull(timeseriesAttr);
        assertThat("_timeseries attribute should exist", timeseriesAttr, is(instanceOf(Attribute.class)));
        assertThat("_timeseries should be KEYWORD type", timeseriesAttr.dataType(), is(DataType.KEYWORD));
    }

    /**
     * Translation: TS k8s | STATS sum_over_time(field) BY TBUCKET(1h)
     *  → TS k8s | STATS VALUES(sum_over_time(field)) BY _tsid, TBUCKET(1h)
     */
    public void testSumOverTimeWithTBucket() {
        assumeTrue("requires metrics command", EsqlCapabilities.Cap.METRICS_GROUP_BY_ALL.isEnabled());
        LogicalPlan plan = planK8s("""
            TS k8s
            | STATS sum_over_time(network.cost) BY TBUCKET(1 hour)
            """);

        TimeSeriesAggregate tsa = findTimeSeriesAggregate(plan);
        assertThat("Should have TimeSeriesAggregate", tsa, is(instanceOf(TimeSeriesAggregate.class)));

        List<Attribute> groupings = tsa.groupings().stream().filter(g -> g instanceof Attribute).map(g -> (Attribute) g).toList();
        assertThat(groupings.size(), is(2));

        boolean hasTsid = groupings.stream().anyMatch(g -> g.name().equals(MetadataAttribute.TSID_FIELD));
        assertThat("Should group by _tsid", hasTsid, is(true));

        boolean hasBucket = groupings.stream().anyMatch(g -> g.name().equalsIgnoreCase("BUCKET"));
        assertThat("Should group by bucket", hasBucket, is(true));

        List<Attribute> output = plan.output();
        boolean hasTimeseries = output.stream().anyMatch(attr -> attr.name().equals(MetadataAttribute.TIMESERIES));
        assertThat("Should have _timeseries in output", hasTimeseries, is(true));

        Attribute timeseriesAttr = output.stream()
            .filter(attr -> attr.name().equals(MetadataAttribute.TIMESERIES))
            .findFirst()
            .orElse(null);

        assertNotNull(timeseriesAttr);
        assertThat("_timeseries attribute should exist", timeseriesAttr, is(instanceOf(Attribute.class)));
        assertThat("_timeseries should be KEYWORD type", timeseriesAttr.dataType(), is(DataType.KEYWORD));
    }

    /**
     * Translation: TS k8s | STATS rate(field) BY TBUCKET(1h)
     *  → TS k8s | STATS VALUES(rate(field)) BY _tsid, TBUCKET(1h)
     */
    public void testRateWithTBucket() {
        assumeTrue("requires metrics command", EsqlCapabilities.Cap.METRICS_GROUP_BY_ALL.isEnabled());
        LogicalPlan plan = planK8s("""
            TS k8s
            | STATS rate(network.total_bytes_out) BY TBUCKET(1 hour)
            """);

        TimeSeriesAggregate tsa = findTimeSeriesAggregate(plan);
        assertThat("Should have TimeSeriesAggregate", tsa, is(instanceOf(TimeSeriesAggregate.class)));

        List<Attribute> groupings = tsa.groupings().stream().filter(g -> g instanceof Attribute).map(g -> (Attribute) g).toList();
        assertThat(groupings.size(), is(2));

        boolean hasTsid = groupings.stream().anyMatch(g -> g.name().equals(MetadataAttribute.TSID_FIELD));
        assertThat("Should group by _tsid", hasTsid, is(true));

        boolean hasBucket = groupings.stream().anyMatch(g -> g.name().equalsIgnoreCase("BUCKET"));
        assertThat("Should group by bucket", hasBucket, is(true));

        List<Attribute> output = plan.output();
        boolean hasTimeseries = output.stream().anyMatch(attr -> attr.name().equals(MetadataAttribute.TIMESERIES));
        assertThat("Should have _timeseries in output", hasTimeseries, is(true));

        Attribute timeseriesAttr = output.stream()
            .filter(attr -> attr.name().equals(MetadataAttribute.TIMESERIES))
            .findFirst()
            .orElse(null);

        assertNotNull(timeseriesAttr);
        assertThat("_timeseries attribute should exist", timeseriesAttr, is(instanceOf(Attribute.class)));
        assertThat("_timeseries should be KEYWORD type", timeseriesAttr.dataType(), is(DataType.KEYWORD));
    }

    /**
     * Wrapped _OVER_TIME functions are not translated.
     */
    public void testAlreadyWrappedAggregateNotModified() {
        assumeTrue("requires metrics command", EsqlCapabilities.Cap.METRICS_GROUP_BY_ALL.isEnabled());
        LogicalPlan planBefore = planK8s("""
            TS k8s
            | STATS MAX(rate(network.total_bytes_out))
            """);

        assertThat("Plan should be valid", planBefore, is(instanceOf(LogicalPlan.class)));
    }

    public void testCountOverTime() {
        assumeTrue("requires metrics command", EsqlCapabilities.Cap.METRICS_GROUP_BY_ALL.isEnabled());
        LogicalPlan plan = planK8s("TS k8s | STATS count = count_over_time(network.cost)");

        TimeSeriesAggregate tsa = findTimeSeriesAggregate(plan);
        assertThat("Should have TimeSeriesAggregate", tsa, is(instanceOf(TimeSeriesAggregate.class)));

        List<Attribute> groupings = tsa.groupings().stream().filter(g -> g instanceof Attribute).map(g -> (Attribute) g).toList();
        assertThat(groupings.size(), is(1));

        boolean hasTsid = groupings.stream().anyMatch(g -> g.name().equals(MetadataAttribute.TSID_FIELD));
        assertThat("Should still group by _tsid", hasTsid, is(true));

        List<Attribute> output = plan.output();
        boolean hasTimeseries = output.stream().anyMatch(attr -> attr.name().equals(MetadataAttribute.TIMESERIES));
        assertThat("Should have _timeseries in output", hasTimeseries, is(true));

        Attribute timeseriesAttr = output.stream()
            .filter(attr -> attr.name().equals(MetadataAttribute.TIMESERIES))
            .findFirst()
            .orElse(null);

        assertNotNull(timeseriesAttr);
        assertThat("_timeseries attribute should exist", timeseriesAttr, is(instanceOf(Attribute.class)));
        assertThat("_timeseries should be KEYWORD type", timeseriesAttr.dataType(), is(DataType.KEYWORD));
    }

    public void testMixedBareOverTimeAndRegularAggregates() {
        assumeTrue("requires metrics command", EsqlCapabilities.Cap.METRICS_GROUP_BY_ALL.isEnabled());

        var error = expectThrows(IllegalArgumentException.class, () -> { planK8s("""
            TS k8s
            | STATS avg_over_time(network.cost), sum(network.total_bytes_in)
            """); });

        assertThat(error.getMessage(), equalTo("""
            Cannot mix time-series aggregate [avg_over_time(network.cost)] and \
            regular aggregate [sum(network.total_bytes_in)] in the same TimeSeriesAggregate."""));
    }

    public void testGroupingKeyInAggregatesListPreserved() {
        assumeTrue("requires metrics command", EsqlCapabilities.Cap.METRICS_GROUP_BY_ALL.isEnabled());

        var error = expectThrows(IllegalArgumentException.class, () -> { planK8s("""
            TS k8s
            | STATS rate(network.total_bytes_out) BY region, TBUCKET(1hour)
            """); });

        assertThat(error.getMessage(), equalTo("Cannot mix time-series aggregate and grouping attributes. Found [region]."));
    }

    private TimeSeriesAggregate findTimeSeriesAggregate(LogicalPlan plan) {
        Holder<TimeSeriesAggregate> tsAggregateHolder = new Holder<>();
        plan.forEachDown(TimeSeriesAggregate.class, tsAggregateHolder::set);
        return tsAggregateHolder.get();
    }
}
