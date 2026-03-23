/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical;

import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.MetadataAttribute;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.util.Holder;
import org.elasticsearch.xpack.esql.optimizer.AbstractLogicalPlanOptimizerTests;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.TimeSeriesAggregate;

import java.util.List;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;

public class TimeSeriesBareAggregationsTests extends AbstractLogicalPlanOptimizerTests {

    protected LogicalPlan planK8s(String query) {
        return analyzerWithEnrichPolicies().addK8s().plans(query).coordinatorLogicalOptimized();
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
        boolean hasTimeseries = output.stream().anyMatch(MetadataAttribute::isTimeSeriesAttribute);
        assertThat("Should have _timeseries in output", hasTimeseries, is(true));

        Attribute timeseriesAttr = output.stream().filter(MetadataAttribute::isTimeSeriesAttribute).findFirst().orElse(null);

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
        boolean hasTimeseries = output.stream().anyMatch(MetadataAttribute::isTimeSeriesAttribute);
        assertThat("Should have _timeseries in output", hasTimeseries, is(true));

        Attribute timeseriesAttr = output.stream().filter(MetadataAttribute::isTimeSeriesAttribute).findFirst().orElse(null);

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
        boolean hasTimeseries = output.stream().anyMatch(MetadataAttribute::isTimeSeriesAttribute);
        assertThat("Should have _timeseries in output", hasTimeseries, is(true));

        Attribute timeseriesAttr = output.stream().filter(MetadataAttribute::isTimeSeriesAttribute).findFirst().orElse(null);

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
        boolean hasTimeseries = output.stream().anyMatch(MetadataAttribute::isTimeSeriesAttribute);
        assertThat("Should have _timeseries in output", hasTimeseries, is(true));

        Attribute timeseriesAttr = output.stream().filter(MetadataAttribute::isTimeSeriesAttribute).findFirst().orElse(null);

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
        boolean hasTimeseries = output.stream().anyMatch(MetadataAttribute::isTimeSeriesAttribute);
        assertThat("Should have _timeseries in output", hasTimeseries, is(true));

        Attribute timeseriesAttr = output.stream().filter(MetadataAttribute::isTimeSeriesAttribute).findFirst().orElse(null);

        assertNotNull(timeseriesAttr);
        assertThat("_timeseries attribute should exist", timeseriesAttr, is(instanceOf(Attribute.class)));
        assertThat("_timeseries should be KEYWORD type", timeseriesAttr.dataType(), is(DataType.KEYWORD));
    }

    public void testMixedBareOverTimeAndRegularAggregates() {
        assumeTrue("requires metrics command", EsqlCapabilities.Cap.METRICS_GROUP_BY_ALL.isEnabled());

        var error = expectThrows(IllegalArgumentException.class, () -> planK8s("""
            TS k8s
            | STATS avg_over_time(network.cost), sum(network.total_bytes_in)
            """));

        assertThat(error.getMessage(), equalTo("""
            Cannot mix time-series aggregate [avg_over_time(network.cost)] and \
            regular aggregate [sum(network.total_bytes_in)] in the same TimeSeriesAggregate."""));
    }

    public void testGroupingKeyInAggregatesListPreserved() {
        assumeTrue("requires metrics command", EsqlCapabilities.Cap.METRICS_GROUP_BY_ALL.isEnabled());

        var error = expectThrows(IllegalArgumentException.class, () -> planK8s("""
            TS k8s
            | STATS rate(network.total_bytes_out) BY region, TBUCKET(1hour)
            """));

        assertThat(
            error.getMessage(),
            equalTo(
                "Only grouping functions are supported (e.g. tbucket) when the time series aggregation function "
                    + "[rate(network.total_bytes_out)] is not wrapped with another aggregation function. Found [region]."
            )
        );
    }

    public void testBucketWithRenamedTimestampThrowsError() {
        assumeTrue("requires metrics command", EsqlCapabilities.Cap.METRICS_GROUP_BY_ALL.isEnabled());

        var error = expectThrows(IllegalArgumentException.class, () -> planK8s("""
            TS k8s
            | EVAL renamed_ts = @timestamp
            | STATS min = min(last_over_time(network.total_bytes_out)) BY bucket = bucket(renamed_ts, 1hour)
            """));

        assertThat(
            error.getMessage(),
            equalTo(
                "Time-series aggregations require direct use of @timestamp which was not found. "
                    + "If @timestamp was renamed in EVAL, use the original @timestamp field instead."
            )
        );
    }

    public void testAliasedGroupingInTsStatsKeepsAliasName() {
        assumeTrue("requires metrics command", EsqlCapabilities.Cap.METRICS_GROUP_BY_ALL.isEnabled());

        LogicalPlan plan = planK8s("""
            TS k8s
            | STATS max_bytes = max(to_long(network.total_bytes_in)) BY foobar = cluster
            """);

        assertThat(plan.output().stream().map(Attribute::name).toList(), equalTo(List.of("max_bytes", "foobar")));
    }

    public void testAliasedGroupingInTsStatsCanBeUsedInKeep() {
        assumeTrue("requires metrics command", EsqlCapabilities.Cap.METRICS_GROUP_BY_ALL.isEnabled());

        LogicalPlan plan = planK8s("""
            TS k8s
            | STATS max_bytes = max(to_long(network.total_bytes_in)) BY foobar = cluster
            | KEEP max_bytes, foobar
            """);

        assertThat(plan.output().stream().map(Attribute::name).toList(), equalTo(List.of("max_bytes", "foobar")));
    }

    private TimeSeriesAggregate findTimeSeriesAggregate(LogicalPlan plan) {
        Holder<TimeSeriesAggregate> tsAggregateHolder = new Holder<>();
        plan.forEachDown(TimeSeriesAggregate.class, tsAggregateHolder::set);
        return tsAggregateHolder.get();
    }
}
