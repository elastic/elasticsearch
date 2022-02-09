/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.rollup;

import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.search.aggregations.bucket.histogram.HistogramAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.AvgAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.MaxAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.MinAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.SumAggregationBuilder;
import org.elasticsearch.search.aggregations.support.ValueType;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.rollup.RollupField;
import org.elasticsearch.xpack.core.rollup.action.RollupJobCaps;
import org.elasticsearch.xpack.core.rollup.job.DateHistogramGroupConfig;
import org.elasticsearch.xpack.core.rollup.job.GroupConfig;
import org.elasticsearch.xpack.core.rollup.job.HistogramGroupConfig;
import org.elasticsearch.xpack.core.rollup.job.MetricConfig;
import org.elasticsearch.xpack.core.rollup.job.RollupJobConfig;
import org.elasticsearch.xpack.core.rollup.job.TermsGroupConfig;

import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;

public class RollupJobIdentifierUtilTests extends ESTestCase {

    private static final List<String> UNITS = new ArrayList<>(DateHistogramAggregationBuilder.DATE_FIELD_UNITS.keySet());

    public void testOneMatch() {
        final GroupConfig group = new GroupConfig(new DateHistogramGroupConfig.CalendarInterval("foo", new DateHistogramInterval("1h")));
        final RollupJobConfig job = new RollupJobConfig("foo", "index", "rollup", "*/5 * * * * ?", 10, group, emptyList(), null);
        RollupJobCaps cap = new RollupJobCaps(job);
        Set<RollupJobCaps> caps = singletonSet(cap);

        DateHistogramAggregationBuilder builder = new DateHistogramAggregationBuilder("foo").field("foo")
            .calendarInterval(job.getGroupConfig().getDateHistogram().getInterval());

        Set<RollupJobCaps> bestCaps = RollupJobIdentifierUtils.findBestJobs(builder, caps);
        assertThat(bestCaps.size(), equalTo(1));
    }

    public void testBiggerButCompatibleInterval() {
        final GroupConfig group = new GroupConfig(new DateHistogramGroupConfig.CalendarInterval("foo", new DateHistogramInterval("1h")));
        final RollupJobConfig job = new RollupJobConfig("foo", "index", "rollup", "*/5 * * * * ?", 10, group, emptyList(), null);
        RollupJobCaps cap = new RollupJobCaps(job);
        Set<RollupJobCaps> caps = singletonSet(cap);

        DateHistogramAggregationBuilder builder = new DateHistogramAggregationBuilder("foo").field("foo")
            .calendarInterval(new DateHistogramInterval("1d"));

        Set<RollupJobCaps> bestCaps = RollupJobIdentifierUtils.findBestJobs(builder, caps);
        assertThat(bestCaps.size(), equalTo(1));
    }

    public void testBiggerButCompatibleFixedInterval() {
        final GroupConfig group = new GroupConfig(new DateHistogramGroupConfig.FixedInterval("foo", new DateHistogramInterval("100s")));
        final RollupJobConfig job = new RollupJobConfig("foo", "index", "rollup", "*/5 * * * * ?", 10, group, emptyList(), null);
        RollupJobCaps cap = new RollupJobCaps(job);
        Set<RollupJobCaps> caps = singletonSet(cap);

        DateHistogramAggregationBuilder builder = new DateHistogramAggregationBuilder("foo").field("foo")
            .fixedInterval(new DateHistogramInterval("1000s"));

        Set<RollupJobCaps> bestCaps = RollupJobIdentifierUtils.findBestJobs(builder, caps);
        assertThat(bestCaps.size(), equalTo(1));
    }

    public void testBiggerButCompatibleFixedIntervalInCalFormat() {
        final GroupConfig group = new GroupConfig(new DateHistogramGroupConfig.FixedInterval("foo", new DateHistogramInterval("1h")));
        final RollupJobConfig job = new RollupJobConfig("foo", "index", "rollup", "*/5 * * * * ?", 10, group, emptyList(), null);
        RollupJobCaps cap = new RollupJobCaps(job);
        Set<RollupJobCaps> caps = singletonSet(cap);

        DateHistogramAggregationBuilder builder = new DateHistogramAggregationBuilder("foo").field("foo")
            .fixedInterval(new DateHistogramInterval("7d"));

        Set<RollupJobCaps> bestCaps = RollupJobIdentifierUtils.findBestJobs(builder, caps);
        assertThat(bestCaps.size(), equalTo(1));
    }

    public void testBiggerButCompatibleFixedMillisInterval() {
        final GroupConfig group = new GroupConfig(new DateHistogramGroupConfig.FixedInterval("foo", new DateHistogramInterval("100ms")));
        final RollupJobConfig job = new RollupJobConfig("foo", "index", "rollup", "*/5 * * * * ?", 10, group, emptyList(), null);
        RollupJobCaps cap = new RollupJobCaps(job);
        Set<RollupJobCaps> caps = singletonSet(cap);

        DateHistogramAggregationBuilder builder = new DateHistogramAggregationBuilder("foo").field("foo")
            .fixedInterval(new DateHistogramInterval("1000ms"));

        Set<RollupJobCaps> bestCaps = RollupJobIdentifierUtils.findBestJobs(builder, caps);
        assertThat(bestCaps.size(), equalTo(1));
    }

    public void testIncompatibleInterval() {
        final GroupConfig group = new GroupConfig(new DateHistogramGroupConfig.CalendarInterval("foo", new DateHistogramInterval("1d")));
        final RollupJobConfig job = new RollupJobConfig("foo", "index", "rollup", "*/5 * * * * ?", 10, group, emptyList(), null);
        RollupJobCaps cap = new RollupJobCaps(job);
        Set<RollupJobCaps> caps = singletonSet(cap);

        DateHistogramAggregationBuilder builder = new DateHistogramAggregationBuilder("foo").field("foo")
            .calendarInterval(new DateHistogramInterval("1h"));

        RuntimeException e = expectThrows(RuntimeException.class, () -> RollupJobIdentifierUtils.findBestJobs(builder, caps));
        assertThat(
            e.getMessage(),
            equalTo(
                "There is not a rollup job that has a [date_histogram] agg on field "
                    + "[foo] which also satisfies all requirements of query."
            )
        );
    }

    public void testIncompatibleFixedCalendarInterval() {
        final GroupConfig group = new GroupConfig(new DateHistogramGroupConfig.FixedInterval("foo", new DateHistogramInterval("5d")));
        final RollupJobConfig job = new RollupJobConfig("foo", "index", "rollup", "*/5 * * * * ?", 10, group, emptyList(), null);
        RollupJobCaps cap = new RollupJobCaps(job);
        Set<RollupJobCaps> caps = singletonSet(cap);

        DateHistogramAggregationBuilder builder = new DateHistogramAggregationBuilder("foo").field("foo")
            .calendarInterval(new DateHistogramInterval("day"));

        RuntimeException e = expectThrows(RuntimeException.class, () -> RollupJobIdentifierUtils.findBestJobs(builder, caps));
        assertThat(
            e.getMessage(),
            equalTo(
                "There is not a rollup job that has a [date_histogram] agg on field "
                    + "[foo] which also satisfies all requirements of query."
            )
        );
    }

    public void testBadTimeZone() {
        final GroupConfig group = new GroupConfig(
            new DateHistogramGroupConfig.CalendarInterval("foo", new DateHistogramInterval("1h"), null, "CET")
        );
        final RollupJobConfig job = new RollupJobConfig("foo", "index", "rollup", "*/5 * * * * ?", 10, group, emptyList(), null);
        RollupJobCaps cap = new RollupJobCaps(job);
        Set<RollupJobCaps> caps = singletonSet(cap);

        DateHistogramAggregationBuilder builder = new DateHistogramAggregationBuilder("foo").field("foo")
            .calendarInterval(new DateHistogramInterval("1h"))
            .timeZone(ZoneOffset.UTC);

        RuntimeException e = expectThrows(RuntimeException.class, () -> RollupJobIdentifierUtils.findBestJobs(builder, caps));
        assertThat(
            e.getMessage(),
            equalTo(
                "There is not a rollup job that has a [date_histogram] agg on field "
                    + "[foo] which also satisfies all requirements of query."
            )
        );
    }

    public void testMetricOnlyAgg() {
        final GroupConfig group = new GroupConfig(new DateHistogramGroupConfig.CalendarInterval("foo", new DateHistogramInterval("1h")));
        final List<MetricConfig> metrics = singletonList(new MetricConfig("bar", singletonList("max")));
        final RollupJobConfig job = new RollupJobConfig("foo", "index", "rollup", "*/5 * * * * ?", 10, group, metrics, null);
        RollupJobCaps cap = new RollupJobCaps(job);
        Set<RollupJobCaps> caps = singletonSet(cap);

        MaxAggregationBuilder max = new MaxAggregationBuilder("the_max").field("bar");

        Set<RollupJobCaps> bestCaps = RollupJobIdentifierUtils.findBestJobs(max, caps);
        assertThat(bestCaps.size(), equalTo(1));
    }

    public void testOneOfTwoMatchingCaps() {
        final GroupConfig group = new GroupConfig(new DateHistogramGroupConfig.CalendarInterval("foo", new DateHistogramInterval("1h")));
        final RollupJobConfig job = new RollupJobConfig("foo", "index", "rollup", "*/5 * * * * ?", 10, group, emptyList(), null);
        RollupJobCaps cap = new RollupJobCaps(job);
        Set<RollupJobCaps> caps = singletonSet(cap);

        DateHistogramAggregationBuilder builder = new DateHistogramAggregationBuilder("foo").field("foo")
            .calendarInterval(new DateHistogramInterval("1h"))
            .subAggregation(new MaxAggregationBuilder("the_max").field("bar"));

        RuntimeException e = expectThrows(RuntimeException.class, () -> RollupJobIdentifierUtils.findBestJobs(builder, caps));
        assertThat(
            e.getMessage(),
            equalTo(
                "There is not a rollup job that has a [max] agg with name [the_max] which also satisfies " + "all requirements of query."
            )
        );
    }

    public void testTwoJobsSameRollupIndex() {
        final GroupConfig group = new GroupConfig(new DateHistogramGroupConfig.CalendarInterval("foo", new DateHistogramInterval("1h")));
        final RollupJobConfig job = new RollupJobConfig("foo", "index", "rollup", "*/5 * * * * ?", 10, group, emptyList(), null);
        RollupJobCaps cap = new RollupJobCaps(job);
        Set<RollupJobCaps> caps = new HashSet<>(2);
        caps.add(cap);

        final GroupConfig group2 = new GroupConfig(new DateHistogramGroupConfig.CalendarInterval("foo", new DateHistogramInterval("1h")));
        final RollupJobConfig job2 = new RollupJobConfig(
            "foo2",
            "index",
            job.getRollupIndex(),
            "*/5 * * * * ?",
            10,
            group2,
            emptyList(),
            null
        );
        RollupJobCaps cap2 = new RollupJobCaps(job2);
        caps.add(cap2);

        DateHistogramAggregationBuilder builder = new DateHistogramAggregationBuilder("foo").field("foo")
            .calendarInterval(new DateHistogramInterval("1h"));

        Set<RollupJobCaps> bestCaps = RollupJobIdentifierUtils.findBestJobs(builder, caps);

        // Both jobs functionally identical, so only one is actually needed to be searched
        assertThat(bestCaps.size(), equalTo(1));
    }

    public void testTwoJobsButBothPartialMatches() {
        final GroupConfig group = new GroupConfig(new DateHistogramGroupConfig.CalendarInterval("foo", new DateHistogramInterval("1h")));
        final List<MetricConfig> metrics = singletonList(new MetricConfig("bar", singletonList("max")));
        final RollupJobConfig job = new RollupJobConfig("foo", "index", "rollup", "*/5 * * * * ?", 10, group, metrics, null);
        RollupJobCaps cap = new RollupJobCaps(job);
        Set<RollupJobCaps> caps = new HashSet<>(2);
        caps.add(cap);

        // TODO Is it what we really want to test?
        final RollupJobConfig job2 = new RollupJobConfig("foo2", "index", "rollup", "*/5 * * * * ?", 10, group, emptyList(), null);
        RollupJobCaps cap2 = new RollupJobCaps(job2);
        caps.add(cap2);

        DateHistogramAggregationBuilder builder = new DateHistogramAggregationBuilder("foo").field("foo")
            .calendarInterval(new DateHistogramInterval("1h"))
            .subAggregation(new MaxAggregationBuilder("the_max").field("bar"))  // <-- comes from job1
            .subAggregation(new MinAggregationBuilder("the_min").field("bar")); // <-- comes from job2

        RuntimeException e = expectThrows(RuntimeException.class, () -> RollupJobIdentifierUtils.findBestJobs(builder, caps));
        assertThat(
            e.getMessage(),
            equalTo(
                "There is not a rollup job that has a [min] agg with name [the_min] which also " + "satisfies all requirements of query."
            )
        );
    }

    public void testComparableDifferentDateIntervals() {
        final GroupConfig group = new GroupConfig(new DateHistogramGroupConfig.CalendarInterval("foo", new DateHistogramInterval("1h")));
        final RollupJobConfig job = new RollupJobConfig("foo", "index", "rollup", "*/5 * * * * ?", 10, group, emptyList(), null);
        RollupJobCaps cap = new RollupJobCaps(job);

        final GroupConfig group2 = new GroupConfig(new DateHistogramGroupConfig.CalendarInterval("foo", new DateHistogramInterval("1d")));
        final RollupJobConfig job2 = new RollupJobConfig(
            "foo2",
            "index",
            job.getRollupIndex(),
            "*/5 * * * * ?",
            10,
            group2,
            emptyList(),
            null
        );
        RollupJobCaps cap2 = new RollupJobCaps(job2);

        DateHistogramAggregationBuilder builder = new DateHistogramAggregationBuilder("foo").field("foo")
            .calendarInterval(new DateHistogramInterval("1d"));

        Set<RollupJobCaps> caps = new HashSet<>(2);
        caps.add(cap);
        caps.add(cap2);
        Set<RollupJobCaps> bestCaps = RollupJobIdentifierUtils.findBestJobs(builder, caps);

        assertThat(bestCaps.size(), equalTo(1));
        assertTrue(bestCaps.contains(cap2));
    }

    public void testComparableDifferentDateIntervalsOnlyOneWorks() {
        final GroupConfig group = new GroupConfig(new DateHistogramGroupConfig.CalendarInterval("foo", new DateHistogramInterval("1h")));
        final RollupJobConfig job = new RollupJobConfig("foo", "index", "rollup", "*/5 * * * * ?", 10, group, emptyList(), null);
        RollupJobCaps cap = new RollupJobCaps(job);

        final GroupConfig group2 = new GroupConfig(new DateHistogramGroupConfig.CalendarInterval("foo", new DateHistogramInterval("1d")));
        final RollupJobConfig job2 = new RollupJobConfig(
            "foo2",
            "index",
            job.getRollupIndex(),
            "*/5 * * * * ?",
            10,
            group2,
            emptyList(),
            null
        );
        RollupJobCaps cap2 = new RollupJobCaps(job2);

        DateHistogramAggregationBuilder builder = new DateHistogramAggregationBuilder("foo").field("foo")
            .calendarInterval(new DateHistogramInterval("1h"));

        Set<RollupJobCaps> caps = new HashSet<>(2);
        caps.add(cap);
        caps.add(cap2);
        Set<RollupJobCaps> bestCaps = RollupJobIdentifierUtils.findBestJobs(builder, caps);

        assertThat(bestCaps.size(), equalTo(1));
        assertTrue(bestCaps.contains(cap));
    }

    public void testComparableNoHistoVsHisto() {
        final GroupConfig group = new GroupConfig(new DateHistogramGroupConfig.CalendarInterval("foo", new DateHistogramInterval("1h")));
        final RollupJobConfig job = new RollupJobConfig("foo", "index", "rollup", "*/5 * * * * ?", 10, group, emptyList(), null);
        RollupJobCaps cap = new RollupJobCaps(job);

        final HistogramGroupConfig histoConfig = new HistogramGroupConfig(100L, "bar");
        final GroupConfig group2 = new GroupConfig(
            new DateHistogramGroupConfig.CalendarInterval("foo", new DateHistogramInterval("1h")),
            histoConfig,
            null
        );
        final RollupJobConfig job2 = new RollupJobConfig(
            "foo2",
            "index",
            job.getRollupIndex(),
            "*/5 * * * * ?",
            10,
            group2,
            emptyList(),
            null
        );
        RollupJobCaps cap2 = new RollupJobCaps(job2);

        DateHistogramAggregationBuilder builder = new DateHistogramAggregationBuilder("foo").field("foo")
            .calendarInterval(new DateHistogramInterval("1h"))
            .subAggregation(new HistogramAggregationBuilder("histo").field("bar").interval(100));

        Set<RollupJobCaps> caps = new HashSet<>(2);
        caps.add(cap);
        caps.add(cap2);
        Set<RollupJobCaps> bestCaps = RollupJobIdentifierUtils.findBestJobs(builder, caps);

        assertThat(bestCaps.size(), equalTo(1));
        assertTrue(bestCaps.contains(cap2));
    }

    public void testComparableNoTermsVsTerms() {
        final GroupConfig group = new GroupConfig(new DateHistogramGroupConfig.CalendarInterval("foo", new DateHistogramInterval("1h")));
        final RollupJobConfig job = new RollupJobConfig("foo", "index", "rollup", "*/5 * * * * ?", 10, group, emptyList(), null);
        RollupJobCaps cap = new RollupJobCaps(job);

        final TermsGroupConfig termsConfig = new TermsGroupConfig("bar");
        final GroupConfig group2 = new GroupConfig(
            new DateHistogramGroupConfig.CalendarInterval("foo", new DateHistogramInterval("1h")),
            null,
            termsConfig
        );
        final RollupJobConfig job2 = new RollupJobConfig(
            "foo2",
            "index",
            job.getRollupIndex(),
            "*/5 * * * * ?",
            10,
            group2,
            emptyList(),
            null
        );
        RollupJobCaps cap2 = new RollupJobCaps(job2);

        DateHistogramAggregationBuilder builder = new DateHistogramAggregationBuilder("foo").field("foo")
            .calendarInterval(new DateHistogramInterval("1h"))
            .subAggregation(new TermsAggregationBuilder("histo").userValueTypeHint(ValueType.STRING).field("bar"));

        Set<RollupJobCaps> caps = new HashSet<>(2);
        caps.add(cap);
        caps.add(cap2);
        Set<RollupJobCaps> bestCaps = RollupJobIdentifierUtils.findBestJobs(builder, caps);

        assertThat(bestCaps.size(), equalTo(1));
        assertTrue(bestCaps.contains(cap2));
    }

    public void testHistoSameNameWrongTypeInCaps() {
        HistogramAggregationBuilder histo = new HistogramAggregationBuilder("test_histo");
        histo.field("foo")
            .interval(1L)
            .subAggregation(new MaxAggregationBuilder("the_max").field("max_field"))
            .subAggregation(new AvgAggregationBuilder("the_avg").field("avg_field"));

        final GroupConfig group = new GroupConfig(
            // NOTE same name but wrong type
            new DateHistogramGroupConfig.CalendarInterval("foo", new DateHistogramInterval("1d"), null, ZoneOffset.UTC.getId()),
            new HistogramGroupConfig(1L, "baz"), // <-- NOTE right type but wrong name
            null
        );
        final List<MetricConfig> metrics = Arrays.asList(
            new MetricConfig("max_field", singletonList("max")),
            new MetricConfig("avg_field", singletonList("avg"))
        );

        final RollupJobConfig job = new RollupJobConfig("foo", "index", "rollup", "*/5 * * * * ?", 10, group, metrics, null);
        Set<RollupJobCaps> caps = singletonSet(new RollupJobCaps(job));

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> RollupJobIdentifierUtils.findBestJobs(histo, caps));
        assertThat(
            e.getMessage(),
            equalTo(
                "There is not a rollup job that has a [histogram] " + "agg on field [foo] which also satisfies all requirements of query."
            )
        );
    }

    public void testMissingDateHisto() {
        DateHistogramAggregationBuilder histo = new DateHistogramAggregationBuilder("test_histo");
        histo.calendarInterval(new DateHistogramInterval("1d"))
            .field("other_field")
            .subAggregation(new MaxAggregationBuilder("the_max").field("max_field"))
            .subAggregation(new AvgAggregationBuilder("the_avg").field("avg_field"));

        final GroupConfig group = new GroupConfig(
            new DateHistogramGroupConfig.CalendarInterval("foo", new DateHistogramInterval("1d"), null, ZoneOffset.UTC.getId())
        );
        final List<MetricConfig> metrics = Arrays.asList(
            new MetricConfig("max_field", singletonList("max")),
            new MetricConfig("avg_field", singletonList("avg"))
        );

        final RollupJobConfig job = new RollupJobConfig("foo", "index", "rollup", "*/5 * * * * ?", 10, group, metrics, null);
        Set<RollupJobCaps> caps = singletonSet(new RollupJobCaps(job));

        Exception e = expectThrows(IllegalArgumentException.class, () -> RollupJobIdentifierUtils.findBestJobs(histo, caps));
        assertThat(
            e.getMessage(),
            equalTo(
                "There is not a rollup job that has a [date_histogram] agg on field "
                    + "[other_field] which also satisfies all requirements of query."
            )
        );
    }

    public void testNoMatchingInterval() {
        DateHistogramAggregationBuilder histo = new DateHistogramAggregationBuilder("test_histo");
        histo.fixedInterval(new DateHistogramInterval("1ms"))
            .field("foo")
            .subAggregation(new MaxAggregationBuilder("the_max").field("max_field"))
            .subAggregation(new AvgAggregationBuilder("the_avg").field("avg_field"));

        final GroupConfig group = new GroupConfig(
            // interval in job is much higher than agg interval above
            new DateHistogramGroupConfig.FixedInterval("foo", new DateHistogramInterval("100d"), null, ZoneOffset.UTC.getId())
        );
        final RollupJobConfig job = new RollupJobConfig("foo", "index", "rollup", "*/5 * * * * ?", 10, group, emptyList(), null);
        Set<RollupJobCaps> caps = singletonSet(new RollupJobCaps(job));

        Exception e = expectThrows(RuntimeException.class, () -> RollupJobIdentifierUtils.findBestJobs(histo, caps));
        assertThat(
            e.getMessage(),
            equalTo(
                "There is not a rollup job that has a [date_histogram] agg on field [foo] "
                    + "which also satisfies all requirements of query."
            )
        );
    }

    public void testDateHistoMissingFieldInCaps() {
        DateHistogramAggregationBuilder histo = new DateHistogramAggregationBuilder("test_histo");
        histo.calendarInterval(new DateHistogramInterval("1d"))
            .field("foo")
            .subAggregation(new MaxAggregationBuilder("the_max").field("max_field"))
            .subAggregation(new AvgAggregationBuilder("the_avg").field("avg_field"));

        final GroupConfig group = new GroupConfig(
            // NOTE different field from the one in the query
            new DateHistogramGroupConfig.CalendarInterval("bar", new DateHistogramInterval("1d"), null, ZoneOffset.UTC.getId())
        );
        final List<MetricConfig> metrics = Arrays.asList(
            new MetricConfig("max_field", singletonList("max")),
            new MetricConfig("avg_field", singletonList("avg"))
        );

        final RollupJobConfig job = new RollupJobConfig("foo", "index", "rollup", "*/5 * * * * ?", 10, group, metrics, null);
        Set<RollupJobCaps> caps = singletonSet(new RollupJobCaps(job));

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> RollupJobIdentifierUtils.findBestJobs(histo, caps));
        assertThat(
            e.getMessage(),
            equalTo(
                "There is not a rollup job that has a [date_histogram] agg on field [foo] which also "
                    + "satisfies all requirements of query."
            )
        );
    }

    public void testHistoMissingFieldInCaps() {
        HistogramAggregationBuilder histo = new HistogramAggregationBuilder("test_histo");
        histo.interval(1)
            .field("foo")
            .subAggregation(new MaxAggregationBuilder("the_max").field("max_field"))
            .subAggregation(new AvgAggregationBuilder("the_avg").field("avg_field"));

        final GroupConfig group = new GroupConfig(
            new DateHistogramGroupConfig.CalendarInterval("bar", new DateHistogramInterval("1d"), null, ZoneOffset.UTC.getId()),
            new HistogramGroupConfig(1L, "baz"), // <-- NOTE right type but wrong name
            null
        );
        final List<MetricConfig> metrics = Arrays.asList(
            new MetricConfig("max_field", singletonList("max")),
            new MetricConfig("avg_field", singletonList("avg"))
        );

        final RollupJobConfig job = new RollupJobConfig("foo", "index", "rollup", "*/5 * * * * ?", 10, group, metrics, null);
        Set<RollupJobCaps> caps = singletonSet(new RollupJobCaps(job));

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> RollupJobIdentifierUtils.findBestJobs(histo, caps));
        assertThat(
            e.getMessage(),
            equalTo(
                "There is not a rollup job that has a [histogram] agg on field [foo] which also " + "satisfies all requirements of query."
            )
        );
    }

    public void testNoMatchingHistoInterval() {
        HistogramAggregationBuilder histo = new HistogramAggregationBuilder("test_histo");
        histo.interval(1)
            .field("bar")
            .subAggregation(new MaxAggregationBuilder("the_max").field("max_field"))
            .subAggregation(new AvgAggregationBuilder("the_avg").field("avg_field"));

        final GroupConfig group = new GroupConfig(
            new DateHistogramGroupConfig.CalendarInterval("foo", new DateHistogramInterval("1d"), null, ZoneOffset.UTC.getId()),
            new HistogramGroupConfig(1L, "baz"), // <-- NOTE right type but wrong name
            null
        );
        final RollupJobConfig job = new RollupJobConfig("foo", "index", "rollup", "*/5 * * * * ?", 10, group, emptyList(), null);
        Set<RollupJobCaps> caps = singletonSet(new RollupJobCaps(job));

        Exception e = expectThrows(RuntimeException.class, () -> RollupJobIdentifierUtils.findBestJobs(histo, caps));
        assertThat(
            e.getMessage(),
            equalTo(
                "There is not a rollup job that has a [histogram] agg on field " + "[bar] which also satisfies all requirements of query."
            )
        );
    }

    public void testHistoIntervalNotMultiple() {
        HistogramAggregationBuilder histo = new HistogramAggregationBuilder("test_histo");
        histo.interval(10)  // <--- interval is not a multiple of 3
            .field("bar")
            .subAggregation(new MaxAggregationBuilder("the_max").field("max_field"))
            .subAggregation(new AvgAggregationBuilder("the_avg").field("avg_field"));

        final GroupConfig group = new GroupConfig(
            new DateHistogramGroupConfig.CalendarInterval("foo", new DateHistogramInterval("1d"), null, "UTC"),
            new HistogramGroupConfig(3L, "bar"),
            null
        );
        final RollupJobConfig job = new RollupJobConfig("foo", "index", "rollup", "*/5 * * * * ?", 10, group, emptyList(), null);
        RollupJobCaps cap = new RollupJobCaps(job);
        Set<RollupJobCaps> caps = singletonSet(cap);

        Exception e = expectThrows(RuntimeException.class, () -> RollupJobIdentifierUtils.findBestJobs(histo, caps));
        assertThat(
            e.getMessage(),
            equalTo(
                "There is not a rollup job that has a [histogram] agg on field " + "[bar] which also satisfies all requirements of query."
            )
        );
    }

    public void testMissingMetric() {
        int i = ESTestCase.randomIntBetween(0, 3);

        final GroupConfig group = new GroupConfig(new DateHistogramGroupConfig.CalendarInterval("foo", new DateHistogramInterval("1h")));
        final List<MetricConfig> metrics = singletonList(new MetricConfig("foo", Arrays.asList("avg", "max", "min", "sum")));
        final RollupJobConfig job = new RollupJobConfig("foo", "index", "rollup", "*/5 * * * * ?", 10, group, emptyList(), null);
        Set<RollupJobCaps> caps = singletonSet(new RollupJobCaps(job));

        String aggType;
        Exception e;
        if (i == 0) {
            e = expectThrows(
                IllegalArgumentException.class,
                () -> RollupJobIdentifierUtils.findBestJobs(new MaxAggregationBuilder("test_metric").field("other_field"), caps)
            );
            aggType = "max";
        } else if (i == 1) {
            e = expectThrows(
                IllegalArgumentException.class,
                () -> RollupJobIdentifierUtils.findBestJobs(new MinAggregationBuilder("test_metric").field("other_field"), caps)
            );
            aggType = "min";
        } else if (i == 2) {
            e = expectThrows(
                IllegalArgumentException.class,
                () -> RollupJobIdentifierUtils.findBestJobs(new SumAggregationBuilder("test_metric").field("other_field"), caps)
            );
            aggType = "sum";
        } else {
            e = expectThrows(
                IllegalArgumentException.class,
                () -> RollupJobIdentifierUtils.findBestJobs(new AvgAggregationBuilder("test_metric").field("other_field"), caps)
            );
            aggType = "avg";
        }
        assertThat(
            e.getMessage(),
            equalTo(
                "There is not a rollup job that has a ["
                    + aggType
                    + "] agg with name "
                    + "[test_metric] which also satisfies all requirements of query."
            )
        );

    }

    public void testValidateFixedInterval() {
        boolean valid = RollupJobIdentifierUtils.validateFixedInterval(
            new DateHistogramInterval("100ms"),
            new DateHistogramInterval("100ms")
        );
        assertTrue(valid);

        valid = RollupJobIdentifierUtils.validateFixedInterval(new DateHistogramInterval("200ms"), new DateHistogramInterval("100ms"));
        assertTrue(valid);

        valid = RollupJobIdentifierUtils.validateFixedInterval(new DateHistogramInterval("1000ms"), new DateHistogramInterval("200ms"));
        assertTrue(valid);

        valid = RollupJobIdentifierUtils.validateFixedInterval(new DateHistogramInterval("5m"), new DateHistogramInterval("5m"));
        assertTrue(valid);

        valid = RollupJobIdentifierUtils.validateFixedInterval(new DateHistogramInterval("20m"), new DateHistogramInterval("5m"));
        assertTrue(valid);

        valid = RollupJobIdentifierUtils.validateFixedInterval(new DateHistogramInterval("100ms"), new DateHistogramInterval("500ms"));
        assertFalse(valid);

        valid = RollupJobIdentifierUtils.validateFixedInterval(new DateHistogramInterval("100ms"), new DateHistogramInterval("5m"));
        assertFalse(valid);

        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> RollupJobIdentifierUtils.validateFixedInterval(new DateHistogramInterval("100ms"), new DateHistogramInterval("minute"))
        );
        assertThat(
            e.getMessage(),
            equalTo(
                "failed to parse setting [date_histo.config.interval] with value "
                    + "[minute] as a time value: unit is missing or unrecognized"
            )
        );

    }

    public void testValidateCalendarInterval() {
        boolean valid = RollupJobIdentifierUtils.validateCalendarInterval(
            new DateHistogramInterval("second"),
            new DateHistogramInterval("second")
        );
        assertTrue(valid);

        valid = RollupJobIdentifierUtils.validateCalendarInterval(new DateHistogramInterval("minute"), new DateHistogramInterval("second"));
        assertTrue(valid);

        valid = RollupJobIdentifierUtils.validateCalendarInterval(new DateHistogramInterval("month"), new DateHistogramInterval("day"));
        assertTrue(valid);

        valid = RollupJobIdentifierUtils.validateCalendarInterval(new DateHistogramInterval("1d"), new DateHistogramInterval("1s"));
        assertTrue(valid);

        valid = RollupJobIdentifierUtils.validateCalendarInterval(new DateHistogramInterval("second"), new DateHistogramInterval("minute"));
        assertFalse(valid);

        valid = RollupJobIdentifierUtils.validateCalendarInterval(new DateHistogramInterval("second"), new DateHistogramInterval("1m"));
        assertFalse(valid);

        // Fails because both are actually fixed
        valid = RollupJobIdentifierUtils.validateCalendarInterval(new DateHistogramInterval("100ms"), new DateHistogramInterval("100ms"));
        assertFalse(valid);
    }

    public void testComparatorMixed() {
        int numCaps = randomIntBetween(1, 10);
        List<RollupJobCaps> caps = new ArrayList<>(numCaps);

        for (int i = 0; i < numCaps; i++) {
            DateHistogramInterval interval;
            DateHistogramGroupConfig dateHistoConfig;
            if (randomBoolean()) {
                interval = getRandomCalendarInterval();
                dateHistoConfig = new DateHistogramGroupConfig.CalendarInterval("foo", interval);
            } else {
                interval = getRandomFixedInterval();
                dateHistoConfig = new DateHistogramGroupConfig.FixedInterval("foo", interval);
            }
            GroupConfig group = new GroupConfig(dateHistoConfig);
            RollupJobConfig job = new RollupJobConfig("foo", "index", "rollup", "*/5 * * * * ?", 10, group, emptyList(), null);
            RollupJobCaps cap = new RollupJobCaps(job);
            caps.add(cap);
        }

        caps.sort(RollupJobIdentifierUtils.COMPARATOR);

        // This only tests for calendar/fixed ordering, ignoring the other criteria
        for (int i = 1; i < numCaps; i++) {
            RollupJobCaps a = caps.get(i - 1);
            RollupJobCaps b = caps.get(i);
            long aMillis = getMillis(a);
            long bMillis = getMillis(b);

            assertThat(aMillis, greaterThanOrEqualTo(bMillis));

        }
    }

    public void testComparatorFixed() {
        int numCaps = randomIntBetween(1, 10);
        List<RollupJobCaps> caps = new ArrayList<>(numCaps);

        for (int i = 0; i < numCaps; i++) {
            DateHistogramInterval interval = getRandomFixedInterval();
            GroupConfig group = new GroupConfig(new DateHistogramGroupConfig.FixedInterval("foo", interval));
            RollupJobConfig job = new RollupJobConfig("foo", "index", "rollup", "*/5 * * * * ?", 10, group, emptyList(), null);
            RollupJobCaps cap = new RollupJobCaps(job);
            caps.add(cap);
        }

        caps.sort(RollupJobIdentifierUtils.COMPARATOR);

        // This only tests for fixed ordering, ignoring the other criteria
        for (int i = 1; i < numCaps; i++) {
            RollupJobCaps a = caps.get(i - 1);
            RollupJobCaps b = caps.get(i);
            long aMillis = getMillis(a);
            long bMillis = getMillis(b);

            assertThat(aMillis, greaterThanOrEqualTo(bMillis));

        }
    }

    public void testComparatorCalendar() {
        int numCaps = randomIntBetween(1, 10);
        List<RollupJobCaps> caps = new ArrayList<>(numCaps);

        for (int i = 0; i < numCaps; i++) {
            DateHistogramInterval interval = getRandomCalendarInterval();
            GroupConfig group = new GroupConfig(new DateHistogramGroupConfig.CalendarInterval("foo", interval));
            RollupJobConfig job = new RollupJobConfig("foo", "index", "rollup", "*/5 * * * * ?", 10, group, emptyList(), null);
            RollupJobCaps cap = new RollupJobCaps(job);
            caps.add(cap);
        }

        caps.sort(RollupJobIdentifierUtils.COMPARATOR);

        // This only tests for calendar ordering, ignoring the other criteria
        for (int i = 1; i < numCaps; i++) {
            RollupJobCaps a = caps.get(i - 1);
            RollupJobCaps b = caps.get(i);
            long aMillis = getMillis(a);
            long bMillis = getMillis(b);

            assertThat(aMillis, greaterThanOrEqualTo(bMillis));

        }
    }

    public void testObsoleteTimezone() {
        // Job has "obsolete" timezone
        DateHistogramGroupConfig dateHisto = new DateHistogramGroupConfig.CalendarInterval(
            "foo",
            new DateHistogramInterval("1h"),
            null,
            "Canada/Mountain"
        );
        GroupConfig group = new GroupConfig(dateHisto);
        RollupJobConfig job = new RollupJobConfig("foo", "index", "rollup", "*/5 * * * * ?", 10, group, emptyList(), null);
        RollupJobCaps cap = new RollupJobCaps(job);
        Set<RollupJobCaps> caps = singletonSet(cap);

        DateHistogramAggregationBuilder builder = new DateHistogramAggregationBuilder("foo").field("foo")
            .calendarInterval(job.getGroupConfig().getDateHistogram().getInterval())
            .timeZone(ZoneId.of("Canada/Mountain"));

        Set<RollupJobCaps> bestCaps = RollupJobIdentifierUtils.findBestJobs(builder, caps);
        assertThat(bestCaps.size(), equalTo(1));

        builder = new DateHistogramAggregationBuilder("foo").field("foo")
            .calendarInterval(job.getGroupConfig().getDateHistogram().getInterval())
            .timeZone(ZoneId.of("America/Edmonton"));

        bestCaps = RollupJobIdentifierUtils.findBestJobs(builder, caps);
        assertThat(bestCaps.size(), equalTo(1));

        // now the reverse, job has "new" timezone

        dateHisto = new DateHistogramGroupConfig.CalendarInterval("foo", new DateHistogramInterval("1h"), null, "America/Edmonton");
        group = new GroupConfig(dateHisto);
        job = new RollupJobConfig("foo", "index", "rollup", "*/5 * * * * ?", 10, group, emptyList(), null);
        cap = new RollupJobCaps(job);
        caps = singletonSet(cap);

        builder = new DateHistogramAggregationBuilder("foo").field("foo")
            .calendarInterval(job.getGroupConfig().getDateHistogram().getInterval())
            .timeZone(ZoneId.of("Canada/Mountain"));

        bestCaps = RollupJobIdentifierUtils.findBestJobs(builder, caps);
        assertThat(bestCaps.size(), equalTo(1));

        builder = new DateHistogramAggregationBuilder("foo").field("foo")
            .calendarInterval(job.getGroupConfig().getDateHistogram().getInterval())
            .timeZone(ZoneId.of("America/Edmonton"));

        bestCaps = RollupJobIdentifierUtils.findBestJobs(builder, caps);
        assertThat(bestCaps.size(), equalTo(1));
    }

    private static long getMillis(RollupJobCaps cap) {
        for (RollupJobCaps.RollupFieldCaps fieldCaps : cap.getFieldCaps().values()) {
            for (Map<String, Object> agg : fieldCaps.getAggs()) {
                if (agg.get(RollupField.AGG).equals(DateHistogramAggregationBuilder.NAME)) {
                    return new DateHistogramInterval(RollupJobIdentifierUtils.retrieveInterval(agg)).estimateMillis();
                }
            }
        }
        return Long.MAX_VALUE;
    }

    private static DateHistogramInterval getRandomFixedInterval() {
        int value = randomIntBetween(1, 1000);
        String unit;
        int randomValue = randomInt(4);
        if (randomValue == 0) {
            unit = "ms";
        } else if (randomValue == 1) {
            unit = "s";
        } else if (randomValue == 2) {
            unit = "m";
        } else if (randomValue == 3) {
            unit = "h";
        } else {
            unit = "d";
        }
        return new DateHistogramInterval(Integer.toString(value) + unit);
    }

    private static DateHistogramInterval getRandomCalendarInterval() {
        return new DateHistogramInterval(UNITS.get(randomIntBetween(0, UNITS.size() - 1)));
    }

    private Set<RollupJobCaps> singletonSet(RollupJobCaps cap) {
        Set<RollupJobCaps> caps = new HashSet<>();
        caps.add(cap);
        return caps;
    }
}
