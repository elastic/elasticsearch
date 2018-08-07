/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.rollup;

import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.search.aggregations.bucket.histogram.HistogramAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.avg.AvgAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.max.MaxAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.min.MinAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.sum.SumAggregationBuilder;
import org.elasticsearch.search.aggregations.support.ValueType;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.rollup.ConfigTestHelpers;
import org.elasticsearch.xpack.core.rollup.action.RollupJobCaps;
import org.elasticsearch.xpack.core.rollup.job.DateHistoGroupConfig;
import org.elasticsearch.xpack.core.rollup.job.GroupConfig;
import org.elasticsearch.xpack.core.rollup.job.HistoGroupConfig;
import org.elasticsearch.xpack.core.rollup.job.MetricConfig;
import org.elasticsearch.xpack.core.rollup.job.RollupJobConfig;
import org.elasticsearch.xpack.core.rollup.job.TermsGroupConfig;
import org.joda.time.DateTimeZone;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import static org.hamcrest.Matchers.equalTo;

public class RollupJobIdentifierUtilTests extends ESTestCase {

    public void testOneMatch() {
        RollupJobConfig.Builder job = ConfigTestHelpers.getRollupJob("foo");
        GroupConfig.Builder group = ConfigTestHelpers.getGroupConfig();
        group.setDateHisto(new DateHistoGroupConfig.Builder().setField("foo").setInterval(new DateHistogramInterval("1h")).build());
        job.setGroupConfig(group.build());
        RollupJobCaps cap = new RollupJobCaps(job.build());
        Set<RollupJobCaps> caps = singletonSet(cap);

        DateHistogramAggregationBuilder builder = new DateHistogramAggregationBuilder("foo").field("foo")
                .dateHistogramInterval(job.getGroupConfig().getDateHisto().getInterval());

        Set<RollupJobCaps> bestCaps = RollupJobIdentifierUtils.findBestJobs(builder, caps);
        assertThat(bestCaps.size(), equalTo(1));
    }

    public void testBiggerButCompatibleInterval() {
        RollupJobConfig.Builder job = ConfigTestHelpers.getRollupJob("foo");
        GroupConfig.Builder group = ConfigTestHelpers.getGroupConfig();
        group.setDateHisto(new DateHistoGroupConfig.Builder().setField("foo").setInterval(new DateHistogramInterval("1h")).build());
        job.setGroupConfig(group.build());
        RollupJobCaps cap = new RollupJobCaps(job.build());
        Set<RollupJobCaps> caps = singletonSet(cap);

        DateHistogramAggregationBuilder builder = new DateHistogramAggregationBuilder("foo").field("foo")
                .dateHistogramInterval(new DateHistogramInterval("1d"));

        Set<RollupJobCaps> bestCaps = RollupJobIdentifierUtils.findBestJobs(builder, caps);
        assertThat(bestCaps.size(), equalTo(1));
    }

    public void testIncompatibleInterval() {
        RollupJobConfig.Builder job = ConfigTestHelpers.getRollupJob("foo");
        GroupConfig.Builder group = ConfigTestHelpers.getGroupConfig();
        group.setDateHisto(new DateHistoGroupConfig.Builder().setField("foo").setInterval(new DateHistogramInterval("1d")).build());
        job.setGroupConfig(group.build());
        RollupJobCaps cap = new RollupJobCaps(job.build());
        Set<RollupJobCaps> caps = singletonSet(cap);

        DateHistogramAggregationBuilder builder = new DateHistogramAggregationBuilder("foo").field("foo")
                .dateHistogramInterval(new DateHistogramInterval("1h"));

        RuntimeException e = expectThrows(RuntimeException.class, () -> RollupJobIdentifierUtils.findBestJobs(builder, caps));
        assertThat(e.getMessage(), equalTo("There is not a rollup job that has a [date_histogram] agg on field " +
                "[foo] which also satisfies all requirements of query."));
    }

    public void testBadTimeZone() {
        RollupJobConfig.Builder job = ConfigTestHelpers.getRollupJob("foo");
        GroupConfig.Builder group = ConfigTestHelpers.getGroupConfig();
        group.setDateHisto(new DateHistoGroupConfig.Builder().setField("foo").setInterval(new DateHistogramInterval("1d"))
                .setTimeZone(DateTimeZone.forID("EST")).build());
        job.setGroupConfig(group.build());
        RollupJobCaps cap = new RollupJobCaps(job.build());
        Set<RollupJobCaps> caps = singletonSet(cap);

        DateHistogramAggregationBuilder builder = new DateHistogramAggregationBuilder("foo").field("foo")
                .dateHistogramInterval(new DateHistogramInterval("1h"))
                .timeZone(DateTimeZone.UTC);

        RuntimeException e = expectThrows(RuntimeException.class, () -> RollupJobIdentifierUtils.findBestJobs(builder, caps));
        assertThat(e.getMessage(), equalTo("There is not a rollup job that has a [date_histogram] agg on field " +
                "[foo] which also satisfies all requirements of query."));
    }

    public void testMetricOnlyAgg() {
        RollupJobConfig.Builder job = ConfigTestHelpers.getRollupJob("foo");
        GroupConfig.Builder group = ConfigTestHelpers.getGroupConfig();
        group.setDateHisto(new DateHistoGroupConfig.Builder().setField("foo").setInterval(new DateHistogramInterval("1h")).build());
        job.setGroupConfig(group.build());
        job.setMetricsConfig(Collections.singletonList(new MetricConfig.Builder()
                .setField("bar")
                .setMetrics(Collections.singletonList("max"))
            .build()));
        RollupJobCaps cap = new RollupJobCaps(job.build());
        Set<RollupJobCaps> caps = singletonSet(cap);

        MaxAggregationBuilder max = new MaxAggregationBuilder("the_max").field("bar");

        Set<RollupJobCaps> bestCaps = RollupJobIdentifierUtils.findBestJobs(max, caps);
        assertThat(bestCaps.size(), equalTo(1));
    }

    public void testOneOfTwoMatchingCaps() {
        RollupJobConfig.Builder job = ConfigTestHelpers.getRollupJob("foo");
        GroupConfig.Builder group = ConfigTestHelpers.getGroupConfig();
        group.setDateHisto(new DateHistoGroupConfig.Builder().setField("foo").setInterval(new DateHistogramInterval("1h")).build());
        job.setGroupConfig(group.build());
        RollupJobCaps cap = new RollupJobCaps(job.build());
        Set<RollupJobCaps> caps = singletonSet(cap);

        DateHistogramAggregationBuilder builder = new DateHistogramAggregationBuilder("foo").field("foo")
                .dateHistogramInterval(new DateHistogramInterval("1h"))
                .subAggregation(new MaxAggregationBuilder("the_max").field("bar"));

        RuntimeException e = expectThrows(RuntimeException.class, () -> RollupJobIdentifierUtils.findBestJobs(builder, caps));
        assertThat(e.getMessage(), equalTo("There is not a rollup job that has a [max] agg with name [the_max] which also satisfies " +
                "all requirements of query."));
    }

    public void testTwoJobsSameRollupIndex() {
        RollupJobConfig.Builder job = ConfigTestHelpers.getRollupJob("foo");
        GroupConfig.Builder group = ConfigTestHelpers.getGroupConfig();
        group.setDateHisto(new DateHistoGroupConfig.Builder().setField("foo").setInterval(new DateHistogramInterval("1h")).build());
        group.setTerms(null);
        group.setHisto(null);
        job.setGroupConfig(group.build());
        RollupJobCaps cap = new RollupJobCaps(job.build());
        Set<RollupJobCaps> caps = new HashSet<>(2);
        caps.add(cap);

        RollupJobConfig.Builder job2 = ConfigTestHelpers.getRollupJob("foo2");
        GroupConfig.Builder group2 = ConfigTestHelpers.getGroupConfig();
        group2.setDateHisto(new DateHistoGroupConfig.Builder().setField("foo").setInterval(new DateHistogramInterval("1h")).build());
        group2.setTerms(null);
        group2.setHisto(null);
        job2.setGroupConfig(group.build());
        job2.setRollupIndex(job.getRollupIndex());
        RollupJobCaps cap2 = new RollupJobCaps(job2.build());
        caps.add(cap2);

        DateHistogramAggregationBuilder builder = new DateHistogramAggregationBuilder("foo").field("foo")
                .dateHistogramInterval(new DateHistogramInterval("1h"));

        Set<RollupJobCaps> bestCaps = RollupJobIdentifierUtils.findBestJobs(builder, caps);

        // Both jobs functionally identical, so only one is actually needed to be searched
        assertThat(bestCaps.size(), equalTo(1));
    }

    public void testTwoJobsButBothPartialMatches() {
        RollupJobConfig.Builder job = ConfigTestHelpers.getRollupJob("foo");
        GroupConfig.Builder group = ConfigTestHelpers.getGroupConfig();
        group.setDateHisto(new DateHistoGroupConfig.Builder().setField("foo").setInterval(new DateHistogramInterval("1h")).build());
        job.setGroupConfig(group.build());
        job.setMetricsConfig(Collections.singletonList(new MetricConfig.Builder()
                .setField("bar")
                .setMetrics(Collections.singletonList("max"))
                .build()));
        RollupJobCaps cap = new RollupJobCaps(job.build());
        Set<RollupJobCaps> caps = new HashSet<>(2);
        caps.add(cap);

        RollupJobConfig.Builder job2 = ConfigTestHelpers.getRollupJob("foo2");
        GroupConfig.Builder group2 = ConfigTestHelpers.getGroupConfig();
        group2.setDateHisto(new DateHistoGroupConfig.Builder().setField("foo").setInterval(new DateHistogramInterval("1h")).build());
        job2.setGroupConfig(group.build());
        job.setMetricsConfig(Collections.singletonList(new MetricConfig.Builder()
                .setField("bar")
                .setMetrics(Collections.singletonList("min"))
                .build()));
        RollupJobCaps cap2 = new RollupJobCaps(job2.build());
        caps.add(cap2);

        DateHistogramAggregationBuilder builder = new DateHistogramAggregationBuilder("foo").field("foo")
                .dateHistogramInterval(new DateHistogramInterval("1h"))
                .subAggregation(new MaxAggregationBuilder("the_max").field("bar"))  // <-- comes from job1
                .subAggregation(new MinAggregationBuilder("the_min").field("bar")); // <-- comes from job2

        RuntimeException e = expectThrows(RuntimeException.class, () -> RollupJobIdentifierUtils.findBestJobs(builder, caps));
        assertThat(e.getMessage(), equalTo("There is not a rollup job that has a [min] agg with name [the_min] which also " +
                "satisfies all requirements of query."));
    }

    public void testComparableDifferentDateIntervals() {
        RollupJobConfig.Builder job = ConfigTestHelpers.getRollupJob("foo");
        GroupConfig.Builder group = ConfigTestHelpers.getGroupConfig();
        group.setDateHisto(new DateHistoGroupConfig.Builder().setField("foo").setInterval(new DateHistogramInterval("1h")).build())
                .setHisto(null)
                .setTerms(null);
        job.setGroupConfig(group.build());
        RollupJobCaps cap = new RollupJobCaps(job.build());

        RollupJobConfig.Builder job2 = ConfigTestHelpers.getRollupJob("foo2").setRollupIndex(job.getRollupIndex());
        GroupConfig.Builder group2 = ConfigTestHelpers.getGroupConfig();
        group2.setDateHisto(new DateHistoGroupConfig.Builder().setField("foo").setInterval(new DateHistogramInterval("1d")).build())
                .setHisto(null)
                .setTerms(null);
        job2.setGroupConfig(group2.build());
        RollupJobCaps cap2 = new RollupJobCaps(job2.build());

        DateHistogramAggregationBuilder builder = new DateHistogramAggregationBuilder("foo").field("foo")
                .dateHistogramInterval(new DateHistogramInterval("1d"));

        Set<RollupJobCaps> caps = new HashSet<>(2);
        caps.add(cap);
        caps.add(cap2);
        Set<RollupJobCaps> bestCaps = RollupJobIdentifierUtils.findBestJobs(builder, caps);

        assertThat(bestCaps.size(), equalTo(1));
        assertTrue(bestCaps.contains(cap2));
    }

    public void testComparableDifferentDateIntervalsOnlyOneWorks() {
        RollupJobConfig.Builder job = ConfigTestHelpers.getRollupJob("foo");
        GroupConfig.Builder group = ConfigTestHelpers.getGroupConfig();
        group.setDateHisto(new DateHistoGroupConfig.Builder().setField("foo").setInterval(new DateHistogramInterval("1h")).build())
                .setHisto(null)
                .setTerms(null);
        job.setGroupConfig(group.build());
        RollupJobCaps cap = new RollupJobCaps(job.build());

        RollupJobConfig.Builder job2 = ConfigTestHelpers.getRollupJob("foo2").setRollupIndex(job.getRollupIndex());
        GroupConfig.Builder group2 = ConfigTestHelpers.getGroupConfig();
        group2.setDateHisto(new DateHistoGroupConfig.Builder().setField("foo").setInterval(new DateHistogramInterval("1d")).build())
                .setHisto(null)
                .setTerms(null);
        job2.setGroupConfig(group2.build());
        RollupJobCaps cap2 = new RollupJobCaps(job2.build());

        DateHistogramAggregationBuilder builder = new DateHistogramAggregationBuilder("foo").field("foo")
                .dateHistogramInterval(new DateHistogramInterval("1h"));

        Set<RollupJobCaps> caps = new HashSet<>(2);
        caps.add(cap);
        caps.add(cap2);
        Set<RollupJobCaps> bestCaps = RollupJobIdentifierUtils.findBestJobs(builder, caps);

        assertThat(bestCaps.size(), equalTo(1));
        assertTrue(bestCaps.contains(cap));
    }

    public void testComparableNoHistoVsHisto() {
        RollupJobConfig.Builder job = ConfigTestHelpers.getRollupJob("foo");
        GroupConfig.Builder group = ConfigTestHelpers.getGroupConfig();
        group.setDateHisto(new DateHistoGroupConfig.Builder().setField("foo").setInterval(new DateHistogramInterval("1h")).build())
                .setHisto(null)
                .setTerms(null);
        job.setGroupConfig(group.build());
        RollupJobCaps cap = new RollupJobCaps(job.build());

        RollupJobConfig.Builder job2 = ConfigTestHelpers.getRollupJob("foo2").setRollupIndex(job.getRollupIndex());
        GroupConfig.Builder group2 = ConfigTestHelpers.getGroupConfig();
        group2.setDateHisto(new DateHistoGroupConfig.Builder().setField("foo").setInterval(new DateHistogramInterval("1h")).build())
                .setHisto(ConfigTestHelpers.getHisto().setInterval(100).setFields(Collections.singletonList("bar")).build())
                .setTerms(null);
        job2.setGroupConfig(group2.build());
        RollupJobCaps cap2 = new RollupJobCaps(job2.build());

        DateHistogramAggregationBuilder builder = new DateHistogramAggregationBuilder("foo").field("foo")
                .dateHistogramInterval(new DateHistogramInterval("1h"))
                .subAggregation(new HistogramAggregationBuilder("histo").field("bar").interval(100));

        Set<RollupJobCaps> caps = new HashSet<>(2);
        caps.add(cap);
        caps.add(cap2);
        Set<RollupJobCaps> bestCaps = RollupJobIdentifierUtils.findBestJobs(builder, caps);

        assertThat(bestCaps.size(), equalTo(1));
        assertTrue(bestCaps.contains(cap2));
    }

    public void testComparableNoTermsVsTerms() {
        RollupJobConfig.Builder job = ConfigTestHelpers.getRollupJob("foo");
        GroupConfig.Builder group = ConfigTestHelpers.getGroupConfig();
        group.setDateHisto(new DateHistoGroupConfig.Builder().setField("foo").setInterval(new DateHistogramInterval("1h")).build())
                .setHisto(null)
                .setTerms(null);
        job.setGroupConfig(group.build());
        RollupJobCaps cap = new RollupJobCaps(job.build());

        RollupJobConfig.Builder job2 = ConfigTestHelpers.getRollupJob("foo2").setRollupIndex(job.getRollupIndex());
        GroupConfig.Builder group2 = ConfigTestHelpers.getGroupConfig();
        group2.setDateHisto(new DateHistoGroupConfig.Builder().setField("foo").setInterval(new DateHistogramInterval("1h")).build())
                .setHisto(null)
            .setTerms(new TermsGroupConfig("bar"));
        job2.setGroupConfig(group2.build());
        RollupJobCaps cap2 = new RollupJobCaps(job2.build());

        DateHistogramAggregationBuilder builder = new DateHistogramAggregationBuilder("foo").field("foo")
                .dateHistogramInterval(new DateHistogramInterval("1h"))
                .subAggregation(new TermsAggregationBuilder("histo", ValueType.STRING).field("bar"));

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

        RollupJobConfig job = ConfigTestHelpers.getRollupJob("foo")
                .setGroupConfig(ConfigTestHelpers.getGroupConfig()
                        .setDateHisto(new DateHistoGroupConfig.Builder()
                                .setInterval(new DateHistogramInterval("1d"))
                                .setField("foo") // <-- NOTE same name but wrong type
                                .setTimeZone(DateTimeZone.UTC)
                                .build())
                        .setHisto(new HistoGroupConfig.Builder()
                                .setFields(Collections.singletonList("baz")) // <-- NOTE right type but wrong name
                                .setInterval(1L)
                                .build())
                        .build())
                .setMetricsConfig(Arrays.asList(new MetricConfig.Builder()
                                .setField("max_field")
                                .setMetrics(Collections.singletonList("max")).build(),
                        new MetricConfig.Builder()
                                .setField("avg_field")
                                .setMetrics(Collections.singletonList("avg")).build()))
                .build();
        Set<RollupJobCaps> caps = singletonSet(new RollupJobCaps(job));

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> RollupJobIdentifierUtils.findBestJobs(histo, caps));
        assertThat(e.getMessage(), equalTo("There is not a rollup job that has a [histogram] " +
                "agg on field [foo] which also satisfies all requirements of query."));
    }

    public void testMissingDateHisto() {
        DateHistogramAggregationBuilder histo = new DateHistogramAggregationBuilder("test_histo");
        histo.dateHistogramInterval(new DateHistogramInterval("1d"))
                .field("other_field")
                .subAggregation(new MaxAggregationBuilder("the_max").field("max_field"))
                .subAggregation(new AvgAggregationBuilder("the_avg").field("avg_field"));

        RollupJobConfig job = ConfigTestHelpers.getRollupJob("foo")
                .setGroupConfig(ConfigTestHelpers.getGroupConfig()
                        .setDateHisto(new DateHistoGroupConfig.Builder()
                                .setInterval(new DateHistogramInterval("1d"))
                                .setField("foo")
                                .setTimeZone(DateTimeZone.UTC)
                                .build())
                        .build())
                .setMetricsConfig(Arrays.asList(new MetricConfig.Builder()
                                .setField("max_field")
                                .setMetrics(Collections.singletonList("max")).build(),
                        new MetricConfig.Builder()
                                .setField("avg_field")
                                .setMetrics(Collections.singletonList("avg")).build()))
                .build();
        Set<RollupJobCaps> caps = singletonSet(new RollupJobCaps(job));

        Exception e = expectThrows(IllegalArgumentException.class, () -> RollupJobIdentifierUtils.findBestJobs(histo,caps));
        assertThat(e.getMessage(), equalTo("There is not a rollup job that has a [date_histogram] agg on field " +
                "[other_field] which also satisfies all requirements of query."));
    }

    public void testNoMatchingInterval() {
        DateHistogramAggregationBuilder histo = new DateHistogramAggregationBuilder("test_histo");
        histo.interval(1)
                .field("foo")
                .subAggregation(new MaxAggregationBuilder("the_max").field("max_field"))
                .subAggregation(new AvgAggregationBuilder("the_avg").field("avg_field"));

        RollupJobConfig job = ConfigTestHelpers.getRollupJob("foo")
                .setGroupConfig(ConfigTestHelpers.getGroupConfig()
                        .setDateHisto(new DateHistoGroupConfig.Builder()
                                .setInterval(new DateHistogramInterval("100d")) // <- interval in job is much higher than agg interval above
                                .setField("foo")
                                .setTimeZone(DateTimeZone.UTC)
                                .build())
                        .build())
                .build();
        Set<RollupJobCaps> caps = singletonSet(new RollupJobCaps(job));

        Exception e = expectThrows(RuntimeException.class, () -> RollupJobIdentifierUtils.findBestJobs(histo, caps));
        assertThat(e.getMessage(), equalTo("There is not a rollup job that has a [date_histogram] agg on field [foo] " +
                "which also satisfies all requirements of query."));
    }

    public void testDateHistoMissingFieldInCaps() {
        DateHistogramAggregationBuilder histo = new DateHistogramAggregationBuilder("test_histo");
        histo.dateHistogramInterval(new DateHistogramInterval("1d"))
                .field("foo")
                .subAggregation(new MaxAggregationBuilder("the_max").field("max_field"))
                .subAggregation(new AvgAggregationBuilder("the_avg").field("avg_field"));

        RollupJobConfig job = ConfigTestHelpers.getRollupJob("foo")
                .setGroupConfig(ConfigTestHelpers.getGroupConfig()
                        .setDateHisto(new DateHistoGroupConfig.Builder()
                                .setInterval(new DateHistogramInterval("1d"))
                                .setField("bar") // <-- NOTE different field from the one in the query
                                .setTimeZone(DateTimeZone.UTC)
                                .build())
                        .build())
                .setMetricsConfig(Arrays.asList(new MetricConfig.Builder()
                                .setField("max_field")
                                .setMetrics(Collections.singletonList("max")).build(),
                        new MetricConfig.Builder()
                                .setField("avg_field")
                                .setMetrics(Collections.singletonList("avg")).build()))
                .build();
        Set<RollupJobCaps> caps = singletonSet(new RollupJobCaps(job));

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> RollupJobIdentifierUtils.findBestJobs(histo, caps));
        assertThat(e.getMessage(), equalTo("There is not a rollup job that has a [date_histogram] agg on field [foo] which also " +
                "satisfies all requirements of query."));
    }

    public void testHistoMissingFieldInCaps() {
        HistogramAggregationBuilder histo = new HistogramAggregationBuilder("test_histo");
        histo.interval(1)
                .field("foo")
                .subAggregation(new MaxAggregationBuilder("the_max").field("max_field"))
                .subAggregation(new AvgAggregationBuilder("the_avg").field("avg_field"));

        RollupJobConfig job = ConfigTestHelpers.getRollupJob("foo")
                .setGroupConfig(ConfigTestHelpers.getGroupConfig()
                        .setDateHisto(new DateHistoGroupConfig.Builder()
                                .setInterval(new DateHistogramInterval("1d"))
                                .setField("bar")
                                .setTimeZone(DateTimeZone.UTC)
                                .build())
                        .setHisto(new HistoGroupConfig.Builder()
                                .setFields(Collections.singletonList("baz")) // <-- NOTE note different field from one used in query
                                .setInterval(1L)
                                .build())
                        .build())
                .setMetricsConfig(Arrays.asList(new MetricConfig.Builder()
                                .setField("max_field")
                                .setMetrics(Collections.singletonList("max")).build(),
                        new MetricConfig.Builder()
                                .setField("avg_field")
                                .setMetrics(Collections.singletonList("avg")).build()))
                .build();
        Set<RollupJobCaps> caps = singletonSet(new RollupJobCaps(job));

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> RollupJobIdentifierUtils.findBestJobs(histo, caps));
        assertThat(e.getMessage(), equalTo("There is not a rollup job that has a [histogram] agg on field [foo] which also " +
                "satisfies all requirements of query."));
    }

    public void testNoMatchingHistoInterval() {
        HistogramAggregationBuilder histo = new HistogramAggregationBuilder("test_histo");
        histo.interval(1)
                .field("bar")
                .subAggregation(new MaxAggregationBuilder("the_max").field("max_field"))
                .subAggregation(new AvgAggregationBuilder("the_avg").field("avg_field"));

        RollupJobConfig job = ConfigTestHelpers.getRollupJob("foo")
                .setGroupConfig(ConfigTestHelpers.getGroupConfig()
                        .setDateHisto(new DateHistoGroupConfig.Builder()
                                .setInterval(new DateHistogramInterval("1d"))
                                .setField("foo")
                                .setTimeZone(DateTimeZone.UTC)
                                .build())
                        .setHisto(new HistoGroupConfig.Builder()
                                .setFields(Collections.singletonList("bar"))
                                .setInterval(100L) // <--- interval in job is much higher than agg interval above
                                .build())
                        .build())
                .build();
        Set<RollupJobCaps> caps = singletonSet(new RollupJobCaps(job));

        Exception e = expectThrows(RuntimeException.class,
                () -> RollupJobIdentifierUtils.findBestJobs(histo, caps));
        assertThat(e.getMessage(), equalTo("There is not a rollup job that has a [histogram] agg on field " +
                "[bar] which also satisfies all requirements of query."));
    }

    public void testMissingMetric() {
        int i = ESTestCase.randomIntBetween(0, 3);

        Set<RollupJobCaps> caps = singletonSet(new RollupJobCaps(ConfigTestHelpers
                .getRollupJob("foo").setMetricsConfig(Collections.singletonList(new MetricConfig.Builder()
                        .setField("foo")
                        .setMetrics(Arrays.asList("avg", "max", "min", "sum")).build()))
                .build()));

        String aggType;
        Exception e;
        if (i == 0) {
            e = expectThrows(IllegalArgumentException.class,
                    () -> RollupJobIdentifierUtils.findBestJobs(new MaxAggregationBuilder("test_metric").field("other_field"), caps));
            aggType = "max";
        } else if (i == 1) {
            e = expectThrows(IllegalArgumentException.class,
                    () -> RollupJobIdentifierUtils.findBestJobs(new MinAggregationBuilder("test_metric").field("other_field"), caps));
            aggType = "min";
        } else if (i == 2) {
            e = expectThrows(IllegalArgumentException.class,
                    () -> RollupJobIdentifierUtils.findBestJobs(new SumAggregationBuilder("test_metric").field("other_field"), caps));
            aggType = "sum";
        } else {
            e = expectThrows(IllegalArgumentException.class,
                    () -> RollupJobIdentifierUtils.findBestJobs(new AvgAggregationBuilder("test_metric").field("other_field"),  caps));
            aggType = "avg";
        }
        assertThat(e.getMessage(), equalTo("There is not a rollup job that has a [" + aggType + "] agg with name " +
                "[test_metric] which also satisfies all requirements of query."));

    }

    private Set<RollupJobCaps> singletonSet(RollupJobCaps cap) {
        Set<RollupJobCaps> caps = new HashSet<>();
        caps.add(cap);
        return caps;
    }
}
