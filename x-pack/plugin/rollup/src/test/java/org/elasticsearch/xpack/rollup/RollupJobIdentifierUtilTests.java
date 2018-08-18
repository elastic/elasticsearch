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
import org.elasticsearch.xpack.core.rollup.action.RollupJobCaps;
import org.elasticsearch.xpack.core.rollup.job.DateHistogramGroupConfig;
import org.elasticsearch.xpack.core.rollup.job.GroupConfig;
import org.elasticsearch.xpack.core.rollup.job.HistogramGroupConfig;
import org.elasticsearch.xpack.core.rollup.job.MetricConfig;
import org.elasticsearch.xpack.core.rollup.job.RollupJobConfig;
import org.elasticsearch.xpack.core.rollup.job.TermsGroupConfig;
import org.joda.time.DateTimeZone;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.hamcrest.Matchers.equalTo;

public class RollupJobIdentifierUtilTests extends ESTestCase {

    public void testOneMatch() {
        final GroupConfig group = new GroupConfig(new DateHistogramGroupConfig("foo", new DateHistogramInterval("1h")));
        final RollupJobConfig job = new RollupJobConfig("foo", "index", "rollup", "*/5 * * * * ?", 10,  group, emptyList(), null);
        RollupJobCaps cap = new RollupJobCaps(job);
        Set<RollupJobCaps> caps = singletonSet(cap);

        DateHistogramAggregationBuilder builder = new DateHistogramAggregationBuilder("foo").field("foo")
                .dateHistogramInterval(job.getGroupConfig().getDateHistogram().getInterval());

        Set<RollupJobCaps> bestCaps = RollupJobIdentifierUtils.findBestJobs(builder, caps);
        assertThat(bestCaps.size(), equalTo(1));
    }

    public void testBiggerButCompatibleInterval() {
        final GroupConfig group = new GroupConfig(new DateHistogramGroupConfig("foo", new DateHistogramInterval("1h")));
        final RollupJobConfig job = new RollupJobConfig("foo", "index", "rollup", "*/5 * * * * ?", 10,  group, emptyList(), null);
        RollupJobCaps cap = new RollupJobCaps(job);
        Set<RollupJobCaps> caps = singletonSet(cap);

        DateHistogramAggregationBuilder builder = new DateHistogramAggregationBuilder("foo").field("foo")
                .dateHistogramInterval(new DateHistogramInterval("1d"));

        Set<RollupJobCaps> bestCaps = RollupJobIdentifierUtils.findBestJobs(builder, caps);
        assertThat(bestCaps.size(), equalTo(1));
    }

    public void testIncompatibleInterval() {
        final GroupConfig group = new GroupConfig(new DateHistogramGroupConfig("foo", new DateHistogramInterval("1d")));
        final RollupJobConfig job = new RollupJobConfig("foo", "index", "rollup", "*/5 * * * * ?", 10,  group, emptyList(), null);
        RollupJobCaps cap = new RollupJobCaps(job);
        Set<RollupJobCaps> caps = singletonSet(cap);

        DateHistogramAggregationBuilder builder = new DateHistogramAggregationBuilder("foo").field("foo")
                .dateHistogramInterval(new DateHistogramInterval("1h"));

        RuntimeException e = expectThrows(RuntimeException.class, () -> RollupJobIdentifierUtils.findBestJobs(builder, caps));
        assertThat(e.getMessage(), equalTo("There is not a rollup job that has a [date_histogram] agg on field " +
                "[foo] which also satisfies all requirements of query."));
    }

    public void testBadTimeZone() {
        final GroupConfig group = new GroupConfig(new DateHistogramGroupConfig("foo", new DateHistogramInterval("1h"), null, "EST"));
        final RollupJobConfig job = new RollupJobConfig("foo", "index", "rollup", "*/5 * * * * ?", 10,  group, emptyList(), null);
        RollupJobCaps cap = new RollupJobCaps(job);
        Set<RollupJobCaps> caps = singletonSet(cap);

        DateHistogramAggregationBuilder builder = new DateHistogramAggregationBuilder("foo").field("foo")
                .dateHistogramInterval(new DateHistogramInterval("1h"))
                .timeZone(DateTimeZone.UTC);

        RuntimeException e = expectThrows(RuntimeException.class, () -> RollupJobIdentifierUtils.findBestJobs(builder, caps));
        assertThat(e.getMessage(), equalTo("There is not a rollup job that has a [date_histogram] agg on field " +
                "[foo] which also satisfies all requirements of query."));
    }

    public void testMetricOnlyAgg() {
        final GroupConfig group = new GroupConfig(new DateHistogramGroupConfig("foo", new DateHistogramInterval("1h")));
        final List<MetricConfig> metrics = singletonList(new MetricConfig("bar", singletonList("max")));
        final RollupJobConfig job = new RollupJobConfig("foo", "index", "rollup", "*/5 * * * * ?", 10,  group, metrics, null);
        RollupJobCaps cap = new RollupJobCaps(job);
        Set<RollupJobCaps> caps = singletonSet(cap);

        MaxAggregationBuilder max = new MaxAggregationBuilder("the_max").field("bar");

        Set<RollupJobCaps> bestCaps = RollupJobIdentifierUtils.findBestJobs(max, caps);
        assertThat(bestCaps.size(), equalTo(1));
    }

    public void testOneOfTwoMatchingCaps() {
        final GroupConfig group = new GroupConfig(new DateHistogramGroupConfig("foo", new DateHistogramInterval("1h")));
        final RollupJobConfig job = new RollupJobConfig("foo", "index", "rollup", "*/5 * * * * ?", 10,  group, emptyList(), null);
        RollupJobCaps cap = new RollupJobCaps(job);
        Set<RollupJobCaps> caps = singletonSet(cap);

        DateHistogramAggregationBuilder builder = new DateHistogramAggregationBuilder("foo").field("foo")
                .dateHistogramInterval(new DateHistogramInterval("1h"))
                .subAggregation(new MaxAggregationBuilder("the_max").field("bar"));

        RuntimeException e = expectThrows(RuntimeException.class, () -> RollupJobIdentifierUtils.findBestJobs(builder, caps));
        assertThat(e.getMessage(), equalTo("There is not a rollup job that has a [max] agg with name [the_max] which also satisfies " +
                "all requirements of query."));
    }

    public void testTwoJobsSameRollupIndex() {
        final GroupConfig group = new GroupConfig(new DateHistogramGroupConfig("foo", new DateHistogramInterval("1h")));
        final RollupJobConfig job = new RollupJobConfig("foo", "index", "rollup", "*/5 * * * * ?", 10,  group, emptyList(), null);
        RollupJobCaps cap = new RollupJobCaps(job);
        Set<RollupJobCaps> caps = new HashSet<>(2);
        caps.add(cap);

        final GroupConfig group2 = new GroupConfig(new DateHistogramGroupConfig("foo", new DateHistogramInterval("1h")));
        final RollupJobConfig job2 =
            new RollupJobConfig("foo2", "index", job.getRollupIndex(), "*/5 * * * * ?", 10,  group2, emptyList(), null);
        RollupJobCaps cap2 = new RollupJobCaps(job2);
        caps.add(cap2);

        DateHistogramAggregationBuilder builder = new DateHistogramAggregationBuilder("foo").field("foo")
                .dateHistogramInterval(new DateHistogramInterval("1h"));

        Set<RollupJobCaps> bestCaps = RollupJobIdentifierUtils.findBestJobs(builder, caps);

        // Both jobs functionally identical, so only one is actually needed to be searched
        assertThat(bestCaps.size(), equalTo(1));
    }

    public void testTwoJobsButBothPartialMatches() {
        final GroupConfig group = new GroupConfig(new DateHistogramGroupConfig("foo", new DateHistogramInterval("1h")));
        final List<MetricConfig> metrics = singletonList(new MetricConfig("bar", singletonList("max")));
        final RollupJobConfig job = new RollupJobConfig("foo", "index", "rollup", "*/5 * * * * ?", 10,  group, metrics, null);
        RollupJobCaps cap = new RollupJobCaps(job);
        Set<RollupJobCaps> caps = new HashSet<>(2);
        caps.add(cap);

        // TODO Is it what we really want to test?
        final RollupJobConfig job2 = new RollupJobConfig("foo2", "index", "rollup", "*/5 * * * * ?", 10,  group, emptyList(), null);
        RollupJobCaps cap2 = new RollupJobCaps(job2);
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
        final GroupConfig group = new GroupConfig(new DateHistogramGroupConfig("foo", new DateHistogramInterval("1h")));
        final RollupJobConfig job = new RollupJobConfig("foo", "index", "rollup", "*/5 * * * * ?", 10,  group, emptyList(), null);
        RollupJobCaps cap = new RollupJobCaps(job);

        final GroupConfig group2 = new GroupConfig(new DateHistogramGroupConfig("foo", new DateHistogramInterval("1d")));
        final RollupJobConfig job2 =
            new RollupJobConfig("foo2", "index", job.getRollupIndex(), "*/5 * * * * ?", 10,  group2, emptyList(), null);
        RollupJobCaps cap2 = new RollupJobCaps(job2);

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
        final GroupConfig group = new GroupConfig(new DateHistogramGroupConfig("foo", new DateHistogramInterval("1h")));
        final RollupJobConfig job = new RollupJobConfig("foo", "index", "rollup", "*/5 * * * * ?", 10,  group, emptyList(), null);
        RollupJobCaps cap = new RollupJobCaps(job);

        final GroupConfig group2 = new GroupConfig(new DateHistogramGroupConfig("foo", new DateHistogramInterval("1d")));
        final RollupJobConfig job2 =
            new RollupJobConfig("foo2", "index", job.getRollupIndex(), "*/5 * * * * ?", 10,  group2, emptyList(), null);
        RollupJobCaps cap2 = new RollupJobCaps(job2);

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
        final GroupConfig group = new GroupConfig(new DateHistogramGroupConfig("foo", new DateHistogramInterval("1h")));
        final RollupJobConfig job = new RollupJobConfig("foo", "index", "rollup", "*/5 * * * * ?", 10,  group, emptyList(), null);
        RollupJobCaps cap = new RollupJobCaps(job);

        final HistogramGroupConfig histoConfig = new HistogramGroupConfig(100L, "bar");
        final GroupConfig group2 = new GroupConfig(new DateHistogramGroupConfig("foo", new DateHistogramInterval("1h")), histoConfig, null);
        final RollupJobConfig job2 =
            new RollupJobConfig("foo2", "index", job.getRollupIndex(), "*/5 * * * * ?", 10,  group2, emptyList(), null);
        RollupJobCaps cap2 = new RollupJobCaps(job2);

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
        final GroupConfig group = new GroupConfig(new DateHistogramGroupConfig("foo", new DateHistogramInterval("1h")));
        final RollupJobConfig job = new RollupJobConfig("foo", "index", "rollup", "*/5 * * * * ?", 10,  group, emptyList(), null);
        RollupJobCaps cap = new RollupJobCaps(job);

        final TermsGroupConfig termsConfig = new TermsGroupConfig("bar");
        final GroupConfig group2 = new GroupConfig(new DateHistogramGroupConfig("foo", new DateHistogramInterval("1h")), null, termsConfig);
        final RollupJobConfig job2 =
            new RollupJobConfig("foo2", "index", job.getRollupIndex(), "*/5 * * * * ?", 10,  group2, emptyList(), null);
        RollupJobCaps cap2 = new RollupJobCaps(job2);

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

        final GroupConfig group = new GroupConfig(
                    // NOTE same name but wrong type
                    new DateHistogramGroupConfig("foo", new DateHistogramInterval("1d"), null, DateTimeZone.UTC.getID()),
                    new HistogramGroupConfig(1L, "baz"), // <-- NOTE right type but wrong name
                    null
                );
        final List<MetricConfig> metrics =
            Arrays.asList(new MetricConfig("max_field", singletonList("max")), new MetricConfig("avg_field", singletonList("avg")));

        final RollupJobConfig job = new RollupJobConfig("foo", "index", "rollup", "*/5 * * * * ?", 10,  group, metrics, null);
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

        final GroupConfig group = new GroupConfig(
                    new DateHistogramGroupConfig("foo", new DateHistogramInterval("1d"), null, DateTimeZone.UTC.getID())
                );
        final List<MetricConfig> metrics =
                    Arrays.asList(new MetricConfig("max_field", singletonList("max")), new MetricConfig("avg_field", singletonList("avg")));

        final RollupJobConfig job = new RollupJobConfig("foo", "index", "rollup", "*/5 * * * * ?", 10,  group, metrics, null);
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

        final GroupConfig group = new GroupConfig(
                    // interval in job is much higher than agg interval above
                    new DateHistogramGroupConfig("foo", new DateHistogramInterval("100d"), null, DateTimeZone.UTC.getID())
                );
        final RollupJobConfig job = new RollupJobConfig("foo", "index", "rollup", "*/5 * * * * ?", 10,  group, emptyList(), null);
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

        final GroupConfig group = new GroupConfig(
                    // NOTE different field from the one in the query
                    new DateHistogramGroupConfig("bar", new DateHistogramInterval("1d"), null, DateTimeZone.UTC.getID())
                );
        final List<MetricConfig> metrics =
                    Arrays.asList(new MetricConfig("max_field", singletonList("max")), new MetricConfig("avg_field", singletonList("avg")));

        final RollupJobConfig job = new RollupJobConfig("foo", "index", "rollup", "*/5 * * * * ?", 10,  group, metrics, null);
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

        final GroupConfig group = new GroupConfig(
                    new DateHistogramGroupConfig("bar", new DateHistogramInterval("1d"), null, DateTimeZone.UTC.getID()),
                    new HistogramGroupConfig(1L, "baz"), // <-- NOTE right type but wrong name
                    null
                );
        final List<MetricConfig> metrics =
                    Arrays.asList(new MetricConfig("max_field", singletonList("max")), new MetricConfig("avg_field", singletonList("avg")));

        final RollupJobConfig job = new RollupJobConfig("foo", "index", "rollup", "*/5 * * * * ?", 10,  group, metrics, null);
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

        final GroupConfig group = new GroupConfig(
                    new DateHistogramGroupConfig("foo", new DateHistogramInterval("1d"), null, DateTimeZone.UTC.getID()),
                    new HistogramGroupConfig(1L, "baz"), // <-- NOTE right type but wrong name
                    null
                );
        final RollupJobConfig job = new RollupJobConfig("foo", "index", "rollup", "*/5 * * * * ?", 10,  group, emptyList(), null);
        Set<RollupJobCaps> caps = singletonSet(new RollupJobCaps(job));

        Exception e = expectThrows(RuntimeException.class,
                () -> RollupJobIdentifierUtils.findBestJobs(histo, caps));
        assertThat(e.getMessage(), equalTo("There is not a rollup job that has a [histogram] agg on field " +
                "[bar] which also satisfies all requirements of query."));
    }

    public void testMissingMetric() {
        int i = ESTestCase.randomIntBetween(0, 3);

        final GroupConfig group = new GroupConfig(new DateHistogramGroupConfig("foo", new DateHistogramInterval("1h")));
        final List<MetricConfig> metrics = singletonList(new MetricConfig("foo", Arrays.asList("avg", "max", "min", "sum")));
        final RollupJobConfig job = new RollupJobConfig("foo", "index", "rollup", "*/5 * * * * ?", 10,  group, emptyList(), null);
        Set<RollupJobCaps> caps = singletonSet(new RollupJobCaps(job));

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
