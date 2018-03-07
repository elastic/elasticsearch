/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.rollup;


import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.search.aggregations.bucket.histogram.ExtendedBounds;
import org.elasticsearch.search.aggregations.bucket.histogram.HistogramAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.range.GeoDistanceAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.avg.AvgAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.max.MaxAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.min.MinAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.sum.SumAggregationBuilder;
import org.elasticsearch.search.aggregations.support.ValueType;
import org.elasticsearch.search.aggregations.support.ValuesSourceAggregationBuilder;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.rollup.ConfigTestHelpers;
import org.elasticsearch.xpack.core.rollup.action.RollupJobCaps;
import org.elasticsearch.xpack.core.rollup.job.DateHistoGroupConfig;
import org.elasticsearch.xpack.core.rollup.job.HistoGroupConfig;
import org.elasticsearch.xpack.core.rollup.job.MetricConfig;
import org.elasticsearch.xpack.core.rollup.job.RollupJobConfig;
import org.elasticsearch.xpack.core.rollup.job.TermsGroupConfig;
import org.hamcrest.Matchers;
import org.joda.time.DateTimeZone;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import static java.util.Collections.emptyList;
import static org.elasticsearch.xpack.rollup.RollupRequestTranslator.translateAggregation;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.core.IsInstanceOf.instanceOf;

public class RollupRequestTranslationTests extends ESTestCase {

    private NamedWriteableRegistry namedWriteableRegistry;

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        SearchModule searchModule = new SearchModule(Settings.EMPTY, false, emptyList());
        List<NamedWriteableRegistry.Entry> entries = new ArrayList<>();
        entries.addAll(searchModule.getNamedWriteables());
        namedWriteableRegistry = new NamedWriteableRegistry(entries);
    }

    public void testBasicDateHisto() {
        // TODO grab some of the logic from DateHistogramTests to build more robust tests
        DateHistogramAggregationBuilder histo = new DateHistogramAggregationBuilder("test_histo");
        histo.dateHistogramInterval(new DateHistogramInterval("1d"))
                .field("foo")
                .extendedBounds(new ExtendedBounds(0L, 1000L))
                .subAggregation(new MaxAggregationBuilder("the_max").field("max_field"))
                .subAggregation(new AvgAggregationBuilder("the_avg").field("avg_field"));
        List<QueryBuilder> filterConditions = new ArrayList<>();

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
        List<RollupJobCaps> caps = Collections.singletonList(new RollupJobCaps(job));

        List<AggregationBuilder> translated = translateAggregation(histo, filterConditions, namedWriteableRegistry, caps);
        assertThat(translated.size(), equalTo(1));
        assertThat(translated.get(0), Matchers.instanceOf(DateHistogramAggregationBuilder.class));
        DateHistogramAggregationBuilder translatedHisto = (DateHistogramAggregationBuilder)translated.get(0);

        assertThat(translatedHisto.dateHistogramInterval(), equalTo(new DateHistogramInterval("1d")));
        assertThat(translatedHisto.field(), equalTo("foo.date_histogram.timestamp"));
        assertThat(translatedHisto.getSubAggregations().size(), equalTo(4));

        Map<String, AggregationBuilder> subAggs = translatedHisto.getSubAggregations()
                .stream().collect(Collectors.toMap(AggregationBuilder::getName, Function.identity()));

        assertThat(subAggs.get("the_max"), Matchers.instanceOf(MaxAggregationBuilder.class));
        assertThat(((MaxAggregationBuilder)subAggs.get("the_max")).field(), equalTo("max_field.max.value"));

        assertThat(subAggs.get("the_avg"), Matchers.instanceOf(SumAggregationBuilder.class));
        SumAggregationBuilder avg = (SumAggregationBuilder)subAggs.get("the_avg");
        assertThat(avg.field(), equalTo("avg_field.avg.value"));

        assertThat(subAggs.get("the_avg._count"), Matchers.instanceOf(SumAggregationBuilder.class));
        assertThat(((SumAggregationBuilder)subAggs.get("the_avg._count")).field(),
                equalTo("avg_field.avg._count"));

        assertThat(subAggs.get("test_histo._count"), Matchers.instanceOf(SumAggregationBuilder.class));
        assertThat(((SumAggregationBuilder)subAggs.get("test_histo._count")).field(),
                equalTo("foo.date_histogram._count"));

        assertThat(filterConditions.size(), equalTo(4));
        for (QueryBuilder q : filterConditions) {
            if (q instanceof TermQueryBuilder) {
                switch (((TermQueryBuilder) q).fieldName()) {
                    case "foo.date_histogram.interval":
                        assertThat(((TermQueryBuilder) q).value().toString(), equalTo(new DateHistogramInterval("1d").toString()));
                        break;
                    case "foo.date_histogram.time_zone":
                        assertThat(((TermQueryBuilder) q).value(), equalTo("UTC"));
                        break;
                    case "_rollup.computed":
                        assertThat(((TermQueryBuilder) q).value(), equalTo("foo.date_histogram"));
                        break;
                    case "_rollup.id":
                        assertThat(((TermQueryBuilder) q).value(), equalTo("foo"));
                        break;
                    default:
                        fail("Unexpected Term Query in filter conditions: [" + ((TermQueryBuilder) q).fieldName() + "]");
                        break;
                }
            } else {
                fail("Unexpected query builder in filter conditions");
            }
        }

    }

    public void testSimpleMetric() {
        int i = ESTestCase.randomIntBetween(0, 2);
        List<AggregationBuilder> translated = new ArrayList<>();
        List<QueryBuilder> filterConditions = new ArrayList<>();

        Class clazz = null;
        String fieldName = null;
        int numAggs = 1;

        List<RollupJobCaps> caps = Collections.singletonList(new RollupJobCaps(ConfigTestHelpers
                .getRollupJob("foo").setMetricsConfig(Collections.singletonList(new MetricConfig.Builder()
                        .setField("foo")
                        .setMetrics(Arrays.asList("avg", "max", "min", "sum")).build()))
                .build()));
        if (i == 0) {
            translated = translateAggregation(new MaxAggregationBuilder("test_metric")
                    .field("foo"), filterConditions, namedWriteableRegistry, caps);
            clazz = MaxAggregationBuilder.class;
            fieldName =  "foo.max.value";
        } else if (i == 1) {
            translated = translateAggregation(new MinAggregationBuilder("test_metric")
                    .field("foo"), filterConditions, namedWriteableRegistry, caps);
            clazz = MinAggregationBuilder.class;
            fieldName =  "foo.min.value";
        } else if (i == 2) {
            translated = translateAggregation(new SumAggregationBuilder("test_metric")
                    .field("foo"), filterConditions, namedWriteableRegistry, caps);
            clazz = SumAggregationBuilder.class;
            fieldName =  "foo.sum.value";
        }

        assertThat(translated.size(), equalTo(numAggs));
        assertThat(translated.get(0), Matchers.instanceOf(clazz));
        assertThat((translated.get(0)).getName(), equalTo("test_metric"));
        assertThat(((ValuesSourceAggregationBuilder)translated.get(0)).field(), equalTo(fieldName));

        assertThat(filterConditions.size(), equalTo(0));
    }

    public void testMissingMetric() {
        int i = ESTestCase.randomIntBetween(0, 3);
        List<QueryBuilder> filterConditions = new ArrayList<>();

        List<RollupJobCaps> caps = Collections.singletonList(new RollupJobCaps(ConfigTestHelpers
                .getRollupJob("foo").setMetricsConfig(Collections.singletonList(new MetricConfig.Builder()
                        .setField("foo")
                        .setMetrics(Arrays.asList("avg", "max", "min", "sum")).build()))
                .build()));

        String aggType;
        Exception e;
        if (i == 0) {
            e = expectThrows(IllegalArgumentException.class, () -> translateAggregation(new MaxAggregationBuilder("test_metric")
                    .field("other_field"), filterConditions, namedWriteableRegistry, caps));
            aggType = "max";
        } else if (i == 1) {
            e = expectThrows(IllegalArgumentException.class, () -> translateAggregation(new MinAggregationBuilder("test_metric")
                    .field("other_field"), filterConditions, namedWriteableRegistry, caps));
            aggType = "min";
        } else if (i == 2) {
            e = expectThrows(IllegalArgumentException.class, () -> translateAggregation(new SumAggregationBuilder("test_metric")
                    .field("other_field"), filterConditions, namedWriteableRegistry, caps));
            aggType = "sum";
        } else {
            e = expectThrows(IllegalArgumentException.class, () -> translateAggregation(new AvgAggregationBuilder("test_metric")
                    .field("other_field"), filterConditions, namedWriteableRegistry, caps));
            aggType = "avg";
        }
        assertThat(e.getMessage(), equalTo("There is not a [" + aggType + "] agg with name " +
                "[other_field] configured in selected rollup indices, cannot translate aggregation."));

    }

    public void testMissingDateHisto() {
        DateHistogramAggregationBuilder histo = new DateHistogramAggregationBuilder("test_histo");
        histo.dateHistogramInterval(new DateHistogramInterval("1d"))
                .field("other_field")
                .subAggregation(new MaxAggregationBuilder("the_max").field("max_field"))
                .subAggregation(new AvgAggregationBuilder("the_avg").field("avg_field"));
        List<QueryBuilder> filterConditions = new ArrayList<>();

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
        List<RollupJobCaps> caps = Collections.singletonList(new RollupJobCaps(job));

        Exception e = expectThrows(IllegalArgumentException.class,
                () -> translateAggregation(histo, filterConditions, namedWriteableRegistry, caps));
        assertThat(e.getMessage(), equalTo("There is not a [date_histogram] agg with name " +
                "[other_field] configured in selected rollup indices, cannot translate aggregation."));
    }

    public void testSelectLowerGranularityDateInterval() {
        DateHistogramAggregationBuilder histo = new DateHistogramAggregationBuilder("test_histo");
        histo.dateHistogramInterval(new DateHistogramInterval("1d"))
                .field("foo")
                .subAggregation(new MaxAggregationBuilder("the_max").field("max_field"))
                .subAggregation(new AvgAggregationBuilder("the_avg").field("avg_field"));
        List<QueryBuilder> filterConditions = new ArrayList<>();

        RollupJobConfig job1 = ConfigTestHelpers.getRollupJob("foo")
                .setGroupConfig(ConfigTestHelpers.getGroupConfig()
                        .setDateHisto(new DateHistoGroupConfig.Builder()
                                .setInterval(new DateHistogramInterval("1h"))
                                .setField("foo")
                                .setTimeZone(DateTimeZone.UTC)
                                .build())
                        .build())
                .setMetricsConfig(Arrays.asList(new MetricConfig.Builder()
                                .setField("max_field")
                                .setMetrics(Collections.singletonList("max"))
                                .build(),
                        new MetricConfig.Builder()
                                .setField("avg_field")
                                .setMetrics(Collections.singletonList("avg"))
                                .build())
                )
                .build();

        RollupJobConfig job2 = ConfigTestHelpers.getRollupJob("foo")
                .setGroupConfig(ConfigTestHelpers.getGroupConfig()
                        .setDateHisto(new DateHistoGroupConfig.Builder()
                                .setInterval(new DateHistogramInterval("1d"))
                                .setField("foo")
                                .setTimeZone(DateTimeZone.UTC)
                                .build())
                        .build())
                .setMetricsConfig(Arrays.asList(new MetricConfig.Builder()
                                .setField("max_field")
                                .setMetrics(Collections.singletonList("max"))
                                .build(),
                        new MetricConfig.Builder()
                                .setField("avg_field")
                                .setMetrics(Collections.singletonList("avg"))
                                .build())
                )
                .build();
        List<RollupJobCaps> caps = new ArrayList<>(2);
        caps.add(new RollupJobCaps(job1));
        caps.add(new RollupJobCaps(job2));

        List<AggregationBuilder> translated = translateAggregation(histo, filterConditions, namedWriteableRegistry, caps);
        assertThat(translated.size(), equalTo(1));
        assertThat(translated.get(0), instanceOf(DateHistogramAggregationBuilder.class));
        DateHistogramAggregationBuilder translatedHisto = (DateHistogramAggregationBuilder)translated.get(0);

        assertThat(translatedHisto.dateHistogramInterval().toString(), equalTo("1d"));
        assertThat(translatedHisto.field(), equalTo("foo.date_histogram.timestamp"));
        assertThat(translatedHisto.getSubAggregations().size(), equalTo(4));

        Map<String, AggregationBuilder> subAggs = translatedHisto.getSubAggregations()
                .stream().collect(Collectors.toMap(AggregationBuilder::getName, Function.identity()));

        assertThat(subAggs.get("the_max"), instanceOf(MaxAggregationBuilder.class));
        assertThat(((MaxAggregationBuilder)subAggs.get("the_max")).field(), equalTo("max_field.max.value"));

        assertThat(subAggs.get("the_avg"), instanceOf(SumAggregationBuilder.class));
        SumAggregationBuilder avg = (SumAggregationBuilder)subAggs.get("the_avg");
        assertThat(avg.field(), equalTo("avg_field.avg.value"));

        assertThat(subAggs.get("the_avg._count"), instanceOf(SumAggregationBuilder.class));
        assertThat(((SumAggregationBuilder)subAggs.get("the_avg._count")).field(),
                equalTo("avg_field.avg._count"));

        assertThat(subAggs.get("test_histo._count"), instanceOf(SumAggregationBuilder.class));
        assertThat(((SumAggregationBuilder)subAggs.get("test_histo._count")).field(),
                equalTo("foo.date_histogram._count"));

        assertThat(filterConditions.size(), equalTo(4));

        for (QueryBuilder q : filterConditions) {
            if (q instanceof TermQueryBuilder) {
                if (((TermQueryBuilder) q).fieldName().equals("foo.date_histogram.interval")) {
                    assertThat(((TermQueryBuilder) q).value().toString(), equalTo("1h"));  // <---- should be instead of 1d
                } else if (((TermQueryBuilder) q).fieldName().equals("foo.date_histogram.time_zone")) {
                    assertThat(((TermQueryBuilder) q).value(), equalTo("UTC"));
                } else if (((TermQueryBuilder) q).fieldName().equals("_rollup.computed")) {
                    assertThat(((TermQueryBuilder) q).value(), equalTo("foo.date_histogram"));
                } else if (((TermQueryBuilder) q).fieldName().equals("_rollup.id")) {
                    assertThat(((TermQueryBuilder) q).value(), equalTo("foo"));
                } else {
                    fail("Unexpected Term Query in filter conditions: [" + ((TermQueryBuilder) q).fieldName() + "]");
                }
            } else {
                fail("Unexpected query builder in filter conditions");
            }
        }
    }

    public void testSelectLowerGranularityInteravl() {
        DateHistogramAggregationBuilder histo = new DateHistogramAggregationBuilder("test_histo");
        histo.interval(3600000)
                .field("foo")
                .subAggregation(new MaxAggregationBuilder("the_max").field("max_field"))
                .subAggregation(new AvgAggregationBuilder("the_avg").field("avg_field"));
        List<QueryBuilder> filterConditions = new ArrayList<>();

        RollupJobConfig job1 = ConfigTestHelpers.getRollupJob("foo")
                .setGroupConfig(ConfigTestHelpers.getGroupConfig()
                        .setDateHisto(new DateHistoGroupConfig.Builder()
                                .setInterval(new DateHistogramInterval("1h"))
                                .setField("foo")
                                .setTimeZone(DateTimeZone.UTC)
                                .build())
                        .build())
                .setMetricsConfig(Arrays.asList(new MetricConfig.Builder()
                                .setField("max_field")
                                .setMetrics(Collections.singletonList("max"))
                                .build(),
                        new MetricConfig.Builder()
                                .setField("avg_field")
                                .setMetrics(Collections.singletonList("avg"))
                                .build())
                )
                .build();

        RollupJobConfig job2 = ConfigTestHelpers.getRollupJob("foo")
                .setGroupConfig(ConfigTestHelpers.getGroupConfig()
                        .setDateHisto(new DateHistoGroupConfig.Builder()
                                .setInterval(new DateHistogramInterval("1d"))
                                .setField("foo")
                                .setTimeZone(DateTimeZone.UTC)
                                .build())
                        .build())
                .setMetricsConfig(Arrays.asList(new MetricConfig.Builder()
                                .setField("max_field")
                                .setMetrics(Collections.singletonList("max"))
                                .build(),
                        new MetricConfig.Builder()
                                .setField("avg_field")
                                .setMetrics(Collections.singletonList("avg"))
                                .build())
                )
                .build();
        List<RollupJobCaps> caps = new ArrayList<>(2);
        caps.add(new RollupJobCaps(job1));
        caps.add(new RollupJobCaps(job2));

        List<AggregationBuilder> translated = translateAggregation(histo, filterConditions, namedWriteableRegistry, caps);
        assertThat(translated.size(), equalTo(1));
        assertThat(translated.get(0), instanceOf(DateHistogramAggregationBuilder.class));
        DateHistogramAggregationBuilder translatedHisto = (DateHistogramAggregationBuilder)translated.get(0);

        assertThat(translatedHisto.interval(), equalTo(3600000L));
        assertThat(translatedHisto.field(), equalTo("foo.date_histogram.timestamp"));
        assertThat(translatedHisto.getSubAggregations().size(), equalTo(4));

        Map<String, AggregationBuilder> subAggs = translatedHisto.getSubAggregations()
                .stream().collect(Collectors.toMap(AggregationBuilder::getName, Function.identity()));

        assertThat(subAggs.get("the_max"), instanceOf(MaxAggregationBuilder.class));
        assertThat(((MaxAggregationBuilder)subAggs.get("the_max")).field(), equalTo("max_field.max.value"));

        assertThat(subAggs.get("the_avg"), instanceOf(SumAggregationBuilder.class));
        SumAggregationBuilder avg = (SumAggregationBuilder)subAggs.get("the_avg");
        assertThat(avg.field(), equalTo("avg_field.avg.value"));

        assertThat(subAggs.get("the_avg._count"), instanceOf(SumAggregationBuilder.class));
        assertThat(((SumAggregationBuilder)subAggs.get("the_avg._count")).field(),
                equalTo("avg_field.avg._count"));

        assertThat(subAggs.get("test_histo._count"), instanceOf(SumAggregationBuilder.class));
        assertThat(((SumAggregationBuilder)subAggs.get("test_histo._count")).field(),
                equalTo("foo.date_histogram._count"));

        assertThat(filterConditions.size(), equalTo(4));

        for (QueryBuilder q : filterConditions) {
            if (q instanceof TermQueryBuilder) {
                if (((TermQueryBuilder) q).fieldName().equals("foo.date_histogram.interval")) {
                    assertThat(((TermQueryBuilder) q).value().toString(), equalTo("1h"));  // <---- should be instead of 1d
                } else if (((TermQueryBuilder) q).fieldName().equals("foo.date_histogram.time_zone")) {
                    assertThat(((TermQueryBuilder) q).value(), equalTo("UTC"));
                } else if (((TermQueryBuilder) q).fieldName().equals("_rollup.computed")) {
                    assertThat(((TermQueryBuilder) q).value(), equalTo("foo.date_histogram"));
                } else if (((TermQueryBuilder) q).fieldName().equals("_rollup.id")) {
                    assertThat(((TermQueryBuilder) q).value(), equalTo("foo"));
                } else {
                    fail("Unexpected Term Query in filter conditions: [" + ((TermQueryBuilder) q).fieldName() + "]");
                }
            } else {
                fail("Unexpected query builder in filter conditions");
            }
        }
    }

    public void testNoMatchingDateInterval() {
        DateHistogramAggregationBuilder histo = new DateHistogramAggregationBuilder("test_histo");
        histo.dateHistogramInterval(new DateHistogramInterval("1d"))
                .field("foo")
                .subAggregation(new MaxAggregationBuilder("the_max").field("max_field"))
                .subAggregation(new AvgAggregationBuilder("the_avg").field("avg_field"));
        List<QueryBuilder> filterConditions = new ArrayList<>();

        RollupJobConfig job = ConfigTestHelpers.getRollupJob("foo")
                .setGroupConfig(ConfigTestHelpers.getGroupConfig()
                        .setDateHisto(new DateHistoGroupConfig.Builder()
                                .setInterval(new DateHistogramInterval("100d")) // <- interval in job is much higher than agg interval above
                                .setField("foo")
                                .setTimeZone(DateTimeZone.UTC)
                                .build())
                        .build())
                .build();
        List<RollupJobCaps> caps = Collections.singletonList(new RollupJobCaps(job));

        Exception e = expectThrows(RuntimeException.class,
                () -> translateAggregation(histo, filterConditions, namedWriteableRegistry, caps));
        assertThat(e.getMessage(), equalTo("Could not find a rolled date_histogram configuration that satisfies the interval [1d]"));
    }

    public void testNoMatchingInterval() {
        DateHistogramAggregationBuilder histo = new DateHistogramAggregationBuilder("test_histo");
        histo.interval(1)
                .field("foo")
                .subAggregation(new MaxAggregationBuilder("the_max").field("max_field"))
                .subAggregation(new AvgAggregationBuilder("the_avg").field("avg_field"));
        List<QueryBuilder> filterConditions = new ArrayList<>();

        RollupJobConfig job = ConfigTestHelpers.getRollupJob("foo")
                .setGroupConfig(ConfigTestHelpers.getGroupConfig()
                        .setDateHisto(new DateHistoGroupConfig.Builder()
                                .setInterval(new DateHistogramInterval("100d")) // <- interval in job is much higher than agg interval above
                                .setField("foo")
                                .setTimeZone(DateTimeZone.UTC)
                                .build())
                        .build())
                .build();
        List<RollupJobCaps> caps = Collections.singletonList(new RollupJobCaps(job));

        Exception e = expectThrows(RuntimeException.class,
                () -> translateAggregation(histo, filterConditions, namedWriteableRegistry, caps));
        assertThat(e.getMessage(), equalTo("Could not find a rolled date_histogram configuration that satisfies the interval [1]"));
    }

    public void testAvgMetric() {
        List<QueryBuilder> filterConditions = new ArrayList<>();
        List<RollupJobCaps> caps = Collections.singletonList(new RollupJobCaps(ConfigTestHelpers
                .getRollupJob("foo").setMetricsConfig(Collections.singletonList(new MetricConfig.Builder()
                        .setField("foo")
                        .setMetrics(Collections.singletonList("avg")).build()))
                .build()));
        List<AggregationBuilder> translated = translateAggregation(new AvgAggregationBuilder("test_metric")
                .field("foo"), filterConditions, namedWriteableRegistry, caps);

        assertThat(translated.size(), equalTo(2));
        Map<String, AggregationBuilder> metrics = translated.stream()
                .collect(Collectors.toMap(AggregationBuilder::getName, Function.identity()));

        assertThat(metrics.get("test_metric"), Matchers.instanceOf(SumAggregationBuilder.class));
        assertThat(((SumAggregationBuilder)metrics.get("test_metric")).field(),
                equalTo("foo.avg.value"));

        assertThat(metrics.get("test_metric._count"), Matchers.instanceOf(SumAggregationBuilder.class));
        assertThat(((SumAggregationBuilder)metrics.get("test_metric._count")).field(),
                equalTo("foo.avg._count"));

        assertThat(filterConditions.size(), equalTo(0));
    }

    public void testStringTerms() throws IOException {

        TermsAggregationBuilder terms = new TermsAggregationBuilder("test_string_terms", ValueType.STRING);
        terms.field("foo")
                .subAggregation(new MaxAggregationBuilder("the_max").field("max_field"))
                .subAggregation(new AvgAggregationBuilder("the_avg").field("avg_field"));
        List<QueryBuilder> filterConditions = new ArrayList<>();

        RollupJobConfig job = ConfigTestHelpers.getRollupJob("foo")
                .setGroupConfig(ConfigTestHelpers.getGroupConfig()
                        .setTerms(new TermsGroupConfig.Builder()
                                .setFields(Collections.singletonList("foo"))
                                .build())
                        .build())
                .setMetricsConfig(Arrays.asList(new MetricConfig.Builder()
                            .setField("max_field")
                            .setMetrics(Collections.singletonList("max")).build(),
                        new MetricConfig.Builder()
                            .setField("avg_field")
                            .setMetrics(Collections.singletonList("avg")).build()))
                .build();
        List<RollupJobCaps> caps = Collections.singletonList(new RollupJobCaps(job));

        List<AggregationBuilder> translated = translateAggregation(terms, filterConditions, namedWriteableRegistry, caps);
        assertThat(translated.size(), equalTo(1));
        assertThat(translated.get(0), Matchers.instanceOf(TermsAggregationBuilder.class));
        TermsAggregationBuilder translatedHisto = (TermsAggregationBuilder)translated.get(0);

        assertThat(translatedHisto.field(), equalTo("foo.terms.value"));
        assertThat(translatedHisto.getSubAggregations().size(), equalTo(4));

        Map<String, AggregationBuilder> subAggs = translatedHisto.getSubAggregations()
                .stream().collect(Collectors.toMap(AggregationBuilder::getName, Function.identity()));

        assertThat(subAggs.get("the_max"), Matchers.instanceOf(MaxAggregationBuilder.class));
        assertThat(((MaxAggregationBuilder)subAggs.get("the_max")).field(), equalTo("max_field.max.value"));

        assertThat(subAggs.get("the_avg"), Matchers.instanceOf(SumAggregationBuilder.class));
        SumAggregationBuilder avg = (SumAggregationBuilder)subAggs.get("the_avg");
        assertThat(avg.field(), equalTo("avg_field.avg.value"));

        assertThat(subAggs.get("the_avg._count"), Matchers.instanceOf(SumAggregationBuilder.class));
        assertThat(((SumAggregationBuilder)subAggs.get("the_avg._count")).field(),
                equalTo("avg_field.avg._count"));

        assertThat(subAggs.get("test_string_terms._count"), Matchers.instanceOf(SumAggregationBuilder.class));
        assertThat(((SumAggregationBuilder)subAggs.get("test_string_terms._count")).field(),
                equalTo("foo.terms._count"));

        assertThat(filterConditions.size(), equalTo(1));
        assertThat(filterConditions.get(0), Matchers.instanceOf(TermQueryBuilder.class));
        TermQueryBuilder computedFilter = (TermQueryBuilder)filterConditions.get(0);
        assertThat(computedFilter.fieldName(), equalTo("_rollup.computed"));
        assertThat(computedFilter.value(), equalTo("foo.terms"));
    }

    public void testBasicHisto() {

        HistogramAggregationBuilder histo = new HistogramAggregationBuilder("test_histo");
        histo.field("foo")
                .interval(1L)
                .extendedBounds(0.0, 1000.0)
                .subAggregation(new MaxAggregationBuilder("the_max").field("max_field"))
                .subAggregation(new AvgAggregationBuilder("the_avg").field("avg_field"));
        List<QueryBuilder> filterConditions = new ArrayList<>();

        RollupJobConfig job = ConfigTestHelpers.getRollupJob("foo")
                .setGroupConfig(ConfigTestHelpers.getGroupConfig()
                        .setHisto(new HistoGroupConfig.Builder()
                                .setFields(Collections.singletonList("foo"))
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
        List<RollupJobCaps> caps = Collections.singletonList(new RollupJobCaps(job));

        List<AggregationBuilder> translated = translateAggregation(histo, filterConditions, namedWriteableRegistry, caps);
        assertThat(translated.size(), equalTo(1));
        assertThat(translated.get(0), Matchers.instanceOf(HistogramAggregationBuilder.class));
        HistogramAggregationBuilder translatedHisto = (HistogramAggregationBuilder)translated.get(0);

        assertThat(translatedHisto.field(), equalTo("foo.histogram.value"));
        assertThat(translatedHisto.getSubAggregations().size(), equalTo(4));

        Map<String, AggregationBuilder> subAggs = translatedHisto.getSubAggregations()
                .stream().collect(Collectors.toMap(AggregationBuilder::getName, Function.identity()));

        assertThat(subAggs.get("the_max"), Matchers.instanceOf(MaxAggregationBuilder.class));
        assertThat(((MaxAggregationBuilder)subAggs.get("the_max")).field(), equalTo("max_field.max.value"));

        assertThat(subAggs.get("the_avg"), Matchers.instanceOf(SumAggregationBuilder.class));
        SumAggregationBuilder avg = (SumAggregationBuilder)subAggs.get("the_avg");
        assertThat(avg.field(), equalTo("avg_field.avg.value"));

        assertThat(subAggs.get("the_avg._count"), Matchers.instanceOf(SumAggregationBuilder.class));
        assertThat(((SumAggregationBuilder)subAggs.get("the_avg._count")).field(),
                equalTo("avg_field.avg._count"));

        assertThat(subAggs.get("test_histo._count"), Matchers.instanceOf(SumAggregationBuilder.class));
        assertThat(((SumAggregationBuilder)subAggs.get("test_histo._count")).field(),
                equalTo("foo.histogram._count"));

        assertThat(filterConditions.size(), equalTo(3));
        for (QueryBuilder q : filterConditions) {
            if (q instanceof TermQueryBuilder) {
                switch (((TermQueryBuilder) q).fieldName()) {
                    case "foo.histogram.interval":
                        assertThat(((TermQueryBuilder) q).value().toString(), equalTo("1"));
                        break;
                    case "_rollup.computed":
                        assertThat(((TermQueryBuilder) q).value(), equalTo("foo.histogram"));
                        break;
                    case "_rollup.id":
                        assertThat(((TermQueryBuilder) q).value(), equalTo("foo"));
                        break;
                    default:
                        fail("Unexpected Term Query in filter conditions: [" + ((TermQueryBuilder) q).fieldName() + "]");
                        break;
                }
            } else {
                fail("Unexpected query builder in filter conditions");
            }
        }
    }

    public void testUnsupportedAgg() {
        GeoDistanceAggregationBuilder geo = new GeoDistanceAggregationBuilder("test_geo", new GeoPoint(0.0, 0.0));
        geo.field("foo")
                .subAggregation(new MaxAggregationBuilder("the_max").field("max_field"))
                .subAggregation(new AvgAggregationBuilder("the_avg").field("avg_field"));
        List<QueryBuilder> filterConditions = new ArrayList<>();

        RollupJobConfig job = ConfigTestHelpers.getRollupJob("foo")
                .setGroupConfig(ConfigTestHelpers.getGroupConfig()
                        .setDateHisto(new DateHistoGroupConfig.Builder()
                                .setInterval(new DateHistogramInterval("100d"))
                                .setField("foo")
                                .setTimeZone(DateTimeZone.UTC)
                                .build())
                        .build())
                .build();
        List<RollupJobCaps> caps = Collections.singletonList(new RollupJobCaps(job));

        Exception e = expectThrows(RuntimeException.class,
                () -> translateAggregation(geo, filterConditions, namedWriteableRegistry, caps));
        assertThat(e.getMessage(), equalTo("Unable to translate aggregation tree into Rollup.  Aggregation [test_geo] is of type " +
                "[GeoDistanceAggregationBuilder] which is currently unsupported."));
    }

    public void testDateHistoMissingFieldInCaps() {
        DateHistogramAggregationBuilder histo = new DateHistogramAggregationBuilder("test_histo");
        histo.dateHistogramInterval(new DateHistogramInterval("1d"))
                .field("foo")
                .subAggregation(new MaxAggregationBuilder("the_max").field("max_field"))
                .subAggregation(new AvgAggregationBuilder("the_avg").field("avg_field"));
        List<QueryBuilder> filterConditions = new ArrayList<>();

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
        List<RollupJobCaps> caps = Collections.singletonList(new RollupJobCaps(job));

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
                () -> translateAggregation(histo, filterConditions, namedWriteableRegistry, caps));
        assertThat(e.getMessage(), equalTo("There is not a [date_histogram] agg with name [foo] configured in selected rollup " +
                "indices, cannot translate aggregation."));
    }

    public void testHistoMissingFieldInCaps() {
        HistogramAggregationBuilder histo = new HistogramAggregationBuilder("test_histo");
        histo.interval(1)
                .field("foo")
                .subAggregation(new MaxAggregationBuilder("the_max").field("max_field"))
                .subAggregation(new AvgAggregationBuilder("the_avg").field("avg_field"));
        List<QueryBuilder> filterConditions = new ArrayList<>();

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
        List<RollupJobCaps> caps = Collections.singletonList(new RollupJobCaps(job));

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
                () -> translateAggregation(histo, filterConditions, namedWriteableRegistry, caps));
        assertThat(e.getMessage(), equalTo("There is not a [histogram] agg with name [foo] configured in selected rollup " +
                "indices, cannot translate aggregation."));
    }

    public void testHistoSameNameWrongTypeInCaps() {
        HistogramAggregationBuilder histo = new HistogramAggregationBuilder("test_histo");
        histo.field("foo")
                .interval(1L)
                .subAggregation(new MaxAggregationBuilder("the_max").field("max_field"))
                .subAggregation(new AvgAggregationBuilder("the_avg").field("avg_field"));
        List<QueryBuilder> filterConditions = new ArrayList<>();

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
        List<RollupJobCaps> caps = Collections.singletonList(new RollupJobCaps(job));

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
                () -> translateAggregation(histo, filterConditions, namedWriteableRegistry, caps));
        assertThat(e.getMessage(), equalTo("There is not a [histogram] agg with name [foo] configured in selected rollup " +
                "indices, cannot translate aggregation."));
    }

    public void testSelectLowerHistoGranularityInterval() {
        HistogramAggregationBuilder histo = new HistogramAggregationBuilder("test_histo");
        histo.interval(3600000)
                .field("bar")
                .subAggregation(new MaxAggregationBuilder("the_max").field("max_field"))
                .subAggregation(new AvgAggregationBuilder("the_avg").field("avg_field"));
        List<QueryBuilder> filterConditions = new ArrayList<>();

        RollupJobConfig job1 = ConfigTestHelpers.getRollupJob("foo")
                .setGroupConfig(ConfigTestHelpers.getGroupConfig()
                        .setDateHisto(new DateHistoGroupConfig.Builder()
                                .setInterval(new DateHistogramInterval("1d"))
                                .setField("foo")
                                .setTimeZone(DateTimeZone.UTC)
                                .build())
                        .setHisto(new HistoGroupConfig.Builder()
                                .setFields(Collections.singletonList("bar"))
                                .setInterval(1L)
                                .build())
                        .build())
                .setMetricsConfig(Arrays.asList(new MetricConfig.Builder()
                                .setField("max_field")
                                .setMetrics(Collections.singletonList("max"))
                                .build(),
                        new MetricConfig.Builder()
                                .setField("avg_field")
                                .setMetrics(Collections.singletonList("avg"))
                                .build())
                )
                .build();

        RollupJobConfig job2 = ConfigTestHelpers.getRollupJob("foo")
                .setGroupConfig(ConfigTestHelpers.getGroupConfig()
                        .setDateHisto(new DateHistoGroupConfig.Builder()
                                .setInterval(new DateHistogramInterval("1d"))
                                .setField("foo")
                                .setTimeZone(DateTimeZone.UTC)
                                .build())
                        .setHisto(new HistoGroupConfig.Builder()
                                .setFields(Collections.singletonList("bar"))
                                .setInterval(100L)
                                .build())
                        .build())
                .setMetricsConfig(Arrays.asList(new MetricConfig.Builder()
                                .setField("max_field")
                                .setMetrics(Collections.singletonList("max"))
                                .build(),
                        new MetricConfig.Builder()
                                .setField("avg_field")
                                .setMetrics(Collections.singletonList("avg"))
                                .build())
                )
                .build();
        List<RollupJobCaps> caps = new ArrayList<>(2);
        caps.add(new RollupJobCaps(job1));
        caps.add(new RollupJobCaps(job2));

        List<AggregationBuilder> translated = translateAggregation(histo, filterConditions, namedWriteableRegistry, caps);
        assertThat(translated.size(), equalTo(1));
        assertThat(translated.get(0), instanceOf(HistogramAggregationBuilder.class));
        HistogramAggregationBuilder translatedHisto = (HistogramAggregationBuilder)translated.get(0);

        assertThat(translatedHisto.interval(), equalTo(3600000.0));
        assertThat(translatedHisto.field(), equalTo("bar.histogram.value"));
        assertThat(translatedHisto.getSubAggregations().size(), equalTo(4));

        Map<String, AggregationBuilder> subAggs = translatedHisto.getSubAggregations()
                .stream().collect(Collectors.toMap(AggregationBuilder::getName, Function.identity()));

        assertThat(subAggs.get("the_max"), instanceOf(MaxAggregationBuilder.class));
        assertThat(((MaxAggregationBuilder)subAggs.get("the_max")).field(), equalTo("max_field.max.value"));

        assertThat(subAggs.get("the_avg"), instanceOf(SumAggregationBuilder.class));
        SumAggregationBuilder avg = (SumAggregationBuilder)subAggs.get("the_avg");
        assertThat(avg.field(), equalTo("avg_field.avg.value"));

        assertThat(subAggs.get("the_avg._count"), instanceOf(SumAggregationBuilder.class));
        assertThat(((SumAggregationBuilder)subAggs.get("the_avg._count")).field(),
                equalTo("avg_field.avg._count"));

        assertThat(subAggs.get("test_histo._count"), instanceOf(SumAggregationBuilder.class));
        assertThat(((SumAggregationBuilder)subAggs.get("test_histo._count")).field(),
                equalTo("bar.histogram._count"));

        assertThat(filterConditions.size(), equalTo(3));

        for (QueryBuilder q : filterConditions) {
            if (q instanceof TermQueryBuilder) {
                if (((TermQueryBuilder) q).fieldName().equals("bar.histogram.interval")) {
                    assertThat(((TermQueryBuilder) q).value().toString(), equalTo("1"));  // <---- should be instead of 100
                } else if (((TermQueryBuilder) q).fieldName().equals("_rollup.computed")) {
                    assertThat(((TermQueryBuilder) q).value(), equalTo("bar.histogram"));
                } else if (((TermQueryBuilder) q).fieldName().equals("_rollup.id")) {
                    assertThat(((TermQueryBuilder) q).value(), equalTo("foo"));
                } else {
                    fail("Unexpected Term Query in filter conditions: [" + ((TermQueryBuilder) q).fieldName() + "]");
                }
            } else {
                fail("Unexpected query builder in filter conditions");
            }
        }
    }

    public void testNoMatchingHistoInterval() {
        HistogramAggregationBuilder histo = new HistogramAggregationBuilder("test_histo");
        histo.interval(1)
                .field("bar")
                .subAggregation(new MaxAggregationBuilder("the_max").field("max_field"))
                .subAggregation(new AvgAggregationBuilder("the_avg").field("avg_field"));
        List<QueryBuilder> filterConditions = new ArrayList<>();

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
        List<RollupJobCaps> caps = Collections.singletonList(new RollupJobCaps(job));

        Exception e = expectThrows(RuntimeException.class,
                () -> translateAggregation(histo, filterConditions, namedWriteableRegistry, caps));
        assertThat(e.getMessage(), equalTo("Could not find a rolled histogram configuration that satisfies the interval [1.0]"));
    }
}
