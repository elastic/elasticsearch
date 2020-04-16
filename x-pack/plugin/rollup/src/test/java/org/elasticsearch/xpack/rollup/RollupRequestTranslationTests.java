/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.rollup;


import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.search.aggregations.bucket.histogram.ExtendedBounds;
import org.elasticsearch.search.aggregations.bucket.histogram.HistogramAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.range.GeoDistanceAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.AvgAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.MaxAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.MinAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.StatsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.SumAggregationBuilder;
import org.elasticsearch.search.aggregations.support.ValueType;
import org.elasticsearch.search.aggregations.support.ValuesSourceAggregationBuilder;
import org.elasticsearch.test.ESTestCase;
import org.hamcrest.Matchers;
import org.junit.Before;

import java.io.IOException;
import java.time.ZoneId;
import java.util.ArrayList;
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
        SearchModule searchModule = new SearchModule(Settings.EMPTY, emptyList());
        List<NamedWriteableRegistry.Entry> entries = new ArrayList<>();
        entries.addAll(searchModule.getNamedWriteables());
        namedWriteableRegistry = new NamedWriteableRegistry(entries);
    }

    public void testBasicDateHisto() {
        DateHistogramAggregationBuilder histo = new DateHistogramAggregationBuilder("test_histo");
        histo.calendarInterval(new DateHistogramInterval("1d"))
                .field("foo")
                .extendedBounds(new ExtendedBounds(0L, 1000L))
                .subAggregation(new MaxAggregationBuilder("the_max").field("max_field"))
                .subAggregation(new AvgAggregationBuilder("the_avg").field("avg_field"));

        List<AggregationBuilder> translated = translateAggregation(histo, namedWriteableRegistry);
        assertThat(translated.size(), equalTo(1));
        assertThat(translated.get(0), Matchers.instanceOf(DateHistogramAggregationBuilder.class));
        DateHistogramAggregationBuilder translatedHisto = (DateHistogramAggregationBuilder)translated.get(0);

        assertThat(translatedHisto.getCalendarInterval(), equalTo(new DateHistogramInterval("1d")));
        assertThat(translatedHisto.field(), equalTo("foo.date_histogram.timestamp"));
        assertThat(translatedHisto.getSubAggregations().size(), equalTo(4));

        Map<String, AggregationBuilder> subAggs = translatedHisto.getSubAggregations()
                .stream().collect(Collectors.toMap(AggregationBuilder::getName, Function.identity()));

        assertThat(subAggs.get("the_max"), Matchers.instanceOf(MaxAggregationBuilder.class));
        assertThat(((MaxAggregationBuilder)subAggs.get("the_max")).field(), equalTo("max_field.max.value"));

        assertThat(subAggs.get("the_avg.value"), Matchers.instanceOf(SumAggregationBuilder.class));
        SumAggregationBuilder avg = (SumAggregationBuilder)subAggs.get("the_avg.value");
        assertThat(avg.field(), equalTo("avg_field.avg.value"));

        assertThat(subAggs.get("the_avg._count"), Matchers.instanceOf(SumAggregationBuilder.class));
        assertThat(((SumAggregationBuilder)subAggs.get("the_avg._count")).field(),
                equalTo("avg_field.avg._count"));

        assertThat(subAggs.get("test_histo._count"), Matchers.instanceOf(SumAggregationBuilder.class));
        assertThat(((SumAggregationBuilder)subAggs.get("test_histo._count")).field(),
                equalTo("foo.date_histogram._count"));
    }

    public void testFormattedDateHisto() {
        DateHistogramAggregationBuilder histo = new DateHistogramAggregationBuilder("test_histo");
        histo.calendarInterval(new DateHistogramInterval("1d"))
            .field("foo")
            .extendedBounds(new ExtendedBounds(0L, 1000L))
            .format("yyyy-MM-dd")
            .subAggregation(new MaxAggregationBuilder("the_max").field("max_field"));

        List<AggregationBuilder> translated = translateAggregation(histo, namedWriteableRegistry);
        assertThat(translated.size(), equalTo(1));
        assertThat(translated.get(0), Matchers.instanceOf(DateHistogramAggregationBuilder.class));
        DateHistogramAggregationBuilder translatedHisto = (DateHistogramAggregationBuilder)translated.get(0);

        assertThat(translatedHisto.getCalendarInterval(), equalTo(new DateHistogramInterval("1d")));
        assertThat(translatedHisto.format(), equalTo("yyyy-MM-dd"));
        assertThat(translatedHisto.field(), equalTo("foo.date_histogram.timestamp"));
    }

    public void testSimpleMetric() {
        int i = ESTestCase.randomIntBetween(0, 2);
        List<AggregationBuilder> translated = new ArrayList<>();

        Class<? extends AggregationBuilder> clazz = null;
        String fieldName = null;
        int numAggs = 1;

        if (i == 0) {
            translated = translateAggregation(new MaxAggregationBuilder("test_metric")
                    .field("foo"), namedWriteableRegistry);
            clazz = MaxAggregationBuilder.class;
            fieldName =  "foo.max.value";
        } else if (i == 1) {
            translated = translateAggregation(new MinAggregationBuilder("test_metric")
                    .field("foo"), namedWriteableRegistry);
            clazz = MinAggregationBuilder.class;
            fieldName =  "foo.min.value";
        } else if (i == 2) {
            translated = translateAggregation(new SumAggregationBuilder("test_metric")
                    .field("foo"), namedWriteableRegistry);
            clazz = SumAggregationBuilder.class;
            fieldName =  "foo.sum.value";
        }

        assertThat(translated.size(), equalTo(numAggs));
        assertThat(translated.get(0), Matchers.instanceOf(clazz));
        assertThat((translated.get(0)).getName(), equalTo("test_metric"));
        assertThat(((ValuesSourceAggregationBuilder)translated.get(0)).field(), equalTo(fieldName));
    }

    public void testUnsupportedMetric() {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
                () -> translateAggregation(new StatsAggregationBuilder("test_metric")
                        .field("foo"), namedWriteableRegistry));
        assertThat(e.getMessage(), equalTo("Unable to translate aggregation tree into Rollup.  Aggregation [test_metric] is of type " +
                "[StatsAggregationBuilder] which is currently unsupported."));
    }

    public void testDateHistoIntervalWithMinMax() {
        DateHistogramAggregationBuilder histo = new DateHistogramAggregationBuilder("test_histo");
        histo.calendarInterval(new DateHistogramInterval("1d"))
                .field("foo")
                .subAggregation(new MaxAggregationBuilder("the_max").field("max_field"))
                .subAggregation(new AvgAggregationBuilder("the_avg").field("avg_field"));

        List<AggregationBuilder> translated = translateAggregation(histo, namedWriteableRegistry);
        assertThat(translated.size(), equalTo(1));
        assertThat(translated.get(0), instanceOf(DateHistogramAggregationBuilder.class));
        DateHistogramAggregationBuilder translatedHisto = (DateHistogramAggregationBuilder)translated.get(0);

        assertThat(translatedHisto.getCalendarInterval().toString(), equalTo("1d"));
        assertThat(translatedHisto.field(), equalTo("foo.date_histogram.timestamp"));
        assertThat(translatedHisto.getSubAggregations().size(), equalTo(4));

        Map<String, AggregationBuilder> subAggs = translatedHisto.getSubAggregations()
                .stream().collect(Collectors.toMap(AggregationBuilder::getName, Function.identity()));

        assertThat(subAggs.get("the_max"), instanceOf(MaxAggregationBuilder.class));
        assertThat(((MaxAggregationBuilder)subAggs.get("the_max")).field(), equalTo("max_field.max.value"));

        assertThat(subAggs.get("the_avg.value"), instanceOf(SumAggregationBuilder.class));
        SumAggregationBuilder avg = (SumAggregationBuilder)subAggs.get("the_avg.value");
        assertThat(avg.field(), equalTo("avg_field.avg.value"));

        assertThat(subAggs.get("the_avg._count"), instanceOf(SumAggregationBuilder.class));
        assertThat(((SumAggregationBuilder)subAggs.get("the_avg._count")).field(),
                equalTo("avg_field.avg._count"));

        assertThat(subAggs.get("test_histo._count"), instanceOf(SumAggregationBuilder.class));
        assertThat(((SumAggregationBuilder)subAggs.get("test_histo._count")).field(),
                equalTo("foo.date_histogram._count"));
    }

    public void testDateHistoLongIntervalWithMinMax() {
        DateHistogramAggregationBuilder histo = new DateHistogramAggregationBuilder("test_histo");
        histo.interval(86400000)
                .field("foo")
                .subAggregation(new MaxAggregationBuilder("the_max").field("max_field"))
                .subAggregation(new AvgAggregationBuilder("the_avg").field("avg_field"));

        List<AggregationBuilder> translated = translateAggregation(histo, namedWriteableRegistry);
        assertThat(translated.size(), equalTo(1));
        assertThat(translated.get(0), instanceOf(DateHistogramAggregationBuilder.class));
        DateHistogramAggregationBuilder translatedHisto = (DateHistogramAggregationBuilder)translated.get(0);

        assertNull(translatedHisto.getCalendarInterval());
        assertThat(translatedHisto.getFixedInterval(), equalTo(new DateHistogramInterval("86400000ms")));
        assertThat(translatedHisto.field(), equalTo("foo.date_histogram.timestamp"));
        assertThat(translatedHisto.getSubAggregations().size(), equalTo(4));

        Map<String, AggregationBuilder> subAggs = translatedHisto.getSubAggregations()
                .stream().collect(Collectors.toMap(AggregationBuilder::getName, Function.identity()));

        assertThat(subAggs.get("the_max"), instanceOf(MaxAggregationBuilder.class));
        assertThat(((MaxAggregationBuilder)subAggs.get("the_max")).field(), equalTo("max_field.max.value"));

        assertThat(subAggs.get("the_avg.value"), instanceOf(SumAggregationBuilder.class));
        SumAggregationBuilder avg = (SumAggregationBuilder)subAggs.get("the_avg.value");
        assertThat(avg.field(), equalTo("avg_field.avg.value"));

        assertThat(subAggs.get("the_avg._count"), instanceOf(SumAggregationBuilder.class));
        assertThat(((SumAggregationBuilder)subAggs.get("the_avg._count")).field(),
                equalTo("avg_field.avg._count"));

        assertThat(subAggs.get("test_histo._count"), instanceOf(SumAggregationBuilder.class));
        assertThat(((SumAggregationBuilder)subAggs.get("test_histo._count")).field(),
                equalTo("foo.date_histogram._count"));

        assertWarnings("[interval] on [date_histogram] is deprecated, use [fixed_interval] " +
            "or [calendar_interval] in the future.");
    }

    public void testDateHistoWithTimezone() {
        ZoneId timeZone = ZoneId.of(randomFrom(ZoneId.getAvailableZoneIds()));
        DateHistogramAggregationBuilder histo = new DateHistogramAggregationBuilder("test_histo");
        histo.fixedInterval(new DateHistogramInterval("86400000ms"))
            .field("foo")
            .timeZone(timeZone);

        List<AggregationBuilder> translated = translateAggregation(histo, namedWriteableRegistry);
        assertThat(translated.size(), equalTo(1));
        assertThat(translated.get(0), instanceOf(DateHistogramAggregationBuilder.class));
        DateHistogramAggregationBuilder translatedHisto = (DateHistogramAggregationBuilder)translated.get(0);

        assertThat(translatedHisto.getFixedInterval().toString(), equalTo("86400000ms"));
        assertThat(translatedHisto.field(), equalTo("foo.date_histogram.timestamp"));
        assertThat(translatedHisto.timeZone(), equalTo(timeZone));
    }

    public void testDeprecatedInterval() {
        DateHistogramAggregationBuilder histo = new DateHistogramAggregationBuilder("test_histo");
        histo.interval(86400000).field("foo");

        List<AggregationBuilder> translated = translateAggregation(histo, namedWriteableRegistry);
        assertThat(translated.size(), equalTo(1));
        assertThat(translated.get(0), instanceOf(DateHistogramAggregationBuilder.class));
        DateHistogramAggregationBuilder translatedHisto = (DateHistogramAggregationBuilder)translated.get(0);

        assertThat(translatedHisto.getFixedInterval().toString(), equalTo("86400000ms"));
        assertThat(translatedHisto.field(), equalTo("foo.date_histogram.timestamp"));
        assertWarnings("[interval] on [date_histogram] is deprecated, use [fixed_interval] " +
            "or [calendar_interval] in the future.");
    }

    public void testDeprecatedDateHistoInterval() {
        DateHistogramAggregationBuilder histo = new DateHistogramAggregationBuilder("test_histo");
        histo.dateHistogramInterval(new DateHistogramInterval("1d")).field("foo");

        List<AggregationBuilder> translated = translateAggregation(histo, namedWriteableRegistry);
        assertThat(translated.size(), equalTo(1));
        assertThat(translated.get(0), instanceOf(DateHistogramAggregationBuilder.class));
        DateHistogramAggregationBuilder translatedHisto = (DateHistogramAggregationBuilder)translated.get(0);

        assertThat(translatedHisto.dateHistogramInterval().toString(), equalTo("1d"));
        assertThat(translatedHisto.field(), equalTo("foo.date_histogram.timestamp"));
        assertWarnings("[interval] on [date_histogram] is deprecated, use [fixed_interval] " +
            "or [calendar_interval] in the future.");


        histo = new DateHistogramAggregationBuilder("test_histo");
        histo.dateHistogramInterval(new DateHistogramInterval("4d")).field("foo");

        translated = translateAggregation(histo, namedWriteableRegistry);
        assertThat(translated.size(), equalTo(1));
        assertThat(translated.get(0), instanceOf(DateHistogramAggregationBuilder.class));
        translatedHisto = (DateHistogramAggregationBuilder)translated.get(0);

        assertThat(translatedHisto.dateHistogramInterval().toString(), equalTo("4d"));
        assertThat(translatedHisto.field(), equalTo("foo.date_histogram.timestamp"));
        assertWarnings("[interval] on [date_histogram] is deprecated, use [fixed_interval] " +
            "or [calendar_interval] in the future.");
    }

    public void testAvgMetric() {
        List<AggregationBuilder> translated = translateAggregation(new AvgAggregationBuilder("test_metric")
                .field("foo"), namedWriteableRegistry);

        assertThat(translated.size(), equalTo(2));
        Map<String, AggregationBuilder> metrics = translated.stream()
                .collect(Collectors.toMap(AggregationBuilder::getName, Function.identity()));

        assertThat(metrics.get("test_metric.value"), Matchers.instanceOf(SumAggregationBuilder.class));
        assertThat(((SumAggregationBuilder)metrics.get("test_metric.value")).field(),
                equalTo("foo.avg.value"));

        assertThat(metrics.get("test_metric._count"), Matchers.instanceOf(SumAggregationBuilder.class));
        assertThat(((SumAggregationBuilder)metrics.get("test_metric._count")).field(),
                equalTo("foo.avg._count"));
    }

    public void testStringTerms() throws IOException {

        TermsAggregationBuilder terms = new TermsAggregationBuilder("test_string_terms").userValueTypeHint(ValueType.STRING);
        terms.field("foo")
                .subAggregation(new MaxAggregationBuilder("the_max").field("max_field"))
                .subAggregation(new AvgAggregationBuilder("the_avg").field("avg_field"));

        List<AggregationBuilder> translated = translateAggregation(terms, namedWriteableRegistry);
        assertThat(translated.size(), equalTo(1));
        assertThat(translated.get(0), Matchers.instanceOf(TermsAggregationBuilder.class));
        TermsAggregationBuilder translatedHisto = (TermsAggregationBuilder)translated.get(0);

        assertThat(translatedHisto.field(), equalTo("foo.terms.value"));
        assertThat(translatedHisto.getSubAggregations().size(), equalTo(4));

        Map<String, AggregationBuilder> subAggs = translatedHisto.getSubAggregations()
                .stream().collect(Collectors.toMap(AggregationBuilder::getName, Function.identity()));

        assertThat(subAggs.get("the_max"), Matchers.instanceOf(MaxAggregationBuilder.class));
        assertThat(((MaxAggregationBuilder)subAggs.get("the_max")).field(), equalTo("max_field.max.value"));

        assertThat(subAggs.get("the_avg.value"), Matchers.instanceOf(SumAggregationBuilder.class));
        SumAggregationBuilder avg = (SumAggregationBuilder)subAggs.get("the_avg.value");
        assertThat(avg.field(), equalTo("avg_field.avg.value"));

        assertThat(subAggs.get("the_avg._count"), Matchers.instanceOf(SumAggregationBuilder.class));
        assertThat(((SumAggregationBuilder)subAggs.get("the_avg._count")).field(),
                equalTo("avg_field.avg._count"));

        assertThat(subAggs.get("test_string_terms._count"), Matchers.instanceOf(SumAggregationBuilder.class));
        assertThat(((SumAggregationBuilder)subAggs.get("test_string_terms._count")).field(),
                equalTo("foo.terms._count"));
    }

    public void testBasicHisto() {

        HistogramAggregationBuilder histo = new HistogramAggregationBuilder("test_histo");
        histo.field("foo")
                .interval(1L)
                .extendedBounds(0.0, 1000.0)
                .subAggregation(new MaxAggregationBuilder("the_max").field("max_field"))
                .subAggregation(new AvgAggregationBuilder("the_avg").field("avg_field"));

        List<AggregationBuilder> translated = translateAggregation(histo, namedWriteableRegistry);
        assertThat(translated.size(), equalTo(1));
        assertThat(translated.get(0), Matchers.instanceOf(HistogramAggregationBuilder.class));
        HistogramAggregationBuilder translatedHisto = (HistogramAggregationBuilder)translated.get(0);

        assertThat(translatedHisto.field(), equalTo("foo.histogram.value"));
        assertThat(translatedHisto.getSubAggregations().size(), equalTo(4));

        Map<String, AggregationBuilder> subAggs = translatedHisto.getSubAggregations()
                .stream().collect(Collectors.toMap(AggregationBuilder::getName, Function.identity()));

        assertThat(subAggs.get("the_max"), Matchers.instanceOf(MaxAggregationBuilder.class));
        assertThat(((MaxAggregationBuilder)subAggs.get("the_max")).field(), equalTo("max_field.max.value"));

        assertThat(subAggs.get("the_avg.value"), Matchers.instanceOf(SumAggregationBuilder.class));
        SumAggregationBuilder avg = (SumAggregationBuilder)subAggs.get("the_avg.value");
        assertThat(avg.field(), equalTo("avg_field.avg.value"));

        assertThat(subAggs.get("the_avg._count"), Matchers.instanceOf(SumAggregationBuilder.class));
        assertThat(((SumAggregationBuilder)subAggs.get("the_avg._count")).field(),
                equalTo("avg_field.avg._count"));

        assertThat(subAggs.get("test_histo._count"), Matchers.instanceOf(SumAggregationBuilder.class));
        assertThat(((SumAggregationBuilder)subAggs.get("test_histo._count")).field(),
                equalTo("foo.histogram._count"));
    }

    public void testUnsupportedAgg() {
        GeoDistanceAggregationBuilder geo = new GeoDistanceAggregationBuilder("test_geo", new GeoPoint(0.0, 0.0));
        geo.field("foo")
                .subAggregation(new MaxAggregationBuilder("the_max").field("max_field"))
                .subAggregation(new AvgAggregationBuilder("the_avg").field("avg_field"));

        Exception e = expectThrows(RuntimeException.class,
                () -> translateAggregation(geo, namedWriteableRegistry));
        assertThat(e.getMessage(), equalTo("Unable to translate aggregation tree into Rollup.  Aggregation [test_geo] is of type " +
                "[GeoDistanceAggregationBuilder] which is currently unsupported."));
    }
}
